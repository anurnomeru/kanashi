#### Kanashi 0.0.1-Alpha
******
Kanashi 是一款基于 kotlin 开发的分布式 NoSQL 内存数据库，支持事务。确切的来说，旨在作者加深学习分布式写出来的玩具，并不对标现有的成熟数据库。Kanashi 基于 netty 来进行通信，基于 raft 实现了选举，可以说是内置了一个粗糙的 NameServer。日志同步相关功能则是由 Kafka 改造而来。此外，Kanashi 还支持事务，目前仅支持 Repeatable read 的隔离级别。

在不严谨的性能测试中：
 - 由服务端本地的日志写入，连同数据引擎对日志的解析，可达到 30W 左右的 TPS（ Intel(R) i7-8700 + SAMSUNG 860 EVO）。
 - 由客户端对服务器单机进行调用，则可达到 6000 TPS。
 - 而如若客户端对服务器集群（3台机器），则只能达到每秒 10 次左右请求。

并不是集群之间协调的效率太差，由于日志沿用了 Kakfa 的 Pull 模型，又由于每个操作需要得到节点半数同意才可真正提交、落盘、告知提交状态。由日志同步，到汇报进度，再到集群提交，最后到 Leader 进行标记提交。整个状态的流转，对于客户端是一个线性的过程。

但是对于多客户端来说，是可以互相不受干扰并行的。我们有理由相信，在多客户端的情况下，集群的整体性能将更好。当然这也充分反映了整套为了保证数据较强一致性流程的不合理，以及客户端设计不合理等问题。


#### 额外的话：

Kanashi 的前身由是作者的另一个框架 Hanabi 改进而来，改善了许多在 Hanabi 中设计不合理的地方。

目前整套架子已经比较完善，实际上可以很快将项目改造成其他类型的框架，如 RPC 框架、分布式调度框架等。

******
### 一、Quick Start

#### 1.1 服务端

配置服务端，服务端的配置位于 kanashi-server 模块下的 kanashi.properties：
```JAVA
#
# set the server name, caution: server name must be configured in client.addr
#
server.name=kanashi.1
#
# addr config composed with client.addr.{serverName}={host}:{port}
#
client.addr.kanashi.1=127.0.0.1:11001
client.addr.kanashi.2=127.0.0.1:11002
client.addr.kanashi.3=127.0.0.1:11003
```
可根据 `client.addr.{serverName}={host}:{port}` 来配置集群信息，目前暂不支持动态节点伸缩。

`server.name` 则声明本机为哪个节点。但不一定要写在配置文件中，可在启动时指定 server_name:kanashi.1 来声明本节点。

![pic1-server](https://images.gitee.com/uploads/images/2020/0405/161140_99d42315_1460144.png)

#### 1.2 客户端

客户端目前及其粗糙，可通过引入 `kanashi-client` 或者直接在 `kanashi-client` 上启动。客户端需要在 `kanashi-client` 模块下的 `application.properties` 配置如下，只需要配置任意一台机器：

```JAVA
kanashi.host=127.0.0.1
kanashi.port=11001
kanashi.reSendBackOffMs=30
```

通过 `KanashiStrTemplate` 来发送指令到服务器：

```JAVA

@Autowired
private KanashiStrTemplate kanashiStrTemplate;

@Test
public void test() {

    kanashiStrTemplate.delete("Anur");
    assert (kanashiStrTemplate.get("Anur") == null);

    kanashiStrTemplate.set("Anur", "1");
    assert (kanashiStrTemplate.get("Anur")
                              .equals("1"));

    kanashiStrTemplate.setIf("Anur", "2", "2");
    assert (kanashiStrTemplate.get("Anur")
                              .equals("1"));
}
```

具体可参照测试用例 `TestWithNonTrx`、`TestWithTrx`、`TestWritingAmountOfMsg`

### 二、项目介绍

Kanashi 大可由以下六大模块组成：选举模块、日志模块、IOC模块、数据引擎模块、业务模块、配置模块。

#### 1.1 选举模块

选举模块是框架中较为成熟的模块，基于 raft 实现，raft 的核心就在于几个定时器：成为 Candidate 并发起新世代操作的任务、选举（拉票）任务、心跳任务。在选举阶段，整个集群将不可用，直到在新的世代(Epoch，项目里成为Generation)，选出唯一的 Leader 后，才由监听器触发通知其他模块。

#### 1.2 日志模块

日志模块是由 Kakfa 改造而来，使用磁盘顺序写来刷盘，一条日志由 `LogItem` 来表示，刷盘时则由 偏移量信息 + `LogItem` 组成一条磁盘日志。一定大小（可在配置中指定）的一系列日志将落盘为 `.log` 文件，文件以第一条日志的偏移量命名。

每个 `.log` 文件都有一个与之对应的 `.index` 索引文件。索引文件采用稀疏索引，每落盘一定大小的日志会记录其在文件中的相对位置(position)。

在集群同意提交某进度时，所有早于此进度的日志都将刷盘。还未获取到此进度的服务可通过 `Fetch` 请求进行同步，同步消息会采用零复制技术直接将磁盘日志发送到网络中。

#### 1.3 IOC模块

项目实现了简单的注入：

```JAVA
/**
 * Created by Anur IjuoKaruKas on 2019/7/8
 *
 * 选举相关的元数据信息都保存在这里
 */
@NigateBean
class ElectionMetaService {

    @NigateInject
    private lateinit var inetSocketAddressConfiguration: InetSocketAddressConfiguration

    @NigateInject
    private lateinit var logService: LogService

    @NigateInject
    private lateinit var kanashiListenerService: NigateListenerService

    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    /**
     * 启动后将之前保存的 GAO 进度重新加载回内存
     */
    @NigatePostConstruct(dependsOn = "LogService")
    private fun init() {
        val initialGAO = logService.getInitialGAO()
        this.generation = initialGAO.generation
        this.offset = initialGAO.offset
    }
}
```

通过 `@NigateBean` 来将某个实例声明为 `bean`，我们可以在其他 `bean` 中，通过 `@NigateInject` 的方式将其注入，同时，也实现了诸如 `PostConstruct` 、 `Listener` 等附加功能。

麻雀虽小，也算是五脏俱全了。这是此项目和 `Hanabi` 项目最大的区别。虽然 `kotlin` 可以很简单的去实现单例模式，但要实现一些其他的附加功能如事件监听，生命周期控制，将使得代码变得十分难以维护。


#### 1.4 数据引擎模块与业务模块

数据引擎模块算是近期才开发的一个模块，整体模块的设计还算满意，封装比较合理，与其他模块基本是解耦的。

数据引擎模块可以看做是日志 `LogItem` 的消费者，查询和写入都采用了责任链模式进行设计。为了实现隔离性，我们将数据分别存储于“未提交部分”、”提交部分“以及“LSM部分”。

初期的设计是考虑将数据真正落盘，并构建成 `LSM` 树的，但这部分开发工作量还是蛮大的，暂且放缓，它需要考虑磁盘的编排，如extent、page、block 等等。另一方面，也要充分考虑性能，比如如何去防止无效的查询使用布隆过滤器等等。

事务方面使用了位图来总览事务的状态，也有利于对整个数据库事务进行“快照”。

业务模块基本就是对数据引擎模块的简单组合与调用：

```java
StrApiTypeEnum.SELECT -> {
    engineDataQueryer.doQuery(engineExecutor, {
        if (it != null && !it.isDelete()) {
            engineExecutor.getEngineResult().setKanashiEntry(it)
        }
        engineExecutor.shotSuccess()
    }, false)
}
```

比较有意思的是事务阻塞控制：

```java
fun acquire(trxId: Long, key: String, whatEverDo: () -> Unit) {
    // 创建或者拿到之前注册的事务，并注册持有此key的锁
    val trxHolder = trxHolderMap.compute(trxId) { _, th -> th ?: TrxHolder(trxId) }!!.also { it.holdKeys.add(key) }

    // 先去排队
    val waitQueue = lockKeeper.compute(key) { _, ll ->
        (ll ?: LinkedList()).also { if (!it.contains(trxId)) it.add(trxId) }
    }!!

    when (waitQueue.first()) {
        // 代表此键无锁
        trxId -> {
            whatEverDo.invoke()
            logger.trace("事务 $trxId 成功获取或重入位于键 $key 上的锁，并成功进行了操作")
        }

        // 代表有锁
        else -> {
            trxHolder.undoEvent.compute(key) { _, undoList ->
                (undoList ?: mutableListOf()).also { it.add(whatEverDo) }
            }
            logger.debug("事务 $trxId 无法获取位于键 $key 上的锁，将等待键上的前一个事务唤醒，且挂起需执行的操作")
        }
    }
}
```

代码采用了函数式的方式，将业务要进行的操作进行提取，如果获取到锁，则只需要简单调用此函数，否则将其放入映射中“等待通知”，并不涉及到真正的阻塞，这个设计很大程度避免了由于键锁带来的线程切换开销。

#### 1.5 配置模块

配置模块的代码大概是一年多前就写了的，目前实际上很多可以配置的地方都是“魔数”直接写死，实现也比较简单，完善的优先度不高。

### 三、需要修复或解决的问题

 -  集群模式相对单机模式来说，在数据引擎这个模块不是过于稳定，缺少测试
 -  缺乏多客户端对集群模式的测试
 -  缺乏多客户端对单机模式的测试
 -  客户端在连接集群以及获取集群信息这块的设计比较生硬
 -  在不优雅关闭的情况下，有时候会出现日志文件读取失败的问题
 -  需要一个事务超时强制提交或回滚的机制
 -  `TrxHolder` 事务快照可能有并发问题，出现的概率很低，比较难复现

### 四、其他

对项目有兴趣，或者想一起折腾这些乱七八糟事情的可以加群，目前基本没人：1035435027

![kanashi](https://images.gitee.com/uploads/images/2020/0405/161140_9dd61ee4_1460144.png)

讨论Java相关技术可以加群：767271344

![二维码](https://images.gitee.com/uploads/images/2020/0405/161207_31fb2c55_1460144.png)

另外找工作中 = =，3年-Java后端，简历如下：

http://anur.ink/upload/2020/4/JAVA%E5%90%8E%E7%AB%AF-3%E5%B9%B4-%E7%BD%91%E7%BB%9C%E7%89%88-4ea872fb596f4553aa9f04cefcb65a84.pdf
