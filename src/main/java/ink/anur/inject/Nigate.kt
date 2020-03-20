package ink.anur.inject

import com.google.common.collect.Lists
import ink.anur.exception.DuplicateBeanException
import ink.anur.exception.KanashiException
import ink.anur.exception.NigateException
import ink.anur.exception.NoSuchBeanException
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.net.JarURLConnection
import java.net.URL
import java.util.HashSet
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.memberFunctions
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.jvm.javaField
import kotlin.reflect.jvm.javaType
import kotlin.system.exitProcess

/**
 * Created by Anur IjuoKaruKas on 2020/2/23
 *
 * 扫描系列
 */
object Nigate {

    class NigateBeanContainer {

        val UNDEFINE_ALIAS = "UNDEFINE"

        /**
         * 必须完成初始注册才能做很多事情，否则直接抛出异常（在初始化阶段，<clinit> 和 <init> 使用注入会牙白！！！）
         */
        var over_registry: Boolean = false

        /**
         * bean 名字与 bean 的映射，只能一对一
         */
        private val NAME_TO_BEAN_MAPPING = mutableMapOf<String, Any>()

        /**
         * bean 名字与 bean 的映射，只能一对一
         */
        private val BEAN_TO_NAME_MAPPING = mutableMapOf<Any, String>()

        /**
         * bean 类型与 bean 的映射，可以一对多
         */
        private val BEAN_CLASS_MAPPING = mutableMapOf<Class<*>, MutableSet<Any>>()

        /**
         * 存储 “类” 与其 “类定义” 的映射集合
         */
        private val BEAN_DEFINITION_MAPPING = mutableMapOf<String, NigateBeanDefinition>()

        /**
         * 存储接口类与其 “实现们” 的映射集合
         */
        private val INTERFACE_MAPPING = mutableMapOf<Class<*>, MutableSet<Class<*>>>()

        /**
         * 延迟加载的 BEAN
         */
        private val LAZY_POSTCONTROL_BEAN = mutableMapOf<Any, Any>()

        /**
         * 已经进行了 postControl
         */
        private val HAS_BEEN_POSTCONTROL = mutableSetOf<Any>()

        /**
         * 避免因为路径问题导致初始化重复扫描
         */
        private val INIT_DUPLICATE_CLEANER = mutableSetOf<String>()

        fun autoRegister(path: String, clazz: Class<*>, anno: NigateBean, fromJar: Boolean) {
            if (INIT_DUPLICATE_CLEANER.add(path)) {
                val name = register(clazz.newInstance(), anno.name)
                BEAN_DEFINITION_MAPPING[name] = NigateBeanDefinition(fromJar, path)
            } else {
                // ignore
            }
        }

        /**
         * 注册一个bean，优先取 alias 取不到则使用 bean 的 simpleName
         */
        fun register(bean: Any, alias: String? = null): String {
            val clazz = bean.javaClass
            val actualName = if (alias == null || alias == UNDEFINE_ALIAS) clazz.simpleName else alias

            if (NAME_TO_BEAN_MAPPING[actualName] != null) {
                throw DuplicateBeanException("bean $clazz 存在重复注册的情况，请使用 @NigateBean(name = alias) 为其中一个起别名")
            }

            NAME_TO_BEAN_MAPPING[actualName] = bean
            BEAN_TO_NAME_MAPPING[bean] = actualName

            BEAN_CLASS_MAPPING.compute(bean.javaClass) { _, v ->
                val set = v ?: mutableSetOf()
                set.add(bean)
                set
            }

            clazz.interfaces.forEach {
                INTERFACE_MAPPING.compute(it) { _, v ->
                    val set = v ?: mutableSetOf()
                    set.add(clazz)
                    set
                }
            }
            return actualName
        }

        private fun <T> getBeanByClassFirstThenName(clazz: Class<T>): T {
            var result: T? = null
            try {
                try {
                    result = getBeanByClass(clazz)
                } catch (t: Throwable) {
                    result = Nigate.getBeanByName(clazz.simpleName) as T
                }
            } catch (t: Throwable) {
                throw NoSuchBeanException("无法根据类 ${clazz.simpleName} 找到唯一的 Bean 或 无法根据名字 ${clazz.simpleName} 找到指定的 Bean ")
            }
            return result!!
        }

        fun getBeanByName(name: String): Any = NAME_TO_BEAN_MAPPING[name] ?: throw NoSuchBeanException("bean named $name is not managed")

        fun <T> getBeanByClass(clazz: Class<T>): T {
            val l = BEAN_CLASS_MAPPING[clazz] ?: throw NoSuchBeanException("bean with type $clazz is not managed")
            if (l.size > 1) {
                throw DuplicateBeanException("bean $clazz 存在多实例的情况，请使用 @NigateInject(name = alias) 选择注入其中的某个 bean")
            }
            return (l.first()) as T
        }

        /**
         * 为某个bean注入成员变量
         */
        fun inject(injected: Any) {
            for (kProperty in injected::class.declaredMemberProperties) {
                for (annotation in kProperty.annotations) {
                    if (annotation.annotationClass == NigateInject::class) {
                        annotation as NigateInject
                        val javaField = kProperty.javaField!!

                        val fieldName = kProperty.name
                        val fieldClass = kProperty.returnType.javaType as Class<*>
                        var injection: Any? = null
                        if (annotation.name == UNDEFINE_ALIAS) {// 如果没有指定别名注入
                            if (fieldClass.isInterface) {// 如果是 接口类型
                                val mutableSet = INTERFACE_MAPPING[fieldClass]
                                if (mutableSet == null) {
                                    throw NoSuchBeanException("不存在接口类型为 $fieldClass 的 Bean！")
                                } else {
                                    if (mutableSet.size == 1) {// 如果只有一个实现，则注入此实现
                                        injection = getBeanByClassFirstThenName(mutableSet.first())
                                    } else if (annotation.useLocalFirst) {// 如果优先使用本地写的类
                                        val localBean = mutableSet.takeIf { BEAN_DEFINITION_MAPPING[it.javaClass.simpleName]?.fromJar == false }
                                        when {
                                            localBean == null -> throw DuplicateBeanException("bean ${injected.javaClass} " +
                                                " 将注入的属性 $fieldName 为接口类型，且存在多个来自【依赖】的子类实现，请改用 @NigateInject(name) 来指定别名注入")
                                            localBean.size > 1 -> throw DuplicateBeanException("bean ${injected.javaClass} " +
                                                " 将注入的属性 $fieldName 为接口类型，且存在多个来自【本地】的子类实现，请改用 @NigateInject(name) 来指定别名注入")
                                            else -> injection = getBeanByClassFirstThenName(mutableSet.first())
                                        }
                                    }
                                }
                            } else {// 如果不是接口类型，直接根据类来注入
                                injection = getBeanByClass(fieldClass)
                            }
                        } else {// 如果指定了别名，直接根据别名注入
                            injection = getBeanByName(annotation.name)
                        }

                        javaField.isAccessible = true
                        javaField.set(injected, injection)
                    }
                }
            }
        }

        fun getManagedBeans(): MutableCollection<Any> {
            return NAME_TO_BEAN_MAPPING.values
        }

        fun postConstruct(beans: MutableCollection<Any>, onStartUp: Boolean) {
            val removes = mutableListOf<Any>()

            for (bean in beans) {
                for (memberFunction in bean::class.memberFunctions) {
                    for (annotation in memberFunction.annotations) {
                        if (annotation.annotationClass == NigatePostConstruct::class) {
                            annotation as NigatePostConstruct
                            val dependsOn = NAME_TO_BEAN_MAPPING[annotation.dependsOn]
                            if (annotation.dependsOn != "-NONE-" && !HAS_BEEN_POSTCONTROL.contains(dependsOn)) {
                                dependsOn
                                    ?: throw NoSuchBeanException("$bean 依赖的 bean ${annotation.dependsOn} 不存在")
                                LAZY_POSTCONTROL_BEAN[bean] = dependsOn
                                if (LAZY_POSTCONTROL_BEAN[dependsOn] != null && LAZY_POSTCONTROL_BEAN[dependsOn] == bean) {
                                    throw NigateException("bean ${BEAN_TO_NAME_MAPPING[dependsOn]} 与 ${BEAN_TO_NAME_MAPPING[bean]} 的 @NigatePostConstruct 构成了循环依赖！")
                                }
                            } else {

                                HAS_BEEN_POSTCONTROL.add(bean)
                                removes.add(
                                    bean
                                )

                                try {
                                    memberFunction.isAccessible = true
                                    memberFunction.call(bean)
                                } catch (e: Exception) {
                                    logger.error("class [${bean::class}] invoke post construct method [${memberFunction.name}] error : ${e.message}")
                                    e.printStackTrace()
                                    if (onStartUp) {
                                        exitProcess(1)
                                    }
                                }
                                val name = BEAN_TO_NAME_MAPPING[bean] ?: throw NoSuchBeanException("无法根据类 ${bean.javaClass.simpleName} 找到唯一的 Bean 找到指定的 BeanName")
                                hasBeanPostConstruct.add(name)
                            }
                        }
                    }
                }
            }
            for (remove in removes) {
                LAZY_POSTCONTROL_BEAN.remove(remove)
            }

            if (LAZY_POSTCONTROL_BEAN.keys.size > 0) {
                postConstruct(LAZY_POSTCONTROL_BEAN.keys, onStartUp)
            }
        }

        fun getClasses(packagePath: String) {
            val path = packagePath.replace(".", "/")
            val resources = Thread.currentThread().contextClassLoader.getResources(path)

            while (resources.hasMoreElements()) {
                val url = resources.nextElement()
                val protocol = url.protocol
                if ("jar".equals(protocol, ignoreCase = true)) {
                    try {
                        getJarClasses(url, packagePath)
                    } catch (e: IOException) {
                        e.printStackTrace()
                    }

                } else if ("file".equals(protocol, ignoreCase = true)) {
                    getFileClasses(url, packagePath)
                }
            }
        }

        //获取file路径下的class文件
        private fun getFileClasses(url: URL, packagePath: String) {
            val filePath = url.file
            val dir = File(filePath)
            val list = dir.list() ?: return
            for (classPath in list) {
                if (classPath.endsWith(".class")) {
                    val path = "$packagePath.${classPath.replace(".class", "")}"
                    registerByClassPath(path)
                } else {
                    getClasses("$packagePath.$classPath")
                }
            }
        }

        //使用JarURLConnection类获取路径下的所有类
        @Throws(IOException::class)
        private fun getJarClasses(url: URL, packagePath: String) {
            val conn = url.openConnection() as JarURLConnection
            val jarFile = conn.jarFile
            val entries = jarFile.entries()
            while (entries.hasMoreElements()) {
                val jarEntry = entries.nextElement()
                val name = jarEntry.name
                if (name.contains(".class") && name.replace("/".toRegex(), ".").startsWith(packagePath)) {
                    val className = name.substring(0, name.lastIndexOf(".")).replace("/", ".")
                    registerByClassPath(className)
                }
            }
        }

        private fun registerByClassPath(classPath: String) {
            try {
                val aClass = Class.forName(classPath)
                for (annotation in aClass.annotations) {
                    if (annotation.annotationClass == NigateBean::class) {
                        annotation as NigateBean
                        beanContainer.autoRegister(classPath, aClass, annotation, true)
                    }
                }
            } catch (e: ClassNotFoundException) {
                e.printStackTrace()
            }
        }

        fun doScan() {
            getClasses("ink.anur")
        }
    }

    class NigateBeanDefinition(
        /**
         * fromJar 代表此实现是有默认的实现而且实现在继承的maven里就已经写好
         */
        val fromJar: Boolean,

        val path: String
    )

    /**
     * bean 容器
     */
    private val beanContainer = NigateBeanContainer()

    private val hasBeanPostConstruct: MutableSet<String> = mutableSetOf()

    private val logger = LoggerFactory.getLogger(this::class.java)

    init {
        try {
            val start = System.currentTimeMillis()
            logger.info("Nigate ==> Registering..")
            beanContainer.doScan()
            beanContainer.over_registry = true
            val allBeans = beanContainer.getManagedBeans()
            logger.info("Nigate ==> Register complete")
            logger.info("Nigate ==> Injecting..")

            for (bean in allBeans) {
                beanContainer.inject(bean)
            }
            logger.info("Nigate ==> Inject complete")

            logger.info("Nigate ==> Invoking postConstruct..")
            beanContainer.postConstruct(allBeans, true)
            logger.info("Nigate ==> Invoke postConstruct complete")

            logger.info("Nigate ==> Registering listener..")
            val nigateListenerService = getBeanByClass(NigateListenerService::class.java)
            for (bean in allBeans) {
                nigateListenerService.registerListenEvent(bean)
            }
            logger.info("Nigate ==> Register complete")


            logger.info("Nigate Started in ${(System.currentTimeMillis() - start) / 1000f} seconds")
        } catch (e: Exception) {
            e.printStackTrace()
            exitProcess(1)
        }
    }

    private fun lazyInit(injected: Any) {

    }

    fun <T> getBeanByClass(clazz: Class<T>): T = beanContainer.getBeanByClass(clazz)

    fun getBeanByName(name: String): Any = beanContainer.getBeanByName(name)

    /**
     * 注册某个bean
     */
    fun registerToNigate(injected: Any, alias: String? = null) {
        if (!beanContainer.over_registry) {
            throw KanashiException("暂时不支持在初始化完成前进行构造注入！")
        }
        beanContainer.register(injected, alias)
        beanContainer.inject(injected)
        beanContainer.postConstruct(Lists.newArrayList(injected), false)
        getBeanByClass(NigateListenerService::class.java).registerListenEvent(injected)
    }

    /**
     * 单纯的注入
     */
    fun injectOnly(injected: Any) {
        beanContainer.inject(injected)
    }
}