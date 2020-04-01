import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ink.anur.DemoApplication;
import ink.anur.core.KanashiStrTemplate;

/**
 * Created by Anur IjuoKaruKas on 2020/4/1
 */
@SpringBootTest(classes = DemoApplication.class)
public class TestWithTrx {

    @Autowired
    private KanashiStrTemplate kanashiStrTemplate;

    @Test
    public void test() {
        String key = "-Anur-";

        kanashiStrTemplate.delete(key);
        assert (kanashiStrTemplate.get(key) == null);

        long trx1 = kanashiStrTemplate.startTransaction();
        long trx2 = kanashiStrTemplate.startTransaction();

        kanashiStrTemplate.set(trx1, key, "Trx1");

        // 事务1 可以读到自己修改的内容
        assert kanashiStrTemplate.get(trx1, key)
                                 .equals("Trx1");

        new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("事务1已提交" + (kanashiStrTemplate.commitTransaction(trx1) ? "成功" : "失败"));
        }).start();

        // 因为事务1还未提交，所以事务2读取不到这个值
        assert kanashiStrTemplate.get(trx2, key) == null;

        // 事务2 虽然读取到这个值为空，但是由于
        // 此修改会使用当前读而阻塞，直到 trx1 提交
        // 后修改此值会失败
        assert !kanashiStrTemplate.setNotExist(trx2, key, "Trx2");

        // 此时再读，读取到原来 key = Trx1
        assert kanashiStrTemplate.get(trx2, key)
                                 .equals("Trx1");

        // 事务2 setIf 成功
        assert kanashiStrTemplate.setIf(trx2, key, "Trx2", "Trx1");

        // 由于修改成功，事务2 读取到原来 key = Trx2
        assert kanashiStrTemplate.get(trx2,key)
                                 .equals("Trx2");

        // 虽然事务2 已经成功将 key 改为 Trx2，但是进行了回滚
        kanashiStrTemplate.rollbackTransaction(trx2);

        // 此时再读，读取到原来 key = Trx1，而不是 Trx2
        assert kanashiStrTemplate.get(key)
                                 .equals("Trx1");
    }
}
