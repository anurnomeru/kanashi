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
        kanashiStrTemplate.delete("Anur");
        assert (kanashiStrTemplate.get("Anur") == null);

        long trx1 = kanashiStrTemplate.startTransaction();
        long trx2 = kanashiStrTemplate.startTransaction();

        kanashiStrTemplate.set(trx1, "Anur", "Trx1");
        // 因为事务1还未提交，所以事务2读取不到这个值
        assert kanashiStrTemplate.get(trx2, "Anur") == null;
        // 事务1 可以读到自己修改的内容
        assert kanashiStrTemplate.get(trx1, "Anur")
                                 .equals("Trx1");

//        kanashiStrTemplate.commitTransaction(trx1);

        // 事务2 的此修改会阻塞，直到 trx1 提交
        kanashiStrTemplate.setIf(trx2, "Anur", "Trx2", "Trx1");
        assert kanashiStrTemplate.get(trx2, "Anur")
                                 .equals("Trx2");

        kanashiStrTemplate.commitTransaction(trx2);
    }
}
