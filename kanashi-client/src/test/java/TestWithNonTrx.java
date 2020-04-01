import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.Assert;
import ink.anur.DemoApplication;
import ink.anur.core.KanashiStrTemplate;

/**
 * Created by Anur IjuoKaruKas on 2020/4/1
 */
@SpringBootTest(classes = DemoApplication.class)
public class TestWithNonTrx {

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

        kanashiStrTemplate.setIf("Anur", "3", "1");
        assert (kanashiStrTemplate.get("Anur")
                                  .equals("3"));

        kanashiStrTemplate.setExist("Anur", "4");
        assert (kanashiStrTemplate.get("Anur")
                                  .equals("4"));

        kanashiStrTemplate.delete("Anur");
        kanashiStrTemplate.setExist("Anur", "4");
        assert (kanashiStrTemplate.get("Anur") == null);

        kanashiStrTemplate.setNotExist("Anur", "5");
        assert (kanashiStrTemplate.get("Anur")
                                  .equals("5"));

        // 最后一个故意报错
        assert (kanashiStrTemplate.get("Anur") == null);
    }
}
