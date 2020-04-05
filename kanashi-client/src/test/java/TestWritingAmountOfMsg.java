import java.util.Random;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ink.anur.DemoApplication;
import ink.anur.core.KanashiStrTemplate;

/**
 * Created by Anur IjuoKaruKas on 2020/4/1
 */
@SpringBootTest(classes = DemoApplication.class)
public class TestWritingAmountOfMsg {

    @Autowired
    private KanashiStrTemplate kanashiStrTemplate;

    @Test
    public void test() {
        for (int i = 0; i < 1000000; i++) {
            kanashiStrTemplate.set(String.valueOf(i), randomString(10, "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"));
            System.out.println(i);
        }
        //
        //        for (int i = 0; i < 1000000; i++) {
        //            System.out.println(kanashiStrTemplate.get(String.valueOf(i)));
        //            if (i % 100 == 0) {
        //                System.out.println(i);
        //            }
        //        }
    }

    /**
     * 随机生成一个字符串，内容取自于range内指定的范围
     */
    public static String randomString(int length, String range) {
        StringBuilder sb = new StringBuilder(length);
        if (range == null || range.equals("")) {
            return "";
        }
        Random r = new Random();
        for (int i = 0; i < length; i++) {
            char ch = range.charAt(r.nextInt(range.length()));
            sb.append(ch);
        }
        return sb.toString();
    }
}
