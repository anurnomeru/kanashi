package ink.anur.pojo.client;

import kotlin.jvm.Synchronized;
import kotlin.jvm.Volatile;

/**
 * Created by Anur IjuoKaruKas on 2020/3/10
 */
public class SimpleScheduledMeta {

    public String name;
    public String cron;

    public byte[] nameArray;
    public byte[] cronArray;
    public int nameArraySize;
    public int cronArraySize;
    public int totalSize;

    @Volatile
    public boolean prepared = false;

    public SimpleScheduledMeta(byte[] nameArray, byte[] cronArray) {
        this.name = new String(nameArray);
        this.cron = new String(cronArray);
    }

    public SimpleScheduledMeta(String name, String cron) {
        this.name = name;
        this.cron = cron;
    }

    @Synchronized
    public void prepareForBytes() {
        if (!prepared) {
            nameArray = name.getBytes();
            cronArray = cron.getBytes();
            nameArraySize = nameArray.length;
            cronArraySize = cronArray.length;
            totalSize = 8 + nameArraySize + cronArraySize;
            prepared = true;
        }
    }
}
