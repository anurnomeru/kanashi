package ink.anur.core;

import java.util.Optional;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ink.anur.inject.Nigate;
import ink.anur.inject.NigateInject;
import ink.anur.pojo.command.KanashiCommandDto;
import ink.anur.pojo.command.KanashiCommandResponse;
import ink.anur.pojo.log.ByteBufferKanashiEntry;
import ink.anur.pojo.log.KanashiCommand;
import ink.anur.pojo.log.common.CommandTypeEnum;
import ink.anur.pojo.log.common.StrApiTypeEnum;
import ink.anur.pojo.log.common.TransactionTypeEnum;
import ink.anur.service.command.KanashiCommandResponseHandlerService;

/**
 * Created by Anur IjuoKaruKas on 2020/3/28
 */
@Component
public class KanashiStrTemplate {

    @NigateInject
    private KanashiCommandResponseHandlerService kanashiCommandResponseHandlerService;

    private long NON_TRX = KanashiCommand.NON_TRX;

    @Autowired
    private KanashiStrTemplate me;

    @PostConstruct
    public void init() {
        Nigate.INSTANCE.injectOnly(this);

        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    me.set("Anur", "1");
                    System.out.println("期待1：" + me.get("Anur"));

                    me.delete("Anur");
                    System.out.println("期待空：" + me.get("Anur"));

                    me.setNotExist("Anur", "2");
                    System.out.println("期待2：" + me.get("Anur"));

                    me.setExist("Anur", "3");
                    System.out.println("期待3：" + me.get("Anur"));

                    me.setIf("Anur", "4", "4");
                    System.out.println("期待3：" + me.get("Anur"));

                    me.setIf("Anur", "4", "3");
                    System.out.println("期待4：" + me.get("Anur"));
                }
            }
        };
        new Thread(runnable).start();
    }

    public String get(String key) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(NON_TRX, TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SELECT, "")));

        if (acquire.getSuccess()) {
            return Optional.ofNullable(acquire.getKanashiEntry())
                           .map(ByteBufferKanashiEntry::getValueString)
                           .orElse(null);
        } else {
            throw new RuntimeException();
        }
    }

    public boolean delete(String key) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(NON_TRX, TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.DELETE, "")));

        return acquire.getSuccess();
    }

    public boolean set(String key, String value) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(NON_TRX, TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SET, value)));

        return acquire.getSuccess();
    }

    public boolean setExist(String key, String value) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(NON_TRX, TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SET_EXIST, value)));

        return acquire.getSuccess();
    }

    public boolean setNotExist(String key, String value) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(NON_TRX, TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SET_NOT_EXIST, value)));

        return acquire.getSuccess();
    }

    public boolean setIf(String key, String value, String expect) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(NON_TRX, TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SET_IF, value, expect)));

        return acquire.getSuccess();
    }
}
