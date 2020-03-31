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
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    //                    System.out.println("正在----请求");
                    //                    System.out.println("期待空：" + me.get("Anur111"));
                    me.set("Anur", "Zzzz");
                    System.out.println("期待空zzzz：" + me.get("Anur"));
                    //                    System.out.println("期待空：" + me.get("aaaaa"));
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

    public void set(String key, String value) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(NON_TRX, TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SET, value)));

        if (!acquire.getSuccess()) {
            throw new RuntimeException();
        }
    }
}
