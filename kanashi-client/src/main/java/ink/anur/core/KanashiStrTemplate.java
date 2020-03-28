package ink.anur.core;

import javax.annotation.PostConstruct;
import org.springframework.stereotype.Component;
import ink.anur.common.Constant;
import ink.anur.core.common.RequestExtProcessor;
import ink.anur.core.request.RequestProcessCentreService;
import ink.anur.inject.Nigate;
import ink.anur.pojo.command.KanashiCommandDto;
import ink.anur.pojo.enumerate.RequestTypeEnum;
import ink.anur.pojo.log.ByteBufferKanashiEntry;
import ink.anur.pojo.log.KanashiCommand;
import ink.anur.pojo.log.base.LogItem;
import ink.anur.pojo.log.common.CommandTypeEnum;
import ink.anur.pojo.log.common.StrApiTypeEnum;
import ink.anur.pojo.log.common.TransactionTypeEnum;

/**
 * Created by Anur IjuoKaruKas on 2020/3/28
 */
@Component
public class KanashiStrTemplate {

    private static String SERVER = Constant.INSTANCE.getSERVER();

    private RequestProcessCentreService requestProcessCentreService;

    private long NON_TRX = KanashiCommand.NON_TRX;

    @PostConstruct
    public void init() {
        requestProcessCentreService = Nigate.INSTANCE.getBeanByClass(RequestProcessCentreService.class);
    }

    String get(String key) {
        requestProcessCentreService.send(SERVER,
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(NON_TRX, TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SELECT)),
            new RequestExtProcessor(RequestTypeEnum.COMMAND_RESPONSE, byteBuffer -> {
                return new ByteBufferKanashiEntry(byteBuffer)
            })
        )
    }
}
