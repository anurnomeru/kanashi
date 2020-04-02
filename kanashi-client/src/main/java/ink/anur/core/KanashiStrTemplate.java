package ink.anur.core;

import java.util.Optional;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import ink.anur.inject.Nigate;
import ink.anur.inject.NigateAfterBootStrap;
import ink.anur.inject.NigateInject;
import ink.anur.pojo.command.KanashiCommandDto;
import ink.anur.pojo.command.KanashiCommandResponse;
import ink.anur.pojo.log.ByteBufferKanashiEntry;
import ink.anur.pojo.log.KanashiCommand;
import ink.anur.pojo.log.common.CommandTypeEnum;
import ink.anur.pojo.log.common.CommonApiTypeEnum;
import ink.anur.pojo.log.common.StrApiTypeEnum;
import ink.anur.pojo.log.common.TransactionTypeEnum;
import ink.anur.service.command.KanashiCommandResponseHandlerService;

/**
 * Created by Anur IjuoKaruKas on 2020/3/28
 */
@Component
public class KanashiStrTemplate implements CommandLineRunner {

    @NigateInject
    private KanashiCommandResponseHandlerService kanashiCommandResponseHandlerService;

    private long NON_TRX = KanashiCommand.NON_TRX;

    @PostConstruct
    public void init() {
        Nigate.INSTANCE.registerToNigate(this, "KanashiStrTemplate");
    }

    @Override
    public void run(String... args) throws Exception {
        kanashiCommandResponseHandlerService.startGetCluster();
    }

    public String get(long trxId, String key) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(trxId, TransactionTypeEnum.LONG, CommandTypeEnum.STR, StrApiTypeEnum.SELECT, "")), 5);

        if (acquire.getSuccess()) {
            return Optional.ofNullable(acquire.getKanashiEntry())
                           .map(ByteBufferKanashiEntry::getValueString)
                           .orElse(null);
        } else {
            throw new RuntimeException();
        }
    }

    public String get(String key) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(NON_TRX, TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SELECT, "")), 5);

        if (acquire.getSuccess()) {
            return Optional.ofNullable(acquire.getKanashiEntry())
                           .map(ByteBufferKanashiEntry::getValueString)
                           .orElse(null);
        } else {
            throw new RuntimeException();
        }
    }

    public boolean delete(long trxId, String key) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(trxId, TransactionTypeEnum.LONG, CommandTypeEnum.STR, StrApiTypeEnum.DELETE, "")), 5);

        return acquire.getSuccess();
    }

    public boolean delete(String key) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(NON_TRX, TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.DELETE, "")), 5);

        return acquire.getSuccess();
    }

    public static void main(String[] args) {
        new KanashiCommandDto("Anur",
            KanashiCommand.Companion.generator(1L, TransactionTypeEnum.LONG, CommandTypeEnum.STR, StrApiTypeEnum.SET, "z")).getKanashiCommand();
    }

    public boolean set(long trxId, String key, String value) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(trxId, TransactionTypeEnum.LONG, CommandTypeEnum.STR, StrApiTypeEnum.SET, value)), 5);

        return acquire.getSuccess();
    }

    public boolean set(String key, String value) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(NON_TRX, TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SET, value)), 5);

        return acquire.getSuccess();
    }

    public boolean setExist(long trxId, String key, String value) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(trxId, TransactionTypeEnum.LONG, CommandTypeEnum.STR, StrApiTypeEnum.SET_EXIST, value)), 5);

        return acquire.getSuccess();
    }

    public boolean setExist(String key, String value) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(NON_TRX, TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SET_EXIST, value)), 5);

        return acquire.getSuccess();
    }

    public boolean setNotExist(long trxId, String key, String value) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(trxId, TransactionTypeEnum.LONG, CommandTypeEnum.STR, StrApiTypeEnum.SET_NOT_EXIST, value)), 5);

        return acquire.getSuccess();
    }

    public boolean setNotExist(String key, String value) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(NON_TRX, TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SET_NOT_EXIST, value)), 5);

        return acquire.getSuccess();
    }

    public boolean setIf(long trxId, String key, String value, String expect) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(trxId, TransactionTypeEnum.LONG, CommandTypeEnum.STR, StrApiTypeEnum.SET_IF, value, expect)), 5);

        return acquire.getSuccess();
    }

    public boolean setIf(String key, String value, String expect) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto(key,
                KanashiCommand.Companion.generator(NON_TRX, TransactionTypeEnum.SHORT, CommandTypeEnum.STR, StrApiTypeEnum.SET_IF, value, expect)), 5);

        return acquire.getSuccess();
    }

    public long startTransaction() {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto("",
                KanashiCommand.Companion.generator(NON_TRX, TransactionTypeEnum.LONG, CommandTypeEnum.COMMON, CommonApiTypeEnum.START_TRX, "")), 5);

        if (acquire.getSuccess()) {
            return Optional.ofNullable(acquire.getKanashiEntry())
                           .map(ByteBufferKanashiEntry::getValueLong)
                           .orElseThrow(RuntimeException::new);
        } else {
            throw new RuntimeException();
        }
    }

    public boolean commitTransaction(long trxId) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto("",
                KanashiCommand.Companion.generator(trxId, TransactionTypeEnum.LONG, CommandTypeEnum.COMMON, CommonApiTypeEnum.COMMIT_TRX, "")), 5);

        return acquire.getSuccess();
    }

    public boolean rollbackTransaction(long trxId) {
        KanashiCommandResponse acquire = kanashiCommandResponseHandlerService.acquire(
            new KanashiCommandDto("",
                KanashiCommand.Companion.generator(trxId, TransactionTypeEnum.LONG, CommandTypeEnum.COMMON, CommonApiTypeEnum.ROLL_BACK, "")), 5);

        return acquire.getSuccess();
    }
}
