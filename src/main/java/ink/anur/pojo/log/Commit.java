package ink.anur.pojo.log;

import java.nio.ByteBuffer;
import ink.anur.pojo.common.AbstractStruct;
import ink.anur.pojo.enumerate.RequestTypeEnum;
import ink.anur.pojo.log.common.GenerationAndOffset;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

/**
 * Created by Anur IjuoKaruKas on 4/8/2019
 * <p>
 * 告知FLLOWER已经可以commit了
 * <p>
 * 集群成员收到 COMMIT 消息时,需要回复一个 COMMIT RESPONSE,表明自己的 commit 进度, leader
 * 则会 cover 自身 commit 进度
 */
public class Commit extends AbstractStruct {

    public static final int CanCommitGenerationOffset = AbstractStruct.Companion.getOriginMessageOverhead();

    public static final int CanCommitGenerationLength = 8;

    public static final int CanCommitOffsetOffset = CanCommitGenerationOffset + CanCommitGenerationLength;

    public static final int CanCommitOffsetLength = 8;

    public static final int BaseMessageOverhead = CanCommitOffsetOffset + CanCommitOffsetLength;

    public Commit(GenerationAndOffset canCommitGAO) {
        init(BaseMessageOverhead, RequestTypeEnum.COMMIT, buffer -> {
            buffer.putLong(canCommitGAO.getGeneration());
            buffer.putLong(canCommitGAO.getOffset());
            return null;
        });
    }

    public Commit(ByteBuffer byteBuffer) {
        this.setBuffer(byteBuffer);
    }

    public GenerationAndOffset getCanCommitGAO() {
        return new GenerationAndOffset(getBuffer().getLong(CanCommitGenerationOffset), getBuffer().getLong(CanCommitOffsetOffset));
    }

    @Override
    public void writeIntoChannel(Channel channel) {
        channel.write(Unpooled.wrappedBuffer(getBuffer()));
    }

    @Override
    public int totalSize() {
        return size();
    }

    @Override
    public String toString() {
        return "Commiter{ GAO => " + getCanCommitGAO() + " }";
    }
}
