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
 * 当收到leader发来的可提交进度时,进行进度提交,并且进行当前最大提交进度的回包
 */
public class CommitResponse extends AbstractStruct {

    public static final int CommitGenerationOffset = AbstractStruct.Companion.getOriginMessageOverhead();

    public static final int CommitGenerationLength = 8;

    public static final int CommitOffsetOffset = CommitGenerationOffset + CommitGenerationLength;

    public static final int CommitOffsetLength = 8;

    public static final int BaseMessageOverhead = CommitOffsetOffset + CommitOffsetLength;

    public CommitResponse(GenerationAndOffset CommitGAO) {
        init(BaseMessageOverhead, RequestTypeEnum.COMMIT_RESPONSE, buffer -> {
            buffer.putLong(CommitGAO.getGeneration());
            buffer.putLong(CommitGAO.getOffset());
            return null;
        });
    }

    public CommitResponse(ByteBuffer byteBuffer) {
        this.setBuffer(byteBuffer);
    }

    public GenerationAndOffset getCommitGAO() {
        return new GenerationAndOffset(getBuffer().getLong(CommitGenerationOffset), getBuffer().getLong(CommitOffsetOffset));
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
        return "CommitResponse{ GAO => " + getCommitGAO() + " }";
    }
}
