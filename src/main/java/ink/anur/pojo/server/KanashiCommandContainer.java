package ink.anur.pojo.server;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import ink.anur.log.common.FetchDataInfo;
import ink.anur.log.logitemset.ByteBufferLogItemSet;
import ink.anur.log.logitemset.FileLogItemSet;
import ink.anur.pojo.common.AbstractTimedStruct;
import ink.anur.pojo.enumerate.RequestTypeEnum;
import ink.anur.util.FileIOUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.DefaultFileRegion;

/**
 * Created by Anur IjuoKaruKas on 2019/3/28
 * <p>
 * 获取 AOF 用
 * <p>
 * 由 4位CRC + 4位类型 + 8位时间戳 + 4位内容长度 + 内容 组成
 * <p>
 * 子类可以实现其 content 部分的内容拓展
 */
public class KanashiCommandContainer extends AbstractTimedStruct {

    private static final int GenerationOffset = AbstractTimedStruct.Companion.getTimestampOffset() + AbstractTimedStruct.Companion.getTimestampLength();

    private static final int GenerationLength = 8;

    private static final int FileLogItemSetSizeOffset = GenerationOffset + GenerationLength;

    private static final int FileLogItemSetSizeLength = 4;

    /**
     * 最基础的 KanashiCommandContainer 大小 ( 不包括byteBufferLogItemSet )
     */
    private static final int BaseMessageOverhead = FileLogItemSetSizeOffset + FileLogItemSetSizeLength;

    public static final long Invalid = -1L;

    private final int fileLogItemSetSize;

    private FileLogItemSet fileLogItemSet;

    public KanashiCommandContainer(FetchDataInfo fetchDataInfo) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(BaseMessageOverhead);
        init(byteBuffer, RequestTypeEnum.FETCH_RESPONSE);

        // 为空代表已无更多更新的消息
        if (fetchDataInfo == null) {
            fileLogItemSetSize = 0;
            byteBuffer.putLong(Invalid);
            byteBuffer.putInt((int) Invalid);
        } else {
            fileLogItemSet = fetchDataInfo.getFos();
            fileLogItemSetSize = fileLogItemSet.sizeInBytes();
            byteBuffer.putLong(fetchDataInfo.getFetchMeta()
                                            .getGeneration());
            byteBuffer.putInt(fileLogItemSetSize);
        }

        byteBuffer.flip();
    }

    public KanashiCommandContainer(ByteBuffer byteBuffer) {
        this.setBuffer(byteBuffer);
        fileLogItemSetSize = getBuffer().getInt(FileLogItemSetSizeOffset);
    }

    public long getGeneration() {
        return getBuffer().getLong(GenerationOffset);
    }

    public ByteBufferLogItemSet read() {
        getBuffer().position(BaseMessageOverhead);
        return new ByteBufferLogItemSet(getBuffer().slice());
    }

    public int getFileLogItemSetSize() {
        return fileLogItemSetSize;
    }

    public FileLogItemSet getFileLogItemSet() {
        return fileLogItemSet;
    }

    @Override
    public void writeIntoChannel(Channel channel) {
        channel.write(Unpooled.wrappedBuffer(getBuffer()));
        if (fileLogItemSetSize > 0) {
            try {
                int start = fileLogItemSet.getStart();
                int end = fileLogItemSet.getEnd();
                int count = end - start;
                channel.write(new DefaultFileRegion(FileIOUtil.openChannel(fileLogItemSet.getFile(), false), start, count));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int totalSize() {
        return size() + fileLogItemSetSize;
    }

    @Override
    public String toString() {
        return "KanashiCommandContainer{ gen => " + getGeneration() + ", fileSize => " + totalSize() + " }";
    }
}
