package ink.anur.pojo.server;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import ink.anur.log.common.FetchDataInfo;
import ink.anur.log.operationset.ByteBufferOperationSet;
import ink.anur.log.operationset.FileOperationSet;
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
public class FetchResponse extends AbstractTimedStruct {

    private static final int GenerationOffset = AbstractTimedStruct.Companion.getTimestampOffset() + AbstractTimedStruct.Companion.getTimestampLength();

    private static final int GenerationLength = 8;

    private static final int FileOperationSetSizeOffset = GenerationOffset + GenerationLength;

    private static final int FileOperationSetSizeLength = 4;

    /**
     * 最基础的 FetchResponse 大小 ( 不包括byteBufferOperationSet )
     */
    private static final int BaseMessageOverhead = FileOperationSetSizeOffset + FileOperationSetSizeLength;

    public static final long Invalid = -1L;

    private final int fileOperationSetSize;

    private FileOperationSet fileOperationSet;

    public FetchResponse(FetchDataInfo fetchDataInfo) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(BaseMessageOverhead);
        init(byteBuffer, RequestTypeEnum.FETCH_RESPONSE);

        // 为空代表已无更多更新的消息
        if (fetchDataInfo == null) {
            fileOperationSetSize = 0;
            byteBuffer.putLong(Invalid);
            byteBuffer.putInt((int) Invalid);
        } else {
            fileOperationSet = fetchDataInfo.getFos();
            fileOperationSetSize = fileOperationSet.sizeInBytes();
            byteBuffer.putLong(fetchDataInfo.getFetchMeta()
                                            .getGeneration());
            byteBuffer.putInt(fileOperationSetSize);
        }

        byteBuffer.flip();
    }

    public FetchResponse(ByteBuffer byteBuffer) {
        this.setBuffer(byteBuffer);
        fileOperationSetSize = getBuffer().getInt(FileOperationSetSizeOffset);
    }

    public long getGeneration() {
        return getBuffer().getLong(GenerationOffset);
    }

    public ByteBufferOperationSet read() {
        getBuffer().position(BaseMessageOverhead);
        return new ByteBufferOperationSet(getBuffer().slice());
    }

    public int getFileOperationSetSize() {
        return fileOperationSetSize;
    }

    public FileOperationSet getFileOperationSet() {
        return fileOperationSet;
    }

    @Override
    public void writeIntoChannel(Channel channel) {
        channel.write(Unpooled.wrappedBuffer(getBuffer()));
        if (fileOperationSetSize > 0) {
            try {
                int start = fileOperationSet.getStart();
                int end = fileOperationSet.getEnd();
                int count = end - start;
                channel.write(new DefaultFileRegion(FileIOUtil.openChannel(fileOperationSet.getFile(), false), start, count));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int totalSize() {
        return size() + fileOperationSetSize;
    }

    @Override
    public String toString() {
        return "FetchResponse{ gen => " + getGeneration() + ", fileSize => " + totalSize() + " }";
    }
}
