package ink.anur.io.common

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2020/2/22
 *
 * 通用的解码器 格式为 32位的length + body
 */
class KanashiDecoder : ByteToMessageDecoder() {
    override fun decode(ctx: ChannelHandlerContext?, buffer: ByteBuf?, list: MutableList<Any>) {
        if (ctx != null && buffer != null) {
            decode(ctx, buffer)?.let { list.add(it) }
        }
    }

    /**
     * 4位 长度  + 消息内容
     */
    private fun decode(ctx: ChannelHandlerContext, buffer: ByteBuf): ByteBuffer? {
        buffer.markReaderIndex()
        val maybeLength = buffer.readInt()
        val remain = buffer.readableBytes()

        if (remain < maybeLength) {
            buffer.resetReaderIndex()
            return null
        } else {
            // ver1.0 通过ByteBuffer直接去读
            val resultOne = ByteBuffer.allocate(maybeLength)
            buffer.readBytes(resultOne)
            resultOne.rewind()

            //            // ver2.0 通过ByteBuf原生提供的API
            //            // 标识此index后的已经读过了
            //            buffer.resetReaderIndex();
            //            buffer.readerIndex(LengthInBytes + maybeLength);
            //
            //            //             第一个字节是长度，和业务无关
            //            ByteBuffer resultTwo = buffer.nioBuffer(LengthInBytes, maybeLength);
            //
            //            byte[] bytesFromResultOne = new byte[maybeLength];
            //            resultOne.get(bytesFromResultOne);
            //
            //            byte[] bytesFromResultTwo = new byte[maybeLength];
            //            resultTwo.get(bytesFromResultTwo);
            //
            //            if (!Arrays.toString(bytesFromResultOne)
            //                       .equals(Arrays.toString(bytesFromResultTwo))) {
            //                System.out.println();
            //            }
            //            resultTwo.rewind();
            //            resultTwo.rewind();

            return resultOne
        }
    }
}