package edu.usfca.cs.dfs.net;

import java.net.InetSocketAddress;

import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.StorageMessages;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

@ChannelHandler.Sharable
public class StorageNodeInboundHandler extends InboundHandler {

    public StorageNodeInboundHandler() {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("[SN]Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("[SN]Connection lost: " + addr);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        /* Writable status of the channel changed */
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper msg) {
        System.out.println("[SN]Received sth!");
        if (msg.hasStoreChunkMsg()) {
            System.out.println("[SN]This is Store Chunk Message...");

            StorageMessages.StoreChunk storeChunkMsg = msg.getStoreChunkMsg();
            System.out.println("[SN]Storing file name: " + storeChunkMsg.getFileName());

            ByteString data = ByteString.copyFromUtf8("Hello World!");
            StorageMessages.StoreChunk responseMsg = StorageMessages.StoreChunk.newBuilder().setFileName("my_file.txt").setChunkId(3).setData(data).build();

            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder().setStoreChunkMsg(responseMsg).build();
            System.out.println("[SN]Send back message");

            Channel chan = ctx.channel();
            ChannelFuture write = chan.write(msgWrapper);
            chan.flush();
            write.addListener(ChannelFutureListener.CLOSE);

        } else if (msg.hasHeartBeatResponse()) {
            StorageMessages.HeartBeatResponse heartBeatResponse = msg.getHeartBeatResponse();
            System.out.println("[SN] Heart Beat Response came from Controller... from me. SN-Id:"
                    + heartBeatResponse.getSnId() + ", status:" + heartBeatResponse.getStatus());

            if (heartBeatResponse.getStatus()) {
                System.out.println("[SN] Perfect!");
            } else {
                /**
                 * TODO: 
                 * PROBLEM
                 */
            }

        } else if (msg.hasRetrieveFileMsg()) {

        } else if (msg.hasStoreChunkResponse()) {

        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        System.out.println("[SN]Flush ctx");
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
