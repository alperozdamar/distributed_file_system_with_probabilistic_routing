package edu.usfca.cs.dfs.net;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;

import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.StorageMessages.StorageNodeInfo;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class InboundHandler
        extends SimpleChannelInboundHandler<StorageMessages.StorageMessageWrapper> {

    private int i = 0;

    public InboundHandler() {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection lost: " + addr);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        /* Writable status of the channel changed */
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper msg) {
        System.out.println("Received sth");
        if (msg.hasStoreChunk()) {
            System.out.println("This is Store Chunk Message...");

            StorageMessages.StoreChunk storeChunkMsg = msg.getStoreChunk();
            System.out.println("Storing file name: " + storeChunkMsg.getFileName() + " - " + (++i));

            ByteString data = ByteString.copyFromUtf8("Hello World!");
            StorageMessages.StoreChunk response = StorageMessages.StoreChunk.newBuilder().setFileName("my_file_" + i + ".txt").setChunkId(3).setData(data).build();

            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder().setStoreChunk(response).build();
            Channel chan = ctx.channel();
            ChannelFuture write = chan.write(msgWrapper);
            chan.flush();
            write.addListener(ChannelFutureListener.CLOSE);

        } else if (msg.hasHeartBeatMsg()) {

            /**
             * I am Controller.
             */

        } else if (msg.hasRetrieveFile()) {

            /**
             * I am Controller
             */

        } else if (msg.hasStoreChunkResponse()) {

            /**
             * I am Controller
             */

        } else if (msg.hasListResponse()) {

            List<StorageMessages.StorageNodeInfo> snInfoList = msg.getListResponse().getSnInfoList();

            for (Iterator iterator = snInfoList.iterator(); iterator.hasNext(); ) {
                StorageNodeInfo storageNodeInfo = (StorageNodeInfo) iterator.next();

                System.out.println("Sn.id:" + storageNodeInfo.getSnId());
                System.out.println("Sn.ip:" + storageNodeInfo.getSnIp());

            }

        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        System.out.println("Flush ctx");
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
