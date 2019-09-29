package edu.usfca.cs.dfs.net;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;

import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.StorageMessages.StorageNodeInfo;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class InboundHandler
        extends SimpleChannelInboundHandler<StorageMessages.StorageMessageWrapper> {

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
        System.out.println("Received sth!");
        if (msg.hasStoreChunkMsg()) {
            System.out.println("This is Store Chunk Message...");

            StorageMessages.StoreChunk storeChunkMsg = msg.getStoreChunkMsg();
            System.out.println("Storing file name: " + storeChunkMsg.getFileName());
            
            ByteString data = ByteString.copyFromUtf8("Hello World!");
            StorageMessages.StoreChunk responseMsg = StorageMessages.StoreChunk.newBuilder()
                    .setFileName("my_file.txt").setChunkId(3).setData(data).build();

            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
                    .setStoreChunkMsg(responseMsg).build();
            System.out.println("Send back message");
            ctx.write(msgWrapper);
        } else if (msg.hasHeartBeatMsg()) {

            /**
             * I am Controller.
             */

        } else if (msg.hasRetrieveFileMsg()) {

            /**
             * I am Controller
             */

        } else if (msg.hasStoreChunkMsg()) {

            /**
             * I am Controller
             */

        } else if (msg.hasListResponse()) {

            List<StorageMessages.StorageNodeInfo> snInfoList = msg.getListResponse().getSnInfoList();

            for (Iterator iterator = snInfoList.iterator(); iterator.hasNext();) {
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
