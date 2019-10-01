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

@ChannelHandler.Sharable
public class ClientInboundHandler extends InboundHandler {

    public ClientInboundHandler() {
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
        System.out.println("[Client]Received sth!");
        if (msg.hasStoreChunkMsg()) {
            System.out.println("[Client]This is Store Chunk Message...");
            StorageMessages.StoreChunk storeChunkMsg = msg.getStoreChunkMsg();
            System.out.println("[Client]Storing file name: " + storeChunkMsg.getFileName());
            ByteString data = ByteString.copyFromUtf8("Hello World!");
            StorageMessages.StoreChunk responseMsg = StorageMessages.StoreChunk.newBuilder().setFileName("my_file.txt").setChunkId(3).setData(data).build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder().setStoreChunkMsg(responseMsg).build();
            System.out.println("[Client]Send back message");
            Channel chan = ctx.channel();
            ChannelFuture write = chan.write(msgWrapper);
            chan.flush();
            write.addListener(ChannelFutureListener.CLOSE);
        } else if (msg.hasHeartBeatMsg()) {

        } else if (msg.hasRetrieveFileMsg()) {

        } else if (msg.hasStoreChunkResponse()) {

            /**
             * TODO:
             * For now for testing...
             */
            System.out.println("[Client]This is Store Chunk Message Response...");
            StorageMessages.StoreChunkResponse storeChunkesponse = msg.getStoreChunkResponse();

            if (storeChunkesponse.getStatus()) {
                System.out.println("[Client] Chunk stored successfully, chunkId:"
                        + storeChunkesponse.getChunkId());
            }

            System.out.println("[Client]  : " + storeChunkesponse.getStatus());

            ByteString data = ByteString.copyFromUtf8("Hello World!");
            StorageMessages.StoreChunk responseMsg = StorageMessages.StoreChunk.newBuilder().setFileName("my_file.txt").setChunkId(3).setData(data).build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder().setStoreChunkMsg(responseMsg).build();
            System.out.println("[Client]Send back message");
            Channel chan = ctx.channel();
            ChannelFuture write = chan.write(msgWrapper);
            chan.flush();
            write.addListener(ChannelFutureListener.CLOSE);

        } else if (msg.hasListResponse()) {
            List<StorageMessages.StorageNodeInfo> snInfoList = msg.getListResponse().getSnInfoList();
            for (Iterator iterator = snInfoList.iterator(); iterator.hasNext();) {
                StorageNodeInfo storageNodeInfo = (StorageNodeInfo) iterator.next();
                System.out.println("[Client]Sn.id:" + storageNodeInfo.getSnId());
                System.out.println("[Client]Sn.ip:" + storageNodeInfo.getSnIp());
            }
        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        System.out.println("[Client]Flush ctx");
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
