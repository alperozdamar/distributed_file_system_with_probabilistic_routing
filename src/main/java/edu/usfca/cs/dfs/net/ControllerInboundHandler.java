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
public class ControllerInboundHandler extends InboundHandler {

    public ControllerInboundHandler() {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("[Controller]Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("[Controller]Connection lost: " + addr);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        /* Writable status of the channel changed */
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper msg) {
        System.out.println("[Controller]Received sth!");

        /***
         * STORE
         */
        if (msg.hasStoreChunkMsg()) {
            System.out.println("[Controller]This is Store Chunk Message...");

            StorageMessages.StoreChunk storeChunkMsg = msg.getStoreChunkMsg();
            System.out.println("[Controller]Storing file name: " + storeChunkMsg.getFileName());

            ByteString data = ByteString.copyFromUtf8("Hello World!");

            StorageMessages.StoreChunkResponse responseMsg = StorageMessages.StoreChunkResponse.newBuilder().setStatus(true).setChunkId(storeChunkMsg.getChunkId()).build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder().setStoreChunkResponse(responseMsg).build();
            System.out.println("[Controller]Send responseMsg back to Client for chunkId:"
                    + storeChunkMsg.getChunkId());

            Channel chan = ctx.channel();
            ChannelFuture write = chan.write(msgWrapper);
            chan.flush();
            write.addListener(ChannelFutureListener.CLOSE);

        }
        /***
         * HEART-BEAT
         *************/
        else if (msg.hasHeartBeatMsg()) {
            StorageMessages.HeartBeat heartBeat = msg.getHeartBeatMsg();
            System.out.println("[Controller] HEARTBEAT Msg came from:" + heartBeat.getSnId());

            StorageMessages.HeartBeatResponse response = StorageMessages.HeartBeatResponse.newBuilder().setStatus(true).setSnId(heartBeat.getSnId()).build();

            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder().setHeartBeatResponse(response).build();
            System.out.println("[Controller]Sending HEARTBEAT RESPONSE back to SN-Id:"
                    + response.getSnId());

            Channel chan = ctx.channel();
            ChannelFuture write = chan.write(msgWrapper);
            chan.flush();
            write.addListener(ChannelFutureListener.CLOSE);

        } else if (msg.hasRetrieveFileMsg()) {

            /**
             * I am Controller
             */

        } else if (msg.hasStoreChunkResponse()) {

            /**
             * I am Controller
             */

        } else if (msg.hasListResponse()) {

            List<StorageMessages.StorageNodeInfo> snInfoList = msg.getListResponse().getSnInfoList();

            for (Iterator iterator = snInfoList.iterator(); iterator.hasNext();) {
                StorageNodeInfo storageNodeInfo = (StorageNodeInfo) iterator.next();

                System.out.println("[Controller]Sn.id:" + storageNodeInfo.getSnId());
                System.out.println("[Controller]Sn.ip:" + storageNodeInfo.getSnIp());

            }

        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        System.out.println("[Controller]Flush ctx");
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
