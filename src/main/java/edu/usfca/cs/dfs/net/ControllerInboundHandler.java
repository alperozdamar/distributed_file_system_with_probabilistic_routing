package edu.usfca.cs.dfs.net;

import com.google.protobuf.ByteString;
import edu.usfca.cs.Utils;
import edu.usfca.cs.dfs.StorageMessages;
import io.netty.channel.*;

import java.net.InetSocketAddress;

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
        Utils.printHeader("[Controller]Received sth!");

        /***
         * STORE
         */
        if (msg.hasStoreChunkMsg()) {
            System.out.println("[Controller]This is Store Chunk Message...");

            StorageMessages.StoreChunk storeChunkMsg = msg.getStoreChunkMsg();
            System.out.println("[Controller]Storing file name: " + storeChunkMsg.getFileName());

            //TODO: Logic code to select available SNs
            StorageMessages.StorageNodeInfo snInfo = StorageMessages.StorageNodeInfo.newBuilder()
                    .setSnIp("192.168.1.10")
                    .setSnPort(8888).build();
            StorageMessages.StoreChunkLocation chunkLocationMsg =
                    StorageMessages.StoreChunkLocation.newBuilder().addSnInfo(snInfo).build();
            StorageMessages.StorageMessageWrapper msgWrapper =
                    StorageMessages.StorageMessageWrapper.newBuilder()
                            .setStoreChunkLocation(chunkLocationMsg).build();
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

        } else if (msg.hasList()) {
            /**
             * Get the list of SN from DB and return to client
             */
            System.out.println("[Controller]Sending back list of SNs information");
            //TODO: Get information from database
            StorageMessages.StorageNodeInfo snInfo = StorageMessages.StorageNodeInfo.newBuilder()
                    .setSnId(1)
                    .setSnIp("192.168.0.1")
                    .setSnPort(6666)
                    .setTotalFreeSpaceInBytes(10000)
                    .setNumOfRetrievelRequest(10)
                    .setNumOfStorageMessage(10).build();
            StorageMessages.ListResponse response = StorageMessages.ListResponse.newBuilder().addSnInfo(snInfo).build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
                    .setListResponse(response).build();
            Channel chan = ctx.channel();
            ChannelFuture write = chan.write(msgWrapper);
            chan.flush();
            write.addListener(ChannelFutureListener.CLOSE);
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
