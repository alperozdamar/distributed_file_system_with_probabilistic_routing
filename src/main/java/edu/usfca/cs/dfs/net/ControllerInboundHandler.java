package edu.usfca.cs.dfs.net;

import java.net.InetSocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.Utils;
import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.DfsControllerStarter;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.config.Constants;
import edu.usfca.cs.dfs.timer.TimerManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

@ChannelHandler.Sharable
public class ControllerInboundHandler extends InboundHandler {

    private static Logger logger = LogManager.getLogger(ControllerInboundHandler.class);

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

    private void handleStoreChunkMsg(ChannelHandlerContext ctx,
                                     StorageMessages.StoreChunk storeChunkMsg) {
        String fileName = storeChunkMsg.getFileName();
        String chunkId = String.valueOf(storeChunkMsg.getChunkId());
        System.out.println("[Controller]This is Store Chunk Message...");
        System.out.println("[Controller]Storing file name: " + fileName);

        //TODO: Logic code to select available SNs
        ChannelFuture write = null;
        for (int i = 1; i <= 12; i++) {
            //TODO: Check if SN have enough storage
            if (i % 3 == 1) {
                //Return 3 continuous nodes
                for (int j = i; j < i + 3; j++) {
                    DfsControllerStarter.getInstance().getBloomFilters().get(i)
                            .put((fileName + chunkId).getBytes());
                    StorageMessages.StorageNodeInfo snInfo = StorageMessages.StorageNodeInfo
                            .newBuilder().setSnIp("192.168.1.10").setSnPort(j).build();
                    StorageMessages.StoreChunkLocation chunkLocationMsg = StorageMessages.StoreChunkLocation
                            .newBuilder().addSnInfo(snInfo).build();
                    StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                            .newBuilder().setStoreChunkLocation(chunkLocationMsg).build();
                    Channel chan = ctx.channel();
                    write = chan.write(msgWrapper);
                    chan.flush().closeFuture();
                }
                break;
            }
        }
        if (write.isDone()) {
            write.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private void handleListMsg(ChannelHandlerContext ctx) {
        /**
         * Get the list of SN from DB and return to client
         */
        System.out.println("[Controller]Sending back list of SNs information");
        //TODO: Get information from database
        StorageMessages.StorageNodeInfo snInfo = StorageMessages.StorageNodeInfo.newBuilder()
                .setSnId(1).setSnIp("192.168.0.1").setSnPort(6666).setTotalFreeSpaceInBytes(10000)
                .setNumOfRetrievelRequest(10).setNumOfStorageMessage(10).build();
        StorageMessages.ListResponse response = StorageMessages.ListResponse.newBuilder()
                .addSnInfo(snInfo).build();
        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder().setListResponse(response).build();
        Channel chan = ctx.channel();
        ChannelFuture write = chan.write(msgWrapper);
        chan.flush();
        write.addListener(ChannelFutureListener.CLOSE);

    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper msg) {
        Utils.printHeader("[Controller]Received sth!");

        /***
         * STORE
         */
        if (msg.hasStoreChunkMsg()) {
            StorageMessages.StoreChunk storeChunkMsg = msg.getStoreChunkMsg();

            System.out.println("[Controller]Storing file name: " + storeChunkMsg.getFileName());

            //TODO: Logic code to select available SNs
            StorageMessages.StorageNodeInfo snInfo = StorageMessages.StorageNodeInfo.newBuilder()
                    .setSnIp("192.168.1.10").setSnPort(8888).build();
            StorageMessages.StoreChunkLocation chunkLocationMsg = StorageMessages.StoreChunkLocation
                    .newBuilder().addSnInfo(snInfo).build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                    .newBuilder().setStoreChunkLocation(chunkLocationMsg).build();
            Channel chan = ctx.channel();
            ChannelFuture write = chan.write(msgWrapper);
            chan.flush();
            write.addListener(ChannelFutureListener.CLOSE);

            handleStoreChunkMsg(ctx, storeChunkMsg);
        }
        /***
         * HEART-BEAT
         *************/
        else if (msg.hasHeartBeatMsg()) {
            StorageMessages.HeartBeat heartBeat = msg.getHeartBeatMsg();
            System.out.println("[Controller] ----------<<<<<<<<<< HEART BEAT From:SN["
                    + heartBeat.getSnId() + "] <<<<<<<<<<<<<<----------------");

            /**
             * LETS ADD to DB if it is not exists in our Hash Map!!
             */
            StorageNode storageNode = DfsControllerStarter.getInstance().getStorageNodeHashMap()
                    .get(heartBeat.getSnId());
            if (storageNode != null) {
                /**
                 * Update lastHeartBeatTime!
                 */
                storageNode.setLastHeartBeatTime(System.currentTimeMillis());
            } else {
                /********************************************
                 * Add to HashMap. 
                 * Add to DB.
                 * Schedule Timer for Heart Beat Timeouts.
                 *******************************************/
                boolean result = DfsControllerStarter.getInstance().addStorageNode(heartBeat);
                if (result) {
                    logger.debug("SN[" + heartBeat.getSnId()
                            + "] successfully subscribed to Controller, status:"
                            + Constants.STATUS_OPERATIONAL);
                    System.out.println("SN[" + heartBeat.getSnId()
                            + "] successfully subscribed to Controller, status:"
                            + Constants.STATUS_OPERATIONAL);
                    /**
                     * Schedule KeepAliveChecker for heart beat timeouts...
                     */
                    TimerManager.getInstance().scheduleKeepAliveCheckTimer(heartBeat.getSnId());
                }
            }

            StorageMessages.HeartBeatResponse response = StorageMessages.HeartBeatResponse
                    .newBuilder().setStatus(true).setSnId(heartBeat.getSnId()).build();

            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                    .newBuilder().setHeartBeatResponse(response).build();

            System.out.println("[Controller] ---------->>>>>>>> HEART BEAT RESPONSE To:SN["
                    + heartBeat.getSnId() + "] >>>>>>>>>>>--------------");

            Channel chan = ctx.channel();
            ChannelFuture write = chan.write(msgWrapper);
            chan.flush();
            write.addListener(ChannelFutureListener.CLOSE_ON_FAILURE); // I keep connection open for heart beats...

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
            /**
             * TODO: Should we send All SNs information?
             * OR
             * Should we send specific SN-ID's information?
             */

            StorageMessages.StorageNodeInfo snInfo = StorageMessages.StorageNodeInfo.newBuilder()
                    .setSnId(1).setSnIp("192.168.0.1").setSnPort(6666)
                    .setTotalFreeSpaceInBytes(10000).setNumOfRetrievelRequest(10)
                    .setNumOfStorageMessage(10).build();
            StorageMessages.ListResponse response = StorageMessages.ListResponse.newBuilder()
                    .addSnInfo(snInfo).build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                    .newBuilder().setListResponse(response).build();
            Channel chan = ctx.channel();
            ChannelFuture write = chan.write(msgWrapper);
            chan.flush();
            write.addListener(ChannelFutureListener.CLOSE);

            handleListMsg(ctx);
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
