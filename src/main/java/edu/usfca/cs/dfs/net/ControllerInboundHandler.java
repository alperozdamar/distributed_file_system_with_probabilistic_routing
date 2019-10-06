package edu.usfca.cs.dfs.net;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import edu.usfca.cs.Utils;
import edu.usfca.cs.db.SqlManager;
import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.DfsControllerStarter;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.bloomfilter.BloomFilter;
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
        int chunkId = storeChunkMsg.getChunkId();
        System.out.println("[Controller]This is Store Chunk Message...");
        System.out.println("[Controller]Storing file name: " + fileName + " - Chunk Id:"
                + storeChunkMsg.getChunkId());

        if (storeChunkMsg.getChunkId() == 0) { //Metadata chunk
            try {
                StorageMessages.FileMetadata fileMetadata = StorageMessages.FileMetadata
                        .parseFrom(storeChunkMsg.getData());

                DfsControllerStarter.getInstance().getFileMetadataHashMap()
                        .put(storeChunkMsg.getFileName(), fileMetadata);
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
        ChannelFuture write = null;
        SqlManager sqlManager = SqlManager.getInstance();
        HashMap<Integer, StorageNode> listSNMap = sqlManager.getAllOperationalSNList();
        Random rand = new Random();

        //loop until find a list of SNs
        boolean selectedSNs = false;
        List<StorageNode> listSN = new ArrayList<StorageNode>(listSNMap.values());
        while (!selectedSNs) {
            //Chose random primary node in list of available
            int index = rand.nextInt(listSN.size());
            StorageNode primarySN = listSN.get(index);
            List<Integer> replicaIds = primarySN.getReplicateSnIdList();
            List<Integer> selectedIds = new ArrayList<Integer>();
            selectedIds.add(primarySN.getSnId());
            logger.debug("[Controller]Primary ID: " + primarySN.getSnId());
            for (int replicaId : replicaIds) {
                StorageNode replicaSN = sqlManager.getSNInformationById(replicaId);
                if (replicaSN.getStatus().equals("DOWN")) {
                    logger.debug("[Controller]Replica is " + replicaSN.getStatus());
                    break;
                } else if (replicaSN.getTotalFreeSpaceInBytes() < storeChunkMsg.getChunkSize()) {
                    logger.debug("[Controller]Replica not enough space: %d < %d\n",
                                 replicaSN.getTotalFreeSpace(),
                                 storeChunkMsg.getChunkSize());
                    break;
                } else {
                    selectedIds.add(replicaId);
                }
            }
            int requiredReplicaNo = Constants.MAX_REPLICA_NUMBER; // It should also work with 1/2 SNs.
            if (DfsControllerStarter.getInstance().getStorageNodeHashMap()
                    .size() < Constants.MAX_REPLICA_NUMBER) {
                requiredReplicaNo = DfsControllerStarter.getInstance().getStorageNodeHashMap()
                        .size();
            }
            if (selectedIds.size() == requiredReplicaNo) {//Enough SNs
                StorageMessages.StoreChunkLocation.Builder chunkLocationMsgBuilder = StorageMessages.StoreChunkLocation
                        .newBuilder().setFileName(storeChunkMsg.getFileName())
                        .setPrimarySnId(primarySN.getSnId()).setChunkId(storeChunkMsg.getChunkId())
                        .setChunkSize(storeChunkMsg.getChunkSize());
                for (int snId : selectedIds) {
                    StorageNode sn = sqlManager.getSNInformationById(snId);
                    HashMap<Integer, BloomFilter> snBloomFilters = DfsControllerStarter
                            .getInstance().getBloomFilters();
                    snBloomFilters.get(snId).put((fileName + chunkId).getBytes());

                    StorageMessages.StorageNodeInfo snInfo = StorageMessages.StorageNodeInfo
                            .newBuilder().setSnIp(sn.getSnIp()).setSnPort(sn.getSnPort()).build();
                    chunkLocationMsgBuilder.addSnInfo(snInfo);
                }
                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder().setStoreChunkLocation(chunkLocationMsgBuilder).build();
                Channel chan = ctx.channel();
                write = chan.write(msgWrapper);
                chan.flush().closeFuture();
                selectedSNs = true;
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
        logger.info("[Controller]Sending back list of SNs information");
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

    private void handleHeartBeat(ChannelHandlerContext ctx,
                                 StorageMessages.StorageMessageWrapper msg) {
        StorageMessages.HeartBeat heartBeat = msg.getHeartBeatMsg();
        int snId = heartBeat.getSnId();
        System.out.println("[Controller] ----------<<<<<<<<<< HEART BEAT From:SN["
                + heartBeat.getSnId() + "] Ip:[" + heartBeat.getSnIp() + "] Port:["
                + heartBeat.getSnPort() + "]<<<<<<<<<<<<<<----------------");

        System.out.println("[Controller] Storage Node Hash Map Size:"
                + DfsControllerStarter.getInstance().getStorageNodeHashMap().size());

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
            snId = SqlManager.getInstance().getMaxSnId() + 1;
            boolean result = DfsControllerStarter.getInstance().addStorageNode(heartBeat, snId);
            if (result) {
                logger.debug("SN[" + snId + "] successfully subscribed to Controller, status:"
                        + Constants.STATUS_OPERATIONAL);
                System.out.println("SN[" + snId + "] successfully subscribed to Controller, status:"
                        + Constants.STATUS_OPERATIONAL);
                /**
                 * Schedule KeepAliveChecker for heart beat timeouts...
                 */
                TimerManager.getInstance().scheduleKeepAliveCheckTimer(snId);
            }
        }

        StorageMessages.HeartBeatResponse response = StorageMessages.HeartBeatResponse.newBuilder()
                .setStatus(true).setSnId(snId).build();

        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder().setHeartBeatResponse(response).build();

        System.out.println("[Controller] ---------->>>>>>>> HEART BEAT RESPONSE To:SN[" + snId
                + "] >>>>>>>>>>>--------------");

        Channel chan = ctx.channel();
        ChannelFuture write = chan.write(msgWrapper);
        chan.flush();
        write.addListener(ChannelFutureListener.CLOSE_ON_FAILURE); // I keep connection open for heart beats...
    }

    private void handleRetrieveFile(ChannelHandlerContext ctx,
                                    StorageMessages.RetrieveFile retrieveFileMsg) {
        String fileName = retrieveFileMsg.getFileName();
        System.out.println("[Controller]Retrieve all chunk location for file:" + fileName);
        StorageMessages.FileMetadata fileMetadata = DfsControllerStarter.getInstance()
                .getFileMetadataHashMap().get(fileName);
        System.out.println("[Controller] File size: " + fileMetadata.getFileSize());
        System.out.println("[Controller] Number of chunk: " + fileMetadata.getNumOfChunks());

        StorageMessages.FileLocation.Builder fileLocationBuilder = StorageMessages.FileLocation
                .newBuilder().setFileName(fileName).setStatus(true);

        SqlManager sqlManager = SqlManager.getInstance();
        HashMap<Integer, StorageNode> listSN = sqlManager.getAllOperationalSNList();
        //Get information of all chunk
        for (int i = 1; i <= fileMetadata.getNumOfChunks(); i++) {
            StorageMessages.StoreChunkLocation.Builder chunkLocationBuilder = StorageMessages.StoreChunkLocation
                    .newBuilder().setChunkId(i);
            boolean available = false;
            for (Map.Entry<Integer, BloomFilter> snBloomFilter : DfsControllerStarter.getInstance()
                    .getBloomFilters().entrySet()) {
                int snId = snBloomFilter.getKey();
                BloomFilter bloomFilter = snBloomFilter.getValue();
                if (bloomFilter.get((fileName + i).getBytes())) {
                    available = true;
                    StorageNode sn = listSN.get(snId);
                    StorageMessages.StorageNodeInfo snInfo = StorageMessages.StorageNodeInfo
                            .newBuilder().setSnIp(sn.getSnIp()).setSnPort(sn.getSnPort()).build();
                    chunkLocationBuilder.addSnInfo(snInfo);
                }
            }
            if (!available) {//TODO: chunk have no data in SN, return not found
                System.out.println("[Controller]Not Available");
                fileLocationBuilder.setStatus(false);
                break;
            } else {
                System.out.println("[Controller]Available");
                fileLocationBuilder.addChunksLocation(chunkLocationBuilder);
            }
        }

        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder().setFileLocation(fileLocationBuilder).build();
        Channel chan = ctx.channel();
        chan.writeAndFlush(msgWrapper);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper msg) {
        Utils.printHeader("[Controller]Received sth!");

        /***
         * STORE
         */
        if (msg.hasStoreChunk()) {
            StorageMessages.StoreChunk storeChunkMsg = msg.getStoreChunk();
            handleStoreChunkMsg(ctx, storeChunkMsg);
        }
        /***
         * HEART-BEAT
         *************/
        else if (msg.hasHeartBeatMsg()) {
            handleHeartBeat(ctx, msg);
        } else if (msg.hasRetrieveFile()) {
            /**
             * I am Controller
             */
            StorageMessages.RetrieveFile retrieveFileMsg = msg.getRetrieveFile();
            handleRetrieveFile(ctx, retrieveFileMsg);
        } else if (msg.hasList()) {
            /**
             * Get the list of SN from DB and return to client
             */
            //TODO: Get information from database
            /**
             * TODO: Should we send All SNs information?
             * OR
             * Should we send specific SN-ID's information?
             */

            handleListMsg(ctx);
        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
