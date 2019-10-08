package edu.usfca.cs.dfs.net;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.protobuf.ByteString;
import edu.usfca.cs.Utils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import edu.usfca.cs.db.SqlManager;
import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.DfsControllerStarter;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.bloomfilter.BloomFilter;
import edu.usfca.cs.dfs.config.Constants;
import edu.usfca.cs.dfs.timer.TimerManager;

@ChannelHandler.Sharable
public class ControllerInboundHandler extends InboundHandler {

    private static Logger logger = LogManager.getLogger(ControllerInboundHandler.class);

    public ControllerInboundHandler() {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        logger.info("[Controller]Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        InetSocketAddress localAaddr = (InetSocketAddress) ctx.channel().localAddress();
        logger.info("[Controller]Connection lost: " + addr);
        NetUtils.getInstance(Constants.CLIENT).releasePort(localAaddr.getPort());
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        /* Writable status of the channel changed */
    }

    private void handleStoreChunkMsg(ChannelHandlerContext ctx,
                                     StorageMessages.StoreChunk storeChunkMsg) {
        String fileName = storeChunkMsg.getFileName();
        int chunkId = storeChunkMsg.getChunkId();
        logger.info("[Controller]This is Store Chunk Message...");
        logger.info("[Controller]Storing file name: " + fileName + " - Chunk Id:"
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
        HashMap<Integer, StorageNode> listSNMap = sqlManager
                .getAllSNByStatusList(Constants.STATUS_OPERATIONAL);
        Random rand = new Random();

        //loop until find a list of SNs
        int requiredReplicaNo = Constants.MAX_REPLICA_NUMBER; // It should also work with 1/2 SNs.
        if (DfsControllerStarter.getInstance().getStorageNodeHashMap()
                .size() < Constants.MAX_REPLICA_NUMBER) {
            requiredReplicaNo = DfsControllerStarter.getInstance().getStorageNodeHashMap().size();
        }
        boolean selectedSNs = false;
        List<StorageNode> listSN = new ArrayList<StorageNode>(listSNMap.values());
        logger.info("[Controller]Required replica number: " + requiredReplicaNo);
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
                write = chan.writeAndFlush(msgWrapper);
                selectedSNs = true;
            }
        }
    }

    private void handleListMsg(ChannelHandlerContext ctx) {
        /**
         * Get the list of SN from DB and return to client
         */
        logger.info("[Controller] Sending back list of SNs information...");

        HashMap<Integer, StorageNode> snMap = DfsControllerStarter.getInstance()
                .getStorageNodeHashMap();

        for (StorageNode sn : snMap.values()) {
            SqlManager.getInstance().updateSnStatistics((int) sn.getTotalRetrievelRequest(),
                                                        (int) sn.getTotalStorageRequest(),
                                                        sn.getTotalFreeSpaceInBytes(),
                                                        sn.getSnId());
        }

        snMap = SqlManager.getInstance().getAllSNByStatusList(null);

        for (StorageNode sn : snMap.values()) {
            StorageMessages.StorageNodeInfo snInfo = StorageMessages.StorageNodeInfo.newBuilder()
                    .setSnId(sn.getSnId()).setSnIp(sn.getSnIp()).setSnPort(sn.getSnPort())
                    .setTotalFreeSpaceInBytes(sn.getTotalFreeSpace())
                    .setNumOfRetrievelRequest((int) sn.getTotalRetrievelRequest())
                    .setNumOfStorageMessage((int) sn.getTotalStorageRequest()).build();
            StorageMessages.ListResponse response = StorageMessages.ListResponse.newBuilder()
                    .addSnInfo(snInfo).build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                    .newBuilder().setListResponse(response).build();
            Channel chan = ctx.channel();
            ChannelFuture write = chan.write(msgWrapper);
            chan.flush();
            write.addListener(ChannelFutureListener.CLOSE);
        }

    }

    private void handleHeartBeat(ChannelHandlerContext ctx,
                                 StorageMessages.StorageMessageWrapper msg) {
        StorageMessages.HeartBeat heartBeat = msg.getHeartBeatMsg();
        //Fix in here when SN go DOWN and OPERATIONAL again, it will receive old Id instead create new old
        int snId = heartBeat.getSnId();
        if(snId==-1){
            StorageNode sn = SqlManager.getInstance().getSnByIpAndPort(heartBeat.getSnIp(), heartBeat.getSnPort());
            if(sn!=null){
                System.out.println("SN in db");
                snId = sn.getSnId();
            } else{
                System.out.println("SN not in db");
            }
        }

        if (DfsControllerStarter.LOG_HEART_BEAT) {
            logger.info("[Controller] ----------<<<<<<<<<< HEART BEAT From:SN["
                    + heartBeat.getSnId() + "] Ip:[" + heartBeat.getSnIp() + "] Port:["
                    + heartBeat.getSnPort() + "]<<<<<<<<<<<<<<----------------");
            logger.info("[Controller] Storage Node Hash Map Size:"
                    + DfsControllerStarter.getInstance().getStorageNodeHashMap().size());
        }

        /**
         * LETS ADD to DB if it is not exists in our Hash Map!!
         */
        StorageNode storageNode = DfsControllerStarter.getInstance().getStorageNodeHashMap()
                .get(snId);
        if (storageNode != null) {

            logger.info("Test.heartBeat.getNumOfRetrievelRequest():"
                    + heartBeat.getNumOfRetrievelRequest());
            logger.info("Test.heartBeat.getNumOfStorageMessage():"
                    + heartBeat.getNumOfStorageMessage());

            /**
             * Update lastHeartBeatTime!
             * Update heartBeatStatus
             */
            storageNode.setLastHeartBeatTime(System.currentTimeMillis());
            storageNode.setTotalRetrievelRequest(heartBeat.getNumOfRetrievelRequest());
            storageNode.setTotalFreeSpace(heartBeat.getTotalFreeSpaceInBytes());
            storageNode.setTotalStorageRequest(heartBeat.getNumOfStorageMessage());

            //Receive Heartbeat from STATUS_DOWN node
            if(storageNode.getStatus().equals(Constants.STATUS_DOWN) || heartBeat.getSnId()==-1){
                Utils.sendChunkOfSourceSnToDestinationSn(snId, snId);
            }
            storageNode.setStatus(Constants.STATUS_OPERATIONAL);
            SqlManager.getInstance().updateSNInformation(snId, Constants.STATUS_OPERATIONAL);
            SqlManager.getInstance().updateSnStatistics(heartBeat.getNumOfRetrievelRequest(), heartBeat.getNumOfStorageMessage(),
                    heartBeat.getTotalFreeSpaceInBytes(), snId);
            SqlManager.getInstance().updateSNReplication(snId, -1);
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
                logger.info("SN[" + snId + "] successfully subscribed to Controller, status:"
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
        if (DfsControllerStarter.LOG_HEART_BEAT) {
            logger.info("[Controller] ---------->>>>>>>> HEART BEAT RESPONSE To:SN[" + snId
                    + "] >>>>>>>>>>>--------------");
        }

        Channel chan = ctx.channel();
        ChannelFuture write = chan.write(msgWrapper);
        chan.flush();
        write.addListener(ChannelFutureListener.CLOSE_ON_FAILURE); // I keep connection open for heart beats...
    }

    private void handleRetrieveFile(ChannelHandlerContext ctx,
                                    StorageMessages.RetrieveFile retrieveFileMsg) {
        String fileName = retrieveFileMsg.getFileName();
        logger.info("[Controller]Retrieve all chunk location for file:" + fileName);
        StorageMessages.FileMetadata fileMetadata = DfsControllerStarter.getInstance()
                .getFileMetadataHashMap().get(fileName);

        if (fileMetadata != null) {

            logger.info("[Controller] File size: " + fileMetadata.getFileSize());
            logger.info("[Controller] Number of chunk: " + fileMetadata.getNumOfChunks());

            StorageMessages.FileLocation.Builder fileLocationBuilder = StorageMessages.FileLocation
                    .newBuilder().setFileName(fileName).setStatus(true);

            SqlManager sqlManager = SqlManager.getInstance();
            HashMap<Integer, StorageNode> listSN = sqlManager
                    .getAllSNByStatusList(Constants.STATUS_OPERATIONAL);
            //Get information of all chunk
            for (int i = 1; i <= fileMetadata.getNumOfChunks(); i++) {
                StorageMessages.StoreChunkLocation.Builder chunkLocationBuilder = StorageMessages.StoreChunkLocation
                        .newBuilder().setChunkId(i);
                boolean available = false;
                for (Map.Entry<Integer, BloomFilter> snBloomFilter : DfsControllerStarter
                        .getInstance().getBloomFilters().entrySet()) {
                    int snId = snBloomFilter.getKey();
                    BloomFilter bloomFilter = snBloomFilter.getValue();
                    if (bloomFilter.get((fileName + i).getBytes())) {
                        available = true;
                        StorageNode sn = listSN.get(snId);
                        //Select backup node in case selected sn is die
                        if (sn == null) {
                            int backupId = sqlManager.getSNInformationById(snId).getBackupId();
                            System.out.println("[SN]BackupId: "+backupId);
                            sn = sqlManager.getSNInformationById(backupId);
                        }
                        StorageMessages.StorageNodeInfo snInfo = StorageMessages.StorageNodeInfo
                                .newBuilder().setSnIp(sn.getSnIp()).setSnPort(sn.getSnPort())
                                .build();
                        chunkLocationBuilder.addSnInfo(snInfo);
                    }
                }
                if (!available) {//TODO: chunk have no data in SN, return not found
                    logger.info("[Controller]Not Available");
                    fileLocationBuilder.setStatus(false);
                    break;
                } else {
                    logger.info("[Controller]Available");
                    fileLocationBuilder.addChunksLocation(chunkLocationBuilder);
                }
            }

            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                    .newBuilder().setFileLocation(fileLocationBuilder).build();
            Channel chan = ctx.channel();
            chan.writeAndFlush(msgWrapper);
        } else {
            logger.info("[Controller] FileName does NOT exists! Filename:" + fileName);

            StorageMessages.FileLocation.Builder fileLocationBuilder = StorageMessages.FileLocation
                    .newBuilder().setFileName(fileName).setStatus(false);
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                    .newBuilder().setFileLocation(fileLocationBuilder).build();
            Channel chan = ctx.channel();
            chan.writeAndFlush(msgWrapper);
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper msg) {
        logger.info("[Controller]Received sth!");

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
