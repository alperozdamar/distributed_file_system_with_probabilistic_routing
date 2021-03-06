package edu.usfca.cs.dfs.net;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.cs.Utils;
import edu.usfca.cs.db.SqlManager;
import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.DfsControllerStarter;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.bloomfilter.BloomFilter;
import edu.usfca.cs.dfs.config.Constants;
import edu.usfca.cs.dfs.timer.TimerManager;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.*;

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
        NetUtils.getInstance(Constants.CONTROLLER).releasePort(localAaddr.getPort());
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

        snMap = SqlManager.getInstance().getAllSNByStatusList(Constants.STATUS_OPERATIONAL);

        StorageMessages.ListResponse.Builder response = StorageMessages.ListResponse.newBuilder();
        for (StorageNode sn : snMap.values()) {
            StorageMessages.StorageNodeInfo snInfo = StorageMessages.StorageNodeInfo.newBuilder()
                    .setSnId(sn.getSnId()).setSnIp(sn.getSnIp()).setSnPort(sn.getSnPort())
                    .setTotalFreeSpaceInBytes(sn.getTotalFreeSpace())
                    .setNumOfRetrievelRequest((int) sn.getTotalRetrievelRequest())
                    .setNumOfStorageMessage((int) sn.getTotalStorageRequest()).build();
            response.addSnInfo(snInfo);
        }
        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder().setListResponse(response).build();
        Channel chan = ctx.channel();
        chan.write(msgWrapper);
        chan.flush();

    }

    private void handleHeartBeat(ChannelHandlerContext ctx,
                                 StorageMessages.StorageMessageWrapper msg) {
        StorageMessages.HeartBeat heartBeat = msg.getHeartBeatMsg();
        //Fix in here when SN go DOWN and OPERATIONAL again, it will receive old Id instead create new old
        int snId = heartBeat.getSnId();
        if (snId == -1) {
            StorageNode sn = SqlManager.getInstance().getSnByIpAndPort(heartBeat.getSnIp(),
                                                                       heartBeat.getSnPort());
            if (sn != null) {
                logger.info("SN in db");
                snId = sn.getSnId();
            } else {
                logger.info("SN not in db");
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
            if (storageNode.getStatus().equals(Constants.STATUS_DOWN)
                    || heartBeat.getSnId() == -1) {
                Utils.sendChunkOfSourceSnToDestinationSn(snId, snId);
                Utils.sendDeleteMessageToBackUpNode(snId);
            }
            storageNode.setStatus(Constants.STATUS_OPERATIONAL);
            SqlManager.getInstance().updateSNInformation(snId, Constants.STATUS_OPERATIONAL);
            SqlManager.getInstance().updateSnStatistics(heartBeat.getNumOfRetrievelRequest(),
                                                        heartBeat.getNumOfStorageMessage(),
                                                        heartBeat.getTotalFreeSpaceInBytes(),
                                                        snId);
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

    private StorageMessages.FileMetadata getFileMetadata(String fileName) {
        logger.info("[Controller] Get file metadata!\n");
        DfsControllerStarter dfsControllerStarter = DfsControllerStarter.getInstance();
        dfsControllerStarter.setFileMetadata(null);
        int chunkId = 0;
        SqlManager sqlManager = SqlManager.getInstance();
        /**
         * Find SNs contain chunk zero
         */
        HashMap<Integer, StorageNode> listSN = sqlManager
                .getAllSNByStatusList(Constants.STATUS_OPERATIONAL);
        boolean available = false;
        List<StorageMessages.StorageNodeInfo> selectedSn = new ArrayList<>();
        for (Map.Entry<Integer, BloomFilter> snBloomFilter : DfsControllerStarter.getInstance()
                .getBloomFilters().entrySet()) {
            int snId = snBloomFilter.getKey();
            logger.info("[Controller] Check SN: %d!\n", snId);
            BloomFilter bloomFilter = snBloomFilter.getValue();
            if (bloomFilter.get((fileName + chunkId).getBytes())) {
                StorageNode sn = listSN.get(snId);
                //Select backup node in case selected sn is die
                if (sn == null) {
                    int backupId = sqlManager.getSNInformationById(snId).getBackupId();
                    logger.info("[SN]BackupId: " + backupId);
                    sn = sqlManager.getSNInformationById(backupId);
                }
                if (sn != null) {
                    available = true;
                    StorageMessages.StorageNodeInfo snInfo = StorageMessages.StorageNodeInfo
                            .newBuilder().setSnIp(sn.getSnIp()).setSnPort(sn.getSnPort()).build();
                    selectedSn.add(snInfo);
                }
            }
        }

        /**
         * Send retrieve file msg to SN, synchronously until found one.
         */
        if (available) {
            logger.info("Someone may contain metadata\n");
            StorageMessages.RetrieveFile retrieveFileMsg = StorageMessages.RetrieveFile.newBuilder()
                    .setFileName(fileName).setChunkId(chunkId).build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                    .newBuilder().setRetrieveFile(retrieveFileMsg).build();

            EventLoopGroup workerGroup = new NioEventLoopGroup();
            MessagePipeline pipeline = new MessagePipeline(Constants.CONTROLLER);

            Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);
            for (StorageMessages.StorageNodeInfo snInfo : selectedSn) {
                if (dfsControllerStarter.getFileMetadata() == null) {
                    ChannelFuture cf = NetUtils.getInstance(Constants.CONTROLLER)
                            .connect(bootstrap, snInfo.getSnIp(), snInfo.getSnPort());
                    Channel chan = cf.channel();
                    chan.write(msgWrapper);
                    chan.flush().closeFuture().syncUninterruptibly();
                } else {
                    logger.info("All ready have metadata!");
                }
            }
        } else {
            logger.info("No one contain metadata\n");
        }
        return dfsControllerStarter.getFileMetadata();
    }

    private void handleRetrieveFile(ChannelHandlerContext ctx,
                                    StorageMessages.RetrieveFile retrieveFileMsg) {
        String fileName = retrieveFileMsg.getFileName();
        logger.info("[Controller]Retrieve all chunk location for file:" + fileName);

        //TODO: Ask SN for chunk 0
        StorageMessages.FileMetadata fileMetadata = this.getFileMetadata(fileName);

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
                ArrayList<StorageNode> selectedSNs = new ArrayList<>();
                for (Map.Entry<Integer, BloomFilter> snBloomFilter : DfsControllerStarter
                        .getInstance().getBloomFilters().entrySet()) {
                    int snId = snBloomFilter.getKey();
                    BloomFilter bloomFilter = snBloomFilter.getValue();
                    if (bloomFilter.get((fileName + i).getBytes())) {
                        StorageNode sn = listSN.get(snId);
                        //Select backup node in case selected sn is die
                        if (sn == null) {
                            int backupId = sqlManager.getSNInformationById(snId).getBackupId();
                            logger.info("[SN]BackupId: " + backupId);
                            sn = sqlManager.getSNInformationById(backupId);
                        }
                        if (sn != null) {
                            available = true;
                            selectedSNs.add(sn);
                        }
                    }
                }
                if (!available) {//TODO: chunk have no data in SN, return not found
                    logger.info("[Controller]Not Available");
                    fileLocationBuilder.setStatus(false);
                    break;
                }
                selectedSNs.sort(new Comparator<StorageNode>() {
                    @Override
                    public int compare(StorageNode o1, StorageNode o2) {
                        int comparable = Long.compare(o1.getTotalRetrievelRequest(), o2.getTotalRetrievelRequest());
                        if(comparable==0){
                            int[] randomCompare = new int[]{-1,1};
                            return randomCompare[new Random().nextInt(randomCompare.length)];
                        }
                        return comparable;
                    }
                });
                for(StorageNode sn : selectedSNs){
                    StorageMessages.StorageNodeInfo snInfo = StorageMessages.StorageNodeInfo
                            .newBuilder().setSnIp(sn.getSnIp()).setSnPort(sn.getSnPort())
                            .build();
                    chunkLocationBuilder.addSnInfo(snInfo);
                }
                logger.info("[Controller]Available");
                fileLocationBuilder.addChunksLocation(chunkLocationBuilder);
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

    private void askForMissingChunk(StorageMessages.HealMyChunk healMyChunk) {
        String fileName = healMyChunk.getFileName();
        int missingChunkId = healMyChunk.getChunkId();
        logger.info("[Controller] Retrieve chunk locations for file:" + fileName + ", chunkId:"
                + missingChunkId);
        //        StorageMessages.FileMetadata fileMetadata = DfsControllerStarter.getInstance()
        //                .getFileMetadataHashMap().get(fileName);

        SqlManager sqlManager = SqlManager.getInstance();
        HashMap<Integer, StorageNode> listSN = sqlManager
                .getAllSNByStatusList(Constants.STATUS_OPERATIONAL);

        /**
         * TODO:Heal
         */
        boolean available = false;
        for (Map.Entry<Integer, BloomFilter> snBloomFilter : DfsControllerStarter.getInstance()
                .getBloomFilters().entrySet()) {
            int snId = snBloomFilter.getKey();
            BloomFilter bloomFilter = snBloomFilter.getValue();
            if (bloomFilter.get((fileName + missingChunkId).getBytes())) {
                available = true;
                StorageNode sn = listSN.get(snId);
                //Select backup node in case selected sn is die
                if (sn == null) {
                    int backupId = sqlManager.getSNInformationById(snId).getBackupId();
                    sn = sqlManager.getSNInformationById(backupId);
                }

                /**
                 * Means that chunk can be in this SN so tell that SN to heal the missing chunk.
                 * Controller will basically proxy healMyChunk to that SN.
                 */
                EventLoopGroup workerGroup = new NioEventLoopGroup();
                MessagePipeline pipeline = new MessagePipeline(Constants.CONTROLLER);
                Bootstrap bootstrap = new Bootstrap().group(workerGroup)
                        .channel(NioSocketChannel.class).option(ChannelOption.SO_KEEPALIVE, true)
                        .handler(pipeline);
                ChannelFuture cf = NetUtils.getInstance(Constants.CONTROLLER)
                        .connect(bootstrap, sn.getSnIp(), sn.getSnPort());
                Channel controllerChannel = cf.channel();
                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder().setHealMyChunk(healMyChunk).build();
                controllerChannel.write(msgWrapper);
                controllerChannel.flush();

                logger.info("[Controller] ---------->>>>>>>> PROXY HealMyChunk MESSAGE To  snId["
                        + sn.getSnId() + "], for HEALING this snId[" + healMyChunk.getHealSnId()
                        + "], for chunkId:[" + missingChunkId + "] >>>>>>>>>>>--------------");
            }
        }
        if (!available) {//TODO: chunk have no data in SN, return not found
            logger.info("[Controller]Not Available");
            /**
             * TODO:
             */
        } else {
            logger.info("[Controller]Available");
            /**
             * TODO:
             */
        }

    }

    private void handleRetrieveFileResponse(ChannelHandlerContext ctx,
                                            StorageMessages.RetrieveFileResponse retrieveFileResponseMsg) {
        logger.info("[Controller]Handle retrieve file response!");
        if (retrieveFileResponseMsg.getChunkId() == 0) {//Handle chunk zero
            logger.info("Receive from: %d: ", retrieveFileResponseMsg.getSnId());
            try {
                StorageMessages.FileMetadata metadata = StorageMessages.FileMetadata
                        .parseFrom(retrieveFileResponseMsg.getData());
                logger.info("File chunk number: %d", metadata.getNumOfChunks());
                DfsControllerStarter.getInstance().setFileMetadata(metadata);
            } catch (InvalidProtocolBufferException e) {
                logger.error(e);
            }
        }
        ctx.close();
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
        } else if (msg.hasHealMyChunk()) {
            StorageMessages.HealMyChunk healMyChunk = msg.getHealMyChunk();
            String fileName = healMyChunk.getFileName();
            int missingChunkId = healMyChunk.getChunkId();
            logger.info("[Controller] ----------<<<<<<<<<< HEAL MY CHUNK MESSAGE From:SN["
                    + healMyChunk.getHealSnId() + "] FileName:[" + fileName + "] MissingChunkId:["
                    + missingChunkId + "]<<<<<<<<<<<<<<----------------");

            askForMissingChunk(healMyChunk);

        } else if (msg.hasRetrieveFileResponse()) {
            handleRetrieveFileResponse(ctx, msg.getRetrieveFileResponse());
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
