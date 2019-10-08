package edu.usfca.cs.dfs.net;

import static edu.usfca.cs.Utils.getMd5;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.ByteString;

import edu.usfca.cs.Utils;
import edu.usfca.cs.db.model.MetaDataOfChunk;
import edu.usfca.cs.dfs.DfsStorageNodeStarter;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.StorageMessages.StorageNodeInfo;
import edu.usfca.cs.dfs.config.ConfigurationManagerSn;
import edu.usfca.cs.dfs.config.Constants;
import edu.usfca.cs.dfs.timer.TimerManager;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

@ChannelHandler.Sharable
public class StorageNodeInboundHandler extends InboundHandler {

    private static Logger logger = LogManager.getLogger(StorageNodeInboundHandler.class);

    public StorageNodeInboundHandler() {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        logger.info("[SN]Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        int mySnId = DfsStorageNodeStarter.getInstance().getStorageNode().getSnId();
        /* A channel has been disconnected */
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        InetSocketAddress localAaddr = (InetSocketAddress) ctx.channel().localAddress();
        logger.info("[SN" + mySnId + "] Connection lost: " + addr);
        NetUtils.getInstance(Constants.STORAGENODE).releasePort(localAaddr.getPort());
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        /* Writable status of the channel changed */
    }

    private void handleStoreChunkMsg(ChannelHandlerContext ctx,
                                     StorageMessages.StoreChunk storeChunkMsg) {
        int mySnId = DfsStorageNodeStarter.getInstance().getStorageNode().getSnId();

        DfsStorageNodeStarter.getInstance().getStorageNode().incrementTotalStorageRequest();

        logger.info("[SN" + mySnId + "] ----------<<<<<<<<<< STORE CHUNK , FileName["
                + storeChunkMsg.getFileName() + "] chunkId:[" + storeChunkMsg.getChunkId()
                + "] PrimarySnId:[" + storeChunkMsg.getPrimarySnId()
                + "]<<<<<<<<<<<<<<----------------");
        String dataChecksum = Utils.getMd5(storeChunkMsg.getData().toByteArray());
        System.out.println("[SN" + mySnId + "]Receive checksum: " + storeChunkMsg.getChecksum());
        logger.info("[SN" + mySnId + "]dataChecksum: " + dataChecksum);

        /**
         * if successfully write into File System return sucess respnse
         */
        boolean result = writeIntoFileSystem(storeChunkMsg);
        StorageMessages.StoreChunkResponse responseMsg = StorageMessages.StoreChunkResponse
                .newBuilder().setChunkId(storeChunkMsg.getChunkId()).setStatus(result).build();
        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder().setStoreChunkResponse(responseMsg).build();
        Channel chan = ctx.channel();
        ChannelFuture write = chan.write(msgWrapper);
        chan.flush();
        write.addListener(ChannelFutureListener.CLOSE);

        logger.info("[SN" + mySnId + "] ---------->>>>>>>> STORE CHUNK RESPONSE For ChunkId"
                + storeChunkMsg.getChunkId() + "result:" + result + "] >>>>>>>>>>>--------------");

        if (result) {
            /** 
             * Replication.
             * Send this chunk to the other 2 replicas.
             */
            replicateChunk(storeChunkMsg);
        }
    }

    /**
     * 
     * Write chunk into File System.
     * 
     * @param storeChunkMsg
     * @return
     */
    private boolean writeIntoFileSystem(StorageMessages.StoreChunk storeChunkMsg) {

        int mySnId = DfsStorageNodeStarter.getInstance().getStorageNode().getSnId();

        boolean result = false;
        /**
         * 1-) Create Directory if not exists. bigdata/whoamI/primaryId/
         */
        String path = createDirectoryIfNecessary(storeChunkMsg.getPrimarySnId());

        if (path != null) {
            /**
             * 2-) Save into file
             */

            Utils.writeChunkIntoFileInStorageNode(path, storeChunkMsg);

            /**
             * 3-) Put into HASHMAP => key:(filename_chunkId) Value: (FileLocation,Checksum,FileName,ChunkId) 
             *      Also put into File System. (/bigdata/whoamI/.)
             */
            String key = storeChunkMsg.getFileName() + "_" + storeChunkMsg.getChunkId();
            MetaDataOfChunk metaDataOfChunk = new MetaDataOfChunk(storeChunkMsg.getChecksum(),
                                                                  path,
                                                                  storeChunkMsg.getChunkId(),
                                                                  storeChunkMsg.getFileName(),
                                                                  storeChunkMsg.getData().size());
            DfsStorageNodeStarter.getInstance().getFileChunkToMetaDataMap().put(key,
                                                                                metaDataOfChunk);
            result = true;
        }
        return result;
    }

    /**
     * 
     * bigdata/whoamI/primaryId/ data
     * 
     */
    private String createDirectoryIfNecessary(int snId) {
        String directoryPath = null;
        try {
            logger.info("Working Directory = " + System.getProperty("user.dir"));
            directoryPath = ConfigurationManagerSn.getInstance().getStoreLocation();
            String whoamI = System.getProperty("user.name");
            directoryPath = System.getProperty("user.dir") + File.separator + directoryPath
                    + File.separator + whoamI + File.separator + snId;

            logger.info("Path:" + directoryPath);
            File directory = new File(directoryPath);
            if (!directory.exists()) {
                logger.info("No Folder");

                Files.createDirectories(Paths.get(directoryPath));

                directory = new File(directoryPath);
                if (!directory.exists()) {
                    logger.info("Folder is created,successfully.");
                } else {
                    logger.info("Folder could not be created!");
                }
            } else {
                logger.info("No need to create folder already exists.");
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return directoryPath;
    }

    private void sendAllFileInFileSystemByNodeId(int snId, String destinationIp,
                                                 int destinationPort) {
        String directoryPath = ConfigurationManagerSn.getInstance().getStoreLocation();
        String whoamI = System.getProperty("user.name");
        directoryPath = System.getProperty("user.dir") + File.separator + directoryPath
                + File.separator + whoamI + File.separator + snId;
        File folder = new File(directoryPath);
        File[] listOfFiles = folder.listFiles();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(Constants.STORAGENODE);
        Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);
        for (int i = 0; listOfFiles != null && i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                ChannelFuture cf = NetUtils.getInstance(Constants.STORAGENODE)
                        .connect(bootstrap, destinationIp, destinationPort);
                File currFile = listOfFiles[i];
                String fileNameInSystem = currFile.getName();
                logger.info("File " + fileNameInSystem);
                String fileName = fileNameInSystem.substring(0, fileNameInSystem.lastIndexOf("_"));
                int chunkId = Integer.parseInt(fileNameInSystem
                        .substring(fileNameInSystem.lastIndexOf("_") + 1));
                byte[] chunkData = Utils
                        .readFromFile(currFile.getPath(), 0, (int) currFile.length(), true);
                StorageMessages.StoreChunk storeChunkMsg = StorageMessages.StoreChunk.newBuilder()
                        .setFileName(fileName).setChunkId(chunkId).setChunkSize(currFile.length())
                        .setData(ByteString.copyFrom(chunkData))
                        .setChecksum(Utils.getMd5(chunkData)).setPrimarySnId(snId).build();
                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder().setStoreChunk(storeChunkMsg).build();
                cf.channel().writeAndFlush(msgWrapper).syncUninterruptibly();
            } else if (listOfFiles[i].isDirectory()) {
                logger.info("Directory " + listOfFiles[i].getName());
            }
        }
        workerGroup.shutdownGracefully();
    };

    private void replicateChunk(StorageMessages.StoreChunk storeChunkMsg) {
        int mySnId = DfsStorageNodeStarter.getInstance().getStorageNode().getSnId();
        /**
         * TODO:
         * I assumed that I have a list of SNs in here.
         */
        List<StorageMessages.StorageNodeInfo> oldSnList = storeChunkMsg.getSnInfoList();
        if (oldSnList != null && oldSnList.size() > 0) {
            List<StorageMessages.StorageNodeInfo> newSnList = new ArrayList<StorageMessages.StorageNodeInfo>();
            for (StorageNodeInfo storageNodeInfo : oldSnList) {
                if (storageNodeInfo.getSnId() != mySnId) {
                    newSnList.add(storageNodeInfo);
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("My SnId:" + mySnId + " is deleted from replica SN list for chunkId:"
                        + storeChunkMsg.getChunkId() + " New Sn List size:" + newSnList.size());
            }
            if (newSnList != null && newSnList.size() > 0) {

                /**
                 * TODO : primary SnId : How to know this in here??
                 */
                StorageMessages.StorageNodeInfo nextSnNode = newSnList.get(0);
                //Send chunk to SN
                EventLoopGroup workerGroup = new NioEventLoopGroup();
                MessagePipeline pipeline = new MessagePipeline(Constants.STORAGENODE);
                Bootstrap bootstrap = new Bootstrap().group(workerGroup)
                        .channel(NioSocketChannel.class).option(ChannelOption.SO_KEEPALIVE, true)
                        .handler(pipeline);
                ChannelFuture cf = NetUtils.getInstance(Constants.STORAGENODE)
                        .connect(bootstrap, nextSnNode.getSnIp(), nextSnNode.getSnPort());
                //                StorageMessages.ReplicaRequest replicaRequest = StorageMessages.ReplicaRequest
                //                        .newBuilder().setStoreChunk(storeChunkMsg).setPrimarySnId(mySnId).build();
                //                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                //                        .newBuilder().setReplicaRequest(replicaRequest).build();

                StorageMessages.StoreChunk.Builder storeChunkMsgBuilder = StorageMessages.StoreChunk
                        .newBuilder().setFileName(storeChunkMsg.getFileName())
                        .setPrimarySnId(storeChunkMsg.getPrimarySnId())
                        .setChunkId(storeChunkMsg.getChunkId()).setData(storeChunkMsg.getData())
                        .setChecksum(storeChunkMsg.getChecksum());
                for (int i = 1; i < newSnList.size(); i++) {
                    storeChunkMsgBuilder = storeChunkMsgBuilder.addSnInfo(newSnList.get(i));
                }
                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder().setStoreChunk(storeChunkMsgBuilder).build();

                Channel chan = cf.channel();
                chan.write(msgWrapper);
                chan.flush().closeFuture().syncUninterruptibly();
                logger.info("[SN] ---------->>>>>>>> REPLICA To SN, snId[" + nextSnNode.getSnId()
                        + "] ,  snIp:[" + nextSnNode.getSnIp() + "] , snPort:["
                        + nextSnNode.getSnPort() + "] >>>>>>>>>>>--------------");

                workerGroup.shutdownGracefully();
            }
        }
    }

    private void handleBackupRequest(ChannelHandlerContext ctx, StorageMessages.BackUp backUpMsg) {
        int mySnId = DfsStorageNodeStarter.getInstance().getStorageNode().getSnId();
        System.out.printf("[SN" + mySnId + "]Send data of %s to backup node port %s!\n",
                          backUpMsg.getSourceSnId(),
                          backUpMsg.getDestinationPort());
        String destinationIp = backUpMsg.getDestinationIp();
        int destinationPort = backUpMsg.getDestinationPort();
        int sourceId = backUpMsg.getSourceSnId();
        sendAllFileInFileSystemByNodeId(sourceId, destinationIp, destinationPort);
    }

    private void handleFileRetrieve(ChannelHandlerContext ctx,
                                    StorageMessages.StorageMessageWrapper msg) {
        DfsStorageNodeStarter.getInstance().getStorageNode().incrementTotalRetrievelRequest();
        StorageMessages.RetrieveFile retrieveFile = msg.getRetrieveFile();
        int chunkId = retrieveFile.getChunkId();
        int mySnId = DfsStorageNodeStarter.getInstance().getStorageNode().getSnId();
        /**
         * Retrieve chunk from File System.
         */
        String key = retrieveFile.getFileName() + "_" + chunkId;
        MetaDataOfChunk metaDataOfChunk = DfsStorageNodeStarter.getInstance()
                .getFileChunkToMetaDataMap().get(key);
        if (metaDataOfChunk != null) {
            logger.info("[SN" + mySnId + "] Retrieve File from Path:" + metaDataOfChunk.getPath());
            logger.info("[SN" + mySnId + "] Receive chunk size: " + metaDataOfChunk.getChunksize());
            /**
             * TODO: Actually we don't need RandomAccessFile to read chunk. Think about it!
             */
            byte[] chunkByteArray = Utils.readFromFile(metaDataOfChunk.getPath()
                    + File.separator + metaDataOfChunk.getFileName() + "_"
                    + metaDataOfChunk.getChunkId(), 0, metaDataOfChunk.getChunksize(), true);
            ByteString data = ByteString.copyFrom(chunkByteArray);
            //logger.info("[SN] Test.Data:" + new String(chunkByteArray));
            String snReadChecksum = Utils.getMd5(chunkByteArray);
            String snWriteChecksum = metaDataOfChunk.getChecksum();
            logger.info("[SN" + mySnId + "]Receive checksum: " + snWriteChecksum);
            logger.info("[SN" + mySnId + "]dataChecksum: " + snReadChecksum);

            if (snReadChecksum.equalsIgnoreCase(snWriteChecksum)) {
                logger.debug("[SN" + mySnId + "] Checksum TEST OK! for chunkId:"
                        + retrieveFile.getChunkId());
            } else {
                logger.error("[SN" + mySnId + "] PROBLEM with Checksum! for chunkId: "
                        + retrieveFile.getChunkId());
                logger.error("[SN" + mySnId + "] ChunkId: " + retrieveFile.getChunkId()
                        + " must be Healed by any other SN.");

                /**
                * 1-) Send Error message to Client for this chunkId!
                * 2-) Delete existing chunkId from file system and remove from hashmap.
                * 3-) Send HealMyChunk(snId,filename,chunkId) message to Controller!
                */
                handleHealMyMissingChunk(ctx, retrieveFile.getFileName(), chunkId, mySnId);

            }
            StorageMessages.RetrieveFileResponse response = StorageMessages.RetrieveFileResponse
                    .newBuilder().setChunkId(chunkId).setFileName(retrieveFile.getFileName())
                    .setData(data).setSnId(mySnId).setResult(true).build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                    .newBuilder().setRetrieveFileResponse(response).build();
            Channel chan = ctx.channel();
            ChannelFuture write = chan.write(msgWrapper);
            chan.flush();
            write.addListener(ChannelFutureListener.CLOSE);
        } else {
            System.out.println("[SN" + mySnId + "] MetaDataOfChunk is NULL! for chunkId:" + chunkId
                    + " in snId:" + mySnId + "Chunk is not in this SN!");
        }
    }

    /**
     * 1-) Send Error message to Client for this chunkId!
     * 2-) Delete existing chunkId from file system and remove from hashmap.
     * 3-) Send RecoverChunk(snId,filename,chunkId) message to Controller! 
     * 
     * @param ctx
     * @param chunkId
     * @param mySnId
     */
    private void handleHealMyMissingChunk(ChannelHandlerContext ctx, String fileName, int chunkId,
                                          int mySnId) {

        /**
         * 1-) Send Error message to Client for this chunkId! 
         */
        StorageMessages.RetrieveFileResponse response = StorageMessages.RetrieveFileResponse
                .newBuilder().setChunkId(chunkId).setFileName(fileName).setSnId(mySnId)
                .setResult(false).build();
        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder().setRetrieveFileResponse(response).build();
        Channel chan = ctx.channel();
        ChannelFuture write = chan.write(msgWrapper);
        chan.flush();
        write.addListener(ChannelFutureListener.CLOSE);

        /**
         * 2-)  Delete existing chunkId from file system and remove from hashmap.
         */

        String key = fileName + "_" + chunkId;
        MetaDataOfChunk metaDataOfChunk = DfsStorageNodeStarter.getInstance()
                .getFileChunkToMetaDataMap().get(key);
        if (metaDataOfChunk == null) {
            logger.error("Already deleted!");
        }
        DfsStorageNodeStarter.getInstance().getFileChunkToMetaDataMap().remove(key);

        if (logger.isDebugEnabled()) {
            logger.debug("[SN" + mySnId + "] ChunkId :" + chunkId
                    + " is deleted from FileChunkToMetaDataMap.");
        }

        /**
         * 3-) Send RecoverChunk(snId,filename,chunkId) message to Controller! 
         */
        StorageMessages.HealMyChunk healMyChunk = StorageMessages.HealMyChunk.newBuilder()
                .setHealSnId(mySnId)
                .setHealSnIp(DfsStorageNodeStarter.getInstance().getStorageNode().getSnIp())
                .setHealSnPort(DfsStorageNodeStarter.getInstance().getStorageNode().getSnPort())
                .setChunkId(chunkId).setFileName(fileName).build();
        msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder().setHealMyChunk(healMyChunk)
                .build();

        /**
         * We created a new channel but we may use existing one which we are using for heartbeat. 
         */
        String controllerIp = ConfigurationManagerSn.getInstance().getControllerIp();
        int controllerPort = ConfigurationManagerSn.getInstance().getControllerPort();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(Constants.CLIENT);
        Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);
        ChannelFuture cf = NetUtils.getInstance(Constants.STORAGENODE)
                .connect(bootstrap,
                         ConfigurationManagerSn.getInstance().getControllerIp(),
                         ConfigurationManagerSn.getInstance().getControllerPort());
        Channel controllerChannel = cf.channel();
        controllerChannel.write(msgWrapper);
        controllerChannel.flush();

        logger.info("[SN" + mySnId
                + "] ---------->>>>>>>> HEAL MY CHUCK MESSAGE To Controller, chunkId[" + chunkId
                + "], controllerIp[" + controllerIp + "], controllerPort:[" + controllerPort
                + "] >>>>>>>>>>>--------------");

    }

    private void helpOtherSnToHealChunk(ChannelHandlerContext ctx, int mySnId,
                                        StorageMessages.HealMyChunk healMyChunk) {

        int healChunkId = healMyChunk.getHealSnId();
        String healFileName = healMyChunk.getFileName();
        /**
         * Retrieve chunk from File System.
         */
        String key = healFileName + "_" + healChunkId;
        MetaDataOfChunk metaDataOfChunk = DfsStorageNodeStarter.getInstance()
                .getFileChunkToMetaDataMap().get(key);
        if (metaDataOfChunk != null) {
            logger.info("[SN" + mySnId + "] Retrieve File from Path:" + metaDataOfChunk.getPath());
            logger.info("[SN" + mySnId + "] Receive chunk size: " + metaDataOfChunk.getChunksize());

            byte[] chunkByteArray = Utils.readFromFile(metaDataOfChunk.getPath()
                    + File.separator + metaDataOfChunk.getFileName() + "_"
                    + metaDataOfChunk.getChunkId(), 0, metaDataOfChunk.getChunksize(), true);
            ByteString data = ByteString.copyFrom(chunkByteArray);
            String snReadChecksum = Utils.getMd5(chunkByteArray);
            String snWriteChecksum = metaDataOfChunk.getChecksum();
            logger.info("[SN" + mySnId + "]Receive checksum: " + snWriteChecksum);
            logger.info("[SN" + mySnId + "]dataChecksum: " + snReadChecksum);

            if (snReadChecksum.equalsIgnoreCase(snWriteChecksum)) {
                logger.debug("[SN" + mySnId + "] Checksum TEST OK! for chunkId:" + healChunkId);

                /**
                 * Send StoreChunkMessage to other SN...
                 * TODO:Heal..
                 */

                int primarySnId = Utils.parsePathToGetPrimarySnId(metaDataOfChunk.getPath());

                logger.debug("[SN" + mySnId + "] primarySnId: " + primarySnId);

                StorageMessages.StoreChunk.Builder storeChunkMsgBuilder = StorageMessages.StoreChunk
                        .newBuilder().setFileName(healFileName).setPrimarySnId(primarySnId) //?? get from hashmap
                        .setChunkId(healChunkId).setData(data).setChecksum(getMd5(chunkByteArray));
                StorageMessages.StorageNodeInfo healedSnInfo = StorageMessages.StorageNodeInfo
                        .newBuilder().setSnIp(healMyChunk.getHealSnIp())
                        .setSnPort(healMyChunk.getHealSnPort()).build();
                storeChunkMsgBuilder = storeChunkMsgBuilder.addSnInfo(healedSnInfo);

                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder().setStoreChunk(storeChunkMsgBuilder).build();

                EventLoopGroup workerGroup = new NioEventLoopGroup();
                MessagePipeline pipeline = new MessagePipeline(Constants.CLIENT);
                Bootstrap bootstrap = new Bootstrap().group(workerGroup)
                        .channel(NioSocketChannel.class).option(ChannelOption.SO_KEEPALIVE, true)
                        .handler(pipeline);
                ChannelFuture cf = NetUtils.getInstance(Constants.STORAGENODE)
                        .connect(bootstrap,
                                 ConfigurationManagerSn.getInstance().getControllerIp(),
                                 ConfigurationManagerSn.getInstance().getControllerPort());
                Channel controllerChannel = cf.channel();
                controllerChannel.write(msgWrapper);
                controllerChannel.flush();

                logger.info("[SN" + mySnId + "] ---------->>>>>>>> STORE CHUNK MESSAGE(HEAL) To SN["
                        + healedSnInfo.getSnId() + "], chunkId[" + healChunkId + "], controllerIp["
                        + healMyChunk.getHealSnIp() + "], controllerPort:["
                        + healMyChunk.getHealSnPort() + "] >>>>>>>>>>>--------------");

            } else {
                logger.error("[SN" + mySnId + "] PROBLEM with Checksum! for chunkId: "
                        + healChunkId);
                logger.error("[SN" + mySnId + "] ChunkId: " + healChunkId
                        + " must be Healed by any other SN.");
                /**
                * 1-) Send Error message to Client for this chunkId!
                * 2-) Delete existing chunkId from file system and remove from hashmap.
                * 3-) Send RecoverChunk(snId,filename,chunkId) message to Controller!
                */
                handleHealMyMissingChunk(ctx, healFileName, healChunkId, mySnId);
            }
        }
    }

    private void handleDeleteBackUpMsg(StorageMessages.DeleteBackUp deleteBackUpMsg) {
        List<Integer> ids = deleteBackUpMsg.getListSnIdList();
        logger.info("Working Directory = " + System.getProperty("user.dir"));
        String directoryPath = ConfigurationManagerSn.getInstance().getStoreLocation();
        String whoamI = System.getProperty("user.name");
        directoryPath = System.getProperty("user.dir") + File.separator + directoryPath
                + File.separator + whoamI;

        logger.info("Path:" + directoryPath);
        for (int id : ids) {
            String pathById = directoryPath + File.separator + id;
            System.out.println("Delete path: " + pathById);
            File directory = new File(directoryPath);
            Utils.deleteDirectory(directory);
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper msg) {
        int mySnId = DfsStorageNodeStarter.getInstance().getStorageNode().getSnId();
        logger.info("[SN" + mySnId + "]Received msg!");
        if (msg.hasStoreChunk()) {
            handleStoreChunkMsg(ctx, msg.getStoreChunk());
        } else if (msg.hasHeartBeatResponse()) {
            StorageMessages.HeartBeatResponse heartBeatResponse = msg.getHeartBeatResponse();
            DfsStorageNodeStarter.getInstance().getStorageNode()
                    .setSnId(heartBeatResponse.getSnId());
            logger.info("[SN" + mySnId
                    + "] Heart Beat Response came from Controller... from me. SN-Id:"
                    + heartBeatResponse.getSnId() + ", status:" + heartBeatResponse.getStatus());
            if (heartBeatResponse.getStatus() && DfsStorageNodeStarter.getInstance()
                    .getHeartBeatSenderTimerHandle() == null) {
                logger.info("[SN" + mySnId + "] My SnId set to :" + heartBeatResponse.getSnId()
                        + " by Controller. Saving it...");
                DfsStorageNodeStarter.getInstance().getStorageNode()
                        .setSnId(heartBeatResponse.getSnId());
                logger.info("[SN" + mySnId + "] Creating Timer for Heart Beats:"
                        + heartBeatResponse.getSnId());
                TimerManager.getInstance().scheduleHeartBeatTimer();
            } else if (heartBeatResponse.getStatus() == false) {
                TimerManager.getInstance()
                        .cancelHeartBeatTimer(DfsStorageNodeStarter.getInstance());
            }
        } else if (msg.hasRetrieveFile()) {
            System.out.println("[SN" + mySnId
                    + "] Retrieve File Request came from Client with fileName: "
                    + msg.getRetrieveFile().getFileName() + " - chunkId: "
                    + msg.getRetrieveFile().getChunkId());
            handleFileRetrieve(ctx, msg);
        } else if (msg.hasStoreChunkResponse()) {

        } else if (msg.hasBackup()) {
            handleBackupRequest(ctx, msg.getBackup());
        } else if (msg.hasHealMyChunk()) {
            StorageMessages.HealMyChunk healMyChunk = msg.getHealMyChunk();
            /**
             * Healing Feature
             */
            logger.info("[SN" + mySnId
                    + "] ----------<<<<<<<<<< HEAL MY CHUNK MESSAGE From Controller. Heal SN["
                    + healMyChunk.getHealSnId() + "] FileName:[" + healMyChunk.getFileName()
                    + "] MissingChunkId:[" + healMyChunk.getChunkId()
                    + "]<<<<<<<<<<<<<<----------------");

            helpOtherSnToHealChunk(ctx, mySnId, healMyChunk);
        } else if (msg.hasDeleteBackUp()) {
            handleDeleteBackUpMsg(msg.getDeleteBackUp());
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
