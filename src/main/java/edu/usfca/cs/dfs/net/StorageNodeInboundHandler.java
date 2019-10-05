package edu.usfca.cs.dfs.net;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.Utils;
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
        System.out.println("[SN]Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("[SN]Connection lost: " + addr);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        /* Writable status of the channel changed */
    }

    private void handleStoreChunkMsg(ChannelHandlerContext ctx,
                                     StorageMessages.StoreChunk storeChunkMsg) {
        DfsStorageNodeStarter.getInstance().getStorageNode().incrementTotalStorageRequest();

        System.out.println("[SN] ----------<<<<<<<<<< STORE CHUNK , FileName["
                + storeChunkMsg.getFileName() + "] chunkId:[" + storeChunkMsg.getChunkId()
                + "] Data:[" + storeChunkMsg.getData().toString()
                + "]<<<<<<<<<<<<<<----------------");

        //Response to client, first node only
        System.out.println("[SN]Sending StoreChunk response message back to Client...");

        /**
         * TODO:
         * 
         * if successfully write into File System return sucess respnse
         */
        boolean result = writeIntoFileSystem(storeChunkMsg);

        if (result) {
            result = true;
        }

        StorageMessages.StoreChunkResponse responseMsg = StorageMessages.StoreChunkResponse
                .newBuilder().setChunkId(storeChunkMsg.getChunkId()).setStatus(result).build();

        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder().setStoreChunkResponse(responseMsg).build();
        Channel chan = ctx.channel();
        ChannelFuture write = chan.write(msgWrapper);
        chan.flush();
        write.addListener(ChannelFutureListener.CLOSE);

        System.out.println("[SN] ---------->>>>>>>> STORE CHUNK RESPONSE For ChunkId"
                + storeChunkMsg.getChunkId() + "] >>>>>>>>>>>--------------");

        /**
         * TODO: 
         * Replication.
         * Send this chunk to the other 2 replicas.
         */
        replicateChunk(storeChunkMsg);
    }

    private boolean writeIntoFileSystem(StorageMessages.StoreChunk storeChunkMsg) {
        boolean result = false;
        try {

            /**
             * Write chunk into File System.
             * 
             * bigdata/whoamI/primaryId/ data
             * 
             */
            String directoryPath = ConfigurationManagerSn.getInstance().getStoreLocation();
            Process whoamI;

            whoamI = Runtime.getRuntime().exec("whoami");

            directoryPath = directoryPath + File.pathSeparator + whoamI + File.separator
                    + storeChunkMsg.getPrimarySnId();

            File directory = new File(directoryPath);
            if (!new File(directoryPath).exists()) {
                System.out.print("No Folder");
                directory.mkdir();
                System.out.print("Folder created");
            } else {
                System.out.print("Folder already exists");
            }

            /**
             * 2-) Save into file
             */

            /**
             * 3-) Put into HASHMAP => key:(filename_chunkId) Value: (FileLocation,Checksum,FileName,ChunkId) 
             *      Also put into File System. (/bigdata/whoamI/.)
             *      
             *     
             */

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

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
                MessagePipeline pipeline = new MessagePipeline(Constants.CLIENT);
                Bootstrap bootstrap = new Bootstrap().group(workerGroup)
                        .channel(NioSocketChannel.class).option(ChannelOption.SO_KEEPALIVE, true)
                        .handler(pipeline);
                ChannelFuture cf = Utils
                        .connect(bootstrap, nextSnNode.getSnIp(), nextSnNode.getSnPort());
                //                StorageMessages.ReplicaRequest replicaRequest = StorageMessages.ReplicaRequest
                //                        .newBuilder().setStoreChunk(storeChunkMsg).setPrimarySnId(mySnId).build();
                //                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                //                        .newBuilder().setReplicaRequest(replicaRequest).build();

                StorageMessages.StoreChunk.Builder storeChunkMsgBuilder = StorageMessages.StoreChunk
                        .newBuilder().setFileName(storeChunkMsg.getFileName())
                        .setChunkId(storeChunkMsg.getChunkId()).setData(storeChunkMsg.getData());
                for (int i = 1; i < newSnList.size(); i++) {
                    storeChunkMsgBuilder = storeChunkMsgBuilder.addSnInfo(newSnList.get(i));
                }
                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder().setStoreChunk(storeChunkMsgBuilder).build();

                Channel chan = cf.channel();
                chan.write(msgWrapper);
                chan.flush().closeFuture().syncUninterruptibly();
                System.out.println("[SN] ---------->>>>>>>> REPLICA To SN, snId["
                        + nextSnNode.getSnId() + "] ,  snIp:[" + nextSnNode.getSnIp()
                        + "] , snPort:[" + nextSnNode.getSnPort() + "] >>>>>>>>>>>--------------");
            }
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper msg) {
        System.out.println("[SN]Received msg!");
        if (msg.hasStoreChunk()) {
            handleStoreChunkMsg(ctx, msg.getStoreChunk());
        } else if (msg.hasHeartBeatResponse()) {
            StorageMessages.HeartBeatResponse heartBeatResponse = msg.getHeartBeatResponse();
            DfsStorageNodeStarter.getInstance().getStorageNode()
                    .setSnId(heartBeatResponse.getSnId());
            System.out.println("[SN] Heart Beat Response came from Controller... from me. SN-Id:"
                    + heartBeatResponse.getSnId() + ", status:" + heartBeatResponse.getStatus());
            if (heartBeatResponse.getStatus() && DfsStorageNodeStarter.getInstance()
                    .getHeartBeatSenderTimerHandle() == null) {
                System.out.println("[SN] My SnId set to :" + heartBeatResponse.getSnId()
                        + " by Controller. Saving it...");
                DfsStorageNodeStarter.getInstance().getStorageNode()
                        .setSnId(heartBeatResponse.getSnId());
                System.out.println("[SN] Creating Timer for Heart Beats:"
                        + heartBeatResponse.getSnId());
                TimerManager.getInstance().scheduleHeartBeatTimer();
            } else if (heartBeatResponse.getStatus() == false) {
                TimerManager.getInstance()
                        .cancelHeartBeatTimer(DfsStorageNodeStarter.getInstance());
            }
        } else if (msg.hasRetrieveFile()) {

            DfsStorageNodeStarter.getInstance().getStorageNode().incrementTotalRetrievelRequest();

            /**
             * TODO:
             * Retrieve chunk from File System.
             */

        } else if (msg.hasStoreChunkResponse()) {

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
