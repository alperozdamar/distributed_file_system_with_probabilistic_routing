package edu.usfca.cs.dfs.net;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.DfsStorageNodeStarter;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.StorageMessages.StorageNodeInfo;
import edu.usfca.cs.dfs.timer.TimerManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

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

        System.out.println("[SN]This is Store Chunk Message...");
        System.out.println("[SN]Storing file name: " + storeChunkMsg.getFileName());
        System.out.println("[SN]Storing chunk Id: " + storeChunkMsg.getChunkId());
        System.out.println("[SN]Storing data: " + storeChunkMsg.getData().toString());

        //Response to client, first node only
        System.out.println("[SN]Send back message");

        StorageMessages.StoreChunkResponse responseMsg = StorageMessages.StoreChunkResponse
                .newBuilder().setChunkId(storeChunkMsg.getChunkId()).setStatus(true).build();

        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder().setStoreChunkResponse(responseMsg).build();
        Channel chan = ctx.channel();
        ChannelFuture write = chan.write(msgWrapper);
        chan.flush();
        write.addListener(ChannelFutureListener.CLOSE);

        DfsStorageNodeStarter.getInstance().getStorageNode().incrementTotalStorageRequest();

        /**
         * TODO: 
         * Replication.
         * Send this chunk to the other 2 replicas.
         */
        replicateChunk(storeChunkMsg);

    }

    public void replicateChunk(StorageMessages.StoreChunk storeChunkMsg) {
        int mySnId = DfsStorageNodeStarter.getInstance().getStorageNode().getSnId();

        /**
         * TODO:
         * I assumed that I have a list of SNs in here.
         */
        List<StorageMessages.StorageNodeInfo> snList = storeChunkMsg.getSnInfoList();

        /**
         * Delete for SN from this list.
         */
        int deletedIndex = 0;
        for (Iterator iterator = snList.iterator(); iterator.hasNext();) {
            StorageNodeInfo storageNodeInfo = (StorageNodeInfo) iterator.next();
            deletedIndex++;
            if (storageNodeInfo.getSnId() == mySnId) {
                break;
            }
        }
        StorageNodeInfo deletedSN = snList.remove(deletedIndex);
        if (logger.isDebugEnabled()) {
            logger.debug("SN:" + deletedSN.getSnId()
                    + " is deleted from replica SN list for chunkId:" + storeChunkMsg.getChunkId());
        }

        StorageMessages.ReplicaRequest replicaRequest = StorageMessages.ReplicaRequest.newBuilder()
                .setStoreChunk(storeChunkMsg).setPrimarySnId(mySnId).build();

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
