package edu.usfca.cs.dfs.net;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.Utils;
import edu.usfca.cs.dfs.DfsStorageNodeStarter;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.StorageMessages.StorageNodeInfo;
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

        StorageMessages.StoreChunkResponse responseMsg = StorageMessages.StoreChunkResponse
                .newBuilder().setChunkId(storeChunkMsg.getChunkId()).setStatus(true).build();

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
