package edu.usfca.cs.dfs.net;

import java.net.InetSocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.DfsStorageNodeStarter;
import edu.usfca.cs.dfs.StorageMessages;
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

    private void handleStoreChunkMsg(ChannelHandlerContext ctx, StorageMessages.StoreChunk storeChunkMsg){

        System.out.println("[SN]This is Store Chunk Message...");
        System.out.println("[SN]Storing file name: " + storeChunkMsg.getFileName());
        System.out.println("[SN]Storing chunk Id: " + storeChunkMsg.getChunkId());

        //Response to client, first node only
        System.out.println("[SN]Send back message");

        StorageMessages.StoreChunkResponse responseMsg = StorageMessages.StoreChunkResponse.newBuilder()
                .setChunkId(storeChunkMsg.getChunkId()).setStatus(true).build();

        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder().setStoreChunkResponse(responseMsg).build();
        Channel chan = ctx.channel();
        ChannelFuture write = chan.write(msgWrapper);
        chan.flush();
        write.addListener(ChannelFutureListener.CLOSE);

        DfsStorageNodeStarter.getInstance().getStorageNode().incrementTotalStorageRequest();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper msg) {
        System.out.println("[SN]Received sth!");
        if (msg.hasStoreChunkMsg()) {
            handleStoreChunkMsg(ctx, msg.getStoreChunkMsg());
        } else if (msg.hasHeartBeatResponse()) {
            StorageMessages.HeartBeatResponse heartBeatResponse = msg.getHeartBeatResponse();
            System.out.println("[SN] Heart Beat Response came from Controller... from me. SN-Id:"
                    + heartBeatResponse.getSnId() + ", status:" + heartBeatResponse.getStatus());

            if (heartBeatResponse.getStatus()) {
                // System.out.println("[SN] Creating Timer for Heart Beats:"
                //         + heartBeatResponse.getSnId());
                // TimerManager.getInstance().scheduleHeartBeatTimer(ConfigurationManagerSn
                //         .getInstance().getHeartBeatPeriodInMilliseconds());

            } else {
                TimerManager.getInstance()
                        .cancelHeartBeatTimer(DfsStorageNodeStarter.getInstance());
            }

        } else if (msg.hasRetrieveFileMsg()) {

            DfsStorageNodeStarter.getInstance().getStorageNode().incrementTotalRetrievelRequest();

        } else if (msg.hasStoreChunkResponse()) {

        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        System.out.println("[SN]Flush ctx");
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
