package edu.usfca.cs.dfs.net;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;

import edu.usfca.cs.dfs.config.ConfigurationManagerClient;
import edu.usfca.cs.dfs.config.Constants;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.ByteString;

import edu.usfca.cs.Utils;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.StorageMessages.StorageNodeInfo;

@ChannelHandler.Sharable
public class ClientInboundHandler extends InboundHandler {

    private static Logger logger = LogManager.getLogger(ClientInboundHandler.class);

    public ClientInboundHandler() {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection lost: " + addr);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        /* Writable status of the channel changed */
    }

    private void handleStoreChunkLocationMsg(ChannelHandlerContext ctx, StorageMessages.StoreChunkLocation chunkLocationMsg){
        System.out.println("[Client]This is Store Chunk Location Message...");
        for (StorageMessages.StorageNodeInfo sn : chunkLocationMsg.getSnInfoList()) {
            System.out.printf("[Client]IP : %s - Port: %d\n", sn.getSnIp(), sn.getSnPort());
        }
        //Send chunk to SN
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(Constants.CLIENT);

        Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);
        ChannelFuture cf = Utils
                .connect(bootstrap,
                        "localhost",
                        8001);

        ByteString data = ByteString.copyFromUtf8("Hello World!");
        StorageMessages.StoreChunk storeChunkMsg = StorageMessages.StoreChunk.newBuilder()
                .setFileName("Test Chunk").setChunkId(88).setData(data).build();
        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder().setStoreChunkMsg(storeChunkMsg).build();
        Channel chan = cf.channel();
        chan.write(msgWrapper);
        chan.flush().closeFuture().syncUninterruptibly();
    }

    private void handleStoreChunkResponseMsg(ChannelHandlerContext ctx, StorageMessages.StoreChunkResponse storeChunkResponseMsg){
        /**
         * TODO:
         * For now for testing...
         */
        System.out.println("[Client]This is Store Chunk Message Response...");

        if (storeChunkResponseMsg.getStatus()) {
            System.out.println("[Client] Chunk stored successfully, chunkId:"
                    + storeChunkResponseMsg.getChunkId());
        }
        System.out.println("[Client]  : " + storeChunkResponseMsg.getStatus());
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper msg) {
        Utils.printHeader("[Client]Received sth!");
        if (msg.hasStoreChunkLocation()) {
            handleStoreChunkLocationMsg(ctx, msg.getStoreChunkLocation());

        } else if (msg.hasStoreChunkResponse()) {
            handleStoreChunkResponseMsg(ctx, msg.getStoreChunkResponse());
        } else if (msg.hasListResponse()) {
            List<StorageMessages.StorageNodeInfo> snInfoList = msg.getListResponse()
                    .getSnInfoList();
            for (Iterator iterator = snInfoList.iterator(); iterator.hasNext();) {
                StorageNodeInfo storageNodeInfo = (StorageNodeInfo) iterator.next();
                System.out.println("[Client]Sn.id:" + storageNodeInfo.getSnId());
                System.out.println("[Client]Sn.ip:" + storageNodeInfo.getSnIp());
            }
        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        System.out.println("[Client]Flush ctx");
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
