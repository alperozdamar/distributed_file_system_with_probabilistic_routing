package edu.usfca.cs.dfs.net;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;

import edu.usfca.cs.dfs.DfsClientStarter;
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

import static edu.usfca.cs.Utils.readFromFile;

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
        List<StorageMessages.StorageNodeInfo> listSNs = chunkLocationMsg.getSnInfoList();
        StorageMessages.StorageNodeInfo firstNode = listSNs.get(0);
        //Send chunk to SN
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(Constants.CLIENT);

        Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);
        ChannelFuture cf = Utils
                .connect(bootstrap,
                        firstNode.getSnIp(),
                        firstNode.getSnPort());

        //Read chunk from file base on chunkId
        int configChunkSize = (int) ConfigurationManagerClient.getInstance().getChunkSizeInBytes();
        byte[] chunk = null;
        try {
            chunk = readFromFile(DfsClientStarter.getInstance().getFileInfo(),
                    configChunkSize*chunkLocationMsg.getChunkId()-1,
                    chunkLocationMsg.getChunkSize());
        } catch (IOException e) {
            e.printStackTrace();
        }

        ByteString data = ByteString.copyFrom(chunk);
        StorageMessages.StoreChunk.Builder storeChunkMsgBuilder = StorageMessages.StoreChunk.newBuilder()
                .setFileName(chunkLocationMsg.getFileName())
                .setChunkId(chunkLocationMsg.getChunkId())
                .setData(data);
        for(int i=1;i<listSNs.size();i++){
            storeChunkMsgBuilder = storeChunkMsgBuilder.addSnInfo(listSNs.get(i));
        }
        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder().setStoreChunkMsg(storeChunkMsgBuilder).build();
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
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
