package edu.usfca.cs.dfs.net;

import static edu.usfca.cs.Utils.getMd5;
import static edu.usfca.cs.Utils.readFromFile;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.ByteString;

import edu.usfca.cs.Utils;
import edu.usfca.cs.dfs.DfsClientStarter;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.StorageMessages.StorageNodeInfo;
import edu.usfca.cs.dfs.config.ConfigurationManagerClient;
import edu.usfca.cs.dfs.config.Constants;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

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

    private void handleStoreChunkLocationMsg(ChannelHandlerContext ctx,
                                             StorageMessages.StoreChunkLocation chunkLocationMsg) {
        System.out.println("[Client]This is Store Chunk Location Message...");
        List<StorageMessages.StorageNodeInfo> listSNs = chunkLocationMsg.getSnInfoList();
        StorageMessages.StorageNodeInfo firstNode = listSNs.get(0);
        //Send chunk to SN
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(Constants.CLIENT);

        Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);
        ChannelFuture cf = Utils.connect(bootstrap, firstNode.getSnIp(), firstNode.getSnPort());

        //Read chunk from file base on chunkId
        long configChunkSize = ConfigurationManagerClient.getInstance().getChunkSizeInBytes();
        byte[] chunk = null;
        if (chunkLocationMsg.getChunkId() == 0) {//metadata chunk
            chunk = DfsClientStarter.getInstance().getMetadata().toByteArray();
        } else {
            chunk = readFromFile(DfsClientStarter.getInstance().getFileInfo(),
                    (int) (configChunkSize * (chunkLocationMsg.getChunkId() - 1)),
                    (int) chunkLocationMsg.getChunkSize());
        }
        System.out.println("[Client] Primary SN Id is: " + chunkLocationMsg.getPrimarySnId());
        System.out.println("[Client] Chunk checksum: " + getMd5(chunk));
        ByteString data = ByteString.copyFrom(chunk);
        StorageMessages.StoreChunk.Builder storeChunkMsgBuilder = StorageMessages.StoreChunk
                .newBuilder()
                .setFileName(chunkLocationMsg.getFileName())
                .setPrimarySnId(chunkLocationMsg.getPrimarySnId())
                .setChunkId(chunkLocationMsg.getChunkId())
                .setData(data)
                .setChecksum(getMd5(chunk));
        for (int i = 1; i < listSNs.size(); i++) {
            storeChunkMsgBuilder = storeChunkMsgBuilder.addSnInfo(listSNs.get(i));
        }
        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder().setStoreChunk(storeChunkMsgBuilder).build();
        Channel chan = cf.channel();
        chan.write(msgWrapper);
        chan.flush().closeFuture().syncUninterruptibly();
    }

    private void handleStoreChunkResponseMsg(ChannelHandlerContext ctx,
                                             StorageMessages.StoreChunkResponse storeChunkResponseMsg) {
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

    private void handleFileLocationMsg(ChannelHandlerContext ctx,
                                       StorageMessages.FileLocation fileLocationMsg) {
        System.out.println("[Client]This is File Location Message Response...");
        if (!fileLocationMsg.getStatus()) {
            System.out.println("[Client]File not found!!!");
            return;
        }
        String fileName = fileLocationMsg.getFileName();
        List<StorageMessages.StoreChunkLocation> chunksLocations = fileLocationMsg
                .getChunksLocationList();
        for (StorageMessages.StoreChunkLocation chunkLocation : chunksLocations) {
            System.out.println("[Client]Chunk:" + chunkLocation.getChunkId());
            for (StorageNodeInfo sn : chunkLocation.getSnInfoList()) {
                System.out.printf("[Client]SN Ip: %s - Port: %d\n", sn.getSnIp(), sn.getSnPort());
            }
        }
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
        } else if (msg.hasFileLocation()) {
            StorageMessages.FileLocation fileLocationMsg = msg.getFileLocation();
            handleFileLocationMsg(ctx, fileLocationMsg);
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
