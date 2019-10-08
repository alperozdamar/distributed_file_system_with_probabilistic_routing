package edu.usfca.cs.dfs.net;

import static edu.usfca.cs.Utils.getMd5;
import static edu.usfca.cs.Utils.readFromFile;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.netty.channel.*;
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
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

@ChannelHandler.Sharable
public class ClientInboundHandler extends InboundHandler {

    private static Logger logger = LogManager.getLogger(ClientInboundHandler.class);
    private final int numOfRetriveChunkThread = 3;
    DfsClientStarter dfsClientStarter = DfsClientStarter.getInstance();

    public ClientInboundHandler() {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        logger.info("Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
        InetSocketAddress localAaddr = (InetSocketAddress) ctx.channel().localAddress();
        logger.info("Connection lost: " + addr);
        NetUtils.getInstance(Constants.CLIENT).releasePort(localAaddr.getPort());
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        /* Writable status of the channel changed */
    }

    private void handleStoreChunkLocationMsg(ChannelHandlerContext ctx,
                                             StorageMessages.StoreChunkLocation chunkLocationMsg) {
        logger.info("[Client]This is Store Chunk Location Message...:"+chunkLocationMsg.getChunkId());
        List<StorageMessages.StorageNodeInfo> listSNs = chunkLocationMsg.getSnInfoList();
        StorageMessages.StorageNodeInfo firstNode = listSNs.get(0);
        //Send chunk to SN
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(Constants.CLIENT);

        Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);
        ChannelFuture cf = NetUtils.getInstance(Constants.CLIENT).connect(bootstrap, firstNode.getSnIp(), firstNode.getSnPort());

        //Read chunk from file base on chunkId
        long configChunkSize = ConfigurationManagerClient.getInstance().getChunkSizeInBytes();
        byte[] chunk = null;
        if (chunkLocationMsg.getChunkId() == 0) {//metadata chunk
            chunk = dfsClientStarter.getMetadata().toByteArray();
        } else {
            chunk = readFromFile(dfsClientStarter.getFileInfo(),
                                 (int) (configChunkSize * (chunkLocationMsg.getChunkId() - 1)),
                                 (int) chunkLocationMsg.getChunkSize(), false);
        }
        logger.info("[Client] Primary SN Id is: " + chunkLocationMsg.getPrimarySnId());
        logger.info("[Client] Chunk checksum: " + getMd5(chunk));
        ByteString data = ByteString.copyFrom(chunk);
        StorageMessages.StoreChunk.Builder storeChunkMsgBuilder = StorageMessages.StoreChunk
                .newBuilder().setFileName(chunkLocationMsg.getFileName())
                .setPrimarySnId(chunkLocationMsg.getPrimarySnId())
                .setChunkId(chunkLocationMsg.getChunkId()).setData(data).setChecksum(getMd5(chunk));
        for (int i = 1; i < listSNs.size(); i++) {
            storeChunkMsgBuilder = storeChunkMsgBuilder.addSnInfo(listSNs.get(i));
        }
        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder().setStoreChunk(storeChunkMsgBuilder).build();
        Channel chan = cf.channel();
        ChannelFuture write = chan.write(msgWrapper);
        chan.flush();
        write.syncUninterruptibly();
        dfsClientStarter.setNumOfSentChunk(dfsClientStarter.getNumOfSentChunk()+1);
        if(dfsClientStarter.getNumOfSentChunk()==dfsClientStarter.getMetadata().getNumOfChunks()){
            ctx.close();
        }
        workerGroup.shutdownGracefully();
    }

    private void handleStoreChunkResponseMsg(ChannelHandlerContext ctx,
                                             StorageMessages.StoreChunkResponse storeChunkResponseMsg) {
        /**
         * TODO:
         * For now for testing...
         */
        logger.info("[Client]This is Store Chunk Message Response...");

        if (storeChunkResponseMsg.getStatus()) {
            logger.info("[Client] Chunk stored successfully, chunkId:"
                    + storeChunkResponseMsg.getChunkId());
        }
        logger.info("[Client]  : " + storeChunkResponseMsg.getStatus());
    }

    private void handleFileLocationMsg(ChannelHandlerContext ctx,
                                       StorageMessages.FileLocation fileLocationMsg) {
        ExecutorService executorService = Executors.newFixedThreadPool(numOfRetriveChunkThread);
        logger.info("[Client]This is File Location Message Response...");
        dfsClientStarter.setRetrieveChunkIds(new HashSet<>());
        if (!fileLocationMsg.getStatus()) {
            logger.info("[Client]File not found!!!");
            return;
        }
        String fileName = fileLocationMsg.getFileName();
        List<StorageMessages.StoreChunkLocation> chunksLocations = fileLocationMsg
                .getChunksLocationList();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(Constants.CLIENT);

        Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);
        for (StorageMessages.StoreChunkLocation chunkLocation : chunksLocations) {
            logger.info("[Client]Chunk:" + chunkLocation.getChunkId());
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    sendRetrieveFileRequestToSN(bootstrap, fileName, chunkLocation);
                }
            });
        }
//        workerGroup.shutdownGracefully();
        executorService.shutdown();
        try {
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ctx.close();
    }

    private void sendRetrieveFileRequestToSN(Bootstrap bootstrap, String fileName,
                                             StorageMessages.StoreChunkLocation chunkLocation) {
        dfsClientStarter.getRetrieveChunkIds().add(chunkLocation.getChunkId());
        for (StorageNodeInfo sn : chunkLocation.getSnInfoList()) {
            logger.info("[Client]SN Ip: " + sn.getSnIp() + " - Port: " + sn.getSnPort());
            if(!dfsClientStarter.getRetrieveChunkIds().contains(chunkLocation.getChunkId())){
                break;
            }
            //Retrieve chunk from SN...
            ChannelFuture cf = NetUtils.getInstance(Constants.CLIENT).connect(bootstrap, sn.getSnIp(), sn.getSnPort());
            StorageMessages.RetrieveFile retrieveFileMsg = StorageMessages.RetrieveFile.newBuilder()
                    .setFileName(fileName).setChunkId(chunkLocation.getChunkId()).build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                    .newBuilder().setRetrieveFile(retrieveFileMsg).build();
            Channel chan = cf.channel();
            chan.write(msgWrapper);
            chan.flush().closeFuture().syncUninterruptibly();
        }
    }

    private void handleRetrieveFileResponse(ChannelHandlerContext ctx, StorageMessages.RetrieveFileResponse retrieveFileResponse) {
        logger.info("[Client] File ChunkId:" + retrieveFileResponse.getChunkId()
                + " came from SnId:" + retrieveFileResponse.getSnId() + " for fileName:"
                + retrieveFileResponse.getFileName() + ", result: "
                + retrieveFileResponse.getResult());

        if (retrieveFileResponse.getResult()) {
            dfsClientStarter.getRetrieveChunkIds().remove(retrieveFileResponse.getChunkId());
            /**
             * Write into output folder in the Client's File System.
             */
            String filePath = "output" + File.separator + retrieveFileResponse.getFileName();
            /**
             * We will merge into Appropriate space in file. 
             */
            byte[] data = retrieveFileResponse.getData().toByteArray();
            long seek = (retrieveFileResponse.getChunkId() - 1)
                    * ConfigurationManagerClient.getInstance().getChunkSizeInBytes();
            logger.info("[Client] Writing into File System with fileName:"
                    + retrieveFileResponse.getFileName() + "ChunkId:"
                    + retrieveFileResponse.getChunkId() + " seek:" + seek + " ,Data Size:"
                    + retrieveFileResponse.getData().size());
            try {
                Utils.writeDataIntoClientFileSystem(filePath, data, seek);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            logger.debug("[Client] ChunkId:" + retrieveFileResponse.getChunkId()
                    + " doesn't exist in this SN:" + retrieveFileResponse.getSnId());
        }
        ctx.close();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper msg) {
        logger.info("[Client]Received sth!");
        if (msg.hasStoreChunkLocation()) {
            handleStoreChunkLocationMsg(ctx, msg.getStoreChunkLocation());

        } else if (msg.hasStoreChunkResponse()) {
            handleStoreChunkResponseMsg(ctx, msg.getStoreChunkResponse());
        } else if (msg.hasListResponse()) {
            List<StorageMessages.StorageNodeInfo> snInfoList = msg.getListResponse()
                    .getSnInfoList();
            System.out.println("Number of SNs: "+snInfoList.size());
            for(StorageNodeInfo storageNodeInfo : snInfoList){
                System.out.println("------------------------------------------");
                System.out.println("[Client]Sn.Id:" + storageNodeInfo.getSnId());
                System.out.println("[Client]Sn.Ip:" + storageNodeInfo.getSnIp());
                System.out.println("[Client]Sn.Port:" + storageNodeInfo.getSnPort());
                System.out.println("[Client]Sn.NumOfRetrievelRequest:"
                        + storageNodeInfo.getNumOfRetrievelRequest());
                System.out.println("[Client]Sn.NumOfStorageRequest:"
                        + storageNodeInfo.getNumOfStorageMessage());
                System.out.println("[Client]Sn.TotalFreeSpace:"
                        + storageNodeInfo.getTotalFreeSpaceInBytes());
            }
            ctx.close();
        } else if (msg.hasFileLocation()) {
            StorageMessages.FileLocation fileLocationMsg = msg.getFileLocation();
            handleFileLocationMsg(ctx, fileLocationMsg);
        } else if (msg.hasRetrieveFileResponse()) {
            StorageMessages.RetrieveFileResponse retrieveFileResponse = msg
                    .getRetrieveFileResponse();
            handleRetrieveFileResponse(ctx, retrieveFileResponse);
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
