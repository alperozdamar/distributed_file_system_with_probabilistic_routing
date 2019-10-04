package edu.usfca.cs.dfs;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.ByteString;

import edu.usfca.cs.Utils;
import edu.usfca.cs.dfs.config.ConfigurationManagerClient;
import edu.usfca.cs.dfs.config.Constants;
import edu.usfca.cs.dfs.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class DfsClientStarter {

    private static Logger           logger    = LogManager.getLogger(DfsClientStarter.class);

    private static DfsClientStarter instance;
    private final static Object     classLock = new Object();

    private String fileInfo = "";

    private DfsClientStarter() {
    }

    /**
     * Singleton: getInstance
     * @return
     */
    public static DfsClientStarter getInstance() {
        synchronized (classLock) {
            if (instance == null) {
                instance = new DfsClientStarter();
            }
            return instance;
        }
    }

    private void listStorageNode(Bootstrap bootstrap) {
        System.out.println("Client will be connected to Controller<"
                + ConfigurationManagerClient.getInstance().getControllerIp() + ":"
                + ConfigurationManagerClient.getInstance().getControllerPort() + ">");
        ChannelFuture cf = Utils
                .connect(bootstrap,
                         ConfigurationManagerClient.getInstance().getControllerIp(),
                         ConfigurationManagerClient.getInstance().getControllerPort());
        //Create LIST message
        StorageMessages.List listMsm = StorageMessages.List.newBuilder().build();
        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder().setList(listMsm).build();
        Channel chan = cf.channel();
        chan.write(msgWrapper);
        chan.flush();
        chan.closeFuture().syncUninterruptibly();
    }

    private void storeFile(Bootstrap bootstrap) {
        Scanner scanner = new Scanner(System.in);
        //  prompt for command.
        System.out.print("Enter your fileName and folder:");
        fileInfo = scanner.next().trim();
        /**
         * TODO:
         * Find the specified file and divide into chunks...
         *
         */
        File file = new File(fileInfo);
        long chunkSize = ConfigurationManagerClient.getInstance().getChunkSizeInBytes();
        long fileSize = file.length();
        System.out.format("The size of the file: %d bytes", fileSize);
        System.out.format("\nThe size of chunks: %d bytes", chunkSize);
        long numOfChunks = (long) Math.ceil((float) fileSize / (float) chunkSize);
        System.out.format("\nNumber Of Chunks is %d for file size:%d bytes", numOfChunks, fileSize);
        long lastChunkByteSize = fileSize % chunkSize;
        System.out.format("\nlastChunkByteSize is %d for file size:%d bytes",
                          lastChunkByteSize,
                          fileSize);

        System.out.println("FileName:" + file.getName());

        byte[] buffer = new byte[(int) chunkSize];
        int read = 0;
        for (int i = 0; i < numOfChunks; i++) {
            ChannelFuture cf = Utils
                    .connect(bootstrap,
                            ConfigurationManagerClient.getInstance().getControllerIp(),
                            ConfigurationManagerClient.getInstance().getControllerPort());
            StorageMessages.StoreChunk storeChunkMsg = StorageMessages.StoreChunk.newBuilder()
                    .setFileName(file.getName())
                    .setChunkId(i + 1)
                    .setChunkSize((int) (i==numOfChunks-1?lastChunkByteSize:chunkSize)).build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                    .newBuilder().setStoreChunkMsg(storeChunkMsg).build();
            cf.channel().writeAndFlush(msgWrapper).syncUninterruptibly();
        }
    }

    public static void main(String[] args) {
        ConfigurationManagerClient.getInstance();
        System.out.println("Client is started with these parameters: "
                + ConfigurationManagerClient.getInstance().toString());
        DfsClientStarter dfsClient = DfsClientStarter.getInstance();

        /**
         * TODO:
         * Connect to the controller If there is no connection.
         * Else thrown an error
         */
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(Constants.CLIENT);

        Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);

        while (true) {
            // create a scanner so we can read the command-line input
            Scanner scanner = new Scanner(System.in);

            //  prompt for command.
            System.out.print("Enter your command: ");

            // get command as String
            String command = scanner.next();

            if (command.equalsIgnoreCase(Constants.LIST)) {
                dfsClient.listStorageNode(bootstrap);
            } else if (command.equalsIgnoreCase(Constants.RETRIEVE)) {

            } else if (command.equalsIgnoreCase(Constants.STORE)) {
                dfsClient.storeFile(bootstrap);
            } else if (command.equalsIgnoreCase(Constants.EXIT)) {
                System.out.println("Client will be shutdown....");
                workerGroup.shutdownGracefully();
                System.exit(0);
            } else {
                System.out.println("Undefined command:" + command);
            }

        }

    }

    public String getFileInfo() {
        return fileInfo;
    }

    public void setFileInfo(String fileInfo) {
        this.fileInfo = fileInfo;
    }

}
