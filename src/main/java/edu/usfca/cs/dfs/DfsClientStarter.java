package edu.usfca.cs.dfs;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.Utils;
import edu.usfca.cs.dfs.config.ConfigurationManagerClient;
import edu.usfca.cs.dfs.config.Constants;
import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.net.NetUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class DfsClientStarter {

    private static Logger                logger           = LogManager
            .getLogger(DfsClientStarter.class);

    private static DfsClientStarter      instance;
    private final static Object          classLock        = new Object();
    private StorageMessages.FileMetadata metadata;
    private int                          numOfSentChunk   = -1;

    private String                       fileInfo         = "";

    private HashSet<Integer>             retrieveChunkIds = new HashSet<>();

    public HashSet<Integer> getRetrieveChunkIds() {
        return retrieveChunkIds;
    }

    public void setRetrieveChunkIds(HashSet<Integer> retrieveChunkIds) {
        this.retrieveChunkIds = retrieveChunkIds;
    }

    public String getFileInfo() {
        return fileInfo;
    }

    public void setFileInfo(String fileInfo) {
        this.fileInfo = fileInfo;
    }

    public StorageMessages.FileMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(StorageMessages.FileMetadata metadata) {
        this.metadata = metadata;
    }

    public int getNumOfSentChunk() {
        return numOfSentChunk;
    }

    public void setNumOfSentChunk(int numOfSentChunk) {
        this.numOfSentChunk = numOfSentChunk;
    }

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
        ChannelFuture cf = NetUtils.getInstance(Constants.CLIENT)
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
    }

    private void storeFile(Bootstrap bootstrap) {
        Scanner scanner = new Scanner(System.in);
        //  prompt for command.
        System.out.print("Enter your fileName and folder:");
        fileInfo = scanner.next().trim();
        File file = new File(fileInfo);
        if (!file.exists()) {
            System.out.println("File not found!");
            return;
        }
        long chunkSize = ConfigurationManagerClient.getInstance().getChunkSizeInBytes();
        long fileSize = file.length();
        System.out.println("The size of the file: " + fileSize + " bytes");
        System.out.println("\nThe size of chunks: " + chunkSize + " bytes");
        int numOfChunks = (int) Math.ceil((float) fileSize / (float) chunkSize);
        System.out.println("\nNumber Of Chunks is " + numOfChunks + " for file size:" + fileSize
                + " bytes");
        long lastChunkByteSize = fileSize - ((numOfChunks - 1) * chunkSize);
        System.out.println("\nlastChunkByteSize is " + lastChunkByteSize + " for file size:"
                + fileSize + " bytes");

        System.out.println("FileName:" + file.getName());
        //Send metadata in chunk 0
        this.metadata = StorageMessages.FileMetadata.newBuilder().setFileSize(fileSize)
                .setNumOfChunks(numOfChunks).build();
        this.numOfSentChunk = -1;

        int numOfSendThread = 3;
        ExecutorService executorService = Executors.newFixedThreadPool(numOfSendThread);
        Channel channel = NetUtils.getInstance(Constants.CLIENT)
                .connect(bootstrap,
                         ConfigurationManagerClient.getInstance().getControllerIp(),
                         ConfigurationManagerClient.getInstance().getControllerPort())
                .channel();
        executorService.execute(new Runnable() {

            @Override
            public void run() {
                StorageMessages.FileMetadata metadata = DfsClientStarter.getInstance()
                        .getMetadata();
                StorageMessages.StoreChunk storeChunkMsg = StorageMessages.StoreChunk.newBuilder()
                        .setFileName(file.getName()).setChunkId(0)
                        .setChunkSize(metadata.getSerializedSize()).setData(metadata.toByteString())
                        .build();
                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder().setStoreChunk(storeChunkMsg).build();
                channel.writeAndFlush(msgWrapper).syncUninterruptibly();
                channel.closeFuture().syncUninterruptibly();
            }
        });

        //Send actual file from chunk 1
        for (int i = 0; i < numOfChunks; i++) {
            int currentChunk = i + 1;
            executorService.execute(new Runnable() {

                @Override
                public void run() {
                    StorageMessages.StoreChunk storeChunkMsg = StorageMessages.StoreChunk
                            .newBuilder().setFileName(file.getName()).setChunkId(currentChunk)
                            .setChunkSize(currentChunk == numOfChunks ? lastChunkByteSize
                                    : chunkSize)
                            .build();
                    StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                            .newBuilder().setStoreChunk(storeChunkMsg).build();
                    channel.writeAndFlush(msgWrapper);
                }
            });
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void retrieveFile(Bootstrap bootstrap) {
        Scanner scanner = new Scanner(System.in);
        //  prompt for command.
        System.out.print("Enter your fileName: ");
        String fileName = scanner.next().trim();

        ChannelFuture cf = NetUtils.getInstance(Constants.CLIENT)
                .connect(bootstrap,
                         ConfigurationManagerClient.getInstance().getControllerIp(),
                         ConfigurationManagerClient.getInstance().getControllerPort());
        StorageMessages.RetrieveFile retrieveFileMsg = StorageMessages.RetrieveFile.newBuilder()
                .setFileName(fileName).build();

        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder().setRetrieveFile(retrieveFileMsg).build();
        Channel chan = cf.channel();
        chan.write(msgWrapper);
        chan.flush().closeFuture().syncUninterruptibly();
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
                dfsClient.retrieveFile(bootstrap);
            } else if (command.equalsIgnoreCase(Constants.STORE)) {
                dfsClient.storeFile(bootstrap);
            } else if (command.equalsIgnoreCase(Constants.CHECKSUM)) {
                dfsClient.checkFilesIntegrity(bootstrap);
            } else if (command.equalsIgnoreCase(Constants.EXIT)) {
                System.out.println("Client will be shutdown....");
                workerGroup.shutdownGracefully();
                System.exit(0);
            } else {
                System.out.println("Undefined command:" + command);
            }

        }

    }

    private void checkFilesIntegrity(Bootstrap bootstrap) {
        Scanner scanner = new Scanner(System.in);
        //  prompt for command.
        System.out.print("Enter your First File: ");
        String fileName = scanner.next().trim();
        System.out.print("Enter your Second File: ");
        String fileName2 = scanner.next().trim();
        try {
            if (Utils.givenFileGeneratingChecksumThenVerifying(fileName, fileName2)) {
                logger.debug("[SUCCESS] Transfer successfully! These 2 files are IDENTICAL!");
            } else {
                logger.debug("[ERROR] Problem in transfer successfully! These 2 files are NOT IDENTICAL!");
            }
        } catch (NoSuchAlgorithmException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
