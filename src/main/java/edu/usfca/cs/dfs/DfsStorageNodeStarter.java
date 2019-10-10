package edu.usfca.cs.dfs;

import edu.usfca.cs.Utils;
import edu.usfca.cs.db.model.MetaDataOfChunk;
import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.config.ConfigManagerSn;
import edu.usfca.cs.dfs.config.Constants;
import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.net.NetUtils;
import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ScheduledFuture;

public class DfsStorageNodeStarter {

    private static Logger                    logger                 = LogManager
            .getLogger(DfsStorageNodeStarter.class);
    private static DfsStorageNodeStarter     instance;
    private final static Object              classLock              = new Object();
    ServerMessageRouter                      messageRouter;
    private ScheduledFuture<?>               heartBeatSenderTimerHandle;                                      //For Timer Manager...
    private ChannelFuture                    channelFuture;                                                   //We will use this one in Timer Task...
    private StorageNode                      storageNode;
    private HashMap<String, MetaDataOfChunk> fileChunkToMetaDataMap = new HashMap<String, MetaDataOfChunk>(); //key:fileName_chunkId 

    private DfsStorageNodeStarter() {
        ConfigManagerSn.getInstance();
    }

    /**
     * Singleton
     *  
     * @return
     */
    public static DfsStorageNodeStarter getInstance() {
        synchronized (classLock) {
            if (instance == null) {
                instance = new DfsStorageNodeStarter();
            }
            return instance;
        }
    }

    private void deleteStorageDirectory() {

        String directoryPath = null;
        try {
            logger.info("Working Directory = " + System.getProperty("user.dir"));
            directoryPath = ConfigManagerSn.getInstance().getStoreLocation();
            String whoamI = System.getProperty("user.name");
            directoryPath = System.getProperty("user.dir") + File.separator + directoryPath
                    + File.separator + whoamI;

            logger.info("Path:" + directoryPath);
            File directory = new File(directoryPath);
            if (directory.exists()) {
                Utils.deleteDirectory(directory);
                logger.info("[SN] Clear directory");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void start() throws IOException {

        try {
            storageNode = new StorageNode(-1,
                                          null,
                                          null,
                                          ConfigManagerSn.getInstance().getMyIp(),
                                          ConfigManagerSn.getInstance().getSnPort(),
                                          calculateTotalFreeSpaceInBytes(),
                                          Constants.STATUS_OPERATIONAL,
                                          -1);

            System.out.println(storageNode.toString());
            this.deleteStorageDirectory();

            //InetAddress myLocalIp = InetAddress.getLocalHost();
            //System.out.println("System IP Address : " + (myLocalIp.getHostAddress()).trim());

            messageRouter = new ServerMessageRouter(Constants.STORAGENODE);
            messageRouter.listen(ConfigManagerSn.getInstance().getSnPort());
            System.out.println("[SN] Listening for connections on port :"
                    + ConfigManagerSn.getInstance().getSnPort());
            MessagePipeline pipeline = new MessagePipeline(Constants.STORAGENODE);
            EventLoopGroup workerGroup = new NioEventLoopGroup();

            Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);

            /**
             * SN will connect to the Controller
             */
            channelFuture = NetUtils.getInstance(Constants.STORAGENODE)
                    .connect(bootstrap, ConfigManagerSn.getInstance().getControllerIp(),
                             ConfigManagerSn.getInstance().getControllerPort());

            StorageMessages.HeartBeat heartBeat = StorageMessages.HeartBeat.newBuilder()
                    .setSnId(storageNode.getSnId()).setSnIp(storageNode.getSnIp())
                    .setSnPort(storageNode.getSnPort())
                    .setTotalFreeSpaceInBytes(storageNode.getTotalFreeSpaceInBytes())
                    .setNumOfRetrievelRequest(0).setNumOfStorageMessage(0).build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                    .newBuilder().setHeartBeatMsg(heartBeat).build();
            Channel chan = channelFuture.channel();
            ChannelFuture write = chan.write(msgWrapper);
            chan.flush();
            write.syncUninterruptibly();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException {
        DfsStorageNodeStarter.getInstance();
        instance.start();
    }

    public long calculateTotalFreeSpaceInBytes() {
        return new File("/").getFreeSpace();
    }

    public ScheduledFuture<?> getHeartBeatSenderTimerHandle() {
        return heartBeatSenderTimerHandle;
    }

    public void setHeartBeatSenderTimerHandle(ScheduledFuture<?> heartBeatSenderTimerHandle) {
        this.heartBeatSenderTimerHandle = heartBeatSenderTimerHandle;
    }

    public ChannelFuture getChannelFuture() {
        return channelFuture;
    }

    public void setChannelFuture(ChannelFuture channelFuture) {
        this.channelFuture = channelFuture;
    }

    public StorageNode getStorageNode() {
        return storageNode;
    }

    public void setStorageNode(StorageNode storageNode) {
        this.storageNode = storageNode;
    }

    public HashMap<String, MetaDataOfChunk> getFileChunkToMetaDataMap() {
        return fileChunkToMetaDataMap;
    }

    public void setFileChunkToMetaDataMap(HashMap<String, MetaDataOfChunk> fileChunkToMetaDataMap) {
        this.fileChunkToMetaDataMap = fileChunkToMetaDataMap;
    }

}
