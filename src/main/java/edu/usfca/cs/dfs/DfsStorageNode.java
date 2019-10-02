package edu.usfca.cs.dfs;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ScheduledFuture;

import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.config.ConfigurationManagerClient;
import edu.usfca.cs.dfs.config.ConfigurationManagerSn;
import edu.usfca.cs.dfs.config.Constants;
import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class DfsStorageNode {

    ServerMessageRouter        messageRouter;

    private ScheduledFuture<?> heartBeatSenderTimerHandle;

    public DfsStorageNode() {
    }

    public void start() throws IOException {

        long totalFreeSpaceInBytes = new File("/").getFreeSpace();
        StorageNode sn = new StorageNode(ConfigurationManagerSn.getInstance().getSnId(),
                                         null,
                                         null,
                                         "localhost",
                                         ConfigurationManagerSn.getInstance().getSnPort(),
                                         totalFreeSpaceInBytes);

        messageRouter = new ServerMessageRouter(Constants.STORAGENODE);
        messageRouter.listen(ConfigurationManagerSn.getInstance().getSnPort());
        System.out.println("[SN] Listening for connections on port :"
                + ConfigurationManagerSn.getInstance().getSnPort());
        MessagePipeline pipeline = new MessagePipeline(Constants.STORAGENODE);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class).option(ChannelOption.SO_KEEPALIVE,
                                                                                                        true).handler(pipeline);

        /**
         * SN will connect to the Controller
         */
        ChannelFuture cf = bootstrap.connect(ConfigurationManagerClient.getInstance().getControllerIp(),
                                             ConfigurationManagerClient.getInstance().getControllerPort());

        StorageMessages.HeartBeat heartBeat = StorageMessages.HeartBeat.newBuilder().setSnId(sn.getSnId()).setTotalFreeSpaceInBytes(sn.getTotalFreeSpace()).setNumOfRetrievelRequest(0).setNumOfStorageMessage(0).build();
        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder().setHeartBeatMsg(heartBeat).build();
        Channel chan = cf.channel();
        ChannelFuture write = chan.write(msgWrapper);
        chan.flush();
        write.syncUninterruptibly();

    }

    public static void main(String[] args) throws IOException {
        DfsStorageNode s = new DfsStorageNode();
        s.start();
    }

    public ScheduledFuture<?> getHeartBeatSenderTimerHandle() {
        return heartBeatSenderTimerHandle;
    }

    public void setHeartBeatSenderTimerHandle(ScheduledFuture<?> heartBeatSenderTimerHandle) {
        this.heartBeatSenderTimerHandle = heartBeatSenderTimerHandle;
    }
}
