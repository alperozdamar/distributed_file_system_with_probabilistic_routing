package edu.usfca.cs.dfs.timer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.DfsStorageNodeStarter;
import edu.usfca.cs.dfs.StorageMessages;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

public class HeartBeatSenderTimerTask implements Runnable {

    private static Logger logger = LogManager.getLogger(HeartBeatSenderTimerTask.class);

    public HeartBeatSenderTimerTask() {

    }

    public void run() {
        try {
            StorageNode storageNode = DfsStorageNodeStarter.getInstance().getStorageNode();
            if (logger.isDebugEnabled()) {
                logger.debug("Apply Heart Beat Timer timeout occurred with snId :"
                        + storageNode.getSnId());
            }

            System.out.println("Apply Heart Beat Timer timeout occurred with snId :"
                    + storageNode.getSnId());

            /**
             * SN will connect to the Controller
             */
            StorageMessages.HeartBeat heartBeat = StorageMessages.HeartBeat.newBuilder()
                    .setSnId(storageNode.getSnId())
                    .setTotalFreeSpaceInBytes(storageNode.getTotalFreeSpaceInBytes())
                    .setNumOfRetrievelRequest(0).setNumOfStorageMessage(0).build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                    .newBuilder().setHeartBeatMsg(heartBeat).build();
            Channel chan = DfsStorageNodeStarter.getInstance().getChannelFuture().channel();
            ChannelFuture write = chan.write(msgWrapper);
            chan.flush();
            write.syncUninterruptibly();

        } catch (Exception e) {
            logger.error("Exception occured in HeartBeat:", e);
        }
    }

}
