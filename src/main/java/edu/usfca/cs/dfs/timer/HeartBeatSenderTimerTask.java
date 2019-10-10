package edu.usfca.cs.dfs.timer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.DfsStorageNodeStarter;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.config.ConfigManagerSn;
import io.netty.channel.Channel;

public class HeartBeatSenderTimerTask implements Runnable {

    private static Logger logger = LogManager.getLogger(HeartBeatSenderTimerTask.class);

    public HeartBeatSenderTimerTask() {
    }

    public void run() {
        StorageNode storageNode = DfsStorageNodeStarter.getInstance().getStorageNode();
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("[SN" + storageNode.getSnId()
                        + "]Heart Beat Timer triggered with snId :" + storageNode.getSnId());
            }

            /**
             * SN will connect to the Controller
             */
            StorageMessages.HeartBeat heartBeat = StorageMessages.HeartBeat.newBuilder()
                    .setSnId(storageNode.getSnId())
                    .setSnIp(ConfigManagerSn.getInstance().getMyIp())
                    .setSnPort(ConfigManagerSn.getInstance().getSnPort())
                    .setTotalFreeSpaceInBytes(DfsStorageNodeStarter.getInstance()
                            .calculateTotalFreeSpaceInBytes())
                    .setNumOfRetrievelRequest((int) DfsStorageNodeStarter.getInstance()
                            .getStorageNode().getTotalRetrievelRequest())
                    .setNumOfStorageMessage((int) DfsStorageNodeStarter.getInstance()
                            .getStorageNode().getTotalStorageRequest())
                    .build();
            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                    .newBuilder().setHeartBeatMsg(heartBeat).build();
            Channel chan = DfsStorageNodeStarter.getInstance().getChannelFuture().channel();
            chan.write(msgWrapper);
            chan.flush();

            logger.info("[SN" + storageNode.getSnId()
                    + "] ---------->>>>>>>> HEART BEAT To Controller, snId[" + heartBeat.getSnId()
                    + "] >>>>>>>>>>>--------------");

        } catch (Exception e) {
            logger.error("[SN" + storageNode.getSnId() + "]Exception occured in HeartBeat:", e);
        }
    }
}
