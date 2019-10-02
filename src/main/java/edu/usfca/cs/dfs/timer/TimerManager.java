package edu.usfca.cs.dfs.timer;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.DfsController;
import edu.usfca.cs.dfs.DfsStorageNode;
import edu.usfca.cs.dfs.config.ConfigurationManagerSn;

public class TimerManager {

    private static Logger                     logger    = LogManager.getLogger(TimerManager.class);
    private final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(ConfigurationManagerSn.getInstance().getThreadNumOfScheduledPoolExecutor());
    private static final TimerManager         instance  = new TimerManager();

    public static TimerManager getInstance() {
        return instance;
    }

    private TimerManager() {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Initiliazing TimerManager. Pool Size:" + scheduler.getCorePoolSize());
            }
        } catch (Exception e) {
            //s_logger.error("Exception while loading properties from " + propertyFileName, e);
        }
    }

    @SuppressWarnings("unchecked")
    public void scheduleKeepAliveCheckTimer(DfsController dfsController, int snId, int timeout) {
        ScheduledFuture timerHandle = scheduler.schedule(new KeepAliveCheckTimerTask(dfsController,
                                                                                     snId,
                                                                                     timeout),
                                                         timeout,
                                                         TimeUnit.MILLISECONDS);
        dfsController.setKeepAliveCheckTimerHandle(timerHandle, snId);
    }

    @SuppressWarnings("unchecked")
    public void cancelKeepAliveCheckTimer(DfsController dfsController, int snId) {
        ScheduledFuture timerHandle = dfsController.getKeepAliveCheckTimerHandle(snId);
        if (timerHandle != null) {
            timerHandle.cancel(true);
            dfsController.setKeepAliveCheckTimerHandle(null, snId);
        }
    }

    @SuppressWarnings("unchecked")
    public void scheduleHeartBeatTimer(DfsStorageNode dfsStorageNode, int snId, int timeout) {
        ScheduledFuture timerHandle = scheduler.schedule(new HeartBeatSenderTimerTask(dfsStorageNode,
                                                                                      snId,
                                                                                      timeout),
                                                         timeout,
                                                         TimeUnit.MILLISECONDS);
        dfsStorageNode.setHeartBeatSenderTimerHandle(timerHandle);
    }

    @SuppressWarnings("unchecked")
    public void cancelHeartBeatTimer(DfsStorageNode dfsStorageNode) {
        ScheduledFuture timerHandle = dfsStorageNode.getHeartBeatSenderTimerHandle();
        if (timerHandle != null) {
            timerHandle.cancel(true);
            dfsStorageNode.setHeartBeatSenderTimerHandle(null);
        }
    }

}
