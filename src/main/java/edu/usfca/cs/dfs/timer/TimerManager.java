package edu.usfca.cs.dfs.timer;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.DfsControllerStarter;
import edu.usfca.cs.dfs.DfsStorageNodeStarter;
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
    public void scheduleKeepAliveCheckTimer(DfsControllerStarter dfsControllerStarter, int snId, int timeout) {
        ScheduledFuture timerHandle = scheduler.schedule(new KeepAliveCheckTimerTask(dfsControllerStarter,
                                                                                     snId,
                                                                                     timeout),
                                                         timeout,
                                                         TimeUnit.MILLISECONDS);
        dfsControllerStarter.setKeepAliveCheckTimerHandle(timerHandle, snId);
    }

    @SuppressWarnings("unchecked")
    public void cancelKeepAliveCheckTimer(DfsControllerStarter dfsControllerStarter, int snId) {
        ScheduledFuture timerHandle = dfsControllerStarter.getKeepAliveCheckTimerHandle(snId);
        if (timerHandle != null) {
            timerHandle.cancel(true);
            dfsControllerStarter.setKeepAliveCheckTimerHandle(null, snId);
        }
    }

    @SuppressWarnings("unchecked")
    public void scheduleHeartBeatTimer(DfsStorageNodeStarter dfsStorageNodeStarter, int snId, int timeout) {
        ScheduledFuture timerHandle = scheduler.schedule(new HeartBeatSenderTimerTask(dfsStorageNodeStarter,
                                                                                      snId,
                                                                                      timeout),
                                                         timeout,
                                                         TimeUnit.MILLISECONDS);
        dfsStorageNodeStarter.setHeartBeatSenderTimerHandle(timerHandle);
    }

    @SuppressWarnings("unchecked")
    public void cancelHeartBeatTimer(DfsStorageNodeStarter dfsStorageNodeStarter) {
        ScheduledFuture timerHandle = dfsStorageNodeStarter.getHeartBeatSenderTimerHandle();
        if (timerHandle != null) {
            timerHandle.cancel(true);
            dfsStorageNodeStarter.setHeartBeatSenderTimerHandle(null);
        }
    }

}
