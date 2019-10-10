package edu.usfca.cs.dfs.timer;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.DfsControllerStarter;
import edu.usfca.cs.dfs.DfsStorageNodeStarter;
import edu.usfca.cs.dfs.config.ConfigManagerController;
import edu.usfca.cs.dfs.config.ConfigManagerSn;

public class TimerManager {

    private static Logger                     logger    = LogManager.getLogger(TimerManager.class);
    private final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(ConfigManagerSn
            .getInstance().getThreadNumOfScheduledPoolExecutor());
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
    public void scheduleKeepAliveCheckTimer(int snId) {
        ScheduledFuture timerHandle = scheduler
                .scheduleAtFixedRate(new KeepAliveCheckTimerTask(snId),
                                     5,
                                     ConfigManagerController.getInstance()
                                             .getKeepAlivePeriodInMilliseconds(),
                                     TimeUnit.MILLISECONDS);
        DfsControllerStarter.getInstance().setKeepAliveCheckTimerHandle(timerHandle, snId);
    }

    @SuppressWarnings("unchecked")
    public void cancelKeepAliveCheckTimer(int snId) {
        ScheduledFuture timerHandle = DfsControllerStarter.getInstance()
                .getKeepAliveCheckTimerHandle(snId);
        if (timerHandle != null) {
            timerHandle.cancel(true);
            DfsControllerStarter.getInstance().setKeepAliveCheckTimerHandle(null, snId);
        }
    }

    @SuppressWarnings("unchecked")
    public void scheduleHeartBeatTimer() {
        ScheduledFuture timerHandle = scheduler
                .scheduleAtFixedRate(new HeartBeatSenderTimerTask(),
                                     10,
                                     ConfigManagerSn.getInstance()
                                             .getHeartBeatPeriodInMilliseconds(),
                                     TimeUnit.MILLISECONDS);
        DfsStorageNodeStarter.getInstance().setHeartBeatSenderTimerHandle(timerHandle);
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
