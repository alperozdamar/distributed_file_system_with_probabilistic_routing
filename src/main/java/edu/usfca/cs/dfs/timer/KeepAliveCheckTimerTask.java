package edu.usfca.cs.dfs.timer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.DfsControllerStarter;

public class KeepAliveCheckTimerTask implements Runnable {

    private static Logger logger = LogManager.getLogger(TimerManager.class);

    int                   sessionId;
    int                   timeOut;

    public KeepAliveCheckTimerTask(DfsControllerStarter dfsControllerStarter, int snId, int _timeOut) {
        timeOut = _timeOut;
    }

    public void run() {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Apply Heart Beat Timer timeout occurred with timerId :");
            }

        } catch (Exception e) {
            logger.error("Exception occured in HeartBeat:", e);
        }
    }

}
