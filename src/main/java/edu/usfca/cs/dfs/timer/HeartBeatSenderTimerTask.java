package edu.usfca.cs.dfs.timer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.DfsStorageNode;

public class HeartBeatSenderTimerTask implements Runnable {

    private static Logger logger = LogManager.getLogger(TimerManager.class);

    int                   timeOut;
    int                   snId;
    DfsStorageNode        dfsStorageNode;

    public HeartBeatSenderTimerTask(DfsStorageNode dfsStorageNode, int snId, int timeOut) {
        this.timeOut = timeOut;
        this.snId = snId;
    }

    public void run() {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Apply Heart Beat Timer timeout occurred with snId :" + snId);
            }

            /***
             * 
             * TODO: Send Heart Beat to Controller...
             */

        } catch (Exception e) {
            logger.error("Exception occured in HeartBeat:", e);
        }
    }

}
