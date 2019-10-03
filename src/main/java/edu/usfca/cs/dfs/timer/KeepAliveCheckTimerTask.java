package edu.usfca.cs.dfs.timer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.db.SqlManager;
import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.DfsControllerStarter;
import edu.usfca.cs.dfs.config.ConfigurationManagerController;
import edu.usfca.cs.dfs.config.Constants;

public class KeepAliveCheckTimerTask implements Runnable {

    private static Logger logger = LogManager.getLogger(KeepAliveCheckTimerTask.class);
    private int           snId;
    private long          timeOut;

    public KeepAliveCheckTimerTask(int snId) {
        this.snId = snId;
        this.timeOut = ConfigurationManagerController.getInstance()
                .getHeartBeatTimeoutInMilliseconds();
    }

    public void run() {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Apply Keep Alive Checker for snId :" + snId);
            }

            StorageNode storageNode = DfsControllerStarter.getInstance().getStorageNodeHashMap()
                    .get(snId);
            if (storageNode != null) {
                long currentTime = System.currentTimeMillis();

                if (logger.isDebugEnabled()) {
                    logger.info("SnId :" + snId + ",LastHeartBeatTime:"
                            + storageNode.getLastHeartBeatTime() + ",currenTime:" + currentTime
                            + ",timeout:" + timeOut);
                    logger.info("(currentTime - timeOut) :" + (currentTime - timeOut));
                    logger.debug("CurrenTime :" + currentTime);
                    logger.debug("LastHeartBeatTime:" + storageNode.getLastHeartBeatTime());
                    logger.debug("Timeout:" + timeOut);
                    logger.debug("(currentTime - timeOut) :" + (currentTime - timeOut));
                }
                if ((currentTime - timeOut) > storageNode.getLastHeartBeatTime()) {
                    logger.debug("Timeout occured for SN[" + snId + "], No heart beat since "
                            + timeOut + " milliseconds!");
                    storageNode.setStatus(Constants.STATUS_DOWN);
                    SqlManager.getInstance().updateSNInformation(snId, Constants.STATUS_DOWN);
                    /**
                     * TODO:
                     * 
                     * Failure Recory issues... A lot of work TODO!! 
                     * 
                     */
                }
            } else {
                /**
                 * STRANGE BIG PROBLEM!!
                 */
            }
        } catch (Exception e) {
            logger.error("Exception occured in HeartBeat:", e);
        }
    }

}
