package edu.usfca.cs.dfs.timer;

import edu.usfca.cs.Utils;
import edu.usfca.cs.db.SqlManager;
import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.DfsControllerStarter;
import edu.usfca.cs.dfs.config.ConfigManagerController;
import edu.usfca.cs.dfs.config.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class KeepAliveCheckTimerTask implements Runnable {

    private static Logger logger = LogManager.getLogger(KeepAliveCheckTimerTask.class);
    private int           snId;
    private long          timeOut;

    public KeepAliveCheckTimerTask(int snId) {
        this.snId = snId;
        this.timeOut = ConfigManagerController.getInstance()
                .getHeartBeatTimeoutInMilliseconds();
    }

    private void backup(HashMap<Integer, StorageNode> availableSNs) {
        int backupId = -1;
        SqlManager sqlManager = SqlManager.getInstance();
        Random rand = new Random();
        List<StorageNode> listSN = new ArrayList<StorageNode>(availableSNs.values());
        while (backupId == -1) {
            int index = rand.nextInt(availableSNs.size());
            StorageNode sn = listSN.get(index);
            backupId = sn.getSnId();
        }
        StorageNode backupNode = sqlManager.getSNInformationById(backupId);
        sqlManager.updateSNReplication(snId, backupNode.getSnId());

        Utils.sendChunkOfSourceSnToDestinationSn(snId, backupId);
    }

    public void run() {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Apply Keep Alive Checker for snId :" + snId);
            }
            DfsControllerStarter dfsControllerStarter = DfsControllerStarter.getInstance();
            StorageNode storageNode = dfsControllerStarter.getStorageNodeHashMap().get(snId);
            //Only check if SN is OPERATIONAL
            if (storageNode != null
                    && storageNode.getStatus().equals(Constants.STATUS_OPERATIONAL)) {
                SqlManager sqlManager = SqlManager.getInstance();
                long currentTime = System.currentTimeMillis();

                if (logger.isDebugEnabled()) {
                    //                    logger.debug("SnId :" + snId + ",LastHeartBeatTime:"
                    //                            + storageNode.getLastHeartBeatTime() + ",currenTime:" + currentTime
                    //                            + ",timeout:" + timeOut);
                    //                    logger.debug("(currentTime - timeOut) :" + (currentTime - timeOut));
                    //                    logger.debug("CurrenTime :" + currentTime);
                    //                    logger.debug("LastHeartBeatTime:" + storageNode.getLastHeartBeatTime());
                    //                    logger.debug("Timeout:" + timeOut);
                    //                    logger.debug("(currentTime - timeOut) :" + (currentTime - timeOut));
                }
                if ((currentTime - timeOut) > storageNode.getLastHeartBeatTime()) {
                    logger.error("Timeout occured for SN[" + snId + "], No heart beat since "
                            + timeOut + " milliseconds!");
                    storageNode.setStatus(Constants.STATUS_DOWN);
                    sqlManager.updateSNInformation(snId, Constants.STATUS_DOWN);
                    HashMap<Integer, StorageNode> availableSNs = sqlManager
                            .getAllSNByStatusList(Constants.STATUS_OPERATIONAL);
                    deleteNeighborSN(availableSNs, snId);
                    ArrayList<StorageNode> listSnBackUpByThisSn = sqlManager.getSnByBackUpId(snId);
                    for(StorageNode sn : listSnBackUpByThisSn){
                        deleteNeighborSN(availableSNs, sn.getSnId());
                    }
                    if (availableSNs.size() == 0) {
                        System.out.println("[BackUp]No SN to replicate");
                        return;
                    }
                    this.backup(availableSNs);
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

    private void deleteNeighborSN(HashMap<Integer, StorageNode> availableSNs, int snId){
        DfsControllerStarter dfsControllerStarter = DfsControllerStarter.getInstance();
        int numOfSn = dfsControllerStarter.getStorageNodeHashMap().size();
        int lowerBound = Math.floorMod(snId - 2, numOfSn) == 0 ? numOfSn
                : Math.floorMod(snId - 2, numOfSn);
        int upperBound = Math.floorMod(snId + 2, numOfSn) == 0 ? numOfSn
                : Math.floorMod(snId + 2, numOfSn);
        logger.info("[BackUp]Upper bound:" + upperBound);
        logger.info("[BackUp]Lower bound:" + lowerBound);
        if (lowerBound < upperBound) {
            for (int i = lowerBound; i <= upperBound; i++) {
                System.out.println("[BackUp]Remove: " + i);
                availableSNs.remove(i);
            }
        } else {
            for (int i = lowerBound; i <= dfsControllerStarter.getStorageNodeHashMap()
                    .size(); i++) {
                System.out.println("[BackUp]Remove: " + i);
                availableSNs.remove(i);
            }
            for (int i = 1; i <= upperBound; i++) {
                System.out.println("[BackUp]Remove: " + i);
                availableSNs.remove(i);
            }
        }
    }

}
