package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ScheduledFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.db.SqlManager;
import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.StorageMessages.HeartBeat;
import edu.usfca.cs.dfs.config.ConfigurationManagerController;
import edu.usfca.cs.dfs.config.Constants;
import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.net.ServerMessageRouter;

public class DfsControllerStarter {

    private static Logger                        logger                       = LogManager
            .getLogger(DfsControllerStarter.class);
    private static DfsControllerStarter          instance;
    private final static Object                  classLock                    = new Object();
    ServerMessageRouter                          messageRouter;
    private HashMap<Integer, ScheduledFuture<?>> keepAliveCheckTimerHandleMap = new HashMap<Integer, ScheduledFuture<?>>();
    private HashMap<Integer, StorageNode>        storageNodeHashMap           = new HashMap<Integer, StorageNode>();

    private DfsControllerStarter() {

    }

    /**
     * Singleton
     *  
     * @return
     */
    public static DfsControllerStarter getInstance() {
        synchronized (classLock) {
            if (instance == null) {
                instance = new DfsControllerStarter();
            }
            return instance;
        }
    }

    public static void main(String[] args) throws IOException {
        DfsControllerStarter s = new DfsControllerStarter();
        s.start();
    }

    public void start() throws IOException {

        SqlManager.getInstance().deleteAllSNs();

        messageRouter = new ServerMessageRouter(Constants.CONTROLLER);
        System.out.println(ConfigurationManagerController.getInstance().toString());

        messageRouter.listen(ConfigurationManagerController.getInstance().getControllerPort());
        System.out.println("[Controller] Listening for connections on port :"
                + ConfigurationManagerController.getInstance().getControllerPort());

        MessagePipeline pipeline = new MessagePipeline(Constants.CONTROLLER);

    }

    public ScheduledFuture<?> getKeepAliveCheckTimerHandle(int snId) {
        return keepAliveCheckTimerHandleMap.get(snId);
    }

    public void setKeepAliveCheckTimerHandle(ScheduledFuture timerHandle, int snId) {
        this.keepAliveCheckTimerHandleMap.put(snId, timerHandle);
    }

    public HashMap<Integer, StorageNode> getStorageNodeHashMap() {
        return storageNodeHashMap;
    }

    public void setStorageNodeHashMap(HashMap<Integer, StorageNode> storageNodeHashMap) {
        this.storageNodeHashMap = storageNodeHashMap;
    }

    public boolean addStorageNode(HeartBeat heartBeat) {

        StorageNode storageNode = new StorageNode(heartBeat.getSnId(),
                                                  null,
                                                  null,
                                                  heartBeat.getSnIp(),
                                                  heartBeat.getSnPort(),
                                                  heartBeat.getTotalFreeSpaceInBytes(),
                                                  Constants.STATUS_OPERATIONAL);

        boolean result = SqlManager.getInstance().insertSN(storageNode);

        if (result) {
            storageNodeHashMap.put(heartBeat.getSnId(), storageNode);
        } else {
            return false;
        }

        return result;
    }

}
