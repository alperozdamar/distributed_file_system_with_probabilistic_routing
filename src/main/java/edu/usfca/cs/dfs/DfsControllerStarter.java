package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ScheduledFuture;

import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.config.ConfigurationManagerController;
import edu.usfca.cs.dfs.config.Constants;
import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.net.ServerMessageRouter;

public class DfsControllerStarter {

    private static DfsControllerStarter          instance;
    private final static Object                  classLock                    = new Object();
    ServerMessageRouter                          messageRouter;
    private ArrayList<StorageNode>               storageNodeList              = new ArrayList<StorageNode>();
    private HashMap<Integer, ScheduledFuture<?>> keepAliveCheckTimerHandleMap = new HashMap<Integer, ScheduledFuture<?>>();

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

        messageRouter = new ServerMessageRouter(Constants.CONTROLLER);
        messageRouter.listen(ConfigurationManagerController.getInstance().getControllerPort());
        System.out.println("[Controller] Listening for connections on port :"
                + ConfigurationManagerController.getInstance().getControllerPort());

        MessagePipeline pipeline = new MessagePipeline(Constants.CONTROLLER);

    }

    public ArrayList<StorageNode> getStorageNodeList() {
        return storageNodeList;
    }

    public void setStorageNodeList(ArrayList<StorageNode> storageNodeList) {
        this.storageNodeList = storageNodeList;
    }

    public ScheduledFuture<?> getKeepAliveCheckTimerHandle(int snId) {
        return keepAliveCheckTimerHandleMap.get(snId);
    }

    public void setKeepAliveCheckTimerHandle(ScheduledFuture timerHandle, int snId) {
        this.keepAliveCheckTimerHandleMap.put(snId, timerHandle);
    }

}
