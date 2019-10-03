package edu.usfca.cs.dfs;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.ScheduledFuture;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

import edu.usfca.cs.db.SqlManager;
import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.StorageMessages.HeartBeat;
import edu.usfca.cs.dfs.bloomfilter.BloomFilter;
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

    private HashMap<Integer, BloomFilter>        bloomFilters                 = new HashMap<Integer, BloomFilter>();

    private DfsControllerStarter() {
        //TODO: Create bloom filter when add storage node to controller
        for (int i = 0; i < 12; i++) {
            bloomFilters
                    .put(i + 1,
                         new BloomFilter(ConfigurationManagerController.getInstance()
                                 .getFilterLength(),
                                         ConfigurationManagerController.getInstance().getHashTime(),
                                         ConfigurationManagerController.getInstance().getSeed()));
        }
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

        //updateLogger("logs/project1_controller.log", "edu.usfca", "edu.usfca");

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
        logger.debug("[Controller] Listening for connections on port :"
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

    public HashMap<Integer, BloomFilter> getBloomFilters() {
        return bloomFilters;
    }

    public void setBloomFilters(HashMap<Integer, BloomFilter> bloomFilters) {
        this.bloomFilters = bloomFilters;
    }

    static void updateLogger(String file_name, String appender_name, String package_name) {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = context.getConfiguration();
        Layout<? extends Serializable> old_layout = configuration.getAppender(appender_name)
                .getLayout();

        //delete old appender/logger
        configuration.getAppender(appender_name).stop();
        configuration.removeLogger(package_name);

        //create new appender/logger
        LoggerConfig loggerConfig = new LoggerConfig(package_name, Level.DEBUG, false);
        FileAppender appender = FileAppender.createAppender(file_name,
                                                            "false",
                                                            "false",
                                                            appender_name,
                                                            "true",
                                                            "true",
                                                            "true",
                                                            "8192",
                                                            old_layout,
                                                            null,
                                                            "false",
                                                            "",
                                                            configuration);
        appender.start();
        loggerConfig.addAppender(appender, Level.DEBUG, null);
        configuration.addLogger(package_name, loggerConfig);

        context.updateLoggers();
    }

}
