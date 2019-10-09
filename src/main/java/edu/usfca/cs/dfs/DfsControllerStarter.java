package edu.usfca.cs.dfs;

import edu.usfca.cs.db.SqlManager;
import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.StorageMessages.HeartBeat;
import edu.usfca.cs.dfs.bloomfilter.BloomFilter;
import edu.usfca.cs.dfs.config.ConfigurationManagerController;
import edu.usfca.cs.dfs.config.Constants;
import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.net.ServerMessageRouter;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.ScheduledFuture;

public class DfsControllerStarter {

    private static Logger                                 logger                       = LogManager
            .getLogger(DfsControllerStarter.class);
    private static DfsControllerStarter                   instance;
    private final static Object                           classLock                    = new Object();
    ServerMessageRouter                                   messageRouter;
    private HashMap<Integer, ScheduledFuture<?>>          keepAliveCheckTimerHandleMap = new HashMap<Integer, ScheduledFuture<?>>();
    private HashMap<Integer, StorageNode>                 storageNodeHashMap           = new HashMap<Integer, StorageNode>();
    private HashMap<Integer, BloomFilter>                 bloomFilters                 = new HashMap<Integer, BloomFilter>();
    private StorageMessages.FileMetadata fileMetadata = null;

    public static final int                               MAX_REPLICATION_NUMBER       = 3;

    public static boolean                                 LOG_HEART_BEAT               = false;

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
        SqlManager.getInstance().deleteAllSNsReplications();

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

    public boolean addStorageNode(HeartBeat heartBeat, int newSnId) {
        StorageNode storageNode = new StorageNode(newSnId,
                                                  null,
                                                  null,
                                                  heartBeat.getSnIp(),
                                                  heartBeat.getSnPort(),
                                                  heartBeat.getTotalFreeSpaceInBytes(),
                                                  Constants.STATUS_OPERATIONAL,
                                                  -1);
        boolean result = SqlManager.getInstance().insertSN(storageNode);
        if (result) {
            storageNodeHashMap.put(newSnId, storageNode);
            bloomFilters
                    .put(newSnId,
                         new BloomFilter(ConfigurationManagerController.getInstance()
                                 .getFilterLength(),
                                         ConfigurationManagerController.getInstance().getHashTime(),
                                         ConfigurationManagerController.getInstance().getSeed()));
            composeRingReplication(newSnId);
        } else {
            return false;
        }
        return result;
    }

    public void composeRingReplication(int newSnId) {
        SqlManager.getInstance().deleteAllSNsReplications();
        if (newSnId <= 1) {
            return;
        }
        if (newSnId == 2) {
            SqlManager.getInstance().insertSNReplication(1, 2);
            SqlManager.getInstance().insertSNReplication(2, 1);
        } else {
            /**
             * 3-4-5-6-7-8-9-10-11-12
             */
            for (int i = 1; i <= newSnId; i++) {
                int replicate1Id = i + 1;
                int replicate2Id = i + 2;

                if (replicate1Id > newSnId) {
                    replicate1Id = 1;
                }
                if (replicate1Id == 1 && replicate2Id > newSnId) {
                    replicate2Id = 2;
                } else if (replicate2Id > newSnId) {
                    replicate2Id = 1;
                }
                SqlManager.getInstance().insertSNReplication(i, replicate1Id);
                SqlManager.getInstance().insertSNReplication(i, replicate2Id);

            }

        }
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

    public StorageMessages.FileMetadata getFileMetadata() {
        return fileMetadata;
    }

    public void setFileMetadata(StorageMessages.FileMetadata fileMetadata) {
        this.fileMetadata = fileMetadata;
    }
}
