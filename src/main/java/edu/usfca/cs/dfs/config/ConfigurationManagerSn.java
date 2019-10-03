package edu.usfca.cs.dfs.config;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * Singleton Configuration Manager for Project1.
 * 
 */
public class ConfigurationManagerSn {

    public static final String            PROJECT_1_SN_CONFIG_FILE         = "config"
            + File.separator + "project1_sn.properties";
    private static ConfigurationManagerSn instance;
    private final static Object           classLock                        = new Object();
    private String                        controllerIp                     = "";
    private int                           controllerPort                   = 9090;
    private int                           snId                             = 0;
    private int                           snPort                           = 9090;
    private int                           threadNumOfScheduledPoolExecutor = 10;
    private int                           heartBeatPeriodInMilliseconds    = 5000;

    private ConfigurationManagerSn() {
        readConfigFile();
    }

    /**
     * Singleton
     *  
     * @return
     */
    public static ConfigurationManagerSn getInstance() {
        synchronized (classLock) {
            if (instance == null) {
                instance = new ConfigurationManagerSn();
            }
            return instance;
        }
    }

    public void readConfigFile() {

        Properties props = new Properties();
        try {
            props.load(new FileInputStream(PROJECT_1_SN_CONFIG_FILE));

            try {
                String snIdString = props.getProperty("snId").trim();
                snId = (snIdString == null) ? 8080 : Integer.parseInt(snIdString);
            } catch (Exception e) {
                snId = 8800;
                e.printStackTrace();
            }

            try {
                String snPortString = props.getProperty("snPort").trim();
                snPort = (snPortString == null) ? 8080 : Integer.parseInt(snPortString);
            } catch (Exception e) {
                snPort = 8800;
                e.printStackTrace();
            }

            controllerIp = props.getProperty("controllerIp");
            if (controllerIp == null) {
                System.out.println("controllerIp property is Null! Please Check configuration file.");
            } else {
                controllerIp = controllerIp.trim();
            }

            try {
                String controllerPortString = props.getProperty("controllerPort").trim();
                controllerPort = (controllerPortString == null) ? 8080
                        : Integer.parseInt(controllerPortString);
            } catch (Exception e) {
                controllerPort = 8800;
                e.printStackTrace();
            }

            try {
                String threadNumString = props.getProperty("threadNumOfScheduledPoolExecutor").trim();
                threadNumOfScheduledPoolExecutor = (threadNumString == null) ? 20
                        : Integer.parseInt(threadNumString);
            } catch (Exception e) {
                threadNumOfScheduledPoolExecutor = 20;
                e.printStackTrace();
            }

            try {
                String heartBeatPeriodInMillisecondsString = props.getProperty("heartBeatPeriodInMilliseconds").trim();
                heartBeatPeriodInMilliseconds = (heartBeatPeriodInMillisecondsString == null) ? 5000
                        : Integer.parseInt(heartBeatPeriodInMillisecondsString);
            } catch (Exception e) {
                heartBeatPeriodInMilliseconds = 5000;
                e.printStackTrace();
            }

        } catch (Exception e) {
            System.err.println("Exception occured while parsing Configuration File:"
                    + PROJECT_1_SN_CONFIG_FILE);
            // e.printStackTrace(); //professor doesn't want stackTrace.
            e.getMessage();
        }
    }

    public String getControllerIp() {
        return controllerIp;
    }

    public void setControllerIp(String controllerIp) {
        this.controllerIp = controllerIp;
    }

    public int getControllerPort() {
        return controllerPort;
    }

    public void setControllerPort(int controllerPort) {
        this.controllerPort = controllerPort;
    }

    public int getSnId() {
        return snId;
    }

    public void setSnId(int snId) {
        this.snId = snId;
    }

    public int getSnPort() {
        return snPort;
    }

    public void setSnPort(int snPort) {
        this.snPort = snPort;
    }

    public int getThreadNumOfScheduledPoolExecutor() {
        return threadNumOfScheduledPoolExecutor;
    }

    public void setThreadNumOfScheduledPoolExecutor(int threadNumOfScheduledPoolExecutor) {
        this.threadNumOfScheduledPoolExecutor = threadNumOfScheduledPoolExecutor;
    }

    @Override
    public String toString() {
        return "ConfigurationManagerSn [controllerIp=" + controllerIp + ", controllerPort="
                + controllerPort + ", snId=" + snId + ", snPort=" + snPort
                + ", threadNumOfScheduledPoolExecutor=" + threadNumOfScheduledPoolExecutor + "]";
    }

    public int getHeartBeatPeriodInMilliseconds() {
        return heartBeatPeriodInMilliseconds;
    }

    public void setHeartBeatPeriodInMilliseconds(int heartBeatPeriodInMilliseconds) {
        this.heartBeatPeriodInMilliseconds = heartBeatPeriodInMilliseconds;
    }

}