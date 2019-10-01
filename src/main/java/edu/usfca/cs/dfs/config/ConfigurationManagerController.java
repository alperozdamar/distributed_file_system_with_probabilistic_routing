package edu.usfca.cs.dfs.config;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * Singleton Configuration Manager for Project1.
 *
 */
public class ConfigurationManagerController {

    private final int defaultLength = 1000;
    private final int defaultHashTime = 3;
    private final int defaultHashSeed = 3;
    private final int defaultControllerPort = 8080;

    public static final String                    PROJECT_1_CONTROLLER_CONFIG_FILE = "config"
            + File.separator + "project1_controller.properties";
    private static ConfigurationManagerController instance;
    private final static Object                   classLock                        = new Object();
    private String                                controllerIp                     = "";
    private int                                   controllerPort                   = defaultControllerPort;

    private int bloomFilterLength = defaultLength;
    private int hashTime = defaultHashTime;
    private int hashSeed = defaultHashSeed;

    private ConfigurationManagerController() {
        readControllerConfigFile();
    }

    /**
     * Singleton
     *
     * @return
     */
    public static ConfigurationManagerController getInstance() {
        synchronized (classLock) {
            if (instance == null) {
                instance = new ConfigurationManagerController();
            }
            return instance;
        }
    }

    public void readControllerConfigFile() {

        Properties props = new Properties();
        try {
            props.load(new FileInputStream(PROJECT_1_CONTROLLER_CONFIG_FILE));

            try {
                String controllerPortString = props.getProperty("controllerPort").trim();
                controllerPort = (controllerPortString == null) ? defaultControllerPort
                        : Integer.parseInt(controllerPortString);
            } catch (Exception e) {
                controllerPort = defaultControllerPort;
                e.printStackTrace();
            }

            try {
                String bloomFilterLengthString = props.getProperty("BLOOM_FILTER_LENGTH").trim();
                this.bloomFilterLength = (bloomFilterLengthString == null) ? defaultLength
                        : Integer.parseInt(bloomFilterLengthString);
            } catch (Exception e) {
                this.bloomFilterLength = defaultLength;
                e.printStackTrace();
            }

            try{
                String hashTimeString = props.getProperty("HASH_TIME").trim();
                this.hashTime = (hashTimeString == null) ? defaultHashTime : Integer.parseInt(hashTimeString);
            } catch (Exception e) {
                this.hashTime = defaultHashTime;
                e.printStackTrace();
            }

            try{
                String hashSeedString = props.getProperty("HASH_SEED").trim();
                this.hashSeed = (hashSeedString == null) ? defaultHashSeed : Integer.parseInt(hashSeedString);
            } catch (Exception e){
                this.hashSeed = defaultHashSeed;
                e.printStackTrace();
            }

        } catch (Exception e) {
            System.err.println("Exception occured while parsing Configuration File:"
                    + PROJECT_1_CONTROLLER_CONFIG_FILE);
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

    public int getBloomFilterLength() {
        return bloomFilterLength;
    }

    public void setBloomFilterLength(int bloomFilterLength) {
        this.bloomFilterLength = bloomFilterLength;
    }

    public int getHashTime() {
        return hashTime;
    }

    public void setHashTime(int hashTime) {
        this.hashTime = hashTime;
    }

    public int getHashSeed() {
        return hashSeed;
    }

    public void setHashSeed(int hashSeed) {
        this.hashSeed = hashSeed;
    }

}
