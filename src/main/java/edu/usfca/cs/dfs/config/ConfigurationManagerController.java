package edu.usfca.cs.dfs.config;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * Singleton Configuration Manager for Project1.
 * 
 */
public class ConfigurationManagerController {

    private final int defaultFilterLength = 1000;
    private final int defaultHashTime = 3;
    private final int defaultHashSeed = 3;
    private final int defaultControllerPort = 8080;


    public static final String                    PROJECT_1_CONTROLLER_CONFIG_FILE = "config"
            + File.separator + "project1_controller.properties";
    private static ConfigurationManagerController instance;
    private final static Object                   classLock                        = new Object();
    private String                                controllerIp                     = "";
    private int                                   controllerPort                   = defaultControllerPort;
    private int                                   filterLength = defaultFilterLength;
    private int                                   hashTime = defaultHashTime;
    private long                                  seed = defaultHashSeed;

    private ConfigurationManagerController() {
        readClientConfigFile();
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

    public void readClientConfigFile() {

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
                String filterLengthString = props.getProperty("BLOOM_FILTER_LENGTH").trim();
                filterLength = (filterLengthString == null) ? defaultFilterLength
                        : Integer.parseInt(filterLengthString);
            } catch (Exception e) {
                filterLength = defaultFilterLength;
                e.printStackTrace();
            }
            try {
                String hashTimeString = props.getProperty("HASH_TIME").trim();
                hashTime = (hashTimeString == null) ? defaultHashTime : Integer.parseInt(hashTimeString);
            } catch (Exception e) {
                hashTime = defaultHashTime;
                e.printStackTrace();
            }
            try {
                String seedString = props.getProperty("HASH_SEED").trim();
                seed = (seedString == null) ? defaultHashSeed : Integer.parseInt(seedString);
            } catch (Exception e) {
                seed = defaultHashSeed;
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

    @Override
    public String toString() {
        return "ConfigurationManagerController [controllerIp=" + controllerIp + ", controllerPort="
                + controllerPort + "]";
    }

    public int getFilterLength() {
        return filterLength;
    }

    public void setFilterLength(int filterLength) {
        this.filterLength = filterLength;
    }

    public int getHashTime() {
        return hashTime;
    }

    public void setHashTime(int hashTime) {
        this.hashTime = hashTime;
    }

    public long getSeed() {
        return seed;
    }

    public void setSeed(long seed) {
        this.seed = seed;
    }

}
