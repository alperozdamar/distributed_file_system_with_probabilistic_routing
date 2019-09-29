package edu.usfca.cs.dfs.config;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * Singleton Configuration Manager for Project1.
 * 
 */
public class ConfigurationManagerClient {

    private static final String               PROJECT_1_CLIENT_CONFIG_FILE = "config"
            + File.separator + "project1_client.properties";

    private static ConfigurationManagerClient instance;
    private final static Object               classLock                    = new Object();

    private String                            controllerIp                 = "";
    private int                               controllerPort               = 9090;
    private long                              chunkSizeInBytes;

    private ConfigurationManagerClient() {
        readClientConfigFile();
    }

    /**
     * Singleton
     *  
     * @return
     */
    public static ConfigurationManagerClient getInstance() {
        synchronized (classLock) {
            if (instance == null) {
                instance = new ConfigurationManagerClient();
            }
            return instance;
        }
    }

    public void readClientConfigFile() {

        Properties props = new Properties();
        try {
            props.load(new FileInputStream(PROJECT_1_CLIENT_CONFIG_FILE));

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
                String chunkSizeInBytesString = props.getProperty("chunkSizeInBytes").trim();
                chunkSizeInBytes = (chunkSizeInBytesString == null) ? 1024
                        : Long.parseLong(chunkSizeInBytesString);
            } catch (Exception e) {
                chunkSizeInBytes = 1024;
                e.printStackTrace();
            }

        } catch (Exception e) {
            System.err.println("Exception occured while parsing Configuration File:"
                    + PROJECT_1_CLIENT_CONFIG_FILE);
            // e.printStackTrace(); //professor doesn't want stackTrace.
            e.getMessage();
        }
    }

    @Override
    public String toString() {
        return "ConfigurationManagerClient [controllerIp=" + controllerIp + ", controllerPort="
                + controllerPort + ", chunkSizeInBytes=" + chunkSizeInBytes + "]";
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

    public long getChunkSizeInBytes() {
        return chunkSizeInBytes;
    }

    public void setChunkSizeInBytes(long chunkSizeInBytes) {
        this.chunkSizeInBytes = chunkSizeInBytes;
    }

}
