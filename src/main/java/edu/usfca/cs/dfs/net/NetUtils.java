package edu.usfca.cs.dfs.net;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.config.ConfigManagerClient;
import edu.usfca.cs.dfs.config.ConfigManagerController;
import edu.usfca.cs.dfs.config.ConfigManagerSn;
import edu.usfca.cs.dfs.config.Constants;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;

public class NetUtils {

    private static Logger         logger         = LogManager.getLogger(NetUtils.class);
    private static Queue<Integer> availablePort  = new LinkedList<>();

    private static NetUtils       instance;
    private final static Object   classLock      = new Object();

    private String                sourceHostName = "";

    private int                   fromPort       = 0;
    private int                   toPort         = 0;
    private int                   myPortRange    = 100;

    private NetUtils(String mode) {
        availablePort = new LinkedList<>();
        switch (mode) {
        case Constants.CLIENT:
            fromPort = Math.max(ConfigManagerClient.getInstance().getFromPort(), fromPort);
            sourceHostName = ConfigManagerClient.getInstance().getMyIp();
            myPortRange = ConfigManagerClient.getInstance().getMyPortRange();
            break;
        case Constants.CONTROLLER:
            fromPort = Math.max(ConfigManagerController.getInstance().getFromPort(), fromPort);
            sourceHostName = ConfigManagerController.getInstance().getMyIp();
            myPortRange = ConfigManagerController.getInstance().getMyPortRange();
            break;
        case Constants.STORAGENODE:
            fromPort = Math.max(ConfigManagerSn.getInstance().getFromPort(), fromPort);
            sourceHostName = ConfigManagerSn.getInstance().getMyIp();
            myPortRange = ConfigManagerSn.getInstance().getMyPortRange();
            break;
        }

        for (int i = fromPort; i < fromPort + myPortRange; i++) {
            availablePort.add(i);
        }
        toPort = fromPort + myPortRange;

    }

    /**
     * Singleton: getInstance
     *
     * @return
     */
    public static NetUtils getInstance(String mode) {
        synchronized (classLock) {
            if (instance == null) {
                instance = new NetUtils(mode);
            }
            return instance;
        }
    }

    public synchronized ChannelFuture connect(Bootstrap bootstrap, String ip, int port) {
        SocketAddress destAddr = new InetSocketAddress(ip, port);
        int sourcePort = 0;
        ChannelFuture cf = null;
        while (cf == null) {
            try {
                if (availablePort.size() > 0) {
                    //            System.out.println("[NetUtils] Size: "+ availablePort.size());
                    sourcePort = availablePort.poll();
                    logger.info("Connect from port: " + sourcePort + " to port: " + port);
                } else {
                    logger.error("[NetUtils] Out of port");
                }
                SocketAddress sourceAddr = new InetSocketAddress(sourceHostName, sourcePort);
                cf = bootstrap.connect(destAddr, sourceAddr);
            } catch (Exception e) {
                cf = null;
                continue;
            }
        }
        cf.syncUninterruptibly();
        return cf;
    }

    public void releasePort(int port) {
        if (port < toPort && port >= fromPort) {
            logger.info("Release port: " + port);
            availablePort.add(port);
        }
    }
}
