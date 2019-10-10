package edu.usfca.cs.dfs.net;

import edu.usfca.cs.dfs.config.ConfigurationManagerClient;
import edu.usfca.cs.dfs.config.ConfigurationManagerController;
import edu.usfca.cs.dfs.config.ConfigurationManagerSn;
import edu.usfca.cs.dfs.config.Constants;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.LinkedList;
import java.util.Queue;

public class NetUtils {

    private static Logger logger = LogManager.getLogger(NetUtils.class);
    private static Queue<Integer> availablePort = new LinkedList<>();

    private static NetUtils instance;
    private final static Object classLock = new Object();

    private String sourceHostName = "";

    private int fromPort = 0;
    private int toPort = 0;

    private NetUtils(String mode) {
        availablePort = new LinkedList<>();
        switch (mode){
            case Constants.CLIENT:
                fromPort = Math.max(ConfigurationManagerClient.getInstance().getFromPort(), fromPort);
                sourceHostName = ConfigurationManagerClient.getInstance().getMyIp();
                break;
            case Constants.CONTROLLER:
                fromPort = Math.max(ConfigurationManagerController.getInstance().getFromPort(), fromPort);
                sourceHostName = ConfigurationManagerController.getInstance().getMyIp();
                break;
            case Constants.STORAGENODE:
                fromPort = Math.max(ConfigurationManagerSn.getInstance().getFromPort(), fromPort);
                sourceHostName = ConfigurationManagerSn.getInstance().getMyIp();
                break;
        }

        for(int i=fromPort;i<fromPort+100;i++){
            availablePort.add(i);
        }
        toPort = fromPort+100;
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
        ChannelFuture cf= null;
        while(cf==null) {
            try {
                if(availablePort.size()>0){
//            System.out.println("[NetUtils] Size: "+ availablePort.size());
                    sourcePort = availablePort.poll();
                    logger.info("Connect from port: %d to port: %d\n",sourcePort, port);
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

    public void releasePort(int port){
        if(port<toPort && port>=fromPort) {
            logger.info("Release port: %d\n",port);
            availablePort.add(port);
        }
    }
}
