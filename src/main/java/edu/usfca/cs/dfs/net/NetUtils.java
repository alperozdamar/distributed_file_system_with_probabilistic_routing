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
        switch (mode){
            case Constants.CLIENT:
                fromPort = Math.max(ConfigurationManagerClient.getInstance().getFromPort(), fromPort);
                sourceHostName = ConfigurationManagerClient.getInstance().getControllerIp();
            case Constants.CONTROLLER:
                fromPort = Math.max(ConfigurationManagerController.getInstance().getFromPort(), fromPort);
                sourceHostName = ConfigurationManagerController.getInstance().getControllerIp();
            case Constants.STORAGENODE:
                fromPort = Math.max(ConfigurationManagerSn.getInstance().getFromPort(), fromPort);
                sourceHostName = ConfigurationManagerSn.getInstance().getControllerIp();
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

    public ChannelFuture connect(Bootstrap bootstrap, String ip, int port) {
        SocketAddress destAddr = new InetSocketAddress(ip, port);
        System.out.println("Available port size:"+availablePort.size());
        int sourcePort = availablePort.poll();
        SocketAddress sourceAddr = new InetSocketAddress(sourceHostName, sourcePort);
        ChannelFuture cf = bootstrap.connect(destAddr, sourceAddr);
        cf.syncUninterruptibly();
        return cf;
    }

    public void releasePort(int port){
        if(port<toPort && port>=fromPort) {
            availablePort.add(port);
        }
    }
}
