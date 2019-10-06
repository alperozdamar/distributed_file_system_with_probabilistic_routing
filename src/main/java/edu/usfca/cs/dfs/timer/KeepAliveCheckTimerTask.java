package edu.usfca.cs.dfs.timer;

import edu.usfca.cs.Utils;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.config.ConfigurationManagerClient;
import edu.usfca.cs.dfs.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.db.SqlManager;
import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.DfsControllerStarter;
import edu.usfca.cs.dfs.config.ConfigurationManagerController;
import edu.usfca.cs.dfs.config.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class KeepAliveCheckTimerTask implements Runnable {

    private static Logger logger = LogManager.getLogger(KeepAliveCheckTimerTask.class);
    private int           snId;
    private long          timeOut;

    public KeepAliveCheckTimerTask(int snId) {
        this.snId = snId;
        this.timeOut = ConfigurationManagerController.getInstance()
                .getHeartBeatTimeoutInMilliseconds();
    }

    private void backup(HashMap<Integer, StorageNode> availableSNs){
        int backupId = -1;
        SqlManager sqlManager = SqlManager.getInstance();
        Random rand = new Random();
        List<StorageNode> listSN = new ArrayList<StorageNode>(availableSNs.values());
        while(backupId==-1){
            int index = rand.nextInt(availableSNs.size());
            StorageNode sn = listSN.get(index);
            backupId = sn.getSnId();
        }
        StorageNode backupNode = sqlManager.getSNInformationById(backupId);

        //Backup data of current node
        //Send current down SN data to backup ID
        StorageNode downNode = sqlManager.getSNReplication(snId);
        ArrayList<Integer> replicateIdList = downNode.getReplicateSnIdList();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(Constants.CONTROLLER);

        Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);
        for(int replicateId : replicateIdList){
            StorageNode snNode = sqlManager.getSNInformationById(replicateId);
            if(snNode.getStatus().equals("DOWN")){
                continue;
            } else {
                ChannelFuture cf = Utils
                        .connect(bootstrap,
                                snNode.getSnIp(),
                                snNode.getSnPort());
                StorageMessages.BackUp backUpMsg = StorageMessages.BackUp.newBuilder()
                        .setDestinationIp(backupNode.getSnIp())
                        .setDestinationPort(backupNode.getSnPort())
                        .setSourceSnId(snId).build();
                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder().setBackup(backUpMsg).build();
                cf.channel().writeAndFlush(msgWrapper).syncUninterruptibly();
                break;
            }
        }

        //Source of replication
        //Send replicate data of current down SN to backup ID
        downNode = sqlManager.getSourceReplicationSnId(snId);
        ArrayList<Integer> sourceIdList = downNode.getSourceSnIdList();
        for(int sourceId : sourceIdList){
            StorageNode sourceNode = sqlManager.getSNInformationById(sourceId);
            String fromIp = "";
            int fromPort = 0;
            if(sourceNode.getStatus().equals("DOWN")){//SourceNode down, get data from sourceNode replica
                ArrayList<Integer> sourceReplicaIdList = sqlManager.getSNReplication(sourceId).getReplicateSnIdList();
                for(int sourceReplicaId : sourceReplicaIdList){
                    StorageNode sourceReplication = sqlManager.getSNInformationById(sourceReplicaId);
                    if(!sourceReplication.getStatus().equals("DOWN")){
                        fromIp = sourceReplication.getSnIp();
                        fromPort = sourceReplication.getSnPort();
                        break;
                    }
                }
            } else {
                fromIp = sourceNode.getSnIp();
                fromPort = sourceNode.getSnPort();
            }
            if(!fromIp.isEmpty() && fromPort!=0){
                ChannelFuture cf = Utils
                        .connect(bootstrap, fromIp, fromPort);
                StorageMessages.BackUp backUpMsg = StorageMessages.BackUp.newBuilder()
                        .setDestinationIp(backupNode.getSnIp())
                        .setDestinationPort(backupNode.getSnPort())
                        .setSourceSnId(sourceId).build();
                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder().setBackup(backUpMsg).build();
                cf.channel().writeAndFlush(msgWrapper).syncUninterruptibly();
            } else {
                System.out.printf("[Controller][BackUp] All source of data %d down!\n",sourceId);
            }
        }
    }

    public void run() {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Apply Keep Alive Checker for snId :" + snId);
            }
            DfsControllerStarter dfsControllerStarter = DfsControllerStarter.getInstance();
            StorageNode storageNode = dfsControllerStarter.getStorageNodeHashMap()
                    .get(snId);
            if (storageNode != null) {
                SqlManager sqlManager = SqlManager.getInstance();
                long currentTime = System.currentTimeMillis();

                if (logger.isDebugEnabled()) {
                    logger.info("SnId :" + snId + ",LastHeartBeatTime:"
                            + storageNode.getLastHeartBeatTime() + ",currenTime:" + currentTime
                            + ",timeout:" + timeOut);
                    logger.info("(currentTime - timeOut) :" + (currentTime - timeOut));
                    logger.debug("CurrenTime :" + currentTime);
                    logger.debug("LastHeartBeatTime:" + storageNode.getLastHeartBeatTime());
                    logger.debug("Timeout:" + timeOut);
                    logger.debug("(currentTime - timeOut) :" + (currentTime - timeOut));
                }
                if ((currentTime - timeOut) > storageNode.getLastHeartBeatTime()) {
                    logger.debug("Timeout occured for SN[" + snId + "], No heart beat since "
                            + timeOut + " milliseconds!");
                    storageNode.setStatus(Constants.STATUS_DOWN);
                    sqlManager.updateSNInformation(snId, Constants.STATUS_DOWN);
                    HashMap<Integer,StorageNode> availableSNs = sqlManager.getAllOperationalSNList();
                    int lowerBound = (snId-2)%dfsControllerStarter.getStorageNodeHashMap().size();
                    int upperBound = (snId+2)%dfsControllerStarter.getStorageNodeHashMap().size();
                    if(lowerBound<upperBound){
                        for(int i=lowerBound;i<=upperBound;i++){
                            availableSNs.remove(i);
                        }
                    } else {
                        for(int i=lowerBound;i<=dfsControllerStarter.getStorageNodeHashMap().size();i++){
                            availableSNs.remove(i);
                        }
                        for(int i=1;i<=lowerBound;i++) {
                            availableSNs.remove(i);
                        }
                    }
                    if(availableSNs.size()==0){
                        System.out.println("No SN to replicate");
                        return;
                    }
                    this.backup(availableSNs);
                }
            } else {
                /**
                 * STRANGE BIG PROBLEM!!
                 */
            }
        } catch (Exception e) {
            logger.error("Exception occured in HeartBeat:", e);
        }
    }

}
