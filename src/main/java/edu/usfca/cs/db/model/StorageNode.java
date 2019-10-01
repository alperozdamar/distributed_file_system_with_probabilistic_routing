package edu.usfca.cs.db.model;

import java.util.ArrayList;
import java.util.Iterator;

public class StorageNode {

    private int                snId;

    private ArrayList<Integer> replicateSnIdList;

    private ArrayList<Integer> backupIdSnList;

    private String             snIp;

    private int                snPort;

    private long               totalFreeSpace;

    private long               totalStorageRequest   = 0;

    private long               totalRetrievelRequest = 0;

    public StorageNode() {
        super();
        // TODO Auto-generated constructor stub
    }

    public StorageNode(int snId, ArrayList<Integer> replicateSnIdList,
                       ArrayList<Integer> backupIdSnList, String snIp, int snPort,
                       long totalFreeSpace) {
        super();
        this.snId = snId;
        this.replicateSnIdList = replicateSnIdList;
        this.backupIdSnList = backupIdSnList;
        this.snIp = snIp;
        this.snPort = snPort;
        this.totalFreeSpace = totalFreeSpace;
        this.totalStorageRequest = 0;
        this.totalRetrievelRequest = 0;
    }

    public int getSnId() {
        return snId;
    }

    public void setSnId(int snId) {
        this.snId = snId;
    }

    public ArrayList<Integer> getReplicateSnIdList() {
        return replicateSnIdList;
    }

    public void setReplicateSnIdList(ArrayList<Integer> replicateSnIdList) {
        this.replicateSnIdList = replicateSnIdList;
    }

    public ArrayList<Integer> getBackupIdSnList() {
        return backupIdSnList;
    }

    public void setBackupIdSnList(ArrayList<Integer> backupIdSnList) {
        this.backupIdSnList = backupIdSnList;
    }

    @Override
    public String toString() {
        StringBuffer stringBuffer = new StringBuffer();

        if (this.snId <= 0) {
            return "No SN found!";
        }

        stringBuffer.append("SnId:");
        stringBuffer.append(this.getSnId());
        stringBuffer.append("|");
        stringBuffer.append("replicationId:");
        if (this.replicateSnIdList != null) {
            for (Iterator iterator = this.getReplicateSnIdList().iterator(); iterator.hasNext();) {
                int replicationId = (int) iterator.next();
                stringBuffer.append(replicationId);
                if (iterator.hasNext())
                    stringBuffer.append(",");
            }
        }
        if (this.backupIdSnList != null) {
            stringBuffer.append("|");
            stringBuffer.append("backUpId:");
            for (Iterator iterator = this.getBackupIdSnList().iterator(); iterator.hasNext();) {
                int backUpId = (int) iterator.next();
                stringBuffer.append(backUpId);
                if (iterator.hasNext())
                    stringBuffer.append(",");
            }
        }
        return stringBuffer.toString();
    }

    public String getSnIp() {
        return snIp;
    }

    public void setSnIp(String snIp) {
        this.snIp = snIp;
    }

    public int getSnPort() {
        return snPort;
    }

    public void setSnPort(int snPort) {
        this.snPort = snPort;
    }

    public long getTotalStorageRequest() {
        return totalStorageRequest;
    }

    public void setTotalStorageRequest(long totalStorageRequest) {
        this.totalStorageRequest = totalStorageRequest;
    }

    public long getTotalRetrievelRequest() {
        return totalRetrievelRequest;
    }

    public void setTotalRetrievelRequest(long totalRetrievelRequest) {
        this.totalRetrievelRequest = totalRetrievelRequest;
    }

    public long getTotalFreeSpace() {
        return totalFreeSpace;
    }

    public void setTotalFreeSpace(long totalFreeSpace) {
        this.totalFreeSpace = totalFreeSpace;
    }

}
