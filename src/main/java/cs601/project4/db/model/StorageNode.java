package cs601.project4.db.model;

import java.util.ArrayList;
import java.util.Iterator;

public class StorageNode {

    private int                snId;

    private ArrayList<Integer> replicateSnIdList;

    private ArrayList<Integer> backupIdSnList;

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

}
