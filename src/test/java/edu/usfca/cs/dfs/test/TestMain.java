package edu.usfca.cs.dfs.test;

import org.junit.Assert;
import org.junit.Test;

import edu.usfca.cs.db.DbManager;
import edu.usfca.cs.db.SqlManager;
import edu.usfca.cs.db.model.StorageNode;

public class TestMain {

    public static void main(String[] args) {
        long fileSize = 118;
        long chunkSize = 2048;
        System.out.format("The size of the file: %d bytes", fileSize);
        System.out.format("\nThe size of chunks: %d bytes", chunkSize);
        long numOfChunks = (long) Math.ceil((float) fileSize / (float) chunkSize);
        System.out.format("\nNumber Of Chunks is %d for file size:%d bytes", numOfChunks, fileSize);
        long lastChunkByteSize = fileSize % chunkSize;
        System.out.format("\nlastChunkByteSize is %d for file size:%d bytes",
                          lastChunkByteSize,
                          fileSize);
    }

    @Test
    public void testDBSnReplicationTable() {
        try {
            DbManager.getInstance();
            StorageNode storageNode = SqlManager.getInstance().getSNReplication(1);
            System.out.println(storageNode.toString());

            SqlManager.getInstance().insertSNReplication(13, 14, 0);
            SqlManager.getInstance().insertSNReplication(13, 15, 0);
            storageNode = SqlManager.getInstance().getSNReplication(13);
            System.out.println(storageNode.toString());

            SqlManager.getInstance().updateSNReplication(13, 14, -1);
            storageNode = SqlManager.getInstance().getSNReplication(13);
            System.out.println(storageNode.toString());

            SqlManager.getInstance().deleteSNReplication(13);
            storageNode = SqlManager.getInstance().getSNReplication(13);
            System.out.println(storageNode.toString());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDBSnInfoTable() {
        try {
            DbManager.getInstance();
            long totalStorageReq = 0;
            long totalFreeSpace = 10000;
            long totalRetrievelReq = 0;
            for (int i = 1; i < 13; i++) {

                boolean result = SqlManager.getInstance().insertSN(i,
                                                                   "192.168.1.1",
                                                                   7070,
                                                                   totalFreeSpace + i * 100,
                                                                   totalStorageReq,
                                                                   totalRetrievelReq);

                Assert.assertTrue(result);
                System.out.println("SN-" + i + " ,successfully inserted!");

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
