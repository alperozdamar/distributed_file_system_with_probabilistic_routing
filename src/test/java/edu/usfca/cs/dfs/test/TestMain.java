package edu.usfca.cs.dfs.test;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import edu.usfca.cs.db.DbManager;
import edu.usfca.cs.db.SqlManager;
import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.config.Constants;

public class TestMain {

    public static void main(String[] args) {
        //        long fileSize = 118;
        //        long chunkSize = 2048;
        //        System.out.format("The size of the file: %d bytes", fileSize);
        //        System.out.format("\nThe size of chunks: %d bytes", chunkSize);
        //        long numOfChunks = (long) Math.ceil((float) fileSize / (float) chunkSize);
        //        System.out.format("\nNumber Of Chunks is %d for file size:%d bytes", numOfChunks, fileSize);
        //        long lastChunkByteSize = fileSize % chunkSize;
        //        System.out.format("\nlastChunkByteSize is %d for file size:%d bytes",
        //                          lastChunkByteSize,
        //                          fileSize);

        long memoryFreeSpace = new File("/").getFreeSpace();

        System.out.println("Disk Free Space:" + memoryFreeSpace);
    }

    @Test
    public void testGetFreeSpaceInFileSystem() {

        long freeSpace = new File("/").getFreeSpace();

        System.out.println("Disk Free Space:" + freeSpace);

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
                StorageNode storageNode = new StorageNode();
                storageNode.setSnId(i);
                storageNode.setSnIp("192.168.1.1");
                storageNode.setSnPort(7070);
                storageNode.setTotalFreeSpace(totalFreeSpace + i * 1000);
                storageNode.setStatus(Constants.STATUS_OPERATIONAL);
                boolean result = SqlManager.getInstance().insertSN(storageNode);

                Assert.assertTrue(result);
                System.out.println("SN-" + i + " ,successfully inserted!");

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
