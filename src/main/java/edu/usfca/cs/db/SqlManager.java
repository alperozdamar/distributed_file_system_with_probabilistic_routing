package edu.usfca.cs.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.db.model.StorageNode;

/**
 * SqlManager is for querying,inserting or updating sql tables. Main class for all SQL operations.
 * 
 */
public class SqlManager {

    private static Logger     logger = LogManager.getLogger(SqlManager.class);
    private static SqlManager instance;

    public static SqlManager getInstance() {
        synchronized (SqlManager.class) {
            if (instance == null) {
                instance = new SqlManager();
            }
        }
        return instance;
    }

    private SqlManager() {

    }

    /**
     * 
     * @param snId
     * @return
     */
    public StorageNode getSNReplication(int snId) {
        StorageNode storageNode = null;
        ArrayList<Integer> replicateSnIdList = null;
        ArrayList<Integer> backupIdSnList = null;
        Connection connection = null;
        String sql = "select * from sn_replication s where s.snId = ?";
        PreparedStatement selectStatement = null;
        try {
            connection = DbManager.getInstance().getBds().getConnection();
            selectStatement = connection.prepareStatement(sql);
            if (logger.isDebugEnabled()) {
                logger.debug("Queried snId:" + snId);
            }
            selectStatement.setInt(1, snId);
            ResultSet resultSet = selectStatement.executeQuery();
            if (resultSet.next()) {
                storageNode = new StorageNode();
                replicateSnIdList = new ArrayList<>();
                backupIdSnList = new ArrayList<>();
                do {
                    storageNode.setSnId(resultSet.getInt("snId"));
                    replicateSnIdList.add(resultSet.getInt("replicaId"));
                    backupIdSnList.add(resultSet.getInt("backupId"));
                } while (resultSet.next());
            } else {
                logger.debug("Storage Node can not be found in DB.");
            }
            storageNode.setBackupIdSnList(backupIdSnList);
            storageNode.setReplicateSnIdList(replicateSnIdList);
            selectStatement.close();
            resultSet.close();
        } catch (SQLException e) {
            logger.error("Error:", e);
            e.printStackTrace();
            return null;
        } catch (Exception e) {
            logger.error("Exception occured:", e);
            e.printStackTrace();
            return null;
        } finally {
            try {
                if (selectStatement != null)
                    selectStatement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return storageNode;
    }

    /**
     * 
     * @param snId
     * @param replicaId
     * @param backupId
     * @return
     */
    public synchronized boolean updateSNReplication(int snId, int replicaId, int backupId) {
        boolean result = false;
        Connection connection = null;
        String sql = SqlConstants.UPDATE_SN_REPLICATION_BY_SNID;
        PreparedStatement updateStatement = null;
        try {
            connection = DbManager.getInstance().getBds().getConnection();
            updateStatement = connection.prepareStatement(sql);
            updateStatement.setInt(1, replicaId);
            updateStatement.setInt(2, backupId);
            updateStatement.setInt(3, snId);
            updateStatement.execute();
            result = true;
        } catch (SQLException e) {
            logger.error("Error:", e);
            e.printStackTrace();
            return false;
        } catch (Exception e) {
            logger.error("Exception occured:", e);
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (updateStatement != null)
                    updateStatement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * 
     * @param snId
     * @return
     */
    public synchronized boolean deleteSNReplication(int snId) {
        boolean result = false;
        Connection connection = null;
        String sql = SqlConstants.DELETE_SN_REPLICATION_BY_SNID;
        PreparedStatement deleteStatement = null;
        try {
            connection = DbManager.getInstance().getBds().getConnection();
            deleteStatement = connection.prepareStatement(sql);
            deleteStatement.setInt(1, snId);
            deleteStatement.execute();
            result = true;
        } catch (SQLException e) {
            logger.error("Error:", e);
            e.printStackTrace();
            return false;
        } catch (Exception e) {
            logger.error("Exception occured:", e);
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (deleteStatement != null)
                    deleteStatement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * 
     * @param snId
     * @param replicaId
     * @param backupId
     * @return
     */
    public synchronized boolean insertSNReplication(int snId, int replicaId, int backupId) {
        boolean result = false;
        Connection connection = null;
        if (logger.isDebugEnabled()) {
            logger.debug("snId:" + snId);
            logger.debug("replicaId:" + replicaId);
            logger.debug("backupId:" + backupId);
        }
        String sql = SqlConstants.INSERT_SN_REPLICATION;
        PreparedStatement insertStatement = null;
        try {
            connection = DbManager.getInstance().getBds().getConnection();
            insertStatement = connection.prepareStatement(sql);
            insertStatement.setInt(1, snId);
            insertStatement.setInt(2, replicaId);
            insertStatement.setInt(3, backupId);
            insertStatement.execute();
            result = true;
        } catch (SQLException e) {
            logger.error("Error:", e);
            e.printStackTrace();
            return false;
        } catch (Exception e) {
            logger.error("Exception occured:", e);
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (insertStatement != null)
                    insertStatement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public synchronized boolean insertSN(int snId, String snIp, int snPort, long totalFreeSpace,
                                         long totalStorageReq, long totalRetrievelReq) {
        boolean result = false;
        Connection connection = null;
        if (logger.isDebugEnabled()) {
            logger.debug("snId:" + snId);
            logger.debug("snIp:" + snIp);
            logger.debug("snPort:" + snPort);
            logger.debug("totalFreeSpace:" + totalFreeSpace);
            logger.debug("totalStorageReq:" + totalStorageReq);
            logger.debug("totalRetrievelReq:" + totalRetrievelReq);
        }
        String sql = SqlConstants.INSERT_SN;
        PreparedStatement insertStatement = null;
        try {
            connection = DbManager.getInstance().getBds().getConnection();
            insertStatement = connection.prepareStatement(sql);
            insertStatement.setInt(1, snId);
            insertStatement.setString(2, snIp);
            insertStatement.setInt(3, snPort);
            insertStatement.setLong(4, totalFreeSpace);
            insertStatement.setLong(5, totalStorageReq);
            insertStatement.setLong(6, totalRetrievelReq);
            insertStatement.execute();
            result = true;
        } catch (SQLException e) {
            logger.error("Error:", e);
            e.printStackTrace();
            return false;
        } catch (Exception e) {
            logger.error("Exception occured:", e);
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (insertStatement != null)
                    insertStatement.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }

}
