package edu.usfca.cs.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

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
                do {
                    storageNode.setSnId(resultSet.getInt("snId"));
                    replicateSnIdList.add(resultSet.getInt("replicaId"));
                } while (resultSet.next());
                storageNode.setReplicateSnIdList(replicateSnIdList);
            } else {
                logger.debug("Storage Node can not be found in DB.");
            }
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
     * @return
     */
    public StorageNode getSNInformationById(int snId) {
        StorageNode storageNode = null;
        Connection connection = null;
        String sql = "select * from sn_information s where s.snId = ?";
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
                do {
                    storageNode.setSnId(resultSet.getInt("snId"));
                    storageNode.setSnIp(resultSet.getString("snIP"));
                    storageNode.setSnPort(resultSet.getInt("snPort"));
                    storageNode.setTotalFreeSpace(resultSet.getLong("totalFreeSpace"));
                    storageNode.setTotalStorageRequest(resultSet.getInt("totalStorageReq"));
                    storageNode.setTotalRetrievelRequest(resultSet.getInt("totalRetrievelReq"));
                    storageNode.setStatus(resultSet.getString("status"));
                } while (resultSet.next());
            } else {
                logger.debug("Storage Node can not be found in DB.");
            }
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
     * @param backupId
     * @return
     */
    public synchronized boolean updateSNReplication(int snId, int backupId) {
        boolean result = false;
        Connection connection = null;
        String sql = SqlConstants.UPDATE_SN_REPLICATION_BY_SNID;
        PreparedStatement updateStatement = null;
        try {
            connection = DbManager.getInstance().getBds().getConnection();
            updateStatement = connection.prepareStatement(sql);
            updateStatement.setInt(1, backupId);
            updateStatement.setInt(2, snId);
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
     * @return
     */
    public synchronized boolean insertSNReplication(int snId, int replicaId) {
        boolean result = false;
        Connection connection = null;
        if (logger.isDebugEnabled()) {
            logger.debug("Inserting sn_replication snId:" + snId + ",replicaId:" + replicaId);
        }
        String sql = SqlConstants.INSERT_SN_REPLICATION;
        PreparedStatement insertStatement = null;
        try {
            connection = DbManager.getInstance().getBds().getConnection();
            insertStatement = connection.prepareStatement(sql);
            insertStatement.setInt(1, snId);
            insertStatement.setInt(2, replicaId);
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

    public synchronized boolean insertSN(StorageNode storageNode) {
        boolean result = false;
        Connection connection = null;
        if (logger.isDebugEnabled()) {
            logger.debug(storageNode.toString());
        }
        String sql = SqlConstants.INSERT_SN;
        PreparedStatement insertStatement = null;
        try {
            connection = DbManager.getInstance().getBds().getConnection();
            insertStatement = connection.prepareStatement(sql);
            insertStatement.setInt(1, storageNode.getSnId());
            insertStatement.setString(2, storageNode.getSnIp());
            insertStatement.setInt(3, storageNode.getSnPort());
            insertStatement.setLong(4, storageNode.getTotalFreeSpace());
            insertStatement.setLong(5, storageNode.getTotalStorageRequest());
            insertStatement.setLong(6, storageNode.getTotalRetrievelRequest());
            insertStatement.setString(7, storageNode.getStatus());
            insertStatement.setInt(8, storageNode.getBackupId());
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

    public boolean deleteAllSNs() {
        boolean result = false;
        Connection connection = null;
        String sql = SqlConstants.DELETE_ALL_SNS;
        PreparedStatement deleteStatement = null;
        try {
            connection = DbManager.getInstance().getBds().getConnection();
            deleteStatement = connection.prepareStatement(sql);
            deleteStatement.execute();
            result = true;
            System.out.println("All SN data is cleared from DB.");
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

    public boolean updateSNInformation(int snId, String newStatus) {
        boolean result = false;
        Connection connection = null;
        String sql = SqlConstants.UPDATE_SN;
        PreparedStatement updateStatement = null;
        try {
            connection = DbManager.getInstance().getBds().getConnection();
            updateStatement = connection.prepareStatement(sql);
            updateStatement.setString(1, newStatus);
            updateStatement.setInt(2, snId);
            updateStatement.execute();
            result = true;
            System.out.println("SN info is updated from DB.");
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

    public HashMap<Integer, StorageNode> getAllSNByStatusList(String status) {
        HashMap<Integer, StorageNode> availableStorageNodeMap = new HashMap<Integer, StorageNode>();
        ArrayList<Integer> replicateSnIdList = null;
        ArrayList<Integer> backupIdSnList = null;
        Connection connection = null;
        String sql = "";
        if (status != null) {
            sql = "select * from sn_information sn, sn_replication sr where status = '" + status
                    + "' and sn.snId=sr.snId";
        } else {
            sql = "select * from sn_information sn, sn_replication sr where sn.snId=sr.snId";
        }
        PreparedStatement selectStatement = null;
        try {
            connection = DbManager.getInstance().getBds().getConnection();
            selectStatement = connection.prepareStatement(sql);
            if (logger.isDebugEnabled()) {
                logger.debug("Getting ALL OPERATIONAL SNs from db.");
            }
            ResultSet resultSet = selectStatement.executeQuery();
            if (resultSet.next()) {
                do {
                    int snId = resultSet.getInt("snId");
                    StorageNode storageNode = availableStorageNodeMap.get(snId);
                    if (storageNode == null) {
                        storageNode = new StorageNode();
                        storageNode.setSnId(snId);
                        storageNode.setStatus(resultSet.getString("status"));
                        storageNode.setSnIp(resultSet.getString("snIP"));
                        storageNode.setSnPort(resultSet.getInt("snPort"));
                        storageNode.setTotalFreeSpace(resultSet.getLong("totalFreeSpace"));
                        storageNode.setTotalStorageRequest(resultSet.getInt("totalStorageReq"));
                        storageNode.setTotalRetrievelRequest(resultSet.getInt("totalRetrievelReq"));
                        replicateSnIdList = new ArrayList<>();
                        backupIdSnList = new ArrayList<>();
                        replicateSnIdList.add(resultSet.getInt("replicaId"));
                        backupIdSnList.add(resultSet.getInt("backupId"));
                        storageNode.setBackupIdSnList(backupIdSnList);
                        storageNode.setReplicateSnIdList(replicateSnIdList);
                        availableStorageNodeMap.put(storageNode.getSnId(), storageNode);
                    } else {
                        storageNode.getBackupIdSnList().add(resultSet.getInt("backupId"));
                        storageNode.getReplicateSnIdList().add(resultSet.getInt("replicaId"));
                    }

                } while (resultSet.next());
            } else {
                logger.debug("Storage Node can not be found in DB.");
            }
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
        return availableStorageNodeMap;
    }

    public boolean deleteAllSNsReplications() {
        boolean result = false;
        Connection connection = null;
        String sql = SqlConstants.DELETE_ALL_SNS_REPLICATION;
        PreparedStatement deleteStatement = null;
        try {
            connection = DbManager.getInstance().getBds().getConnection();
            deleteStatement = connection.prepareStatement(sql);
            deleteStatement.execute();
            result = true;
            System.out.println("All SN_REPLICATION data is cleared from DB.");
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

    public int getMaxSnId() {
        Connection connection = null;
        int maxSnId = 0;
        String sql = "SELECT snId from sn_information ORDER BY snId DESC LIMIT 1";
        PreparedStatement selectStatement = null;
        try {
            connection = DbManager.getInstance().getBds().getConnection();
            selectStatement = connection.prepareStatement(sql);
            if (logger.isDebugEnabled()) {
                logger.debug("Queried MAX(snId).");
            }

            ResultSet resultSet = selectStatement.executeQuery();
            if (resultSet.next()) {
                maxSnId = resultSet.getInt("snId");
            } else {
                logger.debug("No Storage Node can not be found in DB.");
            }
            selectStatement.close();
            resultSet.close();
        } catch (SQLException e) {
            logger.error("Error:", e);
            e.printStackTrace();
            return -1;
        } catch (Exception e) {
            logger.error("Exception occured:", e);
            e.printStackTrace();
            return -1;
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
        return maxSnId;
    }

    /**
     *
     * @param snId
     * @return
     */
    public StorageNode getSourceReplicationSnId(int snId) {
        StorageNode storageNode = null;
        ArrayList<Integer> sourceSnId = null;
        Connection connection = null;
        String sql = "select * from sn_replication s where s.replicaId = ?";
        PreparedStatement selectStatement = null;
        try {
            connection = DbManager.getInstance().getBds().getConnection();
            selectStatement = connection.prepareStatement(sql);
            if (logger.isDebugEnabled()) {
                logger.debug("Queried replicaId:" + snId);
            }
            selectStatement.setInt(1, snId);
            ResultSet resultSet = selectStatement.executeQuery();
            if (resultSet.next()) {
                storageNode = new StorageNode();
                sourceSnId = new ArrayList<>();
                do {
                    storageNode.setSnId(resultSet.getInt("replicaId"));
                    sourceSnId.add(resultSet.getInt("snId"));
                } while (resultSet.next());
                storageNode.setSourceSnIdList(sourceSnId);
            } else {
                logger.debug("Storage Node can not be found in DB.");
            }
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

    public boolean updateSnStatistics(int totalRetrievelRequest, int totalStorageRequest,
                                      long totalFreeSpaceInBytes, int snId) {

        boolean result = false;
        Connection connection = null;
        String sql = SqlConstants.UPDATE_SN_STATISTICS;
        PreparedStatement updateStatement = null;
        try {
            connection = DbManager.getInstance().getBds().getConnection();
            updateStatement = connection.prepareStatement(sql);
            updateStatement.setLong(1, totalFreeSpaceInBytes);
            updateStatement.setInt(2, totalStorageRequest);
            updateStatement.setInt(3, totalRetrievelRequest);
            updateStatement.setInt(4, snId);
            updateStatement.execute();
            result = true;
            System.out.println("SN info is updated from DB.");
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

}
