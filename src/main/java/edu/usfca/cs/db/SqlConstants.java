package edu.usfca.cs.db;

/**
 * Prepared SQL CONSTANTS for using in SqlManager.
 * 
 * @author alperoz
 *
 */
public class SqlConstants {

    public static final String UPDATE_SN_REPLICATION_BY_SNID = "UPDATE sn_replication SET replicaId=?, backupId=? WHERE snId=?";

    public static final String INSERT_SN_REPLICATION         = "INSERT INTO sn_replication (snId,replicaId,backupId) VALUES (?,?,?) ";

    public static final String INSERT_SN                     = "INSERT INTO sn_information (snId,snIp,snPort,totalFreeSpace,totalStorageReq,totalRetrievelReq) VALUES (?,?,?,?,?,?) ";

    public static final String DELETE_SN_REPLICATION_BY_SNID = "DELETE FROM sn_replication WHERE snId=?";

}
