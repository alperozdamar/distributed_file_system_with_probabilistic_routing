package cs601.project4.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cs601.project4.db.model.StorageNode;
import edu.usfca.cs.dfs.config.ConfigurationManagerController;

/**
 * This DB Manager stores db configuration parameters and DB datasource. 
 * 
 *
 */
public class DbManager {

    private static Logger    logger            = LogManager.getLogger(DbManager.class);
    private static DbManager instance;
    private static String    dbUsername        = "user18";
    private static String    dbPassword        = "user18";
    private static String    dbUrl             = "jdbc:mysql://localhost:3306/sn_replication";
    private static String    dbDriver          = "com.mysql.jdbc.Driver";
    private static int       dbNumberOfDbConns = 10;

    private BasicDataSource  bds               = new BasicDataSource();

    public static DbManager getInstance() throws Exception {
        if (instance == null) {
            instance = new DbManager();
        }
        return instance;
    }

    private DbManager() {
        readConfigFile();
        //Set database driver name
        bds.setDriverClassName(dbDriver);
        //Set database url
        bds.setUrl(dbUrl);
        //Set database user
        bds.setUsername(dbUsername);
        //Set database password
        bds.setPassword(dbPassword);
        //Set the connection pool size
        bds.setInitialSize(dbNumberOfDbConns);
        bds.setMinIdle(5);
        bds.setMaxIdle(10);
        bds.setMaxOpenPreparedStatements(100);
    }

    public void readConfigFile() {
        try {
            Properties props = new Properties();
            props.load(new FileInputStream(ConfigurationManagerController.PROJECT_1_CONTROLLER_CONFIG_FILE));
            dbUsername = props.getProperty("DB_USERNAME");
            if (dbUsername == null) {
                throw new IOException("DB_USERNAME parameter is not an appropriate value");
            }
            dbPassword = props.getProperty("DB_PASSWORD");
            if (dbPassword == null) {
                throw new IOException("DB_PASSWORD parameter is not an appropriate value");
            }
            dbUrl = props.getProperty("DB_URL");
            if (dbUrl == null) {
                throw new IOException("DB_URL parameter is not an appropriate value");
            }
            dbDriver = props.getProperty("DB_DRIVER");
            if (dbDriver == null) {
                throw new IOException("DB_DRIVER parameter is not an appropriate value");
            }
            String numberOfCons = props.getProperty("NUMBER_OF_DB_CONNECTIONS");
            if (numberOfCons != null) {
                try {
                    dbNumberOfDbConns = Integer.parseInt(numberOfCons);
                } catch (NumberFormatException nfe) {
                    throw new IOException("NUMBER_OF_DB_CONNECTIONS parameter is not an appropriate value");
                }
            }
            System.out.println("dbUrl:" + dbUrl + ",dbUsername:" + dbUsername + ",dbPassword:"
                    + dbPassword);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public BasicDataSource getBds() {
        return bds;
    }

    public void setBds(BasicDataSource bds) {
        this.bds = bds;
    }

    public static void main(String[] args) {
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

}
