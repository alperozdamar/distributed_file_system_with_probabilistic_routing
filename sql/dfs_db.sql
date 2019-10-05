-- --------------------------------------------------------
-- Host:                         127.0.0.1
-- Server version:               5.7.24-log - MySQL Community Server (GPL)
-- Server OS:                    Win64
-- HeidiSQL Version:             9.5.0.5196
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;


-- Dumping database structure for dfs_db
CREATE DATABASE IF NOT EXISTS `dfs_db` /*!40100 DEFAULT CHARACTER SET latin1 */;
USE `dfs_db`;

-- Dumping structure for table dfs_db.sn_information
CREATE TABLE IF NOT EXISTS `sn_information` (
  `snId` int(11) NOT NULL,
  `snIp` varchar(50) NOT NULL,
  `snPort` int(11) NOT NULL,
  `totalFreeSpace` bigint(40) NOT NULL,
  `totalStorageReq` bigint(20) NOT NULL,
  `totalRetrievelReq` bigint(20) NOT NULL,
  `status` varchar(20) DEFAULT NULL,
  `backupId` int(11) DEFAULT NULL,
  PRIMARY KEY (`snId`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Dumping data for table dfs_db.sn_information: ~1 rows (approximately)
/*!40000 ALTER TABLE `sn_information` DISABLE KEYS */;
/*!40000 ALTER TABLE `sn_information` ENABLE KEYS */;

-- Dumping structure for table dfs_db.sn_replication
CREATE TABLE IF NOT EXISTS `sn_replication` (
  `snId` int(11) NOT NULL,
  `replicaId` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Dumping data for table dfs_db.sn_replication: ~0 rows (approximately)
/*!40000 ALTER TABLE `sn_replication` DISABLE KEYS */;
/*!40000 ALTER TABLE `sn_replication` ENABLE KEYS */;

/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
