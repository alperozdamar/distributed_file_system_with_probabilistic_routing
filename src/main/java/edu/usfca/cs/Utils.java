package edu.usfca.cs;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;

import javax.xml.bind.DatatypeConverter;

import edu.usfca.cs.db.SqlManager;
import edu.usfca.cs.db.model.StorageNode;
import edu.usfca.cs.dfs.StorageMessages;
import edu.usfca.cs.dfs.StorageMessages.StoreChunk;
import edu.usfca.cs.dfs.config.ConfigurationManagerSn;
import edu.usfca.cs.dfs.config.Constants;
import edu.usfca.cs.dfs.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Utils {

    private static Logger logger = LogManager.getLogger(Utils.class);

    public static void printHeader(String header) {
        logger.info("\n-----------------------");
        logger.info(header);
    }

    public static ChannelFuture connect(Bootstrap bootstrap, String ip, int port) {
        ChannelFuture cf = bootstrap.connect(ip, port);
        cf.syncUninterruptibly();
        return cf;
    }

    public static byte[] readFromFile(String filePath, int seek, int chunkSize) {
        logger.info("seek:" + seek);
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile(filePath, "r");
            file.seek(seek);
            byte[] bytes = new byte[chunkSize];

            file.read(bytes);
            file.close();
            return bytes;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean writeChunkIntoFileInStorageNode(String directory,
                                                          StoreChunk storeChunkMsg) {
        String filePath = directory + File.separator + storeChunkMsg.getFileName() + "_"
                + storeChunkMsg.getChunkId();
        FileOutputStream outputStream;
        try {
            outputStream = new FileOutputStream(filePath);
            outputStream.write(storeChunkMsg.getData().toByteArray());
            logger.info("Written chunk checksum: "
                    + Utils.getMd5(storeChunkMsg.getData().toByteArray()));
            outputStream.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public static String getMd5(byte[] chunk) {
        try {

            // Static getInstance method is called with hashing MD5
            MessageDigest md = MessageDigest.getInstance("MD5");

            // digest() method is called to calculate message digest
            //  of an input digest() return array of byte
            byte[] messageDigest = md.digest(chunk);

            // Convert byte array into signum representation
            BigInteger no = new BigInteger(1, messageDigest);

            // Convert message digest into hex value
            String hashtext = no.toString(16);
            while (hashtext.length() < 32) {
                hashtext = "0" + hashtext;
            }
            return hashtext;
        }

        // For specifying wrong message digest algorithms
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean givenFileGeneratingChecksumThenVerifying(String sourceFile,
                                                                   String destinationFile)
            throws NoSuchAlgorithmException, IOException {
        //String checksum = "5EB63BBBE01EEED093CB22BB8F5ACDC3";

        logger.info("GeneratingChecksum.... Please Wait!");

        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(Files.readAllBytes(Paths.get(sourceFile)));
        byte[] digest = md.digest();
        String sourceChecksum = DatatypeConverter.printHexBinary(digest).toUpperCase();

        MessageDigest md2 = MessageDigest.getInstance("MD5");
        md2.update(Files.readAllBytes(Paths.get(destinationFile)));
        byte[] digest2 = md2.digest();
        String destChecksum = DatatypeConverter.printHexBinary(digest2).toUpperCase();

        logger.info("Source CheckSum=" + sourceChecksum);
        logger.info("Dest. CheckSum=" + destChecksum);

        if (sourceChecksum.equals(destChecksum)) {
            logger.info("[SUCCESS]Files are identical!!!");
            return true;
        } else {
            logger.info("[PROBLEM] Md5 not matched!!! Problem in transfering files...");
            return false;
        }

    }

    /**
     * If their maximum compression is greater than 0.6 then compress it
     * @param chunk byte array to compress
     * @return
     */
    public static byte[] compressChunk(byte[] chunk) {
        double entr = entropy(chunk);
        double maxCompression = (1 - entr / 8) * 100;
        if (maxCompression > 0.6) {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream(chunk.length);
            try {
                GZIPOutputStream gzipOS = new GZIPOutputStream(byteStream);
                try {
                    gzipOS.write(chunk);
                } finally {
                    gzipOS.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    byteStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            byte[] compressedData = byteStream.toByteArray();
            return compressedData;
        }
        return chunk;
    }

    /**
     * Calculates the entropy per character/byte of a byte array.
     *
     * @param input array to calculate entropy of
     *
     * @return entropy bits per byte
     */
    public static double entropy(byte[] input) {
        if (input.length == 0) {
            return 0.0;
        }

        /* Total up the occurrences of each byte */
        int[] charCounts = new int[256];
        for (byte b : input) {
            charCounts[b & 0xFF]++;
        }

        double entropy = 0.0;
        for (int i = 0; i < 256; ++i) {
            if (charCounts[i] == 0.0) {
                continue;
            }

            double freq = (double) charCounts[i] / input.length;
            entropy -= freq * (Math.log(freq) / Math.log(2));
        }

        return entropy;
    }

    public static void sendAllFileInFileSystemByNodeId(int snId) {
        String directoryPath = null;
        directoryPath = ConfigurationManagerSn.getInstance().getStoreLocation();
        String whoamI = System.getProperty("user.name");
        directoryPath = System.getProperty("user.dir") + File.separator + directoryPath
                + File.separator + whoamI + File.separator + snId;
        File folder = new File(directoryPath);
        File[] listOfFiles = folder.listFiles();
        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                logger.info("File " + listOfFiles[i].getName());

            } else if (listOfFiles[i].isDirectory()) {
                logger.info("Directory " + listOfFiles[i].getName());
            }
        }
    }

    public static void writeDataIntoClientFileSystem(String filePath, byte[] data, long seek)
            throws IOException {
        RandomAccessFile file = new RandomAccessFile(filePath, "rw");

        file.seek(seek);

        logger.info("[Client] Receive data checksum:" + Utils.getMd5(data));

        file.write(data);
        file.close();
    }

    //    public static void compareCheckSum(String sourceFile, String destinationFile)
    //            throws NoSuchAlgorithmException, IOException {
    //        //String checksum = "5EB63BBBE01EEED093CB22BB8F5ACDC3";
    //
    //        logger.info("GeneratingChecksum.... Please Wait!");
    //
    //        MessageDigest md = MessageDigest.getInstance("MD5");
    //        md.update(Files.readAllBytes(Paths.get(sourceFile)));
    //        byte[] digest = md.digest();
    //        String sourceChecksum = DatatypeConverter.printHexBinary(digest).toUpperCase();
    //
    //        MessageDigest md2 = MessageDigest.getInstance("MD5");
    //        md2.update(Files.readAllBytes(Paths.get(destinationFile)));
    //        byte[] digest2 = md2.digest();
    //        String destChecksum = DatatypeConverter.printHexBinary(digest2).toUpperCase();
    //
    //        if (sourceChecksum.equals(destChecksum)) {
    //            logger.info("[SUCCESS]Files are identical!!!");
    //        } else {
    //            logger.info("[PROBLEM] Md5 not matched!!! Problem in transfering files...");
    //        }
    //        logger.info("SrceCheckSum=" + sourceChecksum);
    //        logger.info("DestCheckSum=" + destChecksum);
    //    }

    public static void sendChunkOfSourceSnToDestinationSn(int sourceSnId, int destinationSnId){
        SqlManager sqlManager = SqlManager.getInstance();
        //Backup data of current node
        //Send current down SN data to backup ID
        StorageNode sourceNode = sqlManager.getSNReplication(sourceSnId);
        StorageNode destinationNode = sqlManager.getSNInformationById(destinationSnId);
        ArrayList<Integer> replicateIdList = sourceNode.getReplicateSnIdList();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(Constants.CONTROLLER);

        Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);
        for (int replicateId : replicateIdList) {
            StorageNode snNode = sqlManager.getSNInformationById(replicateId);
            if (snNode.getStatus().equals("DOWN")) {
                continue;
            } else {
                ChannelFuture cf = Utils.connect(bootstrap, snNode.getSnIp(), snNode.getSnPort());
                StorageMessages.BackUp backUpMsg = StorageMessages.BackUp.newBuilder()
                        .setDestinationIp(destinationNode.getSnIp())
                        .setDestinationPort(destinationNode.getSnPort()).setSourceSnId(sourceSnId).build();
                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder().setBackup(backUpMsg).build();
                System.out.printf("Request data of %d send to replica: %d\n", sourceSnId, replicateId);
                cf.channel().writeAndFlush(msgWrapper).syncUninterruptibly();
                break;
            }
        }

        //Source of replication
        //Send replicate data of current down SN to backup ID
        sourceNode = sqlManager.getSourceReplicationSnId(sourceSnId);
        ArrayList<Integer> sourceIdList = sourceNode.getSourceSnIdList();
        for (int sourceId : sourceIdList) {
            System.out.println("Source Id: " + sourceId);
            sourceNode = sqlManager.getSNInformationById(sourceId);
            String fromIp = "";
            int fromPort = 0;
            if (sourceNode.getStatus().equals("DOWN")) {//SourceNode down, get data from sourceNode replica
                ArrayList<Integer> sourceReplicaIdList = sqlManager.getSNReplication(sourceId)
                        .getReplicateSnIdList();
                for (int sourceReplicaId : sourceReplicaIdList) {
                    StorageNode sourceReplication = sqlManager
                            .getSNInformationById(sourceReplicaId);
                    if (!sourceReplication.getStatus().equals("DOWN")) {
                        fromIp = sourceReplication.getSnIp();
                        fromPort = sourceReplication.getSnPort();
                        break;
                    }
                }
            } else {
                fromIp = sourceNode.getSnIp();
                fromPort = sourceNode.getSnPort();
            }
            if (!fromIp.isEmpty() && fromPort != 0) {
                ChannelFuture cf = Utils.connect(bootstrap, fromIp, fromPort);
                StorageMessages.BackUp backUpMsg = StorageMessages.BackUp.newBuilder()
                        .setDestinationIp(destinationNode.getSnIp())
                        .setDestinationPort(destinationNode.getSnPort()).setSourceSnId(sourceId).build();
                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                        .newBuilder().setBackup(backUpMsg).build();
                System.out
                        .printf("Request data of %d send to source port %d\n", sourceId, fromPort);
                cf.channel().writeAndFlush(msgWrapper).syncUninterruptibly();
            } else {
                System.out.printf("[Controller][BackUp] All source of data %d down!\n", sourceId);
            }
        }
    }

}
