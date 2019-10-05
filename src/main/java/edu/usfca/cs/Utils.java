package edu.usfca.cs;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.GZIPOutputStream;

import edu.usfca.cs.dfs.StorageMessages.StoreChunk;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;

public class Utils {

    public static void printHeader(String header) {
        System.out.println("\n-----------------------");
        System.out.println(header);
    }

    public static ChannelFuture connect(Bootstrap bootstrap, String ip, int port) {
        ChannelFuture cf = bootstrap.connect(ip, port);
        cf.syncUninterruptibly();
        return cf;
    }

    public static byte[] readFromFile(String filePath, int seek, int chunkSize) throws IOException {
        System.out.println("seek:" + seek);
        RandomAccessFile file = new RandomAccessFile(filePath, "r");
        file.seek(seek);
        byte[] bytes = new byte[chunkSize];

        file.read(bytes);
        file.close();
        return bytes;
    }

    public static boolean writeChunkIntoFile(String directory, StoreChunk storeChunkMsg) {
        String filePath = directory + File.separator + storeChunkMsg.getFileName() + "_"
                + storeChunkMsg.getChunkId();
        //FileOutputStream outputStream;
        //        try {
        //
        //            Path path = Paths.get(filePath);
        //            BufferedWriter writer = Files.newBufferedWriter(path,
        //                                                            Charset.forName("UTF-8"),
        //                                                            StandardOpenOption.CREATE,
        //                                                            StandardOpenOption.APPEND);
        //            String data = storeChunkMsg.getData().toStringUtf8();
        //            System.out.println("Written chunk:" + data);
        //            writer.write(data, 0, data.length());
        //            writer.flush();
        //            writer.close();
        //            // outputStream = new FileOutputStream(filePath);
        //            //storeChunkMsg.getData().writeTo(outputStream);
        //            //outputStream.write(storeChunkMsg.getData().toByteArray());
        //            //outputStream.close();
        //
        //        } catch (FileNotFoundException e) {
        //            e.printStackTrace();
        //            return false;
        //        } catch (IOException e) {
        //            e.printStackTrace();
        //            return false;
        //        }
        try {
            Path path = Paths.get(filePath);
            // Open the file, creating it if it doesn't exist
            try (final BufferedWriter out = Files
                    .newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE)) {
                String data = storeChunkMsg.getData().toStringUtf8();
                System.out.println("Written chunk:" + data);
                out.write(data, 0, data.length());
            }
        } catch (Exception e) {

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
}
