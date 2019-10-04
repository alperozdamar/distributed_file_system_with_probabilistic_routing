package edu.usfca.cs;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {
    public static void printHeader(String header){
        System.out.println("\n-----------------------");
        System.out.println(header);
    }

    public static ChannelFuture connect(Bootstrap bootstrap, String ip, int port) {
        ChannelFuture cf = bootstrap.connect(ip, port);
        cf.syncUninterruptibly();
        return cf;
    }

    public static byte[] readFromFile(String filePath, int seek, int chunkSize)
            throws IOException {
        System.out.println("seek:" + seek);
        RandomAccessFile file = new RandomAccessFile(filePath, "r");
        file.seek(seek);
        byte[] bytes = new byte[chunkSize];

        file.read(bytes);
        file.close();
        return bytes;
    }


    public static String getMd5(byte[] chunk)
    {
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
}
