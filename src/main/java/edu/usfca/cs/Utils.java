package edu.usfca.cs;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;

import java.io.IOException;
import java.io.RandomAccessFile;

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
}
