package edu.usfca.cs;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;

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
}
