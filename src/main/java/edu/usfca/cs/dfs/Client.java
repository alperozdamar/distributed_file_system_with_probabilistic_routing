package edu.usfca.cs.dfs;

import java.io.IOException;

import com.google.protobuf.ByteString;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import edu.usfca.cs.dfs.net.MessagePipeline;

public class Client {

    public Client() {

    }

    public static void main(String[] args)
    throws IOException {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline();

        Bootstrap bootstrap = new Bootstrap()
            .group(workerGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(pipeline);

        ChannelFuture cf = bootstrap.connect("localhost", 7777);
        cf.syncUninterruptibly();

        ByteString data = ByteString.copyFromUtf8("Hello World!");
        StorageMessages.StoreChunk storeChunkMsg
            = StorageMessages.StoreChunk.newBuilder()
                .setFileName("my_file.txt")
                .setChunkId(3)
                .setData(data)
                .build();

        StorageMessages.StorageMessageWrapper msgWrapper =
            StorageMessages.StorageMessageWrapper.newBuilder()
                .setStoreChunkMsg(storeChunkMsg)
                .build();

        Channel chan = cf.channel();
        ChannelFuture write = chan.write(msgWrapper);
        chan.flush();
        write.syncUninterruptibly();

        /* Don't quit until we've disconnected: */
        System.out.println("Shutting down");
        workerGroup.shutdownGracefully();
    }
}
