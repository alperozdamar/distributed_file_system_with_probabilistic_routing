package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Client {


    /**
     * Every Client request will be sent by a seperate Thread.
     */
    private static ExecutorService threadPoolForClientRequests = Executors.newFixedThreadPool(30);

    public Client() {

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        EventLoopGroup workerGroup = new NioEventLoopGroup(100);
        MessagePipeline pipeline = new MessagePipeline("TestClient");

        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(pipeline);

        int thread = 5;

        Channel[] channels = new Channel[thread];

        for(int i=0;i<thread;i++){
            channels[i] = bootstrap.connect("localhost", 7777).syncUninterruptibly().channel();
        }

        List<ChannelFuture> writes = new ArrayList<>();

        ByteString data = ByteString.copyFromUtf8("Hello World!");
        for(int i=0;i<100;i++) {
            StorageMessages.StoreChunk storeChunkMsg = StorageMessages.StoreChunk.newBuilder().setFileName("my_file_"+i+".txt").setChunkId(3).setData(data).build();

            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder().setStoreChunk(storeChunkMsg).build();

            writes.add(channels[(i+1)%thread].write(msgWrapper));
        }

        for(int i=0;i<thread;i++){
            channels[i].flush();
            channels[i].closeFuture();
        }

        for (ChannelFuture write : writes) {
            write.syncUninterruptibly();
        }
        /* Don't quit until we've disconnected: */
        System.out.println("Shutting down");
        workerGroup.shutdownGracefully();
    }
}
