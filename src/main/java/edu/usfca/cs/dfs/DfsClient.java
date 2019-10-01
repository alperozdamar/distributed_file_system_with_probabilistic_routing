package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.config.ConfigurationManagerClient;
import edu.usfca.cs.dfs.config.Constants;
import edu.usfca.cs.dfs.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class DfsClient {

    /**
     * Every Client request will be sent by a seperate Thread.
     */
    private static ExecutorService threadPoolForClientRequests = Executors.newFixedThreadPool(30);

    public DfsClient() {
    }

    public static void main(String[] args) throws IOException {
        ConfigurationManagerClient.getInstance();
        System.out.println("Client is started with these parameters: "
                + ConfigurationManagerClient.getInstance().toString());

        /**
         * TODO:
         * Connect to the controller If there is no connection.
         * Else thrown an error
         */
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline(Constants.CLIENT);

        Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class).option(ChannelOption.SO_KEEPALIVE,
                                                                                                        true).handler(pipeline);

        ChannelFuture cf = bootstrap.connect(ConfigurationManagerClient.getInstance().getControllerIp(),
                                             ConfigurationManagerClient.getInstance().getControllerPort());
        cf.syncUninterruptibly();

        while (true) {
            // create a scanner so we can read the command-line input
            Scanner scanner = new Scanner(System.in);

            //  prompt for command.
            System.out.print("Enter your command: ");

            // get command as String
            String command = scanner.next();

            System.out.println("Client will be connected to Controller<"
                    + ConfigurationManagerClient.getInstance().getControllerIp() + ":"
                    + ConfigurationManagerClient.getInstance().getControllerPort() + ">");

            if (command.equalsIgnoreCase(Constants.LIST)) {

            } else if (command.equalsIgnoreCase(Constants.RETRIEVE)) {

            } else if (command.equalsIgnoreCase(Constants.STORE)) {
                ByteString data = ByteString.copyFromUtf8("Hello World!");
                StorageMessages.StoreChunk storeChunkMsg = StorageMessages.StoreChunk.newBuilder().setFileName("my_file.txt").setChunkId(88).setData(data).build();
                StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder().setStoreChunkMsg(storeChunkMsg).build();
                Channel chan = cf.channel();
                ChannelFuture write = chan.write(msgWrapper);
                chan.flush();
                write.syncUninterruptibly();
            } else if (command.equalsIgnoreCase(Constants.EXIT)) {
                System.out.println("Client will be shutdown....");
                System.exit(0);
            } else {
                System.out.println("Undefined command:" + command);
            }

        }

    }

}
