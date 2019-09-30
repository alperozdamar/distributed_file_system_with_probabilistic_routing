package edu.usfca.cs.dfs;

import java.io.IOException;

import edu.usfca.cs.dfs.config.ConfigurationManagerController;
import edu.usfca.cs.dfs.config.Constants;
import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.net.ServerMessageRouter;

public class DfsStorageNode {

    ServerMessageRouter messageRouter;

    public DfsStorageNode() {
    }

    public void start() throws IOException {

        messageRouter = new ServerMessageRouter(Constants.STORAGENODE);
        messageRouter.listen(ConfigurationManagerController.getInstance().getControllerPort());
        System.out.println("Listening for connections on port :"
                + ConfigurationManagerController.getInstance().getControllerPort());
        MessagePipeline pipeline = new MessagePipeline(Constants.STORAGENODE);

    }

    public static void main(String[] args) throws IOException {
        DfsStorageNode s = new DfsStorageNode();
        s.start();
    }
}
