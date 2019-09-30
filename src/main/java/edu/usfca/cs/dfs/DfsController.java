package edu.usfca.cs.dfs;

import java.io.IOException;

import edu.usfca.cs.dfs.config.ConfigurationManagerController;
import edu.usfca.cs.dfs.config.Constants;
import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.net.ServerMessageRouter;

public class DfsController {

    ServerMessageRouter messageRouter;

    public DfsController() {
    }

    public void start() throws IOException {

        messageRouter = new ServerMessageRouter(Constants.CONTROLLER);
        messageRouter.listen(ConfigurationManagerController.getInstance().getControllerPort());
        System.out.println("[Controller] Listening for connections on port :"
                + ConfigurationManagerController.getInstance().getControllerPort());

        MessagePipeline pipeline = new MessagePipeline(Constants.CONTROLLER);

    }

    public static void main(String[] args) throws IOException {
        DfsController s = new DfsController();
        s.start();
    }
}
