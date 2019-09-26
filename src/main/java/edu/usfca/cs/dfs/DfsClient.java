package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.Scanner;

import edu.usfca.cs.dfs.config.ConfigurationManager;
import edu.usfca.cs.dfs.config.Constants;

public class DfsClient {

    public DfsClient() {
    }

    public static void main(String[] args) throws IOException {

        ConfigurationManager.getInstance();
        System.out.println("Client is started with these parameters: "
                + ConfigurationManager.getInstance().toString());

        while (true) {
            // create a scanner so we can read the command-line input
            Scanner scanner = new Scanner(System.in);

            //  prompt for command.
            System.out.print("Enter your command: ");

            // get command as String
            String command = scanner.next();

            if (command.equalsIgnoreCase(Constants.CONNECT)) {
                System.out.println("Client will be connected to Controller<"
                        + ConfigurationManager.getInstance().getControllerIp() + ":"
                        + ConfigurationManager.getInstance().getControllerPort() + ">");

                /**
                 * TODO:
                 * Connect to the controller If there is no connection.
                 * Else thrown an error
                 */

            } else if (command.equalsIgnoreCase(Constants.LIST)) {

            } else if (command.equalsIgnoreCase(Constants.RETRIEVE)) {

            } else if (command.equalsIgnoreCase(Constants.STORE)) {

            } else if (command.equalsIgnoreCase(Constants.EXIT)) {
                System.out.println("Client will be shutdown....");
                System.exit(0);
            } else {
                System.out.println("Undefined command:" + command);
            }

        }

    }

}
