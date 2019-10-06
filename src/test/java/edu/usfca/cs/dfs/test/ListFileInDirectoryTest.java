package edu.usfca.cs.dfs.test;

import edu.usfca.cs.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ListFileInDirectoryTest {
    public static void main(String[] args) {
        Utils.sendAllFileInFileSystemByNodeId(3);
    }
}
