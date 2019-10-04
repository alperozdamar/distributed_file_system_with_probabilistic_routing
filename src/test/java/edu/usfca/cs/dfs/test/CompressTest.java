package edu.usfca.cs.dfs.test;


import edu.usfca.cs.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CompressTest {
    public static void main(String[] args) {
        byte[] test = null;
        try {
            test = Files.readAllBytes(Paths.get("input/test.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] compressedData = Utils.compressChunk(test);
        if(test.length == compressedData.length){
            System.out.println("No compression");
        } else if(test.length>compressedData.length){
            System.out.println("Data compressed");
        }
        System.out.println("File bytes: "+test.length);
        System.out.println("Compress bytes: "+compressedData.length);
    }
}
