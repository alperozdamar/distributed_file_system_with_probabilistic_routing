package edu.usfca.cs.dfs.test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

public class MyRandomAccessFileTest {

    public static void main(String[] args) {
        try {
            String filePath = "input/test.txt";

            HashMap<Integer, byte[]> chunkMap = new HashMap<Integer, byte[]>();

            int chunkSize = 30;

            RandomAccessFile raFile = new RandomAccessFile(filePath, "rw");
            int chunkId = 1;
            for (int i = 0; i < raFile.length(); i = i + chunkSize) {
                if (i == 0) {
                    chunkMap.put(chunkId, readChunksFromFile(filePath, 0, chunkSize));
                } else {
                    chunkMap.put(chunkId, readChunksFromFile(filePath, i, chunkSize));
                }
                chunkId++;
            }

            for (Integer key : chunkMap.keySet()) {
                byte[] value = (byte[]) chunkMap.get(key);
                System.out.println("Chunk[" + key + "]:" + new String(value));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void appendData(String filePath, String data) throws IOException {
        RandomAccessFile raFile = new RandomAccessFile(filePath, "rw");
        raFile.seek(raFile.length());
        System.out.println("current pointer = " + raFile.getFilePointer());
        raFile.write(data.getBytes());
        raFile.close();

    }

    private static void writeData(String filePath, String data, int seek) throws IOException {
        RandomAccessFile file = new RandomAccessFile(filePath, "rw");
        file.seek(seek);
        file.write(data.getBytes());
        file.close();
    }

    private static byte[] readChunksFromFile(String filePath, int seek, int chunkSize)
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
