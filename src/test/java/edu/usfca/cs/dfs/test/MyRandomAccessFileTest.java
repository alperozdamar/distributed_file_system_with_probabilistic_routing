package edu.usfca.cs.dfs.test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

public class MyRandomAccessFileTest {

    public static void main(String[] args) {
        try {
            // file content is "ABCDEFGH"
            String filePath = "input/test.txt";

            HashMap<Integer, byte[]> chunkMap = new HashMap<Integer, byte[]>();

            RandomAccessFile raFile = new RandomAccessFile(filePath, "rw");
            int chunkId = 1;
            for (int i = 0; i < raFile.length(); i = i + 5) {
                if (i == 0) {
                    chunkMap.put(chunkId, readCharsFromFile(filePath, 0, 5));
                } else {
                    chunkMap.put(chunkId, readCharsFromFile(filePath, i, 5));
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

    private static byte[] readCharsFromFile(String filePath, int seek, int chunkSize)
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
