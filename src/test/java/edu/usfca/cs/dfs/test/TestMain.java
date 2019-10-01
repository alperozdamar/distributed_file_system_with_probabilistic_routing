package edu.usfca.cs.dfs.test;

public class TestMain {

    public static void main(String[] args) {
        long fileSize = 118;
        long chunkSize = 2048;
        System.out.format("The size of the file: %d bytes", fileSize);
        System.out.format("\nThe size of chunks: %d bytes", chunkSize);
        long numOfChunks = (long) Math.ceil((float) fileSize / (float) chunkSize);
        System.out.format("\nNumber Of Chunks is %d for file size:%d bytes", numOfChunks, fileSize);
        long lastChunkByteSize = fileSize % chunkSize;
        System.out.format("\nlastChunkByteSize is %d for file size:%d bytes",
                          lastChunkByteSize,
                          fileSize);
    }

}
