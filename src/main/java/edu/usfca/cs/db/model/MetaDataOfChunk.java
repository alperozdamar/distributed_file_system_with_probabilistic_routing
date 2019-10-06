package edu.usfca.cs.db.model;

public class MetaDataOfChunk {

    private String checksum;

    private String path;

    private int    chunkId;

    private String fileName;

    private int    chunksize;

    public MetaDataOfChunk(String checksum, String path, int chunkId, String fileName,
                           int chunksize) {
        super();
        this.checksum = checksum;
        this.path = path;
        this.chunkId = chunkId;
        this.fileName = fileName;
        this.chunksize = chunksize;
    }

    public String getChecksum() {
        return checksum;
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getChunkId() {
        return chunkId;
    }

    public void setChunkId(int chunkId) {
        this.chunkId = chunkId;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getChunksize() {
        return chunksize;
    }

    public void setChunksize(int chunksize) {
        this.chunksize = chunksize;
    }

}
