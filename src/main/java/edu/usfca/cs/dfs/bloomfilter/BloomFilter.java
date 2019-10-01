package edu.usfca.cs.dfs.bloomfilter;

import java.util.BitSet;

import com.sangupta.murmur.Murmur3;

public class BloomFilter {

    private int    filterLength;
    private int    hashTime;
    private long   seed;
    private long   numberOfItems = 0;
    private BitSet bloomFilter;

    public int getFilterLength() {
        return filterLength;
    }

    public void setFilterLength(int filterLength) {
        this.filterLength = filterLength;
    }

    public int getHashTime() {
        return hashTime;
    }

    public void setHashTime(int hashTime) {
        this.hashTime = hashTime;
    }

    public long getSeed() {
        return seed;
    }

    public void setSeed(long seed) {
        this.seed = seed;
    }

    public long getNumberOfItems() {
        return numberOfItems;
    }

    public void setNumberOfItems(long numberOfItems) {
        this.numberOfItems = numberOfItems;
    }

    public BitSet getBloomFilter() {
        return bloomFilter;
    }

    public void setBloomFilter(BitSet bloomFilter) {
        this.bloomFilter = bloomFilter;
    }

    /**
     * Create a BloomFilter with required parameters
     *
     * @param filterLength
     * @param hashTime
     * @param seed
     */
    public BloomFilter(int filterLength, int hashTime, long seed) {
        this.filterLength = filterLength;
        this.hashTime = hashTime;
        this.seed = seed;
        bloomFilter = new BitSet(this.filterLength);
    }

    private long[] getBitLocations(byte[] data, int filterLength, int hashTime) {
        long[] results = new long[hashTime];
        long hash1 = Murmur3.hash_x86_32(data, data.length, this.seed);
        long hash2 = Murmur3.hash_x86_32(data, data.length, hash1);
        for (int i = 0; i < hashTime; i++) {
            results[i] = (hash1 + i * hash2) % filterLength;
        }
        return results;
    }

    public boolean get(byte[] data) {
        /**
         * 1. Hash it!!!
         */
        long[] bitLocationArray = getBitLocations(data, this.filterLength, this.hashTime);

        /**
         * 2. Loop through bitLocation
         */
        for (int i = 0; i < bitLocationArray.length; i++) {
            if (!this.bloomFilter.get((int) bitLocationArray[i])) {
                return false;
            }
        }

        /**
         * 3. If any one location is 0 return false;
         * else return true!
         */
        return true;
    }

    public void put(byte[] data) {
        long[] bitLocations = getBitLocations(data, this.filterLength, this.hashTime);
        for (int i = 0; i < bitLocations.length; i++) {
            this.bloomFilter.set((int) bitLocations[i]);
        }
        this.numberOfItems++;
    }

    public float falsePositive() {
        //p = pow(1 - exp(-hashTime / (filterLength / numberOfItems)), hashTime)
        double exp = Math.exp((double) -this.hashTime
                / (double) (this.filterLength / this.numberOfItems));

        double p = Math.pow(1 - exp, this.hashTime);
        return (float) p;
    }
}
