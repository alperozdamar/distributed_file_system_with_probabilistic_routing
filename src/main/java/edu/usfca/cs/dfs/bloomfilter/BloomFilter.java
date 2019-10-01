package edu.usfca.cs.dfs.bloomfilter;

import java.util.BitSet;

import com.sangupta.murmur.Murmur3;

import edu.usfca.cs.dfs.config.ConfigurationManagerBloomFilter;

public class BloomFilter {

    public static final int  filterLength  = ConfigurationManagerBloomFilter.getInstance().getBloomFilterLength();
    public static final int  hashTime      = ConfigurationManagerBloomFilter.getInstance().getHashTime();
    public static final long seed          = ConfigurationManagerBloomFilter.getInstance().getHashSeed();

    public static long       numberOfItems = 0;

    public static BitSet     bloomFilter   = new BitSet(filterLength);

    private static long[] getBitLocations(byte[] data, int filterLength, int hashTime) {
        long[] results = new long[hashTime];
        long hash1 = Murmur3.hash_x86_32(data, data.length, seed);
        long hash2 = Murmur3.hash_x86_32(data, data.length, hash1);
        for (int i = 0; i < hashTime; i++) {
            results[i] = (hash1 + i * hash2) % filterLength;
        }
        return results;
    }

    public static boolean get(byte[] data) {
        /**
         * 1. Hash it!!!
         */
        long[] bitLocationArray = getBitLocations(data, filterLength, hashTime);

        /**
         * 2. Loop through bitLocation
         */
        for (int i = 0; i < bitLocationArray.length; i++) {
            if (!bloomFilter.get((int) bitLocationArray[i])) {
                return false;
            }
        }

        /**
         * 3. If any one location is 0 return false;
         * else return true!
         */
        return true;
    }

    public static void put(byte[] data) {
        long[] bitLocations = getBitLocations(data, filterLength, hashTime);
        for (int i = 0; i < bitLocations.length; i++) {
            bloomFilter.set((int) bitLocations[i]);
        }

        numberOfItems++;
    }

    public static float falsePositive() {

        //p = pow(1 - exp(-hashTime / (filterLength / numberOfItems)), hashTime)
        double exp = Math.exp((double) -hashTime / (double) (filterLength / numberOfItems));

        double p = Math.pow(1 - exp, hashTime);
        return (float) p;
    }
}
