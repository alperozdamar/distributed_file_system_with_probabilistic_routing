package edu.usfca.cs.dfs.bloomfilter;

import java.util.BitSet;

import com.sangupta.murmur.Murmur3;

public class BloomFilter {

    public static final int  filterLength  = 10;
    public static final int  hashTime      = 3;
    public static final long seed          = 3;

    public static long       numberOfItems = 0;

    public static BitSet     bloomFilter   = new BitSet(filterLength);

    private static long[] getBitLocations(byte[] data, int filterLength, int hashTime) {
        long[] results = new long[hashTime];
        long hash1 = Murmur3.hash_x86_32(data, data.length, seed);
        long hash2 = Murmur3.hash_x86_32(data, data.length, hash1);
        for (int i = 0; i < hashTime; i++) {
            results[i] = (hash1 + i * hash2) % filterLength;
            //System.out.println(results[i]);
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

        //        System.out.println("******");
        //        for (int i = 0; i < bloomFilter.length; i++) {
        //            System.out.println(bloomFilter[i]);
        //        }

    }

    public static float falsePositive() {

        //p = pow(1 - exp(-hashTime / (filterLength / numberOfItems)), hashTime)

        System.out.println("before:"
                + (double) -hashTime / (double) (filterLength / numberOfItems));
        double exp = Math.exp((double) -hashTime / (double) (filterLength / numberOfItems));
        System.out.println("Exp:" + exp);

        //String formato = String.format("%.2f");
        //System.out.printf("%.2f", exp);

        double p = Math.pow(1 - exp, hashTime);
        System.out.println("p:" + p);
        return (float) p;
    }

    public static void main(String[] args) {
        put("Hello World".getBytes());
        put("Hiep".getBytes());
        //put("Nat".getBytes());
        //put("Alper".getBytes());

        //        boolean result = get("Hello World".getBytes());
        boolean result = get("Nat".getBytes());

        if (result) {
            System.out.println("MAY BE baby!");
        } else {
            System.out.println("No!!!");
        }

        System.out.println("False Positive:" + falsePositive());

    }
}
