package edu.usfca.cs.dfs.test;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import edu.usfca.cs.dfs.bloomfilter.BloomFilter;
import edu.usfca.cs.dfs.config.ConfigManagerController;

public class BloomFilterTest {

    @Test
    public void testPutsAndGets() {
        System.out.println("******************* TEST #1 **************B*");
        BloomFilter bf = new BloomFilter(ConfigManagerController.getInstance().getFilterLength(),
                                         ConfigManagerController.getInstance().getHashTime(),
                                         ConfigManagerController.getInstance().getSeed());
        String randomString = generateRandomString();
        for (int i = 0; i < 100; i++) {
            bf.put(randomString.getBytes());
        }
        bf.put("Hiep".getBytes());
        bf.put("Nat".getBytes());
        bf.put("Alper".getBytes());
        bf.put("Ali".getBytes());

        String searchText = "Alper";

        boolean result = bf.get(searchText.getBytes());

        Assert.assertEquals(result, true);

        if (result) {
            System.out.println("MAY BE baby! for:" + searchText);
        } else {
            System.out.println("No!!! for:" + searchText);
        }

        searchText = "Hello!!!!!!";
        result = bf.get(searchText.getBytes());
        Assert.assertEquals(result, false);
        if (result) {
            System.out.println("MAY BE baby! for:" + searchText);
        } else {
            System.out.println("No!!! for:" + searchText);
        }

        System.out.println("False Positive:" + bf.falsePositive());
    }

    @Test
    public void testFalsePositiveFunction() {
        BloomFilter bf = new BloomFilter(ConfigManagerController.getInstance().getFilterLength(),
                                         ConfigManagerController.getInstance().getHashTime(),
                                         ConfigManagerController.getInstance().getSeed());
        System.out.println("******************* TEST #2 ****************");
        String randomString = generateRandomString();
        for (int i = 0; i < 100; i++) {
            bf.put(randomString.getBytes());
        }
        System.out.println("Number of items in the filter(n):" + bf.getFilterLength());
        System.out.println("Number of Hash Time(k):" + bf.getHashTime());
        System.out.println("Number of bits in the filter(m):" + bf.getNumberOfItems());
        System.out.println("False Positive:" + bf.falsePositive());

        /**
         * n = 100
            p = 0.857951642 (1 in 1)
            m = 100 (13B)
            k = 3
         */
        Assert.assertEquals(bf.falsePositive(), 0.857951642, 0.0001);

    }

    public static String generateRandomString() {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 10;
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit
                    + (int) (random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        return buffer.toString();
    }

}
