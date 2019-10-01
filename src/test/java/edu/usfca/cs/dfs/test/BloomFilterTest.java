package edu.usfca.cs.dfs.test;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import edu.usfca.cs.dfs.bloomfilter.BloomFilter;

public class BloomFilterTest {

    @Test
    public void testPutsAndGets() {
        System.out.println("******************* TEST #1 ****************");
        String randomString = generateRandomString();
        for (int i = 0; i < 100; i++) {
            BloomFilter.put(randomString.getBytes());
        }
        BloomFilter.put("Hiep".getBytes());
        BloomFilter.put("Nat".getBytes());
        BloomFilter.put("Alper".getBytes());
        BloomFilter.put("Ali".getBytes());

        String searchText = "Alper";

        boolean result = BloomFilter.get(searchText.getBytes());

        Assert.assertEquals(result, true);

        if (result) {
            System.out.println("MAY BE baby! for:" + searchText);
        } else {
            System.out.println("No!!! for:" + searchText);
        }

        searchText = "Hello!!!!!!";
        result = BloomFilter.get(searchText.getBytes());
        Assert.assertEquals(result, false);
        if (result) {
            System.out.println("MAY BE baby! for:" + searchText);
        } else {
            System.out.println("No!!! for:" + searchText);
        }

        System.out.println("False Positive:" + BloomFilter.falsePositive());
    }

    @Test
    public void testFalsePositiveFunction() {
        BloomFilter.numberOfItems = 0;
        System.out.println("******************* TEST #2 ****************");
        String randomString = generateRandomString();
        for (int i = 0; i < 100; i++) {
            BloomFilter.put(randomString.getBytes());
        }
        System.out.println("Number of items in the filter(n):" + BloomFilter.filterLength);
        System.out.println("Number of Hash Time(k):" + BloomFilter.hashTime);
        System.out.println("Number of bits in the filter(m):" + BloomFilter.numberOfItems);
        System.out.println("False Positive:" + BloomFilter.falsePositive());

        /**
         * n = 100
            p = 0.857951642 (1 in 1)
            m = 100 (13B)
            k = 3
         */
        Assert.assertEquals(BloomFilter.falsePositive(), 0.857951642, 0.0001);

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
