package deduplication.classification.utils;

import smile.math.Math;

/**
 * This is an modification of the CrossValidation class found in the SMILE package.
 * I wanted the permutation to be done with the same seed, so that we don't get inconsistencies across executions.
 */
public class CrossValidationStandardSeedPermutation {
    /**
     * The number of rounds of cross validation.
     */
    public final int k;
    /**
     * The index of training instances.
     */
    public final int[][] train;
    /**
     * The index of testing instances.
     */
    public final int[][] test;

    /**
     * Constructor.
     * @param n the number of samples.
     * @param k the number of rounds of cross validation.
     */
    public CrossValidationStandardSeedPermutation(int n, int k) {
        if (n < 0) {
            throw new IllegalArgumentException("Invalid sample size: " + n);
        }

        if (k < 0 || k > n) {
            throw new IllegalArgumentException("Invalid number of CV rounds: " + k);
        }

        this.k = k;

        Math.setSeed(0L);
        int[] index = Math.permutate(n);

        train = new int[k][];
        test = new int[k][];

        int chunk = n / k;
        for (int i = 0; i < k; i++) {
            int start = chunk * i;
            int end = chunk * (i + 1);
            if (i == k-1) end = n;

            train[i] = new int[n - end + start];
            test[i] = new int[end - start];
            for (int j = 0, p = 0, q = 0; j < n; j++) {
                if (j >= start && j < end) {
                    test[i][p++] = index[j];
                } else {
                    train[i][q++] = index[j];
                }
            }
        }
    }
}
