package deduplication.similarities.utils.string.token;

import deduplication.similarities.utils.StringSimilarityMeasure;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by jokoum on 12/11/16.
 */
public class JaccardSimilarity extends StringSimilarityMeasure implements Serializable {
    String delimiter;

    public JaccardSimilarity(String id, Boolean enabledCache, Map<String, Object> parameters) {
        this(id, enabledCache, SimilarityMeasureType.Jaccard, parameters);
    }

    public JaccardSimilarity(String id, Boolean enabledCache, SimilarityMeasureType typeSimMsr, Map<String, Object> parameters) {
        super(id, enabledCache, typeSimMsr, parameters);
        delimiter =  parameters.containsKey("delimiter") ? (String) parameters.get("delimiter") : " ";
    }

    @Override
    protected Double _similarity(String s1, String s2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        final Set<String> s1Toks = new HashSet<>(Arrays.asList(s1.split(delimiter)));
        final Set<String> s2Toks = new HashSet<>(Arrays.asList(s2.split(delimiter)));

        final Set<String> union = new HashSet<String>(s1Toks);
        union.addAll(s2Toks);
//        final Set<String> intersection = new HashSet<String>(s1Toks);
//        intersection.retainAll(s2Toks);

        final int intersection = (s1Toks.size() + s2Toks.size()) - union.size();

        //return JaccardSimilarity
        return (double) intersection / union.size();
    }

//    public static class JaccardNGrams extends JaccardSimilarity {
//        Integer n;
////        BlockingKeyGenerator ac = new BlockingKeyGenerator("jaccard_ngrams", new ArrayList<>(),
////                BlockingKeyGenerator.AttributesCombinationType.CONCATENATE, new HashMap<>());
//
//        public JaccardNGrams(String name, Boolean enabledCache, Map<String, Object> parameters) {
//            super(name, enabledCache, SimilarityMeasureType.JaccardNGrams, parameters);
//            if (parameters.containsKey("n")) {
//                this.n = Integer.valueOf(String.valueOf(parameters.get("n")));
//            } else {
//                this.n = 3;
//            }
//        }
//
//        @Override
//        protected Double _similarity(String s1, String s2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
//            final Set<String> s1Toks = new HashSet<>(ac.getNGrams(s1, n, new ArrayList<>()));
//            final Set<String> s2Toks = new HashSet<>(ac.getNGrams(s2, n, new ArrayList<>()));
//
//            final Set<String> union = new HashSet<String>(s1Toks);
//            union.addAll(s2Toks);
//
//            final int intersection = (s1Toks.size() + s2Toks.size()) - union.size();
//
//            //return JaccardSimilarity
//            return (double) intersection / union.size();
//        }
//    }

//    public static class JaccardNGramsOptimized extends JaccardNGrams {
//        protected static final int N_GRAMS = 3;
//        public JaccardNGramsOptimized(String name, Boolean enabledCache, Map<String, Object> parameters) {
//            super(name, enabledCache, parameters);
//        }
//
////        protected int findBinarySearchPosition(int[] array, int value) {
////
////        }
//
//        public int compare(char[] a, int froma, char[] b, int fromb, int charsToCompare)
//        {
//            if (a == null || b == null) {
//                return -1;
//            }
//
//            for (int i = 0; i < charsToCompare; ++i) {
//                if (a[froma + i] < b[fromb + i]) {
//                    return -1;
//                } else if (a[froma + i] > b[fromb + i]) {
//                    return 1;
//                }
//            }
//            return 0;
//        }
//
//        protected int fineTunePosition(
//                int[] a,
//                int a_cnt,
//                char[] a_chr,
//                int[] b,
//                int b_cnt,
//                char[] b_chr
//        ) {
//            while (
//                    posHashCode -1 >= 0 &&
//                            hashCode == a[posHashCode-1] &&
//                            (cmpStr = compare(a_chr, posHashCode-1, a_chr, i, N_GRAMS)) > 0) { // As long as your are smaller than the other
//                --posHashCode;
//            }
//
//            while (
//                    posHashCode +1 >= 0 &&
//                            hashCode == a[posHashCode+1] &&
//                            (cmpStr = compare(a_chr, posHashCode+1, a_chr, i, N_GRAMS)) < 0) { // As long as your are larger than the other
//                --posHashCode;
//            }
//        }
//
//        @Override
//        protected Double _similarity(String s1, String s2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
//            int[] a = new int[s1.length() - N_GRAMS + 1], a_idx = new int[s1.length() - N_GRAMS + 1];
//            int a_cnt = 0;
//            char[] a_chr = s1.toCharArray();
//            int[] b = new int[s2.length() - N_GRAMS + 1], b_idx = new int[s2.length() - N_GRAMS + 1];
//            int b_cnt = 0;
//            char[] b_chr = s2.toCharArray();
//
//
//            for (int i = 0; i < s1.length(); ++i) {
//                int hashCode = 0;
//                for (int j = 0; j < N_GRAMS; ++j) {
//                    hashCode += a_chr[i];
//                }
//                int posHashCode = Arrays.binarySearch(a, 0, a_cnt, hashCode);
//                int cmpStr = 0;
//
//                if (hashCode == a[posHashCode]) {
//                    cmpStr = compare(a_chr, posHashCode, a_chr, i, N_GRAMS);
//                    if (cmpStr == 0) {
//                        // The same string exists, do nothing
//                        continue;
//                    } else { // Find the position based on string comparison
//
//                    }
//                } else { // Find the position based on hashCode
//
//
//                }
//            }
//
//        }
//    }

//    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
//        JaccardNGrams jng = new JaccardNGrams("", Boolean.FALSE, Collections.emptyMap());
//
////        String s1 = "abcdefg";
////        String s2 = "abcdef";
//        String s1 = "abcde";
//        String s2 = "bcd";
//
//        System.out.println(jng.similarity(s1, s2));
//    }
}
