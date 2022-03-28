package deduplication.nonduplicates.indexing.utils;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jokoum on 10/9/16.
 */
public class BlockingKeyGenerator implements Serializable {

    protected String id;
    protected List<String> selectedAttributes;
    protected BlockingKeyGeneratorType blockingType;
    protected Map<String, Object> parameters;

    /* For MinHash */
    static Random r = new Random(0);
    static int numHash = 6;
    static int bandBy = 1; // 1: No banding
    static int[] randomHashNumber;

    /* For phonetic encoding */
    protected transient DoubleMetaphone dm = new DoubleMetaphone();

    public BlockingKeyGenerator(String id, List<String> selectedAttributes, BlockingKeyGeneratorType blockingType, Map<String, Object> parameters) {
        this.id = id;
        this.selectedAttributes = selectedAttributes;
        this.blockingType = blockingType;
        this.parameters = parameters;

        if (parameters.containsKey("num_hash")) {
            this.numHash = Integer.valueOf((String) parameters.get("num_hash"));
        }

        if (parameters.containsKey("band_by")) {
            this.bandBy = Integer.valueOf((String) parameters.get("band_by"));
        }

        randomHashNumber = new int[numHash];
        for (int i = 0 ; i < numHash; ++i) {
            randomHashNumber[i] = r.nextInt() + 1;
        }
    }

    public enum BlockingKeyGeneratorType {
        SIMPLE,

        CONCATENATE,

        CONCATENATE_SORTED_TOKENS,

        TOKENIZE_LIST, TOKENIZE_SET,

        CHARACTER_N_GRAM_LIST, CHARACTER_N_GRAM_SET, CHARACTER_N_GRAM_SET_AS_SINGLE_STRING,

        WORD_N_GRAM_SET,

        N_PREFIX,

        MINHASH_ON_NGRAMS_SET, N_GRAM_LIST_ON_TOKENIZE_LIST, PHONETIC_ENCODING_LIST_ON_TOKENIZE_LIST,

        PHONETIC_ENCODING
    }

    //    NONE(null), CONCATENATE(null), TOKENIZE(null), N_GRAM(Map parameters);

    public Collection<String> apply(Map<String, String> r) {
        if (r.get(selectedAttributes.get(0)) == null) {
            return Collections.emptyList();
        }
        switch (blockingType) {
            case SIMPLE:
                return Collections.singleton(r.get(selectedAttributes.get(0)));
            case CONCATENATE:
                String concatenatedValue = concatenate(r);
                return Collections.singletonList(concatenatedValue);
            case CHARACTER_N_GRAM_LIST:
                return getCharacterNGrams(r, (Integer) parameters.getOrDefault("ngrams", 4), new ArrayList<>());
            case CHARACTER_N_GRAM_SET:
                return getCharacterNGrams(r, (Integer) parameters.getOrDefault("ngrams", 4), new HashSet<>());
            case WORD_N_GRAM_SET:
                return getWordNGrams(r,
                        Integer.valueOf(String.valueOf(parameters.getOrDefault("ngrams", 4))),
                        String.valueOf(parameters.get("delimiter")));
            case CHARACTER_N_GRAM_SET_AS_SINGLE_STRING: // This has to be ordered
                TreeSet ts = (TreeSet) getCharacterNGrams(r, (Integer)parameters.getOrDefault("ngrams", 4), new TreeSet<>());
                return Collections.singleton(String.join("|", ts));
            case N_PREFIX:
                Integer n = (Integer) parameters.getOrDefault("ngrams", "4");
                concatenatedValue = concatenate(r);
                String resultValue = n > concatenatedValue.length() ? concatenatedValue : concatenatedValue.substring(0, n);
                return Collections.singleton(resultValue);
            case TOKENIZE_LIST:
                return tokenize(r, (String) parameters.getOrDefault("delimiter", " "), new ArrayList<>());
            case TOKENIZE_SET:
                return tokenize(r, (String) parameters.getOrDefault("delimiter", " "), new HashSet<>());
            case CONCATENATE_SORTED_TOKENS:
                Collection<String> tokensSorted = tokenize(r, (String) parameters.getOrDefault("delimiter", " "), new TreeSet<>());
                return Collections.singleton(String.join("|", tokensSorted));
            case MINHASH_ON_NGRAMS_SET:
                Set<String> cl = (Set<String>) getCharacterNGrams(r, (Integer)parameters.get("ngrams"), new TreeSet<>());
                long[] sgn = getSignature(cl, numHash);
                Long[] sgnBoxed = ArrayUtils.toObject(sgn);
                List<Long> sgnList = Arrays.asList(sgnBoxed);
                int bands = numHash / bandBy;
                Collection<String> groupedCl = new ArrayList<>();
                for (int i = 0; i < bands; ++i) {
                    groupedCl.add(String.join("|", sgnList.subList(i * bandBy, (i+1) * bandBy).stream().map(Object::toString).collect(Collectors.toList())));
                }
                return groupedCl;
            case PHONETIC_ENCODING:
                concatenatedValue = concatenate(r);
                String phoneticEncoding = (String) parameters.get("phoneticencoding");
                switch (phoneticEncoding) {
                    case "doublemetaphone":
                        String phonetic = dm.doubleMetaphone(concatenatedValue);
                        if (phonetic == null) {
                            phonetic = concatenatedValue;
                        }
                        return Collections.singletonList(phonetic);
                }
            case N_GRAM_LIST_ON_TOKENIZE_LIST:
                Collection<String> toks = tokenize(r, (String) parameters.get("delimiter"), new ArrayList<>());

                Collection<String> nGrams = new ArrayList<>();
                for (String tok : toks) {
                    nGrams.addAll(getCharacterNGrams(tok, (Integer)parameters.get("ngrams"), new ArrayList<>()));
                }
                return nGrams;
            case PHONETIC_ENCODING_LIST_ON_TOKENIZE_LIST:
                toks = tokenize(r, (String) parameters.get("delimiter"), new ArrayList<>());
                phoneticEncoding = (String) parameters.get("phoneticencoding");

                Collection<String> phonEncdList = new ArrayList<>();
                for (String tok : toks) {
                    switch (phoneticEncoding) {
                        case "doublemetaphone":
                            String phonetic = dm.doubleMetaphone(tok);
                            if (phonetic == null) {
                                phonetic = tok;
                            }
                            phonEncdList.add(phonetic);
                    }
                }
                return phonEncdList;
        }
        return Collections.emptyList();
    }

    private String concatenate(Map<String, String> r) {
        if (selectedAttributes.size() == 1) {
            return r.get(selectedAttributes.get(0));
        } else {
            return String.join("|", selectedAttributes.stream().map(r::get).collect(Collectors.toList()));
        }
    }

    private Collection<String> tokenize(Map<String, String> r, String delimiters, Collection<String> cl) {
        for (String attribute : selectedAttributes) {
            if (!r.containsKey(attribute) || r.get(attribute).trim().equals("")) {
                continue;
            }
            cl.addAll(tokenizeString(r.get(attribute), delimiters));
        }
        return cl;
    }

    private List<String> tokenizeString(String s, String delimiters) {
        //            String[] tokens = r.get(attribute).split(delimiters);
        if(s == null || s.trim().equals("")) {
            return new ArrayList<>();
        }
        List<String> tokens = Lists.newArrayList((Splitter.on(CharMatcher.anyOf(delimiters)).split(s).iterator()));
//            List<String> tokensList = new ArrayList<>(Arrays.asList(tokens));
        tokens = tokens.stream().map(String::trim).filter(tok -> !tok.trim().equals("")).collect(Collectors.toList());
//            cl.addAll(new ArrayList<>(Arrays.asList()).stream().map(String::trim).collect(Collectors.toList()));
//            cl.addAll(new ArrayList<>(Arrays.asList(r.get(attribute).split(delimiters))).stream().map(String::trim).collect(Collectors.toList()));
        return tokens;
    }

    private Collection<String> getCharacterNGrams(Map<String, String> r, int nGrams, Collection<String> cl) {
        List<String> tokens = new LinkedList<>();
        for (String attribute : selectedAttributes) {
            tokens.addAll(getCharacterNGrams(r.get(attribute), nGrams, cl));
        }
        return tokens;
    }

    public Collection<String> getCharacterNGrams(String s, int n, Collection<String> cl) {
        if (s.length() <= n) {
            cl.add(s);
        } else {
            for (int i = 0 ; i < s.length() - n + 1; ++i) {
                cl.add(s.substring(i, i + n));
            }
        }

        return cl;
    }

    private Collection<String> getWordNGrams(Map<String, String> r, int nGrams, String delimiter) {
        Collection<String> results = new ArrayList<>();
        for (String attribute : selectedAttributes) {
            results.addAll(getWordNGrams(r.get(attribute), nGrams, delimiter));
        }
        return results;
    }

    public Collection<String> getWordNGrams(String s, int n, String delimiter) {
        Collection<String> results = new ArrayList<>();
        List<String> tokens = tokenizeString(s, delimiter);
        if (tokens.size() <= n) {
            results.add(s);
        } else {
            for (int i = 0 ; i < tokens.size() - n + 1; ++i) {
                results.add(String.join(" ", tokens.subList(i, i + n)));
            }
        }
        return results;
    }

    //using circular shifts: http://en.wikipedia.org/wiki/Circular_shift
    //http://stackoverflow.com/questions/5844084/java-circular-shift-using-bitwise-operations
    //circular shifts XOR random number
    private long getHash(int value, int random, int shift){
        //the first hash function comes from string.hashCode()
        //http://www.codatlas.com/github.com/openjdk-mirror/jdk7u-jdk/master/src/share/classes/java/lang/String.java?keyword=String&line=1494
        if(shift == 0)
            return value;
        int rst = (value >>> shift) | (value << (Integer.SIZE - shift));
        return rst ^ random;
    }

    public Double calculateSignaturesSimilarity(long[] signature1, long[] signature2, int numHash) {
        int similarity = 0;
        for (int i = 0; i < numHash; i++){
            if (signature1[i] == signature2[i]) {
                ++similarity;
            }
        }
        return (double)similarity / numHash;
    }

    public long[] getSignature(Set<String> s, int numHash) {
        long[] minHashValues = new long[numHash];
        Arrays.fill(minHashValues, Long.MAX_VALUE);
        for (int i = 0; i < numHash; i++){
            for(String tok : s) {
                minHashValues[i] = Math.min(minHashValues[i], getHash(tok.hashCode(), randomHashNumber[i], i));
            }
        }
        return minHashValues;
    }
}
