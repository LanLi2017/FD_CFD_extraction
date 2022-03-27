package deduplication.similarities.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import deduplication.similarities.utils.string.edit.*;
import deduplication.similarities.utils.string.hybrid.MongeElkan;
import deduplication.similarities.utils.string.hybrid.MongeElkanSymmetric;
import deduplication.similarities.utils.string.hybrid.StableMatching;
import deduplication.similarities.utils.string.miscellaneous.ExactMatchSimilarity;
import deduplication.similarities.utils.string.miscellaneous.RuleBasedSimilarity;
import deduplication.similarities.utils.string.token.JaccardSimilarity;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Created by jokoum on 1/4/17.
 */
public abstract class SimilarityMeasure {
    protected final static Gson gson = new GsonBuilder().enableComplexMapKeySerialization().setPrettyPrinting().create();

    protected String id;
    protected Map<String, Object> parameters;

    public SimilarityMeasure(String id, Map<String, Object> parameters) {
        this.id = id;
        this.parameters = parameters;
    }

    public static enum SimilarityMeasureType {
        // String
        Levenshtein, DamerauLevenshtein, Hamming, JaroWinkler, LongestCommonSubsequence,

        MongeElkan, MongeElkanLevenshtein, MongeElkanDamerauLevenshtein, MongeElkanJaroWinkler,
        MongeElkanSymmetric, MongeElkanSymmetricLevenshtein, MongeElkanSymmetricDamerauLevenshtein, MongeElkanSymmetricJaroWinkler,

        StableMatching, StableMatchingLevenshtein, StableMatchingDamerauLevenshtein, StableMatchingJaroWinkler,

        RuleBased,
        Jaccard, JaccardNGrams,
        DatasetSpecific,

        ExactMatch,

        // Geolocation
        Haversine
        // TODO
    }
//    public static Set<String> stringSimilarityMeasureTypes = new HashSet<>();
//    static {
//        Arrays.stream(SimilarityMeasureType.values()).forEach(x -> stringSimilarityMeasureTypes.add(x.toString()));
//    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public static SimilarityMeasure valueOf(ConfigurationUtils.SimilarityMeasureCfg msrCfg) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, ClassNotFoundException {
        switch (msrCfg.similarityMeasureType) {
            case ExactMatch:
                return new ExactMatchSimilarity(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters);
            case Jaccard:
                return new JaccardSimilarity(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters);
//            case JaccardNGrams:
//                return new JaccardSimilarity.JaccardNGrams(msrCfg.name, msrCfg.enabled_cache, msrCfg.parameters);
            case Levenshtein:
                return new Levenshtein(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters);
            case DamerauLevenshtein:
                return new DamerauLevenshtein(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters);
            case Hamming:
                return new Hamming(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters);
            case JaroWinkler:
                return new JaroWinkler(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters);
            case MongeElkanLevenshtein:
                return new MongeElkan.MongeElkanLevenshtein(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters);
            case MongeElkanDamerauLevenshtein:
                return new MongeElkan.MongeElkanDamerauLevenshtein(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters);
            case MongeElkanJaroWinkler:
                return new MongeElkan.MongeElkanJaroWinkler(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters);

            case MongeElkanSymmetric:
                StringSimilarityMeasure tokenSimilarityMeasure = (StringSimilarityMeasure) SimilarityMeasure.valueOf
                        ((ConfigurationUtils.SimilarityMeasureCfg) msrCfg.parameters.get("token_measure"));
                return new MongeElkanSymmetric(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters, tokenSimilarityMeasure);
            case MongeElkanSymmetricLevenshtein:
                return new MongeElkanSymmetric.MongeElkanSymmetricLevenshtein(msrCfg.id, msrCfg.enabled_cache);
            case MongeElkanSymmetricDamerauLevenshtein:
                return new MongeElkanSymmetric.MongeElkanSymmetricDamerauLevenshtein(msrCfg.id, msrCfg.enabled_cache);
            case MongeElkanSymmetricJaroWinkler:
                return new MongeElkanSymmetric.MongeElkanSymmetricJaroWinkler(msrCfg.id, msrCfg.enabled_cache);

            case StableMatching:
                tokenSimilarityMeasure = (StringSimilarityMeasure) SimilarityMeasure.valueOf
                        ((ConfigurationUtils.SimilarityMeasureCfg) msrCfg.parameters.get("token_measure"));
                return new StableMatching(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters, tokenSimilarityMeasure);
            case StableMatchingLevenshtein:
                return new StableMatching.StableMatchingLevenshtein(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters);
            case StableMatchingDamerauLevenshtein:
                return new StableMatching.StableMatchingDamerauLevenshtein(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters);
            case StableMatchingJaroWinkler:
                return new StableMatching.StableMatchingJaroWinkler(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters);

            case LongestCommonSubsequence:
                return new LongestCommonSubsequence(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters);
            case RuleBased:
                return new RuleBasedSimilarity(msrCfg.id, msrCfg.enabled_cache, msrCfg.parameters);
//            case DatasetSpecific:
//                Object[] arguments = new Object[] {msrCfg.name, msrCfg.enabled_cache, msrCfg.parameters};
//                Class measureClass = Class.forName(msrCfg.measureClass);
//                Class[] argsClass = new Class[] { String.class, Boolean.class, Map.class };
//                Constructor constructor = measureClass.getConstructor(argsClass);
//
//                return (StringSimilarityMeasure) constructor.newInstance(arguments);
            default:
                return null;
        }
    }

    public abstract Double similarity(Object o1, Object o2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException;

    @Override
    public String toString() {
        return id;
    }
}
