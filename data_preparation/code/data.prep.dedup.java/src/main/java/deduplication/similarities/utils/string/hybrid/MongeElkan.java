package deduplication.similarities.utils.string.hybrid;

import deduplication.similarities.utils.StringSimilarityMeasure;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.DamerauLevenshtein;
import org.simmetrics.metrics.JaroWinkler;
import org.simmetrics.metrics.Levenshtein;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jokoum on 2/22/17.
 */
public class MongeElkan extends StringSimilarityMeasure {

    org.simmetrics.metrics.MongeElkan me = null;

    public MongeElkan(String id, Boolean enabledCache, Map<String, Object> parameters, StringMetric tokenMeasure) {
        this(id, enabledCache, SimilarityMeasureType.MongeElkan, parameters, tokenMeasure);
    }

    public MongeElkan(String id, Boolean enabledCache, SimilarityMeasureType similarityMeasureType, Map<String, Object> parameters, StringMetric tokenMeasure) {
        super(id, enabledCache, similarityMeasureType, parameters);
        this.me = new org.simmetrics.metrics.MongeElkan(tokenMeasure);
    }

    @Override
    protected Double _similarity(String s1, String s2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        return (double) me.compare(Arrays.asList(s1.split(" ")), Arrays.asList(s2.split(" ")));
    }

    public static class MongeElkanLevenshtein extends MongeElkan {

        public MongeElkanLevenshtein(String id, Boolean enabledCache, Map<String, Object> parameters) {
            super(id, enabledCache, SimilarityMeasureType.MongeElkanLevenshtein, parameters, new Levenshtein());
        }
    }

    public static class MongeElkanDamerauLevenshtein extends MongeElkan {

        public MongeElkanDamerauLevenshtein(String id, Boolean enabledCache, Map<String, Object> parameters) {
            super(id, enabledCache, SimilarityMeasureType.MongeElkanDamerauLevenshtein, parameters, new DamerauLevenshtein());
        }
    }

    public static class MongeElkanJaroWinkler extends MongeElkan {

        public MongeElkanJaroWinkler(String id, Boolean enabledCache, Map<String, Object> parameters) {
            super(id, enabledCache, SimilarityMeasureType.MongeElkanJaroWinkler, parameters, new JaroWinkler());
        }
    }

    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
//        MongeElkanLevenshtein msr = new MongeElkanLevenshtein("1", Boolean.FALSE, new HashMap<>());
        deduplication.similarities.utils.string.edit.Levenshtein msr = new deduplication.similarities.utils.string.edit.Levenshtein("1", Boolean.FALSE, new HashMap<>());
        System.out.println(msr.similarity("PU", "UP"));
    }
}
