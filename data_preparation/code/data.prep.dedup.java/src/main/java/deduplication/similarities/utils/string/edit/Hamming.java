package deduplication.similarities.utils.string.edit;

import deduplication.similarities.utils.StringSimilarityMeasure;
import org.simmetrics.StringDistance;
import org.simmetrics.metrics.HammingDistance;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Created by jokoum on 1/4/17.
 */
public class Hamming extends StringSimilarityMeasure {
    StringDistance hm = HammingDistance.forString();
    public Hamming(String id, Boolean enabledCache, Map<String, Object> parameters) {
        super(id, enabledCache, SimilarityMeasureType.Hamming, parameters);
    }

    @Override
    protected Double _similarity(String s1, String s2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        if (s1.length() != s2.length()) {
            return 0.0;
        } else {
            return 1.0 - (hm.distance(s1, s2) / (double) s1.length());
        }
    }
}
