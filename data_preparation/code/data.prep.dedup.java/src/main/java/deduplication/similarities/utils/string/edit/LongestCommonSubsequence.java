package deduplication.similarities.utils.string.edit;

import deduplication.similarities.utils.StringSimilarityMeasure;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.StringMetrics;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Created by jokoum on 12/19/16.
 */
public class LongestCommonSubsequence extends StringSimilarityMeasure implements Serializable {
    public static StringMetric longestCommonSubsequence = StringMetrics.longestCommonSubsequence();

    public LongestCommonSubsequence(String id, Boolean enabledCache, Map<String, Object> parameters) {
        super(id, enabledCache, SimilarityMeasureType.LongestCommonSubsequence, parameters);
    }

    @Override
    protected Double _similarity(String s1, String s2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        return Double.valueOf(longestCommonSubsequence.compare(s1, s2));
    }
}
