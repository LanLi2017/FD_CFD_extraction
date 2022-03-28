package deduplication.similarities.utils.string.edit;

import deduplication.similarities.utils.StringSimilarityMeasure;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Created by jokoum on 12/11/16.
 */
public class JaroWinkler extends StringSimilarityMeasure {

    org.simmetrics.metrics.JaroWinkler jaroWinkler = new org.simmetrics.metrics.JaroWinkler();
    public JaroWinkler(String id, Boolean enabledCache, Map<String, Object> parameters) {
        super(id, enabledCache, SimilarityMeasureType.JaroWinkler, parameters);
    }

    @Override
    protected Double _similarity(String s1, String s2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
//        return 1.0 - StringUtils.getJaroWinklerDistance(s1, s2);
        return (double) jaroWinkler.compare(s1, s2);
    }
}
