package deduplication.similarities.utils.string.miscellaneous;

import deduplication.similarities.utils.StringSimilarityMeasure;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Created by jokoum on 12/11/16.
 */
public class ExactMatchSimilarity extends StringSimilarityMeasure {
    public ExactMatchSimilarity(String id, Boolean enabledCache, Map<String, Object> parameters) {
        super(id, enabledCache, SimilarityMeasureType.ExactMatch , parameters);
    }

    @Override
    protected Double _similarity(String s1, String s2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        return s1.equals(s2) ? 1.0 : 0.0;
    }
}
