package deduplication.similarities.utils.string.edit;

import deduplication.similarities.utils.StringSimilarityMeasure;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Created by jokoum on 10/10/16.
 */
public class Levenshtein extends StringSimilarityMeasure implements Serializable {
    public Levenshtein(String id, Boolean enabledCache, Map<String, Object> additionalParameters) {
        super(id, enabledCache, SimilarityMeasureType.Levenshtein, additionalParameters);
    }

    @Override
    protected Double _similarity(String s1, String s2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        int editDst = StringUtils.getLevenshteinDistance(s1, s2);
//        return 1.0 - ((double) editDst / (s1.length() + s2.length()));
        return 1.0 - ((double) editDst / Integer.max(s1.length(), s2.length()));
    }
}
