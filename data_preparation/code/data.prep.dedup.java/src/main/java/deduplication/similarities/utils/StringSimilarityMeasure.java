package deduplication.similarities.utils;

import deduplication.similarities.utils.string.edit.Levenshtein;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Created by jokoum on 10/9/16.
 */
public abstract class StringSimilarityMeasure extends SimilarityMeasure {
    protected SimilarityMeasureType type;

    protected SymmetricTable<String, Double> cache;
    protected Boolean enabledCache = Boolean.FALSE;

    protected Boolean checkNullValues = Boolean.TRUE;

    public StringSimilarityMeasure(String id, Boolean enabledCache, SimilarityMeasureType type, Map<String, Object> parameters) {
        this(id, enabledCache, Boolean.TRUE, type, parameters);
    }

    public StringSimilarityMeasure(String id, Boolean enabledCache, Boolean checkNullValues, SimilarityMeasureType type, Map<String, Object> parameters) {
        super(id, parameters);
        this.type = type;
        this.checkNullValues = checkNullValues;

        if (parameters.containsKey("token_measure")) {
            try {
                SimilarityMeasure tokenMeasure = SimilarityMeasure.valueOf(gson.fromJson(gson.toJson(parameters.get("token_measure")), ConfigurationUtils.SimilarityMeasureCfg.class));
                parameters.put("token_measure", tokenMeasure);
            } catch (Exception e) {
                e.printStackTrace();
                parameters.put("token_measure", new Levenshtein(id + "_token_measure", Boolean.FALSE, new HashMap<>()));
            }
        }
        this.enabledCache = enabledCache;
        if (this.enabledCache) {
            cache = new SymmetricTable<>();
        }
    }

    protected Double getCached(String s1, String s2) {
        if (enabledCache) {
            if (cache.contains(s1, s2)) {
                return cache.get(s1, s2);
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    protected void setCached(String s1, String s2, Double similarity) {
        if (enabledCache) {
            cache.put(s1, s2, similarity);
        }
    }

    @Override
    public Double similarity(Object s1, Object s2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        return similarity((String) s1, (String) s2);
    }

    Set<String> nullValues = new HashSet<String>(Arrays.asList("", "_"));
    public Double similarity(String s1, String s2) throws
            IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        if ( checkNullValues && ((s1 == null || s2 == null) || (nullValues.contains(s1) || nullValues.contains(s2)))) {
            return 0.0;
//            return 0.5;
//            return 0.85;
//            return 1.0;
        }
        if (enabledCache) {
            Double sim = getCached(s1, s2);
            if (sim != null) {
                return sim;
            } else {
                sim = _similarity(s1, s2);
                if (Double.isNaN(sim)) {
                    sim = 0.0;
                }
                setCached(s1, s2, sim);
                return sim;
            }
        } else {
            Double sim = _similarity(s1, s2);
            if (Double.isNaN(sim)) {
                sim = 0.0;
            }
            return sim;
        }
    }

    protected abstract Double _similarity(String s1, String s2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException;

    public SimilarityMeasureType getType() {
        return type;
    }

    @Override
    public String toString() {
        return id + " " + type.toString();
    }
}
