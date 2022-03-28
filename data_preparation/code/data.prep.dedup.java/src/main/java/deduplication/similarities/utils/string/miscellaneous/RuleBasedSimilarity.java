package deduplication.similarities.utils.string.miscellaneous;

import deduplication.similarities.utils.StringSimilarityMeasure;
import org.javatuples.Pair;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

/**
 * Created by jokoum on 12/19/16.
 */
public class RuleBasedSimilarity extends StringSimilarityMeasure {
    public RuleBasedSimilarity(String id, Boolean enabledCache, Map<String, Object> parameters) {
        super(id, enabledCache, SimilarityMeasureType.RuleBased, parameters);
    }

    @Override
    protected Double _similarity(String s1, String s2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        List<Pair<String, String>> rules = (List<Pair<String, String>>) parameters.get("rulesPairs");
        Double similarityValid = (Double) parameters.get("similarityValid");
        Double similarityInvalid = (Double) parameters.get("similarityInvalid");

        Double sim = null;
        for (Object o : rules) {
            Pair<String, String> pr = gson.fromJson(gson.toJson(o), Pair.class);
            if (s1.equals(pr.getValue0()) && s2.equals(pr.getValue1()) || (s2.equals(pr.getValue0()) && (s1.equals(pr.getValue1())))) {
                sim = similarityValid;
                break;
            }
        }
        if (sim == null) {
            sim = similarityInvalid;
        }
        return sim;
    }
}
