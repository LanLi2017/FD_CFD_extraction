package deduplication.similarities.utils.string.edit;

import deduplication.similarities.utils.StringSimilarityMeasure;
import deduplication.similarities.utils.string.edit.utils.DamerauLevenshteinByKevinAlgorithm;

import java.util.Map;

/**
 * Created by koumarelas on 7/19/16.
 */
public class DamerauLevenshtein extends StringSimilarityMeasure {

//    private final static Damerau dmr = new Damerau();
    private final static DamerauLevenshteinByKevinAlgorithm dmr = new DamerauLevenshteinByKevinAlgorithm(1,1,1,1);
//    private final static DamerauLevenshteinByClavinAlgorithm dmr = new DamerauLevenshteinByClavinAlgorithm();

    public DamerauLevenshtein(String id, Boolean enabledCache, Map<String, Object> parameters) {
        super(id, enabledCache, SimilarityMeasureType.DamerauLevenshtein, parameters);
    }

    @Override
    protected Double _similarity(String s1, String s2) {
//            sim = 1.0 - dmr.distance(s1, s2)/ Math.max(s1.length(), s2.length());
        return 1.0 - ((double)dmr.execute(s1, s2)/ Math.max(s1.length(), s2.length()));
//        return 1.0 - ((double)DamerauLevenshteinByClavinAlgorithm.damerauLevenshteinDistance(s1, s2)/ Math.max(s1.length(), s2.length()));
    }
}
