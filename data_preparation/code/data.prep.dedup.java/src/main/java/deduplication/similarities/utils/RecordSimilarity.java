package deduplication.similarities.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jokoum on 11/7/16.
 */
public class RecordSimilarity implements Comparable<RecordSimilarity>{
    protected String id = null;

    public RecordSimilarity(String id) {
        this(id, 0.0);
    }

    public RecordSimilarity(String id, Double overallSimilarity) {
        this.id = id;
        this.overallSimilarity = overallSimilarity;
    }

    public RecordSimilarity(String id, String similarityPoint, Double similarity) {
        this(id);
        setSimilarity(similarityPoint, similarity);
    }

    public RecordSimilarity(RecordSimilarity o) {
//        this(o.name);
        this(o.id, o.overallSimilarity);
        this.similarities.putAll(o.similarities);
        this.overallSimilarity = o.overallSimilarity;
    }

    /* Similarities */
    protected Map<String, Double> similarities = new HashMap<>();
//    Double sumWeightedSimilarities = 0.0;
//    Double sumSimilarityWeights = 0.0;

    private Double overallSimilarity = 0.0;

    public void setSimilarity(String similarityPoint, Double similarity) {
        if (!similarities.containsKey(similarityPoint)) {
            similarities.put(similarityPoint, similarity);
//            sumWeightedSimilarities += similarityPoint.getWeight() * similarity;
//            sumSimilarityWeights += similarityPoint.getWeight();
            overallSimilarity += similarity;
        } else {
            Double currentSimilarity = similarities.get(similarityPoint);
            similarities.put(similarityPoint, similarity);
//            sumWeightedSimilarities += similarity - currentSimilarity;
            overallSimilarity += similarity - currentSimilarity;
        }
//        overallSimilarity = sumWeightedSimilarities / sumSimilarityWeights;
    }

    public Double getOverallSimilarity() {
        return overallSimilarity;
    }

//    public void setOverallSimilarity(Double overallSimilarity) {
//        this.overallSimilarity = overallSimilarity;
//    }

    @Override
    public int compareTo(RecordSimilarity o) {
        int x = 0;
        if (o != null && o instanceof RecordSimilarity) {
            o = (RecordSimilarity)o;
            x = id.compareTo(o.id);
            if (x != 0) return x;

            return 0;
//            x = overallSimilarity.compareTo(((RecordResultSimilarity) o).overallSimilarity);
//            if (x != 0) return x;
        }
        return -1;
    }

    public Double getSimilarity(String point) {
        if (similarities.containsKey(point)) {
            return similarities.get(point);
        } else {
            return null;
        }
    }

    public void clearSimilarities() {
        similarities.clear();
        overallSimilarity = 0.0;
    }

    public Map<String, Double> getSimilarities() {
        return similarities;
    }

    public void setSimilarities(Map<String, Double> similarities) {
        this.similarities = similarities;
    }

    public String getId() {
        return id;
    }
}
