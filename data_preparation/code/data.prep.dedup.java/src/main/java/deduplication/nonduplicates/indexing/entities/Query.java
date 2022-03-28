package deduplication.nonduplicates.indexing.entities;

import java.util.Map;

/**
 * Created by koumarelas on 9/7/16.
 */
public class Query extends Record {

    public enum RetrievalType {
        ALL, TOP_K, TOP_K_PERCENTAGE, RANGE_SEARCH
    }

    protected RetrievalType retrievalType;
    protected Double retrievalThreshold;

    public Query(Map<String, String> attributeValues) {
        this(attributeValues, Query.RetrievalType.ALL, -1.0);
    }

    public Query(Map<String, String> attributeValues, RetrievalType retrievalType, Double retrievalThreshold) {
        super(attributeValues);
        this.retrievalType = retrievalType;
        this.retrievalThreshold = retrievalThreshold;
    }

    public Query clone() {
        return new Query(values, retrievalType, retrievalThreshold);
    }

    public Record toRecord() {
        return new Record(values);
    }

    public RetrievalType getRetrievalType() {
        return retrievalType;
    }

    public Double getRetrievalThreshold() {
        return retrievalThreshold;
    }

    public void setRetrievalType(RetrievalType retrievalType) {
        this.retrievalType = retrievalType;
    }

    public void setRetrievalThreshold(Double retrievalThreshold) {
        this.retrievalThreshold = retrievalThreshold;
    }
}
