package deduplication.classification.utils;

import java.util.HashMap;
import java.util.Map;

public class SimilaritiesPair extends DedupPair {
    private Map<String, Double> sims;

    public SimilaritiesPair(String dataset, String id, String id1, String id2, PairClass pairClass, Map<String, Double> sims) {
        super(dataset, id, id1, id2, pairClass);
        this.sims = sims;
    }

    public SimilaritiesPair clone() {
        Map<String, Double> simsCopy = new HashMap<>();
        sims.forEach(simsCopy::put);
        return new SimilaritiesPair(dataset, id, id1, id2, pairClass, simsCopy);
    }

    public Map<String, Double> getSims() {
        return sims;
    }

//    @Override
//    public int compareTo(DedupPair o) {
//        SimilaritiesPair sp = (SimilaritiesPair) o;
//        Comparator<SimilaritiesPair> compareByFirstName = Comparator
//                .comparing(SimilaritiesPair::getDataset)
//                .thenComparing(SimilaritiesPair::getId1)
//                .thenComparing(SimilaritiesPair::getId2);
//        return compareByFirstName.compare(this, sp);
//    }
}
