package deduplication.nonduplicates.indexing.batchindexingmethods;

import deduplication.nonduplicates.indexing.entities.Query;
import org.javatuples.Pair;

import java.util.*;

public class RecordParticipationCalculation {

    public static Map<String, Double> submoduleParticipationSimple(Query q, Map<String, Set<String>> blockingSchemesResults) {
        return keepSubsetBasedOnQueryType(q, combinedParticipationNormalized(blockingSchemesResults));
    }

    private static Map<String, Double> combinedParticipationNormalized(Map<String, Set<String>> modulesResultsSimple) {
        Map<String, Double> idToParticipation = new HashMap<>();
        Double maxParticipation = -1.0;
        for (Map.Entry<String, Set<String>> e: modulesResultsSimple.entrySet()) {
            for (String id: e.getValue()) {
                Double participation = idToParticipation.getOrDefault(id, 0.0) + 1.0;
                idToParticipation.put(id, participation);

                maxParticipation = Double.max(maxParticipation, participation);
            }
        }

//        Map<String, Double> idToParticipationNormalized = new HashMap<>();
//        for (Map.Entry<String, Integer> e: idToParticipation.entrySet()) {
//            idToParticipationNormalized.put(e.getKey(), e.getValue() / (double)maxParticipation);
//        }
//        return idToParticipationNormalized;

        // In-place update
        for (Map.Entry<String, Double> e: idToParticipation.entrySet()) {
            idToParticipation.put(e.getKey(), e.getValue() / (double)maxParticipation);
        }
        return idToParticipation;
    }

    private static Map<String, Double> keepSubsetBasedOnQueryType(Query q, Map<String, Double> idToParticipationNormalized) {
        Map<String, Double> idToParticipationNormalizedFiltered = new HashMap<>();
        switch(q.getRetrievalType()) {
            case ALL:
                for (Map.Entry<String, Double> e : idToParticipationNormalized.entrySet()) {
                    idToParticipationNormalizedFiltered.put(e.getKey(), e.getValue());
                }
                break;
            case TOP_K:
                Integer topK = q.getRetrievalThreshold().intValue();
                Double[] topKParticipations = new Double[topK];
                Arrays.fill(topKParticipations, -1.0);
                String[] topKIDs = new String[topK];
                Arrays.fill(topKIDs, null);

                for (Map.Entry<String, Double> e : idToParticipationNormalized.entrySet()) {
                    Integer idx = Arrays.binarySearch(topKParticipations, e.getValue());

                    if (idx < 0) {
                        idx += 1;
                        idx = Math.abs(idx);
                    }
                    if (idx >= topK) {
                        idx = topK - 1;
                    }

                    // Shift values
                    for (int i = 1; i <= idx; ++i) {
                        topKParticipations[i - 1] = topKParticipations[i];
                        topKIDs[i - 1] = topKIDs[i];
                    }
                    topKParticipations[idx] = e.getValue();
                    topKIDs[idx] = e.getKey();

                }

                for (int i = 0; i < topK; ++i) {
                    if (topKParticipations[i] != -1) {
                        idToParticipationNormalizedFiltered.put(topKIDs[i], topKParticipations[i]);
                    }
                }
                break;
            case TOP_K_PERCENTAGE:
                Double topKPercentage = q.getRetrievalThreshold();
                List<Pair<String, Double>> topKPercentageParticipants = new ArrayList<>();

                for (Map.Entry<String, Double> e : idToParticipationNormalized.entrySet()) {
                    topKPercentageParticipants.add(new Pair<>(e.getKey(), e.getValue()));
                }

                topKPercentageParticipants.sort(Comparator.comparing(Pair::getValue1));

                for (Pair<String, Double> p : topKPercentageParticipants.subList(0, (int) (topKPercentage * topKPercentageParticipants.size()))) {
                    idToParticipationNormalizedFiltered.put(p.getValue0(), p.getValue1());
                }

                break;
            case RANGE_SEARCH:
                Double queryThreshold = q.getRetrievalThreshold();
                for (Map.Entry<String, Double> e : idToParticipationNormalized.entrySet()) {
                    if (e.getValue() >= queryThreshold) {
                        idToParticipationNormalizedFiltered.put(e.getKey(), e.getValue());
                    }
                }
                break;
        }
        return idToParticipationNormalizedFiltered;
    }

}
