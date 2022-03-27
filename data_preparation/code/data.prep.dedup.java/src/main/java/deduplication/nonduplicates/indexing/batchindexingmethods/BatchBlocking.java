package deduplication.nonduplicates.indexing.batchindexingmethods;

import deduplication.nonduplicates.indexing.entities.Query;
import deduplication.nonduplicates.indexing.entities.Record;
import deduplication.nonduplicates.indexing.utils.BlockingKeyGenerator;
import deduplication.nonduplicates.indexing.utils.PairUtils;
import deduplication.similarities.utils.SimilarityMeasure;
import deduplication.similarities.utils.SymmetricTable;
import deduplication.similarities.utils.string.edit.Levenshtein;
import org.apache.log4j.Logger;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import static deduplication.nonduplicates.indexing.batchindexingmethods.RecordParticipationCalculation.submoduleParticipationSimple;

/**
 * Created by jokoum on 30/03/17.
 */
public class BatchBlocking {
    protected Logger logger = Logger.getLogger(BatchBlocking.class);

    public List<Pair<Pair<String,String>, Double>> getPairsOrderedBySimilarity(Set<Pair<String, String>> matches,
                                                                                       Map<String, BlockingKeyGenerator> combiners,
                                                                                       Collection<Record> records) {

        SimilarityMeasure msr = new Levenshtein("best_attribute_similarity", Boolean.FALSE, Collections.emptyMap());

        Map<String, Record> id2rec = new HashMap<>();
        records.forEach(x -> id2rec.put(x.getID(), x));

        List<Pair<Pair<String,String>, Double>> pairsWSims = Collections.synchronizedList(new ArrayList<>());

        matches.parallelStream().forEach(p -> {
            Double sumSim = 0.0;
            int cntSims = 0;
            for (Map.Entry<String, BlockingKeyGenerator> e: combiners.entrySet()) {
                Collection<String> tokens1 = e.getValue().apply(id2rec.get(p.getValue0()).getValues());
                Collection<String> tokens2 = e.getValue().apply(id2rec.get(p.getValue1()).getValues());

                try {
                    Double sim = msr.similarity(tokens1.iterator().next(), tokens2.iterator().next());
                    sumSim += sim;
                    ++cntSims;
                } catch (IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e1) {
                    e1.printStackTrace();
                }
            }
            if (cntSims == 0) {
                return;
            }
            sumSim = sumSim / cntSims;

            pairsWSims.add(new Pair<>(p, sumSim));
        });

        pairsWSims.sort((o1, o2) -> {
            if (o1 != null && o2 != null) {
                return o1.getValue1().compareTo(o2.getValue1());
            } else {
                System.out.println("error");
                return 0;
            }
        });

        return pairsWSims;
    }

    public Set<Triplet<String, String, Double>> getMatchedPairs(Collection<Record> records, Map<String, BlockingKeyGenerator> combiners) {
        logger.info("Starting batch blocking, with " + records.size() + " records");

        Map<String, HashMap<String, List<String>>> attr2val2ids = new HashMap<>();
        for (String attributeScheme: combiners.keySet()) {
            attr2val2ids.put(attributeScheme, new HashMap<>());
        }

        logger.info("Creating blocks");
        records.stream().forEach(r -> {

            combiners.forEach((key, value) -> {
                Collection<String> tokens = value.apply(r.getValues());
                if (tokens.size() == 1) {
                    String token = tokens.iterator().next();
                    if (token == null || token.equals("")) {
                        return;
                    }
                }
                HashMap<String, List<String>> val2ids = attr2val2ids.get(key);

                tokens.forEach(val -> {
                    List<String> ids = null;
                    if (val2ids.containsKey(val)) {
                        ids = val2ids.get(val);
                    } else {
                        ids = new ArrayList<>();
                    }
                    ids.add(r.getID());
                    val2ids.put(val, ids);
                });
            });

        });
        logger.info( "Finished creating blocks");
        for (String attr: attr2val2ids.keySet()) {
            if (attr2val2ids.get(attr).containsKey("")) {
                attr2val2ids.get(attr).remove("");
            }
        }
        Collection<Triplet<String, String, Double>> matches = createPairsFromBlocks(attr2val2ids,
                new Query(null, Query.RetrievalType.ALL, 0.0));

        Set<Triplet<String, String, Double>> sortedMatches = new TreeSet<>();
        sortedMatches.addAll(matches);

        return sortedMatches;
    }

    private Collection<Triplet<String, String, Double>> createPairsFromBlocks(Map<String, HashMap<String, List<String>>> attr2val2ids, Query q) {
        logger.info("Collecting pairs");

//        Map<String, Set<String>> blockingSchemesResults = new HashMap<>();
        Map<String, Set<String>> blockingSchemesResults = new ConcurrentHashMap<>();

        /* We will give each pair an ID, so that we can re-use the method to calculate the combinedParticipation
            from IRMModule.
        */
        PairUtils.PairsWithInfo pairsNDPL = new PairUtils.PairsWithInfo("NDPL_BLOCKING_CONCURRENT", new TreeSet<>());
        attr2val2ids.entrySet().stream().forEach(attr2val2idsEntry -> {
            logger.info("Collecting pairs from blocking scheme: " + attr2val2idsEntry.getKey());
            Set<String> attrPairIDs = new ConcurrentSkipListSet<>();
            attr2val2idsEntry.getValue().entrySet().parallelStream().filter(Objects::nonNull).forEach(val2idsEntry -> {

                List<String> ids = val2idsEntry.getValue();

                logger.info(attr2val2idsEntry.getKey() + "\t" + val2idsEntry.getKey() + "\t" + "(" + ids.size() + ")");
                for (int i = 0; i < ids.size(); ++i) {
                    String id1 = ids.get(i);
                    if (id1 == null) {
                        continue;
                    }
                    for (int j = i+1; j < ids.size(); ++j) {
                        String id2 = ids.get(j);
                        if (id2 == null) {
                            continue;
                        }
                        if (id1.compareTo(id2) > 0) {
                            String tmp = id1;
                            id1 = id2;
                            id2 = tmp;
                        }
                        Pair<String, String> p = new Pair<>(id1, id2);
                        String pairID = null;
                        synchronized (pairsNDPL) {
                            pairsNDPL.addPair(p);
                            pairID = pairsNDPL.getPairToID(p);
                        }
                        attrPairIDs.add(pairID);
                    }
                }
            });
            blockingSchemesResults.put(attr2val2idsEntry.getKey(), attrPairIDs);
        });

        blockingSchemesResults.entrySet().forEach(x -> {
            blockingSchemesResults.put(x.getKey(), new HashSet<>(x.getValue()));
        });

        logger.info("Normalizing participations");
        Map<String, Double> idToParticipationNormalizedFiltered = submoduleParticipationSimple(q, blockingSchemesResults);
        logger.info("Finished normalizing participations");

        List<Triplet<String, String, Double>> results = new ArrayList<>();
        for (Map.Entry<String, Double> e: idToParticipationNormalizedFiltered.entrySet()) {
            Pair<String, String> p = pairsNDPL.getIDToPair(e.getKey());
            results.add(new Triplet<>(p.getValue0(), p.getValue1(), e.getValue()));
        }

        logger.info("Finished collecting pairs");
        return results;
    }

    public SymmetricTable<String, String> getMatchedPairs(Map<String, Record> records, Map<String, Object> parameters) {
        return null;
    }
}
