package deduplication.similarities;

import deduplication.nonduplicates.indexing.entities.Record;
import deduplication.nonduplicates.indexing.utils.DomainUtils;
import deduplication.nonduplicates.indexing.utils.PairUtils;
import deduplication.similarities.utils.StringSimilarityMeasure;
import deduplication.similarities.utils.string.geolocation.Haversine;
import deduplication.similarities.utils.string.hybrid.MongeElkan;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimilarityCalculation {
    final static Logger logger = Logger.getLogger(SimilarityCalculation.class);

    protected StringSimilarityMeasure msr = null;
    protected StringSimilarityMeasure msrGeolocation = null;

    public SimilarityCalculation() {
        msr = new MongeElkan.MongeElkanLevenshtein("dummy", false, Collections.emptyMap());
        msrGeolocation = new Haversine("dummy_geolocation", false, Collections.emptyMap());
    }

    public Map<Integer, Map<String, Double>> calculateSimilaritiesInParallel(Map<String, Map<String, String>> data,
                                                                      List<Pair<String, String>> pairs,
                                                                      Map<String, String> properties) {
        String attributesToCompareStr = properties.get("attributes_to_compare");
        List<String> attributesToCompare = Arrays.asList(attributesToCompareStr.split("\\|"));

        AtomicInteger progress = new AtomicInteger();

        Map<Integer, Map<String, Double>> similaritiesPerPair = new ConcurrentHashMap<>();
        IntStream.range(0, pairs.size()).parallel().forEach(i -> {
            Pair<String, String> p = pairs.get(i);
            Map<String, String> r1 = data.get(p.getValue0());
            Map<String, String> r2 = data.get(p.getValue1());
            if (r1 == null || r2 == null) {
                System.out.println("*** Pair : " + p + " NOT FOUND ***!");
                return;
            }
            Map<String, Double> recordSimilarities = new HashMap<>();
            for (String attr: attributesToCompare) {
                String v1 = r1.getOrDefault(attr, "");
                String v2 = r2.getOrDefault(attr, "");
                Double sim = 0.0;
                if (v1 == null || v2 == null || v1.equals("") || v2.equals("")) {
                    sim = 0.0;
                } else {
                    try {
                        switch (attr) {
                            case "latitude_longitude":
                                sim = msrGeolocation.similarity(v1, v2);
                                break;
                            default:
                                sim = msr.similarity(v1, v2);
                                break;
                        }
                    } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
                recordSimilarities.put(attr, sim);
            }
            similaritiesPerPair.put(i, recordSimilarities);

            Integer counter = progress.incrementAndGet();
            if (pairs.size() > 100 && counter % (pairs.size() / 100) == 0) {
                logger.info(counter / (pairs.size() / 100.0) + "%" + " --> " + counter);
                System.out.flush();
            }
        });

        return similaritiesPerPair;
    }


    public void exportPairsWithSimilarities(DomainUtils du, Map<String, Record> records, Path pairsFile, PairUtils.PairsWithInfo pairsInfo,
                                            Map<Pair<String, String>, Map<String, Double>> pairsSimilarities) throws IOException {

        if (!new File(pairsFile.getParent().toString()).exists()) {
            new File(pairsFile.getParent().toString()).mkdirs();
        }

        Set<String> info = new TreeSet<>();
        info.addAll(pairsInfo.infoHeld);

        Set<String> attributesForSimilarity = new TreeSet<>(du.getAttributeSimilarityMap().keySet());
        for (String attrSim: attributesForSimilarity) {
            info.add("sim_" + attrSim);
        }
        info.add("sim_total");

        List<String> header = new ArrayList<>(Arrays.asList("id","id1", "id2"));
        header.addAll(info);

        StringSimilarityMeasure msr = new MongeElkan.MongeElkanLevenshtein("mel", false, Collections.emptyMap());

        try (CSVPrinter printer = new CSVPrinter(new FileWriter(pairsFile.toString()), CSVFormat.TDF)) {
            printer.printRecord(header);
            Integer counter = 0;
            for (Pair<String, String> p : pairsInfo.pairs) {
                Map<String, String> values = new HashMap<>();
                values.put("id", (counter++).toString());
                values.put("id1", p.getValue0());
                values.put("id2", p.getValue1());
                for (String k : info) {
                    values.put(k, pairsInfo.getInfoFromPair(p).getOrDefault(k, ""));
                }
                Map<String, Double> pairSimilarities = pairsSimilarities.getOrDefault(p, null);
                if (pairSimilarities == null || pairSimilarities.size() == 0) {
                    continue;
                }
                Double total = 0.0;

                Integer nonNullAttributes = 0;
                for (String attrSim: attributesForSimilarity) {
                    Double similarity = pairSimilarities.getOrDefault(attrSim, 0.0);
                    if (pairSimilarities.containsKey(attrSim)) {
                        nonNullAttributes += 1;
                    }
                    total += similarity;
                    values.put("sim_" + attrSim, pairSimilarities.getOrDefault(attrSim, -1.0).toString());
                }
                values.put("sim_total", String.valueOf(total / nonNullAttributes));

                printer.printRecord(header.stream().map(x -> values.get(x).replaceAll("\n", "")).collect(Collectors.toList()));
            }
        }
    }

    public void exportClusterPairsWithSimilaritiesAndParticipation(DomainUtils du, Path pairsFile, PairUtils.PairsWithInfo pairsInfo,
                                                                   Map<Pair<String, String>, Map<String, Double>> pairsSimilarities) throws IOException {

        if (!new File(pairsFile.getParent().toString()).exists()) {
            new File(pairsFile.getParent().toString()).mkdirs();
        }

        Set<String> info = new TreeSet<>();
        info.addAll(pairsInfo.infoHeld);

        Set<String> attributesForSimilarity = new TreeSet<>(du.getAttributeSimilarityMap().keySet());
        for (String attrSim: attributesForSimilarity) {
            info.add("sim_" + attrSim);
        }
        info.add("sim_total");

        List<String> header = new ArrayList<>(Arrays.asList("id","id_cluster", "cluster_size", "id1", "id2"));
        header.addAll(info);

        try (CSVPrinter printer = new CSVPrinter(new FileWriter(pairsFile.toString()), CSVFormat.TDF)) {
            printer.printRecord(header);
            Integer counterPair = 0;
            Integer counterCluster = 0;
            for (Set<String> cluster : pairsInfo.uf.getComponentsParallel()) {
                ++counterCluster;
                List<String> clusterList = new ArrayList<>(cluster);
                for (int i = 0; i < cluster.size(); ++i) {
                    for (int j = i+1; j < cluster.size(); ++j) {
                        Pair<String, String> p = new Pair<>(clusterList.get(i), clusterList.get(j));
                        Map<String, String> values = new HashMap<>();
                        values.put("id", (counterPair++).toString());
                        values.put("id_cluster", (counterCluster).toString());
                        values.put("cluster_size", String.valueOf(cluster.size()));
                        values.put("id1", p.getValue0());
                        values.put("id2", p.getValue1());
                        for (String k : info) {
                            values.put(k, pairsInfo.getInfoFromPair(p).getOrDefault(k, ""));
                        }
                        Map<String, Double> pairSimilarities = pairsSimilarities.getOrDefault(p, null);
                        if (pairSimilarities != null) {
                            Double total = 0.0;

                            Integer nonNullAttributes = 0;
                            for (String attrSim: attributesForSimilarity) {
                                Double similarity = pairSimilarities.getOrDefault(attrSim, 0.0);
                                if (pairSimilarities.containsKey(attrSim)) {
                                    nonNullAttributes += 1;
                                }
                                total += similarity;
                                values.put("sim_" + attrSim, pairSimilarities.getOrDefault(attrSim, -1.0).toString());
                            }
                            values.put("sim_total", String.valueOf(total / nonNullAttributes));
                        }

                        printer.printRecord(header.stream().map(x -> values.get(x).replaceAll("\n", "")).collect(Collectors.toList()));
                    }
                }
            }
        }
    }

}
