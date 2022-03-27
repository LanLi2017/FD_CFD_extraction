package deduplication.nonduplicates;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import deduplication.nonduplicates.indexing.DeduplicationIndexing;
import deduplication.nonduplicates.indexing.entities.Record;
import deduplication.nonduplicates.indexing.utils.DomainUtils;
import deduplication.nonduplicates.indexing.utils.PairUtils;
import deduplication.nonduplicates.indexing.utils.RecordUtils;
import deduplication.similarities.SimilarityCalculation;
import deduplication.similarities.utils.ConfigurationUtils;
import deduplication.similarities.utils.SimilarityMeasure;
import deduplication.similarities.utils.StringSimilarityMeasure;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This class allows the generation of non-duplicates.
 */
public class GeneratePairsNDPLController {
    protected Logger logger = Logger.getLogger(GeneratePairsNDPLController.class);

    protected void execute(String domain, PairUtils.NDPLGenerationStrategy generationStrategy,
                           Integer ndplRandomGrowthFactor) throws SQLException, IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Config config = ConfigFactory.load();
        Path baseDataDir = Paths.get(config.getString("data_base_dir"));

        ConfigurationUtils.SimilarityMeasureCfg simCfg = new ConfigurationUtils.SimilarityMeasureCfg(
                "",
//                SimilarityMeasure.SimilarityMeasureType.JaccardNGrams,
//                SimilarityMeasure.SimilarityMeasureType.Levenshtein,
                SimilarityMeasure.SimilarityMeasureType.MongeElkanLevenshtein,
                Collections.emptyMap(), Boolean.FALSE
        );

        DomainUtils du = new DomainUtils(domain, simCfg, config);
        RecordUtils ru = new RecordUtils();

        logger.info("["+domain+"] Loading records");
        Map<String, Record> records = ru.importRecords(
                baseDataDir.resolve(du.getDatasetFilename()),
                du.getAttributeSimilarityMap().keySet(),
                du.getDatasetRelationname()
        );
        logger.info("["+domain+"] Loaded records");

        PairUtils pu = new PairUtils(records, du.getAttributes());

        DeduplicationIndexing dis = new DeduplicationIndexing(
                du,
                records,
                pu.importPairsAndExpand(baseDataDir.resolve(du.getGoldstandardFilename()),"DPL")
        );

        logger.info("["+domain+"] Started calculating NDPL pairs");
        PairUtils.PairsDPLAndNDPL allPairs = dis.generateNDPLPairsAndReturnWithDPL(generationStrategy, ndplRandomGrowthFactor);
        logger.info("["+domain+"] Finished calculating NDPL pairs");

        Map<Pair<String, String>, Map<String, Double>> pairsSimilarities = processCalculateSimilarities(du, records, allPairs);

        SimilarityCalculation sc = new SimilarityCalculation();

        Path baseWorkspaceDir = Paths.get(config.getString("workspace_base_dir"));

        logger.info("["+domain+"] Exporting DPL pairs: " + allPairs.getPairsDPL().getPairs().size());
        Path pairsDPLFile = baseWorkspaceDir.resolve(domain + "_goldstandard_DPL_pairs_"+simCfg.similarityMeasureType.toString()+".tsv");
        sc.exportPairsWithSimilarities(du, records, pairsDPLFile, allPairs.getPairsDPL(), pairsSimilarities);
        Path pairsDPLClustersFile = baseWorkspaceDir.resolve(domain + "_goldstandard_DPL_clusters_pairs_"+simCfg.similarityMeasureType.toString()+".tsv");
        sc.exportClusterPairsWithSimilaritiesAndParticipation(du, pairsDPLClustersFile, allPairs.getPairsDPL(), pairsSimilarities);

        logger.info("["+domain+"] Exporting NDPL pairs: " + allPairs.getPairsNDPL().getPairs().size());
        Path pairsNDPLFile = baseWorkspaceDir.resolve(domain + "_goldstandard_NDPL_pairs_" + generationStrategy + "_" +
                ndplRandomGrowthFactor + "_" + simCfg.similarityMeasureType.toString() + ".tsv");
//        sc.exportClusterPairsWithSimilaritiesAndParticipation(du, pairsNDPLFile, allPairs.getPairsNDPL(), pairsSimilarities);
        sc.exportPairsWithSimilarities(du, records, pairsNDPLFile, allPairs.getPairsNDPL(), pairsSimilarities);

    }

    protected Map<Pair<String, String>, Map<String, Double>> processCalculateSimilarities(DomainUtils du,
        Map<String, Record> records, PairUtils.PairsDPLAndNDPL allPairs) {

        Map<Pair<String, String>, Map<String, Double>> pairsSimilarities = new HashMap<>();

        Map<String, StringSimilarityMeasure> attributeSimilarityMap = du.getAttributeSimilarityMap();

        Map<String, String> properties = new HashMap<>();
//        properties.put("attributes_to_compare", String.join("\\|", new TreeSet<>(attributeSimilarityMap.keySet())));
        properties.put("attributes_to_compare", String.join("|", new TreeSet<>(attributeSimilarityMap.keySet())));

        Map<String, Map<String, String>> recordsAsHM = records.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().getValues()
        ));

        String domain = du.getDomain();

        logger.info("["+domain+"] Calculating DPL pairs: " + allPairs.getPairsDPL().getPairs().size()+ " similarities.");
        pairsSimilarities.putAll(calculateSimilaritiesForSet(recordsAsHM, allPairs.getPairsDPL().getPairs(), properties));
        logger.info("["+domain+"] Calculated DPL pairs: " + allPairs.getPairsDPL().getPairs().size()+ " similarities.");

        logger.info("["+domain+"] Calculating NDPL pairs: " + allPairs.getPairsNDPL().getPairs().size()+ " similarities.");
        pairsSimilarities.putAll(calculateSimilaritiesForSet(recordsAsHM, allPairs.getPairsNDPL().getPairs(), properties));
        logger.info("["+domain+"] Calculated NDPL pairs: " + allPairs.getPairsNDPL().getPairs().size()+ " similarities.");

        return pairsSimilarities;
    }

    private Map<Pair<String, String>, Map<String, Double>> calculateSimilaritiesForSet(
            Map<String, Map<String, String>> recordsAsHM, Set<Pair<String, String>> pairs, Map<String, String> properties) {
        SimilarityCalculation sc = new SimilarityCalculation();

        List<Pair<String, String>> listPairs = new ArrayList<>(pairs);
        Map<Integer, Map<String, Double>> similaritiesByIndex = sc.calculateSimilaritiesInParallel(recordsAsHM, listPairs, properties);

        Map<Pair<String, String>, Map<String, Double>> similaritiesByPair = similaritiesByIndex.entrySet().stream()
                .filter(x -> x.getValue() != null && x.getValue().size() > 0).collect(Collectors.toMap(
                e -> listPairs.get(e.getKey()),
                Map.Entry::getValue
        ));

        return similaritiesByPair;
    }


    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
//        int parallelism = 8;
//        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(parallelism));

        GeneratePairsNDPLController pu = new GeneratePairsNDPLController();

            List<String> domains = Arrays.asList(
//                    "cddb"
                    "census"
//                    "cora"
//                    "restaurants"
//                    "movies"
//                    "hotels"
            );

            List<PairUtils.NDPLGenerationStrategy> generationStrategies = Arrays.asList(
//                    PairUtils.NDPLGenerationStrategy.RANDOMLY
                    PairUtils.NDPLGenerationStrategy.BLOCKING
            );

            Integer ndplRandomGrowthFactor = 10;

            for (PairUtils.NDPLGenerationStrategy generationStrategy: generationStrategies) {
                for (String domain : domains) {
                    pu.execute(domain, generationStrategy, ndplRandomGrowthFactor);
                }
            }
//        }
    }

}
