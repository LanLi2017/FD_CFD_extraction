package deduplication.nonduplicates.indexing;

import deduplication.nonduplicates.indexing.batchindexingmethods.BatchBlocking;
import deduplication.nonduplicates.indexing.entities.Attributes;
import deduplication.nonduplicates.indexing.entities.Record;
import deduplication.nonduplicates.indexing.utils.*;
import org.apache.log4j.Logger;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;

public class DeduplicationIndexing {

    protected Logger logger = Logger.getLogger(DeduplicationIndexing.class);

    // dataset (records R)
    protected Path dataset = null;
    protected Map<String, Record> records = null;

    // gold standard (DPL)
    protected Path goldStandard = null;
    protected PairUtils.PairsWithInfo pairsDPL = null;

    protected Map<String, BlockingKeyGenerator> attributeCombiners = null;

    protected PairUtils pu = null;
    protected DomainUtils du = null;

//    public DeduplicationIndexing(DomainUtils du, PairUtils pu, Path dataset, Path goldStandard, String domain) {
//        this.du = du;
//        this.pu = pu;
//        this.dataset = dataset;
//        this.goldStandard = goldStandard;
//
//        RecordUtils ru = new RecordUtils();
//        Attributes attributes = new Attributes(dataset);
//        try {
//            this.records = ru.importRecords(dataset, attributes.getAttributes(), domain);
//        } catch (SQLException | IOException | ClassNotFoundException e) {
//            e.printStackTrace();
//            logger.fatal("Could not import dataset: " + dataset);
//            System.exit(-1);
//        }
//        this.pairsDPL = pu.importPairsAndExpand(goldStandard, "DPL");
//        attributeCombiners = new HashMap<>();
//    }

    public DeduplicationIndexing(DomainUtils du, Map<String, Record> records, PairUtils.PairsWithInfo pairsDPL) {
        this.du = du;
        this.records = records;
        this.pairsDPL = pairsDPL;
        attributeCombiners = du.getAttributeCombiners();
    }

    public PairUtils.PairsDPLAndNDPL generateNDPLPairsAndReturnWithDPL(PairUtils.NDPLGenerationStrategy generationStrategy, Integer ndplRandomGrowthFactor) throws IOException, SQLException, ClassNotFoundException {
        logger.info("Generating NDPL pairs");
        PairUtils.PairsWithInfo pairsNDPL = generateNDPLPairsWithStrategy(generationStrategy, ndplRandomGrowthFactor, pairsDPL);

        PairUtils.PairsDPLAndNDPL pairsGS = new PairUtils.PairsDPLAndNDPL();
        pairsGS.setPairsDPL(pairsDPL);
        pairsGS.setPairsNDPL(pairsNDPL);

        return pairsGS;
    }

    private PairUtils.PairsWithInfo generateNDPLPairsWithStrategy(PairUtils.NDPLGenerationStrategy generationStrategy, Integer ndplRandomGrowthFactor, PairUtils.PairsWithInfo pairsDPL) {
        Set<Triplet<String, String, Double>> pairsNDPLWithParticipationScore = null;
        switch (generationStrategy) {
            case BLOCKING:
                pairsNDPLWithParticipationScore = generateNDPLPairsByBlocking(pairsDPL.getUf());
                break;
            case RANDOMLY:
                pairsNDPLWithParticipationScore = generateNDPLPairsRandomly(pairsDPL.getUf(), pairsDPL.pairs.size() * ndplRandomGrowthFactor);
                break;
        }
        PairUtils.PairsWithInfo pairsNDPL = new PairUtils.PairsWithInfo("NDPL", new HashSet<>());
        pairsNDPLWithParticipationScore.forEach(x -> {
            Pair<String, String> p = new Pair<>(x.getValue0(), x.getValue1());
            pairsNDPL.addPair(p);
            pairsNDPL.addInfoToPair(p, "participation", x.getValue2().toString());
        });
        logger.info("Generated " + pairsNDPLWithParticipationScore.size() + " NDPL pairs.");
        return pairsNDPL;
    }

    private Set<Triplet<String, String, Double>> generateNDPLPairsByBlocking(UnionFind<String> ufGS) {
        BatchBlocking bb = new BatchBlocking();
        Set<Triplet<String, String, Double>> matches = bb.getMatchedPairs(records.values(), attributeCombiners);
//        Set<DedupPair<String, String>> matches = bb.getLimitedMatchedPairsAvoidGS(dataset, experimentAttributes, attributeCombiners, pairsGS.sizeNodes(), ufGS);

        Set<Triplet<String, String, Double>> pairsNDPL = new HashSet<>();
        for (Triplet<String, String, Double> trpl : matches) {
            if (ufGS.connected(trpl.getValue0(), trpl.getValue1())) {
                continue;
            }
            pairsNDPL.add(new Triplet<>(trpl.getValue0(), trpl.getValue1(), trpl.getValue2()));
        }
        return pairsNDPL;
    }

    private Set<Triplet<String, String, Double>> generateNDPLPairsRandomly(UnionFind<String> ufGS, Integer limit) {
        List<String> keys  = new ArrayList<>(records.keySet());

        Collections.shuffle(keys);

        Set<Triplet<String, String, Double>> pairsNDPL = new HashSet<>();
        outerLoop:for (int i = 0; i < keys.size(); ++i) {
            Record ri = records.get(keys.get(i));

            for (int j = i+1; j < keys.size(); ++j) {
                Record rj = records.get(keys.get(j));

                if (!ufGS.connected(ri.getID(), rj.getID())) {
                    pairsNDPL.add(new Triplet<>(ri.getID(), rj.getID(), -1.0));

                    if (pairsNDPL.size() == limit) {
                        break outerLoop;
                    }
                }
            }
        }
        return pairsNDPL;
    }
}
