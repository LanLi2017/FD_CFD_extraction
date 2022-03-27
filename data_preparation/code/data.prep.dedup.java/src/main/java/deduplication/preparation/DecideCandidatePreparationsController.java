package deduplication.preparation;

import deduplication.classification.utils.DedupPair;
import deduplication.preparation.utils.Preparation;
import deduplication.utils.DataPrepDedupController;
import deduplication.utils.DataPrepDedupIO;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

/**
 * This controller aids the decision of candidate preparations based on similarities (phase 3).
 */
public class DecideCandidatePreparationsController extends DataPrepDedupController {
    protected Logger logger = Logger.getLogger(DecideCandidatePreparationsController.class);

    public static void main(String[] args) throws SQLException {
        Map<String, String> m = new HashMap<>();
        if (args.length == 0) {
            System.exit(-1);
        }
        for(String arg: args) {
            String[] toks = arg.split("==");
            m.put(toks[0], toks[1]);
        }

        DecideCandidatePreparationsController dgp = new DecideCandidatePreparationsController(m);
        dgp.execute();
    }

    public DecideCandidatePreparationsController(Map<String, String> m) {
        super(m);
    }

    public static class AttributeAndPair{
        protected String attribute;
        protected DedupPair pair;

        public AttributeAndPair(String attribute, DedupPair pair) {
            this.attribute = attribute;
            this.pair = pair;
        }

        public String getAttribute() {
            return attribute;
        }

        public DedupPair getPair() {
            return pair;
        }

        @Override
        public int hashCode() {
            int x = attribute.hashCode();
            x *= pair.hashCode();
            return x;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof AttributeAndPair)) {
                return false;
            } else {
                AttributeAndPair o = (AttributeAndPair) obj;
                return (this.attribute.equals(o.attribute) && this.pair.equals(o.pair));
            }
        }
    }

    public void execute() throws SQLException {
        DataPrepDedupIO dpdIO = new DataPrepDedupIO(dbDriver, dbConnectionURL, dbUsername, dbPassword);
        Connection conn = dpdIO.getConn();

        dpdIO.createTableCandidatePreparations();

        // Storing the initial similarities of every attribute of every record pair.
        Map<AttributeAndPair, Double> attributeAndPairToSimilarity = new HashMap<>();
        for (String attribute: attributes) {
            Map<String, Map<String, String>> S = dpdIO.importSinglePreparationCubeTable(dataset, xstandard, attribute, new HashSet<>(), "similarities");
            for (String id12: S.keySet()) {
                String[] idToks = id12.split("_");
                DedupPair dp = new DedupPair(dataset, id12, idToks[0], idToks[1], DedupPair.PairClass.valueOf(S.get(id12).get("pair_class").toUpperCase()));
                AttributeAndPair aap = new AttributeAndPair(attribute, dp);
                Double similarity = Double.valueOf(S.get(id12).get(attribute));
                attributeAndPairToSimilarity.put(aap, similarity);
            }
        }

        Set<String> candidatePreparations = new ConcurrentSkipListSet<>();
        chainedPreparations.entrySet().parallelStream().forEach(e -> {
            int dplCount = 0, ndplCount = 0;
            double dplSum = 0.0, ndplSum = 0.0;

            String attributeChain = e.getKey();
            List<Preparation> attributePreparations = e.getValue();
            String attribute = attributePreparations.get(0).getAttribute();
            Set<String> attributePreparators = attributePreparations.stream().map(Preparation::getPreparator).collect(Collectors.toSet());
            Map<String, Map<String, String>> Sprepared = dpdIO.importSinglePreparationCubeTable(dataset, xstandard, attribute, attributePreparators, "similarities");
            for (String id12: Sprepared.keySet()) {
                String[] idToks = id12.split("_");
                DedupPair dp = new DedupPair(dataset, id12, idToks[0], idToks[1], DedupPair.PairClass.valueOf(Sprepared.get(id12).get("pair_class").toUpperCase()));
                AttributeAndPair aap = new AttributeAndPair(attribute, dp);

                Double similarityOriginal = attributeAndPairToSimilarity.get(aap);
                Double similarityPrepared = Double.valueOf(Sprepared.get(id12).get(attribute));

                if (dp.getPairClass() == DedupPair.PairClass.DPL) {
                    dplCount++;
                    dplSum += similarityPrepared - similarityOriginal;
                } else {
                    ndplCount++;
                    ndplSum += similarityPrepared - similarityOriginal;
                }
            }

            double meanDifferenceDpl = dplSum / dplCount;
            double meanDifferenceNdpl = ndplSum / ndplCount;
            double ndplToDplRatio = ((double) ndplCount) / dplCount;

            if (meanDifferenceDpl - (ndplToDplRatio * meanDifferenceNdpl) > 0.0) {
                candidatePreparations.add(e.getKey());
            }
        });
        exportCandidatePreparations(candidatePreparations, dpdIO);
    }

    public void exportCandidatePreparations(Set<String> candidatePreparations, DataPrepDedupIO dpdIO) throws SQLException {
        Connection conn = dpdIO.getConn();

        List<String> candidatePreparationsList = new ArrayList<>(candidatePreparations);
        candidatePreparationsList.sort(String::compareTo);
        // Create the preparation signature.
        String candidatePreparationsString = String.join("-", candidatePreparationsList);
        dpdIO.clearTableCandidatePreparations(dataset, xstandard);

        List<String> header = dpdIO.getCandidatePreparationsHeader();
        header.remove("id");

        System.out.println("Preparing SQL statement");
        String query = " insert into candidate_preparations" +
                " (" + header.stream().map(x -> "`" + x + "`").collect(Collectors.joining(", ")) + ")"
                + " values (" + header.stream().map(c -> "?")
                .collect(Collectors.joining(", "))+")";
        PreparedStatement stmt = conn.prepareStatement(query);
        System.out.println("Prepared SQL statement");

        Map<String, Object> classificationMap = new HashMap<>();
        classificationMap.put("dataset", dataset);
        classificationMap.put("xstandard", xstandard);
        classificationMap.put("candidate_preparations", candidatePreparationsString);
        try {
            int j = 0;
            for (String h : header) {
                stmt.setObject(++j, classificationMap.get(h));
            }
            stmt.addBatch();
            stmt.executeBatch();
            conn.commit();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}
