package deduplication.similarities;

import deduplication.preparation.Preparators;
import deduplication.preparation.utils.Preparation;
import deduplication.utils.DataPrepDedupController;
import deduplication.utils.DataPrepDedupIO;
import me.tongfei.progressbar.ProgressBar;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This controller class calculates the similarity difference cube. This means that it calculates the difference of
 * the pair similarity on a given attribute, before and after the preparation.
 */

public class ProcessSimilarityDifferencesController extends DataPrepDedupController {

    final static Logger logger = Logger.getLogger(ProcessSimilarityCalculationController.class);

    public ProcessSimilarityDifferencesController(Map<String, String> m) {
        super(m);
    }

    public static void main(String[] args) throws SQLException {
        Map<String, String> m = new HashMap<>();
        if (args.length == 0) {
            System.exit(-1);
        }
        for(String arg: args) {
            String[] toks = arg.split("==");
            m.put(toks[0], toks[1]);
        }
        ProcessSimilarityDifferencesController psd = new ProcessSimilarityDifferencesController(m);
        psd.calculateSimilarityDifferencesProgressivelyExport();
    }

    public void calculateSimilarityDifferencesProgressivelyExport() throws SQLException {
        DataPrepDedupIO dpdIO = new DataPrepDedupIO(dbDriver, dbConnectionURL, dbUsername, dbPassword);

        Connection conn = dpdIO.getConn();
        Map<String, Map<String, String>> pairs = dpdIO.importTable("select * from "+ dataset + "_pairs_of_" + xstandard);
        dpdIO.createTableSimilaritiesDifferencesCube();

        // If the table already existed just clear these preparations
        chainedPreparations.forEach((key, attributePreparations) -> {
            String attribute = attributePreparations.get(0).getAttribute();
            Set<String> attributePreparators = attributePreparations.stream().map(Preparation::getPreparator).collect(Collectors.toSet());

            dpdIO.clearTableSimilaritiesDifferencesCubePreparation(dataset, xstandard, attribute, attributePreparators);
        });

        final int BATCH_SIZE = 2000;
        List<String> header = dpdIO.getSimilarityDifferencesCubeHeader();
        header.remove("id");

        String query = " insert into preparation_similarities_differences_cube" +
                " (" + String.join(", ", header) + ")"
                + " values (" + header.stream().map(c -> "?")
                .collect(Collectors.joining(", "))+")";
        PreparedStatement stmt = conn.prepareStatement(query);

        try (ProgressBar pb = new ProgressBar("Loading data " + dataset, chainedPreparations.size())) { // name, initial max
            chainedPreparations.entrySet().forEach(e -> {
                pb.step();
                String attributeChain = e.getKey();
                List<Preparation> attributePreparations = e.getValue();
                String attribute = attributePreparations.get(0).getAttribute();
                Set<String> attributePreparators = attributePreparations.stream().map(Preparation::getPreparator).collect(Collectors.toSet());

                // Pbefore, Pafter: <record ID, record>. Since we're loading records from the similarity cube, the
                // record essentially contains among other information the similarity that the record had at that state.
                Map<String, Map<String, String>> Pbefore = dpdIO.importSinglePreparationCubeTable(dataset, xstandard, attribute, new HashSet<>(), "similarities");
                Map<String, Map<String, String>> Pafter = dpdIO.importSinglePreparationCubeTable(dataset, xstandard, attribute, attributePreparators, "similarities");

                pairs.values().forEach(p -> {
                    Double simBefore = Double.valueOf(Pbefore.get(p.get("id1") + "_" + p.get("id2")).get(attribute));
                    Double simAfter = Double.valueOf(Pafter.get(p.get("id1") + "_" + p.get("id2")).get(attribute));

                    Map<String, Object> valueToSimilarityPreparationCube = getValueToSimilarityDifferencesPreparationCube(dataset,
                            p.get("id1"), p.get("id2"), p.get("class"), attribute, attributePreparators, simBefore, simAfter);
                    int j = 0;
                    try {
                        for (String h : header) {
                            stmt.setObject(++j, valueToSimilarityPreparationCube.get(h));
                        }
                        stmt.addBatch();
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }

                    if (pb.getCurrent() % BATCH_SIZE == BATCH_SIZE - 1) {
                        try {
                            stmt.executeBatch();
                            conn.commit();
                        } catch (SQLException ex) {
                            ex.printStackTrace();
                        }
                    }
                });
                try {
                    stmt.executeBatch();
                    conn.commit();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            });
        }
        dpdIO.close();
    }

    protected Map<String, Object> getValueToSimilarityDifferencesPreparationCube(String dataset, String recordID1,
                     String recordID2, String pairClass, String attribute, Set<String> preparators, Double similarityBefore, Double similarityAfter) {
        Map<String, Object> m = new HashMap<>();
//        m.put("name", "-1");
        m.put("dataset", dataset);
        m.put("xstandard", xstandard);
        m.put("attribute", attribute);
        m.put("record_id1", recordID1);
        m.put("record_id2", recordID2);
        m.put("pair_class", pairClass);
        List<String> allPreparators = Preparators.getAllPreparatorNames();
        for (String preparator: allPreparators) {
            m.put(preparator, preparators.contains(preparator));
        }
        m.put("similarity_before", similarityBefore);
        m.put("similarity_after", similarityAfter);
        m.put("difference", similarityAfter - similarityBefore);
        return m;
    }
}
