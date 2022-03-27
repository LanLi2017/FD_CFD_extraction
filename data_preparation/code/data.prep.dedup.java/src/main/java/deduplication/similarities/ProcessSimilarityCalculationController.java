package deduplication.similarities;

import deduplication.preparation.Preparators;
import deduplication.preparation.utils.Preparation;
import deduplication.similarities.utils.StringSimilarityMeasure;
import deduplication.similarities.utils.string.geolocation.Haversine;
import deduplication.similarities.utils.string.hybrid.MongeElkan;
import deduplication.utils.DataPrepDedupController;
import deduplication.utils.DataPrepDedupIO;
import me.tongfei.progressbar.ProgressBar;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This controller class helps with calculating pair similarities for each preparation.
 */
public class ProcessSimilarityCalculationController extends DataPrepDedupController {
    final static Logger logger = Logger.getLogger(ProcessSimilarityCalculationController.class);

    public static void main(String[] args) throws SQLException {
        Map<String, String> m = new HashMap<>();
        if (args.length == 0) {
            System.exit(-1);
        }
        for(String arg: args) {
            String[] toks = arg.split("==");
            m.put(toks[0], toks[1]);
        }

        ProcessSimilarityCalculationController psc = new ProcessSimilarityCalculationController(m);
        psc.calculateSimilarityProgressivelyExport();
    }

    public ProcessSimilarityCalculationController(Map<String, String> m) {
        super(m);
    }

    public void calculateSimilarityProgressivelyExport() throws SQLException {
        DataPrepDedupIO dpdIO = new DataPrepDedupIO(dbDriver, dbConnectionURL, dbUsername, dbPassword);

        Connection conn = dpdIO.getConn();
        Map<String, Map<String, String>> pairs = dpdIO.importTable("select * from "+ dataset + "_pairs_of_" + xstandard);
        dpdIO.createTableSimilaritiesCube();

        // If the table already existed just clear these preparations
        chainedPreparations.forEach((key, attributePreparations) -> {
            String attribute = attributePreparations.get(0).getAttribute();
            Set<String> attributePreparators = attributePreparations.stream().map(Preparation::getPreparator).collect(Collectors.toSet());

            dpdIO.clearTableSimilaritiesCubePreparation(dataset, xstandard, attribute, attributePreparators);
        });

        final int BATCH_SIZE = 10000;
        List<String> header = dpdIO.getSimilarityCubeHeader();
        header.remove("id");

        String query = " insert into preparation_similarities_cube" +
                " (" + String.join(", ", header) + ")"
                + " values (" + header.stream().map(c -> "?")
                .collect(Collectors.joining(", "))+")";
        PreparedStatement stmt = conn.prepareStatement(query);

        StringSimilarityMeasure msr = new MongeElkan.MongeElkanLevenshtein("mel", false, Collections.emptyMap());
        StringSimilarityMeasure msrGeolocation = new Haversine("hvrsn", false, Collections.emptyMap());


        long start = System.currentTimeMillis();
        exportPairsSingleThreaded(dpdIO, conn, pairs, BATCH_SIZE, header, stmt, msr, msrGeolocation);
//        exportPairsMultiThreaded(dpdIO, pairs, BATCH_SIZE, header, msr, msrGeolocation);
        long finish = System.currentTimeMillis();
        System.out.println("Execution time: " + (finish - start));
    }

    public void exportPairsSingleThreaded(DataPrepDedupIO dpdIO, Connection conn, Map<String, Map<String, String>> pairs, int BATCH_SIZE, List<String> header, PreparedStatement stmt, StringSimilarityMeasure msr, StringSimilarityMeasure msrGeolocation) {
        try (ProgressBar pb = new ProgressBar("Similarity calculation for " + dataset, chainedPreparations.size())) { // name, initial max
            chainedPreparations.entrySet().forEach(e -> {
                pb.step();
                String attributeChain = e.getKey();
                List<Preparation> attributePreparations = e.getValue();
                String attribute = attributePreparations.get(0).getAttribute();
                Set<String> attributePreparators = attributePreparations.stream().map(Preparation::getPreparator).collect(Collectors.toSet());
                Map<String, Map<String, String>> R = dpdIO.importSinglePreparationCubeTable(dataset, xstandard, attribute, attributePreparators, "data");
                Map<String, Double> S = new ConcurrentHashMap<>();
                pairs.values().parallelStream().forEach(p -> {
                    if (!R.containsKey(p.get("id1")) || !R.containsKey(p.get("id2"))) {
                        return;
                    }
                    String v1 = R.get(p.get("id1")).get(attribute);
                    String v2 = R.get(p.get("id2")).get(attribute);

                    Double sim = 0.0;
                    if (v1 == null || v2 == null || v1.equals("") || v2.equals("")) {
                        sim = 0.0;
                    } else {
                        try {
                            switch (attribute) {
                                case "latitude_longitude": // For geolocation similarity use msrGeolocation.
                                    sim = msrGeolocation.similarity(v1, v2);
                                    break;
                                default: // For all alphanumeric values use msr.
                                    sim = msr.similarity(v1, v2);
                                    break;
                            }
                        } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException xc) {
                            xc.printStackTrace();
                        }
                    }
                    S.put(p.get("id1") + "_" + p.get("id2"), sim);
                });

                pairs.values().forEach(p -> {
                    Double sim = S.get(p.get("id1") + "_" + p.get("id2"));
                    Map<String, Object> valueToSimilarityPreparationCube = getValueToSimilarityPreparationCube(dataset,
                            p.get("id1"), p.get("id2"), p.get("class"), attribute, attributePreparators, sim);
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

    public void exportPairsMultiThreaded(DataPrepDedupIO dpdIO, Map<String, Map<String, String>> pairs, int BATCH_SIZE,
                                         List<String> header, StringSimilarityMeasure msr, StringSimilarityMeasure msrGeolocation) {
        try (ProgressBar pb = new ProgressBar("Similarity calculation for " + dataset, chainedPreparations.size())) { // name, initial max
            chainedPreparations.entrySet().parallelStream().forEach(e -> {
                pb.step();
                String attributeChain = e.getKey();
                List<Preparation> attributePreparations = e.getValue();
                String attribute = attributePreparations.get(0).getAttribute();
                Set<String> attributePreparators = attributePreparations.stream().map(Preparation::getPreparator).collect(Collectors.toSet());

                Connection conn = null;
                try {
                    Class.forName(dbDriver);
                    conn = DriverManager.getConnection(dbConnectionURL, dbUsername, dbPassword);
                    conn.setAutoCommit(false);
                } catch (ClassNotFoundException | SQLException ex) {
                    ex.printStackTrace();
                }

                String cubeType = "data";

                StringBuilder sb = new StringBuilder();
                sb.append("SELECT * FROM preparation_" + cubeType + "_cube WHERE `dataset` = '" + dataset + "'");
                sb.append(" and `xstandard` = '" + xstandard + "'");
                if (attribute != null) {
                    sb.append(" and `attribute` = '" + attribute + "'");
                }

                List<String> allPreparators = Preparators.getAllPreparatorNames();

                for (String prp : allPreparators) {
                    sb.append(" and `" + prp + "` = " + (attributePreparators.contains(prp)) + "");
                }
                sb.append(";");

                String sql = sb.toString();

                Map<String, Map<String, String>> records = new HashMap<>();
                try {
                    PreparedStatement stmtRead = conn.prepareStatement(sql);
                    ResultSet rs = stmtRead.executeQuery(sql);

                    while (rs.next()) {
                        String rid = null;
                        Map<String, String> record = null;
                        rid = rs.getString("record_id");
                        record = records.getOrDefault(rid, new HashMap<>());
                        record.put(rs.getString("attribute"), String.valueOf(rs.getObject("value")));
                        records.put(rid, record);
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }

                Map<String, Double> S = new ConcurrentHashMap<>();
                pairs.values().parallelStream().forEach(p -> {
                    if (!records.containsKey(p.get("id1")) || !records.containsKey(p.get("id2"))) {
                        return;
                    }
                    String v1 = records.get(p.get("id1")).get(attribute);
                    String v2 = records.get(p.get("id2")).get(attribute);

                    Double sim = 0.0;
                    if (v1 == null || v2 == null || v1.equals("") || v2.equals("")) {
                        sim = 0.0;
                    } else {
                        try {
                            switch (attribute) {
                                case "latitude_longitude":
                                    sim = msrGeolocation.similarity(v1, v2);
                                    break;
                                default:
                                    sim = msr.similarity(v1, v2);
                                    break;
                            }
                        } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException xc) {
                            xc.printStackTrace();
                        }
                    }
                    S.put(p.get("id1") + "_" + p.get("id2"), sim);
                });

                String query = " insert into preparation_similarities_cube" +
                        " (" + String.join(", ", header) + ")"
                        + " values (" + header.stream().map(c -> "?")
                        .collect(Collectors.joining(", "))+")";
                PreparedStatement stmtWrite = null;
                try {
                    stmtWrite = conn.prepareStatement(query);
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
                Connection finalConn = conn;
                PreparedStatement finalStmtWrite = stmtWrite;
                pairs.values().forEach(p -> {
                    Double sim = S.get(p.get("id1") + "_" + p.get("id2"));
                    Map<String, Object> valueToSimilarityPreparationCube = getValueToSimilarityPreparationCube(dataset,
                            p.get("id1"), p.get("id2"), p.get("class"), attribute, attributePreparators, sim);
                    int j = 0;
                    try {
                        for (String h : header) {
                            finalStmtWrite.setObject(++j, valueToSimilarityPreparationCube.get(h));
                        }
                        finalStmtWrite.addBatch();
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }

                    if (pb.getCurrent() % BATCH_SIZE == BATCH_SIZE - 1) {
                        try {
                            finalStmtWrite.executeBatch();
                            finalConn.commit();
                        } catch (SQLException ex) {
                            ex.printStackTrace();
                        }
                    }
                });
                try {
                    stmtWrite.executeBatch();
                    conn.commit();
                    conn.close();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            });
        }
        dpdIO.close();
    }

    protected Map<String, Object> getValueToSimilarityPreparationCube(String dataset, String recordID1, String recordID2, String pairClass, String attribute, Set<String> preparators, Double similarity) {
        Map<String, Object> m = new HashMap<>();
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
        m.put("similarity", similarity);
        return m;
    }
}
