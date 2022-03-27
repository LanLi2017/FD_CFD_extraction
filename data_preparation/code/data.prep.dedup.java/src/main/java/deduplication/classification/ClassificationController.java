package deduplication.classification;

import deduplication.classification.classifiers.ThresholdBasedClassifier;
import deduplication.classification.utils.DedupPair;
import deduplication.classification.utils.ExperimentEvaluation;
import deduplication.preparation.Preparators;
import deduplication.preparation.utils.Preparation;
import deduplication.utils.DataPrepDedupController;
import deduplication.utils.DataPrepDedupIO;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.log4j.Logger;
import org.javatuples.Pair;
import org.javatuples.Quintet;
import org.javatuples.Triplet;
import smile.data.Attribute;
import smile.data.NumericAttribute;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

/**
 * This class controller executes the classification for duplicate detection using the similarities provided.
 */
public class ClassificationController extends DataPrepDedupController {
    protected Logger logger = Logger.getLogger(ClassificationController.class);

    protected String classifier;

//    protected Map<DedupPair, Map<String, Double>> pairsSimilarities = null;
    protected Attribute[] attributesSMILE = null;
//    protected Pair<double[][], int[]> XY = null;
    protected DedupPair[] pairs = null;

    public static void main(String[] args) throws IOException, SQLException {
        Map<String, String> m = new HashMap<>();
        if (args.length == 0) {
            System.exit(-1);
        }
        for(String arg: args) {
            String[] toks = arg.split("==");
            m.put(toks[0], toks[1]);
        }
        String classifier = m.get("classifier");

        ClassificationController classificationController = new ClassificationController(m, classifier);
        classificationController.execute();
    }


    public ClassificationController(Map<String, String> m, String classifier) {
        super(m);
        this.classifier = classifier;
    }

    public void execute() throws SQLException {
        DataPrepDedupIO dpdIO = new DataPrepDedupIO(dbDriver, dbConnectionURL, dbUsername, dbPassword);
        Connection conn = dpdIO.getConn();

        Set<String> attributesRepresentedByPreparations = new HashSet<>();
        for (String signature: chainedPreparations.keySet()) {
            String[] toks = signature.split("@");
            String attribute = toks[0];
            attributesRepresentedByPreparations.add(attribute);
        }
        Set<String> remainingAttributes = new HashSet<>(attributes);
        remainingAttributes.removeAll(attributesRepresentedByPreparations);
        for (String attribute: remainingAttributes) {
            chainedPreparations.put(attribute, new ArrayList<>(Arrays.asList(new Preparation(dataset, "_",
                    attribute, 100, Boolean.FALSE))));
        }

        dpdIO.createTableClassification();

        long start = System.currentTimeMillis();
        Pair<Set<String>, Map<DedupPair, Map<String, Double>>> loadedData = loadDataSingleThread(dpdIO);
//        Pair<Set<String>, Map<DedupPair, Map<String, Double>>> loadedData = loadDataMultiThreaded();
        long stop = System.currentTimeMillis();
        System.out.println("Loading data took: " + (stop-start) + " ms");

        Set<String> Xfeatures = loadedData.getValue0();
        Map<DedupPair, Map<String, Double>> S = loadedData.getValue1();

        System.out.println("Converting to arrays");

        List<String> XfeaturesList = new ArrayList<>(Xfeatures);
        XfeaturesList.sort(String::compareTo);

        attributesSMILE = new Attribute[XfeaturesList.size()];
        for (int i = 0; i < XfeaturesList.size(); ++i) {
            attributesSMILE[i] = new NumericAttribute(XfeaturesList.get(i));
        }

        List<DedupPair> dpl = new ArrayList<>(), ndpl = new ArrayList<>();
        S.keySet().stream().forEach(dp -> {
            switch (dp.getPairClass()) {
                case DPL:
                    dpl.add(dp);
                    break;
                case NDPL:
                    ndpl.add(dp);
                    break;
            }
        });

        Random r = new Random(0);
//        dpl.sort(DedupPair::compareTo);
        dpl.sort(Comparator.comparing(DedupPair::toString));
//        ndpl.sort(DedupPair::compareTo);
        ndpl.sort(Comparator.comparing(DedupPair::toString));
        Collections.shuffle(dpl, r);
        Collections.shuffle(ndpl, r);

        String classificationStyle = parameters.get("classification_style");

        List<DedupPair> StrainList = new ArrayList<>(), StestList = new ArrayList<>();
        Triple<double[][], int[], DedupPair[]> XYpairsTrain = null;
        Triple<double[][], int[], DedupPair[]> XYpairsTest;
        switch (classificationStyle) {
            case "train_test":
                double trainRatio = 0.7;

                StrainList.addAll(dpl.subList(0, (int) (dpl.size() * trainRatio)));
                StrainList.addAll(ndpl.subList(0, (int) (dpl.size() * trainRatio)));
                StestList.addAll(dpl.subList((int) (dpl.size() * trainRatio), dpl.size()));
                StestList.addAll(ndpl.subList((int) (dpl.size() * trainRatio), ndpl.size()));
                XYpairsTrain = convertToDoubleArray(StrainList, S, attributesSMILE);
                break;
            case "test":
                StestList.addAll(dpl.subList(0, dpl.size()));
                StestList.addAll(ndpl.subList(0, ndpl.size()));
                break;
            case "kfold":
                StrainList.addAll(dpl.subList(0, dpl.size()));
                StrainList.addAll(ndpl.subList(0, dpl.size()));
                XYpairsTrain = convertToDoubleArray(StrainList, S, attributesSMILE);
                break;
        }
        XYpairsTest = convertToDoubleArray(StestList, S, attributesSMILE);

        ExperimentEvaluation ee = null;
        switch (classifier) {
            case "THRESHOLD":
                if (classificationStyle.equals("kfold")) {
                    ee = new ThresholdBasedClassifier(new Pair<>(XYpairsTrain.getLeft(), XYpairsTrain.getMiddle()),
                            XYpairsTrain.getRight(), attributesSMILE)
                            .executeKFold();
                } else if(classificationStyle.equals("test")) {
                    ee = new ThresholdBasedClassifier(null, null, attributesSMILE)
                            .execute(false, new Triplet<>(XYpairsTest.getLeft(), XYpairsTest.getMiddle(), XYpairsTest.getRight()));
                }
                break;
            default:

                break;
        }

        exportExperimentEvaluation(dpdIO, conn, XfeaturesList, ee);

        dpdIO.close();
    }

    protected Pair<Set<String>, Map<DedupPair, Map<String, Double>>> loadDataSingleThread(DataPrepDedupIO dpdIO) {
        Set<String> Xfeatures = new HashSet<>();
        Map<DedupPair, Map<String, Double>> S = new HashMap<>();
        try (ProgressBar pb = new ProgressBar("Loading data " + dataset, chainedPreparations.size())) { // name, initial max
            chainedPreparations.entrySet().forEach(e -> {
                pb.step();
                String attributeChain = e.getKey();
                List<Preparation> attributePreparations = e.getValue();
                String attribute = attributePreparations.get(0).getAttribute();
                Set<String> attributePreparators = attributePreparations.stream().map(Preparation::getPreparator).collect(Collectors.toSet());
                Map<String, Map<String, String>> Ppreparation = dpdIO.importSinglePreparationCubeTable(dataset, xstandard, attribute, attributePreparators, "similarities");

                // Preseve order
                List<String> attributePreparatorsList = attributePreparations.stream().map(Preparation::getPreparator).collect(Collectors.toList());;
//                attributePreparatorsList.sort(String::compareTo);
                String attributePreparatorsStr = String.join("~", attributePreparatorsList);

                for (Map.Entry<String, Map<String, String>> similarityEntry: Ppreparation.entrySet()) {
                    String recordID12 = similarityEntry.getKey();
                    String[] recordID12Toks = recordID12.split("_");
                    String recordID1 = recordID12Toks[0];
                    String recordID2 = recordID12Toks[1];
                    Double similarity = Double.valueOf(similarityEntry.getValue().get(attribute));

                    DedupPair dp = new DedupPair(dataset, recordID12, recordID1, recordID2,
                            DedupPair.PairClass.valueOf(similarityEntry.getValue().get("pair_class").toUpperCase()));
                    Map<String, Double> pairSimilarities = S.getOrDefault(dp, new HashMap<>());
                    pairSimilarities.put(attribute + "@" + attributePreparatorsStr, similarity);
                    S.put(dp, pairSimilarities);
                    Xfeatures.add(attribute + "@" + attributePreparatorsStr);
                }
            });
        }
        return new Pair<>(Xfeatures, S);
    }

    protected Pair<Set<String>, Map<DedupPair, Map<String, Double>>> loadDataMultiThreaded() {
        Set<String> Xfeatures = new ConcurrentSkipListSet<>();

        ConcurrentLinkedQueue<Quintet<String, String, String, String, Double>> S = new ConcurrentLinkedQueue<>();
//        Map<DedupPair, Map<String, Double>> S = new ConcurrentHashMap<>();
        try (ProgressBar pb = new ProgressBar("Loading data " + dataset, chainedPreparations.size())) { // name, initial max
            chainedPreparations.entrySet().parallelStream().forEach(e -> {
                pb.step();
                String attributeChain = e.getKey();
                List<Preparation> attributePreparations = e.getValue();
                String attribute = attributePreparations.get(0).getAttribute();
                Set<String> attributePreparators = attributePreparations.stream().map(Preparation::getPreparator).collect(Collectors.toSet());
                // Preserves order
                List<String> attributePreparatorsList = attributePreparations.stream().map(Preparation::getPreparator).collect(Collectors.toList());;
//                attributePreparatorsList.sort(String::compareTo);
                String attributePreparatorsStr = String.join("~", attributePreparatorsList);
                Connection conn = null;
                try {
                    Class.forName(dbDriver);
                    conn = DriverManager.getConnection(dbConnectionURL, dbUsername, dbPassword);
                    conn.setAutoCommit(false);
                } catch (ClassNotFoundException | SQLException ex) {
                    ex.printStackTrace();
                }

                String cubeType = "similarities";

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

                try {
                    PreparedStatement stmt = conn.prepareStatement(sql);
                    ResultSet rs = stmt.executeQuery(sql);

                    while (rs.next()) {
                        String recordID1 = rs.getString("record_id1");
                        String recordID2 = rs.getString("record_id2");
                        Double similarity = (Double) rs.getObject("similarity");
                        String pairClass = String.valueOf(rs.getObject("pair_class"));
                        S.add(new Quintet<>(recordID1, recordID2, pairClass, attribute + "@" + attributePreparatorsStr, similarity));
                        Xfeatures.add(attribute + "@" + attributePreparatorsStr);
                    }
                } catch (SQLException xc) {
                    xc.printStackTrace();
                }
            });
        }
        Map<DedupPair, Map<String, Double>> Sconverted = new HashMap<>();
        S.forEach(p -> {
            String recordID1 = p.getValue0();
            String recordID2 = p.getValue1();
            String pairClass = p.getValue2();
            String attrSignature = p.getValue3();
            Double similarity = p.getValue4();
            DedupPair dp = new DedupPair(dataset, recordID1 + "_" + recordID2, recordID1, recordID2,
            DedupPair.PairClass.valueOf(pairClass.toUpperCase()));
            Map<String, Double> pairSimilarities = Sconverted.getOrDefault(dp, new ConcurrentHashMap<>());
            pairSimilarities.put(attrSignature, similarity);
            Sconverted.put(dp, pairSimilarities);
        });

        return new Pair<>(Xfeatures, Sconverted);
    }

    public void exportExperimentEvaluation(DataPrepDedupIO dpdIO, Connection conn, List<String> xfeaturesList, ExperimentEvaluation ee) throws SQLException {
        System.out.println(ee.toMap());

        String preparations = String.join("-", xfeaturesList); // Create the preparation signature.
        dpdIO.clearTableClassification(dataset, xstandard, preparations, classifier); // Clear previous value

        List<String> header = dpdIO.getClassificationHeader();
        header.remove("id");

        String query = " insert into classification" +
                " (" + header.stream().map(x -> "`" + x + "`").collect(Collectors.joining(", ")) + ")"
                + " values (" + header.stream().map(c -> "?")
                .collect(Collectors.joining(", "))+")";
        PreparedStatement stmt = conn.prepareStatement(query);

        Map<String, Object> classificationMap = new HashMap<>();
        classificationMap.put("dataset", dataset);
        classificationMap.put("xstandard", xstandard);
        classificationMap.put("preparations", preparations);
        classificationMap.put("classifier", classifier);
        classificationMap.put("classifier_f1", String.valueOf(ee.getClassificationEvaluation().calculateFMeasure(1.0)));
        classificationMap.put("classifier_precision", String.valueOf(ee.getClassificationEvaluation().calculatePrecision()));
        classificationMap.put("classifier_recall", String.valueOf(ee.getClassificationEvaluation().calculateRecall()));
        if (ee.getPrecisionRecallInfo() != null) {
            classificationMap.put("auc_pr", ee.toMap().get("auc_pr"));
            classificationMap.put("auc_pr_best_f1", ee.toMap().get("auc_pr_f1"));
            classificationMap.put("auc_pr_best_precision", ee.toMap().get("auc_pr_f1_precision"));
            classificationMap.put("auc_pr_best_recall", ee.toMap().get("auc_pr_f1_recall"));
            classificationMap.put("auc_pr_precisions", ee.getPrecisionRecallInfo().get("auc_pr_precisions"));
            classificationMap.put("auc_pr_recalls", ee.getPrecisionRecallInfo().get("auc_pr_recalls"));
            classificationMap.put("auc_pr_thresholds", ee.getPrecisionRecallInfo().get("auc_pr_thresholds"));
        }

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

    private Triple<double[][], int[], DedupPair[]> convertToDoubleArray(List<DedupPair> listOfPairs, Map<DedupPair, Map<String, Double>> S,
                                                           Attribute[] attributesSMILE) {

        double[][] X = new double[listOfPairs.size()][attributesSMILE.length];
        int[] Y = new int[listOfPairs.size()];
        DedupPair[] pairs = new DedupPair[listOfPairs.size()];

        for (int i = 0; i < listOfPairs.size(); i++) {
            X[i] = new double[attributesSMILE.length];
            DedupPair p = listOfPairs.get(i);
            Map<String, Double> similarities = S.get(p);
            for (int j = 0; j < attributesSMILE.length; ++j) {
                Double sim = similarities.get(attributesSMILE[j].getName());
                X[i][j] = sim;
            }
            Y[i] = p.getPairClass().ordinal();
            pairs[i] = p.clone();
        }

        return Triple.of(X, Y, pairs);
    }

    public DedupPair[] getPairs() {
        return pairs;
    }
}
