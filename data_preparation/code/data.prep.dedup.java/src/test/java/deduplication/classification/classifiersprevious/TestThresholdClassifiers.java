//package deduplication.classification.classifiersprevious;
//
//import deduplication.classification.ClassificationController;
//
//import java.io.IOException;
//import java.nio.file.Paths;
//import java.util.*;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//
//import static smile.math.Math.round;
//
//public class TestThresholdClassifiers {
//
//    public static void main(String[] args) {
//        String XPath = "";
//        String YPath = "";
//
//        String argsParameters = "classifier=THRESHOLD "+
//                "pairs_similarities=/data/projects/data_preparation/workspace/cora_10000/baseline-addr@_-auth@_-date@_-mrgdvls@_-pgs@_-ttl@_/similarities.tsv "+
//                "classification_dir=/data/projects/data_preparation/workspace/cora_10000/folds/0/baseline-addr@_-auth@_-date@_-mrgdvls@_-pgs@_-ttl@_/classification/ "+
//                "attributes=* "+
//                "execution_mode=SEARCH_SPACE_APPLY_BEST "+
//                "split_train_test=/data/projects/data_preparation/workspace/cora_10000/folds/0/split_indexes_0.tsv";
//
//        List<String> argsList = Arrays.asList(argsParameters.split(" "));
//        Map<String, String> m = new HashMap<>();
//        if (argsList.size() == 0) {
//            System.exit(-1);
//        }
//        for(String arg: argsList) {
//            String[] toks = arg.split("=");
//            m.put(toks[0], toks[1]);
//        }
//
//        ClassificationController cls = new ClassificationController(
//                ClassificationController.Classifiers.valueOf(m.get("classifier").toUpperCase()),
//                10, //Integer.valueOf(m.get("k_folds")),
//                Paths.get(m.get("pairs_similarities")),
//                Paths.get(m.get("classification_dir")),
//                m.get("attributes"),
//                ClassificationController.ExecutionMode.valueOf(m.get("execution_mode")),
//                !m.get("split_train_test").equals("None") ? Paths.get(m.get("split_train_test")) : null,
//                m
//        );
//        try {
//            cls.initialize();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        ThresholdClassifier cl1 = new ThresholdClassifier(cls.getXY(), cls.getPairs(), cls.getFolds(), cls.getAttributesSMILE(), cls.getExternalSplitTrainTest());
//        ThresholdClassifierOptimized cl2 = new ThresholdClassifierOptimized(cls.getXY(), cls.getPairs(), cls.getFolds(), cls.getAttributesSMILE(), cls.getExternalSplitTrainTest());
//
//        List<Double> thresholds = new ArrayList<>();
//
//        for (Double threshold: IntStream.range(1,101).mapToDouble(i -> round(0.01*i, 2)).boxed().collect(Collectors.toList())) {
//            thresholds.add(threshold);
//        }
//
//        List<String> metricsToCheck = Arrays.asList("tp", "fp", "tn", "fn");
//
//        double cl2Start = System.currentTimeMillis();
////        TreeMap<Double, ClassificationResult> thresholdToCR = cl2.evaluateOnThresholdsOptimized(thresholds, cls.getXY().getValue0(), cls.getXY().getValue1());
//        TreeMap<Double, ClassificationResult> thresholdToCR = cl2.evaluateOnThresholds(thresholds, cls.getXY().getValue0(), cls.getXY().getValue1());
//        double cl2Finish = System.currentTimeMillis();
//
//        double cl1Start = System.currentTimeMillis();
//        for (Double threshold: thresholds) {
//            ClassificationResult cr1 = cl1.evaluateOnThreshold(threshold, cls.getXY().getValue0(), cls.getXY().getValue1());
//        }
//        double cl1Finish = System.currentTimeMillis();
//
//        System.out.println("CL1: " + (cl1Finish - cl1Start));
//        System.out.println("CL2: " + (cl2Finish - cl2Start));
//
//        for (Double threshold: thresholds) {
//            ClassificationResult cr1 = cl1.evaluateOnThreshold(threshold, cls.getXY().getValue0(), cls.getXY().getValue1());
//            ClassificationResult cr2 = thresholdToCR.get(threshold);
//
//            for( String metric: metricsToCheck) {
//                if (!cr1.toMap().get(metric).equals(cr2.toMap().get(metric))) {
//                    System.err.println("ERROR! " + metric + " does not match!");
//                    System.exit(-1);
//                }
//            }
//
//        }
//
//        System.out.println("Hello World");
//    }
//
//}
