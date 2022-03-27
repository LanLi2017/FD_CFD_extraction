package deduplication.classification.classifiers;

import deduplication.classification.utils.DedupPair;
import deduplication.classification.utils.ReadFromPython;
import org.javatuples.Pair;
import org.nd4j.evaluation.classification.ROC;
import org.nd4j.evaluation.curves.PrecisionRecallCurve;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import smile.data.Attribute;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class Classifier {

    protected Pair<double[][], int[]> XY;
    protected DedupPair[] pairs;

    protected Map<String, double[][]> setToX = new HashMap<>();
    protected Map<String, int[]> setToY = new HashMap<>();
    protected Map<String, DedupPair[]> setToPairs = new HashMap<>();

    protected Attribute[] attributesSMILE;

    public Classifier(Pair<double[][], int[]> XY, DedupPair[] pairs, Attribute[] attributesSMILE) {
        this.XY = XY;
        this.pairs = pairs;
        this.attributesSMILE = attributesSMILE;
    }

    protected void splitToTrainPredictionTest(double trainRatio, double validationRatio, double testRatio) {
        List<Integer> dpls = new ArrayList<>(), ndpls = new ArrayList<>();
        for (int i = 0; i < pairs.length; ++i) {
            switch (pairs[i].getPairClass()) {
                case DPL:
                    dpls.add(i);
                    break;
                case NDPL:
                    ndpls.add(i);
                    break;
            }
        }

        int numFeatures = XY.getValue0()[0].length;

        Map<String, Double> subsetToRatio = new HashMap<>();
        subsetToRatio.put("train", trainRatio);
        subsetToRatio.put("validation", validationRatio);
        subsetToRatio.put("test", testRatio);

        List<String> subsetOrder = Arrays.asList("train", "validation", "test");

        int count = 0, countDpl = 0, countNdpl = 0;
        for (String subset: subsetOrder) {
            int dplSize = (int) (subsetToRatio.get(subset) * dpls.size());
            int ndplSize = (int) (subsetToRatio.get(subset) * ndpls.size());
            if (subset.equals("test")) {
                dplSize = dpls.size() - countDpl;
                ndplSize = ndpls.size() - countNdpl;
            }
            int setSize = dplSize + ndplSize;

            double[][] Xset = new double[setSize][numFeatures];
            int[] Yset = new int[setSize];
            DedupPair[] pairsSet = new DedupPair[setSize];

            for (int i = 0; i < dplSize; ++i) {
                for (int j = 0; j < numFeatures; ++j) {
                    Xset[i][j] = XY.getValue0()[dpls.get(countDpl + i)][j];
                }
                Yset[i] = XY.getValue1()[dpls.get(countDpl + i)];
                pairsSet[i] = pairs[dpls.get(countDpl + i)];
            }

            for (int i = 0; i < ndplSize; ++i) {
                for (int j = 0; j < numFeatures; ++j) {
                    Xset[dplSize + i][j] = XY.getValue0()[ndpls.get(countNdpl + i)][j];
                }
                Yset[dplSize + i] = XY.getValue1()[ndpls.get(countNdpl + i)];
                pairsSet[dplSize + i] = pairs[ndpls.get(countNdpl + i)];
            }

//            for (int i = 0; i < setSize; ++i) {
//                for (int j = 0; j < numFeatures; ++j) {
//                    Xset[i][j] = XY.getValue0()[count + i][j];
//                }
//                Yset[i] = XY.getValue1()[count + i];
//                pairsSet[i] = pairs[count + i];
//            }

            setToX.put(subset, Xset);
            setToY.put(subset, Yset);
            setToPairs.put(subset, pairsSet);

            count += setSize;
            countDpl += dplSize;
            countNdpl += ndplSize;
        }
    }

    protected Map<String, String> getPrecisionRecallCurve(double[] probabilities, int[] labels) {
//        return getPrecisionRecallCurvePython(probabilities, labels);
        return getPrecisionRecallCurveJava(probabilities, labels);
    }

    protected Map<String, String> getPrecisionRecallCurvePython(double[] probabilities, int[] labels) {
        try {
            return ReadFromPython.executeFromPython(probabilities, labels);
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    protected Map<String, String> getPrecisionRecallCurveJava(double[] probabilities, int[] labels) {
//        double[] similarities = Stream.of(XY.getValue0()).mapToDouble(x -> x[finalTotalSimIdx]).toArray().clone();

//        double auc = AUCwithPR.measure(labels, probabilities);
//        System.out.println("AUC PR: " + auc);

//        ThresholdCurve thresholdCurve = new ThresholdCurve();
//        ArrayList<Prediction> predictionsForWeka = new ArrayList<>();
//        for (int i = 0; i < labels.length; ++i) {
//            Prediction predictionWeka = new NominalPrediction(labels[i], NominalPrediction.makeDistribution(similarities[i], 2));
//            predictionsForWeka.add(predictionWeka);
//        }
//        Instances instancesPR = thresholdCurve.getCurve(predictionsForWeka);

        ROC roc = new ROC();
//        INDArray probAndLabel = Nd4j.zeros(labels.length, 2);
        INDArray prob = Nd4j.zeros(labels.length, 1);
        INDArray label = Nd4j.zeros(labels.length, 1);
        for (int i = 0; i < labels.length; ++i) {
            int[] indexProb = new int[]{i, 0};
//            probAndLabel.putScalar(indexProb, probabilities[i]);
//            int[] indexLabel = new int[]{i, 1};
//            probAndLabel.putScalar(indexLabel, labels[i]);

            prob.putScalar(indexProb, probabilities[i]);
            label.putScalar(indexProb, labels[i]);
        }
//        roc.setProbAndLabel(probAndLabel);
//        roc.setExampleCount(labels.length);
        roc.eval(label, prob);

        PrecisionRecallCurve prc = roc.getPrecisionRecallCurve();

        Map<String, String> m = new HashMap<>();
        m.put("auc_pr_precisions", Arrays.stream(prc.getPrecision()).boxed().map(Object::toString).collect(Collectors.joining("_")));
        m.put("auc_pr_recalls", Arrays.stream(prc.getRecall()).boxed().map(Object::toString).collect(Collectors.joining("_")));
        m.put("auc_pr_thresholds", Arrays.stream(prc.getThreshold()).boxed().map(Object::toString).collect(Collectors.joining("_")));
        m.put("auc_pr", String.valueOf(prc.calculateAUPRC()));

        return m;
    }

//    public double calculateMeanAveragePrecision(double[] probabilities, int[] labels) {
////        float map = 0;
////        for(size_t i=0; i<queries.size(); i++){
////            std::vector<std::string> topkTest;
////            //populate topkTest somehow using k
////            float correct = 0;
////            float ap = 0;
////            for(size_t j=0; j<topkTest.size(); j++){
////                //if topkTest[j] belongs to the true positives, increment the number of correct images
////                if(std::find(truePositives.begin(), truePositives.end(), topkTest[j]) != queries.end())
////                ap += ++correct / (j+1);
////                map += ap / topkTest.size();
////            }
////            map /= queries.size(),
//        double map = 0.0;
//        for (int i = 0; i < probabilities.length; ++i) {
//            double ap = 0.0;
//            double correct = 0.0;
//            if (probabilities[i] == labels[i]) {
//                ap += ++correct / ()
//            }
//        }
//
//    }
}
