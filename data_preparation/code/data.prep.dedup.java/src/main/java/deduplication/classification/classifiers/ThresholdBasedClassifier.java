package deduplication.classification.classifiers;

import deduplication.classification.utils.*;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import smile.data.Attribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

public class ThresholdBasedClassifier extends Classifier {
    protected CrossValidationStandardSeedPermutation cv = null;

    public ThresholdBasedClassifier(Pair<double[][], int[]> XY, DedupPair[] pairs, Attribute[] attributesSMILE) {
        super(XY, pairs, attributesSMILE);
    }

    protected Double findOptimalThreshold() {
        if (XY != null && XY.getValue0() != null && XY.getValue1() != null) {
            double[] similarities = Stream.of(XY.getValue0()).mapToDouble(x -> DoubleStream.of(x).average().getAsDouble()).toArray();
            List<Double> bestThresholds = new ArrayList<>();
            cv = new CrossValidationStandardSeedPermutation(similarities.length, 10);
            for (int i = 0; i < cv.k; ++i) {
                double[] similaritiesFold = smile.math.Math.slice(similarities, cv.test[i]);
                int[] labelsFold = smile.math.Math.slice(XY.getValue1(), cv.test[i]);

                Map<String,String> prcFold = getPrecisionRecallCurve(similaritiesFold, labelsFold);

                String precisionsStr = prcFold.get("auc_pr_precisions");
                List<Double> precisions = Arrays.asList(precisionsStr.split("_")).stream().mapToDouble(x -> x.equals("nan") ? -1.0 : Double.valueOf(x.trim())).boxed().collect(Collectors.toList());
                String recallsStr = prcFold.get("auc_pr_recalls");
                List<Double> recalls = Arrays.asList(recallsStr.split("_")).stream().mapToDouble(x -> x.equals("nan") ? -1.0 : Double.valueOf(x.trim())).boxed().collect(Collectors.toList());
                String thresholdsStr = prcFold.get("auc_pr_thresholds");
                List<Double> thresholds = Arrays.asList(thresholdsStr.split("_")).stream().mapToDouble(x -> x.equals("nan") ? -1.0 :Double.valueOf(x.trim())).boxed().collect(Collectors.toList());

                int bestIndex = -1;
                double bestScore = -1;
                for (int j = 0; j < precisions.size(); ++j) {
                    double f1 = ClassificationEvaluation.calculateFMeasure(1.0, precisions.get(j), recalls.get(j));
                    if (f1 > bestScore) {
                        bestScore = f1;
                        bestIndex = j;
                    }
                }
                bestThresholds.add(thresholds.get(bestIndex));
            }
            return bestThresholds.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
        } else {
            return null;
        }
    }

    public ExperimentEvaluation executeKFold() {
        long start = System.nanoTime();
        int tp = 0, fp = 0, fn = 0, tn = 0;

        double[][] X = XY.getValue0();
        int[] Y = XY.getValue1();

        cv = new CrossValidationStandardSeedPermutation(X.length, 10);
        for (int i = 0; i < cv.k; ++i) {
            double[][] XFoldTrain = smile.math.Math.slice(X, cv.train[i]);
            int[] labelsFoldTrain = smile.math.Math.slice(Y, cv.train[i]);
            double[][] XFoldTest = smile.math.Math.slice(X, cv.test[i]);
            int[] labelsFoldTest = smile.math.Math.slice(Y, cv.test[i]);

            double[] simsTrain = Stream.of(XFoldTrain).mapToDouble(x -> DoubleStream.of(x).average().getAsDouble()).toArray();
            double averageSimTrain = Arrays.stream(simsTrain).boxed().mapToDouble(x -> x).average().getAsDouble();

            double[] predictionsTest = new double[XFoldTest.length];
            for (int j = 0; j < XFoldTest.length; ++j) {
                Double similarity = Arrays.stream(XFoldTest[j]).average().getAsDouble();
//            predictions[i] = similarity >= threshold ? similarity : 0.0;
                
                predictionsTest[j] = similarity >= averageSimTrain ? 1.0 : 0.0;
                if (labelsFoldTest[j] == 1 && predictionsTest[j] == (double)labelsFoldTest[j]) {
                    ++tp;
                } else if (labelsFoldTest[j] == 1 && predictionsTest[j] != (double)labelsFoldTest[j]) {
                    ++fn;
                } else if (labelsFoldTest[j] == 0 && predictionsTest[j] == (double)labelsFoldTest[j]) {
                    ++tn;
                } else if (labelsFoldTest[j] == 0 && predictionsTest[j] != (double)labelsFoldTest[j]) {
                    ++fp;
                }
            }
        }

        return new ExperimentEvaluation(new ClassificationEvaluation(tp, fp, tn, fn),
                null,
                null,
                System.nanoTime() - start);
    }

    public ExperimentEvaluation execute(Boolean withPairs, Triplet<double[][], int[], DedupPair[]> XYtestPairs) {
        long start = System.nanoTime();

        Double threshold = findOptimalThreshold();
        System.out.println("Threshold selected: " + threshold);
//        int[] labels = XY.getValue1().clone();
//        double[] similarities = Stream.of(XY.getValue0()).mapToDouble(x -> DoubleStream.of(x).average().getAsDouble()).toArray();

        double[] predictions = new double[XYtestPairs.getValue0().length];
        for (int i = 0; i < XYtestPairs.getValue0().length; ++i) {
            Double similarity = Arrays.stream(XYtestPairs.getValue0()[i]).average().getAsDouble();
//            predictions[i] = similarity >= threshold ? similarity : 0.0;
            if (threshold == null) {
                predictions[i] = similarity;
            } else {
                predictions[i] = similarity >= threshold ? 1.0 : 0.0;
            }

        }

        Map<String,String> prc = getPrecisionRecallCurve(predictions, XYtestPairs.getValue1());

        if (withPairs) {
            ArrayList<ClassificationDedupPair>
                    tpList = new ArrayList<>(),
                    fpList = new ArrayList<>(),
                    fnList = new ArrayList<>(),
                    tnList = new ArrayList<>();
            return new ExperimentEvaluation(null,
                    new ClassificationEvaluation.ClassificationEvaluationExtended(tpList, fpList, tnList, fnList),
                    prc,
                    System.nanoTime() - start);
        } else {
            int tp = 0, fp = 0, fn = 0, tn = 0;

            int[] labels = XYtestPairs.getValue1();
            for (int i = 0; i < predictions.length; ++i) {
                if (labels[i] == 1 && predictions[i] == (double)labels[i]) {
                    ++tp;
                } else if (labels[i] == 1 && predictions[i] != (double)labels[i]) {
                    ++fn;
                } else if (labels[i] == 0 && predictions[i] == (double)labels[i]) {
                    ++tn;
                } else if (labels[i] == 0 && predictions[i] != (double)labels[i]) {
                    ++fp;
                }
            }

            return new ExperimentEvaluation(new ClassificationEvaluation(tp, fp, tn, fn),
                    null,
                    prc,
                    System.nanoTime() - start);
        }
//            System.gc();
//            System.runFinalization();
    }

    public DedupPair[] getPairs() {
        return pairs;
    }

    public void setPairs(DedupPair[] pairs) {
        this.pairs = pairs;
    }
}
