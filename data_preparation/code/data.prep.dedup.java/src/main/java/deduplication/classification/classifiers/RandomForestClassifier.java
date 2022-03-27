package deduplication.classification.classifiers;

import deduplication.classification.utils.ClassificationDedupPair;
import deduplication.classification.utils.ClassificationEvaluation;
import deduplication.classification.utils.DedupPair;
import deduplication.classification.utils.ExperimentEvaluation;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import smile.classification.RandomForest;
import smile.data.Attribute;

import java.util.ArrayList;
import java.util.Map;

public class RandomForestClassifier extends Classifier {
    public RandomForestClassifier(Pair<double[][], int[]> XY, DedupPair[] pairs, Attribute[] attributesSMILE) {
        super(XY, pairs, attributesSMILE);
    }

    public ExperimentEvaluation execute(Boolean withPairs, Triplet<double[][], int[], DedupPair[]> XYtestPairs) {
        long start = System.nanoTime();
        RandomForest rf = new RandomForest(XY.getValue0(), XY.getValue1(), 256);

        double[] predictions = new double[XYtestPairs.getValue0().length];
        for (int i = 0; i < XYtestPairs.getValue0().length; ++i) {
            predictions[i] = rf.predict(XYtestPairs.getValue0()[i]);
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
            return new ExperimentEvaluation(new ClassificationEvaluation(tp, fp, tn, fn),
                    null,
                    prc,
                    System.nanoTime() - start);
        }
//            System.gc();
//            System.runFinalization();
    }
}
