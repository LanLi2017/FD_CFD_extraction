package deduplication.classification.utils;

import java.util.Comparator;

public class ClassificationDedupPair extends DedupPair {
    protected DedupPair.PairClass classifiedAs;

    public enum ClassificationLabel {
        TP, FP, TN, FN
    }

    public ClassificationDedupPair(String dataset, String id, String id1, String id2, DedupPair.PairClass pairClass, DedupPair.PairClass classifiedAs) {
        super(dataset, id, id1, id2, pairClass);
        this.classifiedAs = classifiedAs;
    }

    public ClassificationLabel getPairMark(DedupPair.PairClass pairClass, DedupPair.PairClass classifiedAs) {
        if (pairClass == DedupPair.PairClass.DPL && classifiedAs == DedupPair.PairClass.DPL) {
            return ClassificationLabel.TP;
        } else if (pairClass == DedupPair.PairClass.DPL && classifiedAs == DedupPair.PairClass.NDPL) {
            return ClassificationLabel.FN;
        } else if (pairClass == DedupPair.PairClass.NDPL && classifiedAs == DedupPair.PairClass.DPL) {
            return ClassificationLabel.FP;
        } else if (pairClass == DedupPair.PairClass.NDPL && classifiedAs == DedupPair.PairClass.NDPL) {
            return ClassificationLabel.TN;
        }
        return null;
    }

    @Override
    public int compareTo(DedupPair o) {
        if (!(o instanceof ClassificationDedupPair)) {
            return 0;
        } else {
            Comparator<ClassificationDedupPair> cmp = Comparator
                    .comparing(ClassificationDedupPair::getDataset)
                    .thenComparing(ClassificationDedupPair::getId1)
                    .thenComparing(ClassificationDedupPair::getId2)
                    .thenComparing(ClassificationDedupPair::getPairClass)
                    .thenComparing(ClassificationDedupPair::getClassifiedAs);

            return cmp.compare(this, (ClassificationDedupPair) o);
        }
    }

    public DedupPair.PairClass getClassifiedAs() {
        return classifiedAs;
    }

    public ClassificationLabel getClassificationLabel() {
        return getPairMark(pairClass, classifiedAs);
    }
}
