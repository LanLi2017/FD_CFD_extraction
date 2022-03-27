package deduplication.classification.utils;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ClassificationEvaluation {

    protected Integer tp;
    protected Integer fp;
    protected Integer tn;
    protected Integer fn;

    public ClassificationEvaluation(Integer tp, Integer fp, Integer tn, Integer fn) {
        this.tp = tp;
        this.fp = fp;
        this.tn = tn;
        this.fn = fn;
    }

    public static class ClassificationEvaluationExtended extends ClassificationEvaluation {

        protected List<ClassificationDedupPair> tp = null;
        protected List<ClassificationDedupPair> fp = null;
        protected List<ClassificationDedupPair> tn = null;
        protected List<ClassificationDedupPair> fn = null;

        public ClassificationEvaluationExtended(List<ClassificationDedupPair> tp, List<ClassificationDedupPair> fp,
                                            List<ClassificationDedupPair> tn, List<ClassificationDedupPair> fn) {
            super(tp.size(), fp.size(), tn.size(), fn.size());
            this.tp = tp;
            this.fp = fp;
            this.tn = tn;
            this.fn = fn;
        }

        public Integer getTp() {
            return tp.size();
        }

        public Integer getFp() {
            return fp.size();
        }

        public Integer getFn() {
            return fn.size();
        }

        public Integer getTn() {
            return tn.size();
        }

        public List<ClassificationDedupPair> getTpList() {
            return tp;
        }

        public List<ClassificationDedupPair> getFpList() {
            return fp;
        }

        public List<ClassificationDedupPair> getTnList() {
            return tn;
        }

        public List<ClassificationDedupPair> getFnList() {
            return fn;
        }
    }

    public void addClassificationResult(ClassificationEvaluation other) {
        this.tp += other.tp;
        this.fp += other.fp;
        this.tn += other.tn;
        this.fn += other.fn;
    }


    public Double calculatePrecision() {
        if (tp == 0 && fp == 0) {
            return 0.0;
        } else {
            return 1.0 * tp / (tp + fp);
        }
    }

    public Double calculateRecall() {
        if (tp == 0 && fn == 0) {
            return 0.0;
        } else {
            return 1.0 * tp / (tp + fn);
        }
    }

    public Double calculateFMeasure(double beta) {
        double p = calculatePrecision();
        double r = calculateRecall();
        double beta2 = beta * beta;
        if (p == 0 && r == 0) {
            return 0.0;
        } else {
            return (1.0 + beta2) * p * r / (beta2 * p + r);
        }
    }

    public static Double calculateFMeasure(double beta, double p, double r) {
        double beta2 = beta * beta;
        if (p == 0 && r == 0) {
            return 0.0;
        } else {
            return (1.0 + beta2) * p * r / (beta2 * p + r);
        }
    }

    public Map<String, Double> toMap() {
        Map<String, Double> m = new TreeMap<>();

        m.put("tp", (double) tp);
        m.put("fp", (double) fp);
        m.put("tn", (double) tn);
        m.put("fn", (double) fn);

        m.put("precision", calculatePrecision());
        m.put("recall", calculateRecall());
        m.put("f05", calculateFMeasure(0.5));
        m.put("f1", calculateFMeasure(1.0));
        m.put("f2", calculateFMeasure(2.0));

        return m;
    }

    @Override
    public String toString() {
        return calculateFMeasure(1.0).toString();
    }

    public Integer getTp() {
        return tp;
    }

    public Integer getFp() {
        return fp;
    }

    public Integer getTn() {
        return tn;
    }

    public Integer getFn() {
        return fn;
    }

    public void setTp(Integer tp) {
        this.tp = tp;
    }

    public void setFp(Integer fp) {
        this.fp = fp;
    }

    public void setTn(Integer tn) {
        this.tn = tn;
    }

    public void setFn(Integer fn) {
        this.fn = fn;
    }
}
