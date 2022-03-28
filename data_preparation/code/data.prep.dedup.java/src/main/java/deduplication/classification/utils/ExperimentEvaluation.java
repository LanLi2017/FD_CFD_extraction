package deduplication.classification.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.*;
import java.util.stream.Collectors;

public class ExperimentEvaluation {

    protected ClassificationEvaluation classificationEvaluation = null;
    protected ClassificationEvaluation.ClassificationEvaluationExtended classificationEvaluationExtended = null;
//    protected PrecisionRecallCurve prc = null;
    protected HardwareInfo hardwareInfo = null;
    protected Long executionTimeClassification = -1L;
    protected Long executionTimeClassificationSearchSpace = -1L;
    protected Integer parametersSearchSpace = -1;

    protected Map<String, String> precisionRecallInfo = new HashMap<>();

    public ExperimentEvaluation(ClassificationEvaluation classificationEvaluation,
                                ClassificationEvaluation.ClassificationEvaluationExtended classificationEvaluationExtended,
                                Map<String, String> precisionRecallInfo,
                                Long executionTimeClassification
                                ) {

        this.classificationEvaluation = classificationEvaluation;
        this.classificationEvaluationExtended = classificationEvaluationExtended;
        this.precisionRecallInfo = precisionRecallInfo;
        this.executionTimeClassification = executionTimeClassification;
        this.hardwareInfo = new HardwareInfo();
        hardwareInfo.retrieve();
    }

    public Map<String, Double> toMap() {
        Map<String, Double> m = new TreeMap<>();

        if (classificationEvaluation != null) {
            m.putAll(classificationEvaluation.toMap());
        }
        if (classificationEvaluationExtended != null) {
            m.putAll(classificationEvaluationExtended.toMap());
        }
        if (precisionRecallInfo != null) {
            if (precisionRecallInfo.containsKey("auc_pr")) {
                m.put("auc_pr", Double.valueOf(precisionRecallInfo.get("auc_pr")));
                if (Double.isNaN(m.get("auc_pr"))) {
                    m.put("auc_pr", -1.0);
                }
            }

            // Calculate best precision recall and F-1
            double bestPrecision = -1.0, bestRecall=-1.0, bestF1=-1.0;

            String precisionsStr = precisionRecallInfo.get("auc_pr_precisions");
            List<Double> precisions = Arrays.asList(precisionsStr.substring(1, precisionsStr.length()-1).split("_")).stream().mapToDouble(x -> Double.valueOf(x.trim())).boxed().collect(Collectors.toList());
            String recallsStr = precisionRecallInfo.get("auc_pr_recalls");
            List<Double> recalls = Arrays.asList(recallsStr.substring(1, recallsStr.length()-1).split("_")).stream().mapToDouble(x -> Double.valueOf(x.trim())).boxed().collect(Collectors.toList());
            for (int i = 0; i < precisions.size(); ++i) {
                if (precisions.get(i) == -1 || recalls.get(i) == -1) {
                    continue;
                }
                double currentF1 = ClassificationEvaluation.calculateFMeasure(1.0, precisions.get(i), recalls.get(i));
                if (currentF1 > bestF1) {
                    bestF1 = currentF1;
                    bestPrecision = precisions.get(i);
                    bestRecall = recalls.get(i);
                }
            }
            m.put("auc_pr_f1_precision", bestPrecision);
            m.put("auc_pr_f1_recall", bestRecall);
            m.put("auc_pr_f05", ClassificationEvaluation.calculateFMeasure(0.5, bestPrecision, bestRecall));
            m.put("auc_pr_f1", bestF1);
            m.put("auc_pr_f2", ClassificationEvaluation.calculateFMeasure(2.0, bestPrecision, bestRecall));

        }

        m.put("executionTimeClassification", executionTimeClassification.doubleValue());

        m.putAll(hardwareInfo.toMap());

        return m;
    }

    public ClassificationEvaluation getClassificationEvaluation() {
        if (classificationEvaluationExtended != null) {
            return classificationEvaluationExtended;
        } else {
            return classificationEvaluation;
        }
    }

    public ClassificationEvaluation.ClassificationEvaluationExtended getClassificationEvaluationExtended() {
        return classificationEvaluationExtended;
    }

    public void setExecutionTimeClassification(Long executionTimeClassification) {
        this.executionTimeClassification = executionTimeClassification;
    }

    public HardwareInfo getHardwareInfo() {
        return hardwareInfo;
    }

    public Long getExecutionTimeClassification() {
        return executionTimeClassification;
    }

    public static class HardwareInfo {
        protected static OperatingSystemMXBean systemMonitor = null;

        protected Integer availableProcessors = -1;

        protected Long totalMemory = -1L;
        protected Long maxMemory = -1L;
        protected Long usedMemory = -1L;
        protected Long freeMemory = -1L;

        protected Double systemCPULoad = -1.0;

        public HardwareInfo() {
            systemMonitor = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        }

        public void retrieve() {
            availableProcessors = Runtime.getRuntime().availableProcessors();
            systemCPULoad = systemMonitor.getSystemLoadAverage();

            maxMemory = (Runtime.getRuntime().maxMemory() / 1024) / 1024;
            totalMemory = (Runtime.getRuntime().totalMemory() / 1024) / 1024;
            freeMemory = (Runtime.getRuntime().freeMemory() / 1024) / 1024;
            usedMemory = totalMemory - freeMemory;
        }

        public Map<String, Double> toMap() {
            Map<String, Double> m = new TreeMap<>();

            m.put("availableProcessors", availableProcessors.doubleValue());
            m.put("totalMemory", totalMemory.doubleValue());
            m.put("maxMemory", maxMemory.doubleValue());
            m.put("usedMemory", usedMemory.doubleValue());
            m.put("freeMemory", freeMemory.doubleValue());
            m.put("systemCPULoad", systemCPULoad);

            return m;
        }
    }

    @Override
    public String toString() {
        return classificationEvaluation.calculateFMeasure(1.0).toString();
    }

    public Long getExecutionTimeClassificationSearchSpace() {
        return executionTimeClassificationSearchSpace;
    }

    public void setExecutionTimeClassificationSearchSpace(Long executionTimeClassificationSearchSpace) {
        this.executionTimeClassificationSearchSpace = executionTimeClassificationSearchSpace;
    }

    public Integer getParametersSearchSpace() {
        return parametersSearchSpace;
    }

    public void setParametersSearchSpace(Integer parametersSearchSpace) {
        this.parametersSearchSpace = parametersSearchSpace;
    }

//    public PrecisionRecallCurve getPrecisionRecallCurve() {
//        return prc;
//    }


    public Map<String, String> getPrecisionRecallInfo() {
        return precisionRecallInfo;
    }
}
