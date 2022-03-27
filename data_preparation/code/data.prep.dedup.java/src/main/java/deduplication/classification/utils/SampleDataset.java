package deduplication.classification.utils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SampleDataset {

    protected double[][] X;
    protected int[] Yint;
    protected double[] Y;
    protected DedupPair[] dedupPairs;

    public SampleDataset(double[][] X, int[] Yint, DedupPair[] dedupPairs) {
        this.X = X;
        this.Yint = Yint;
        this.dedupPairs = dedupPairs;
    }

    public Map<String, int[]> stratifiedSampleTrainValidationTest(double trainRatio, double validationRatio, double testRatio) {
        Map<String, Double> setRatios = new HashMap<>();
        setRatios.put("train", trainRatio);
        setRatios.put("validation", validationRatio);
        setRatios.put("test", testRatio);
        if (((int) (setRatios.get("train") * X.length)) == 0) {
            System.out.println("Not enough data");
            return new HashMap<>();
        }

        Map<Integer, List<Integer>> indiciesByClass = new HashMap<>();
        for (int i = 0; i < X.length; ++i) {
            List<Integer> indicies = indiciesByClass.getOrDefault(Yint[i], new ArrayList<>());
            indicies.add(i);
            indiciesByClass.put(Yint[i], indicies);
        }

        Random r = new Random(0);
//        indiciesByClass.values().forEach(l -> Collections.shuffle(l, r));
        indiciesByClass.keySet().stream().sorted().forEach(k -> {
            List<Integer> l = indiciesByClass.get(k);
            Collections.shuffle(l,r);
            indiciesByClass.put(k, l);
        });

        Map<String, List<Integer>> indiciesBySet = new HashMap<>();
        Map<String, Map<Integer, Integer>> classCountersBySet = new HashMap<>();
        for (String set : setRatios.keySet()) {
            Map<Integer, Integer> classCounters = classCountersBySet.getOrDefault(set, new HashMap<>());
            for (Integer xClass : indiciesByClass.keySet()) {
                int counter = classCounters.getOrDefault(xClass, 0);
                int extraClassSize = (int) (setRatios.get(set) * indiciesByClass.get(xClass).size());
                List<Integer> extraIndicies = indiciesByClass.get(xClass).subList(counter, counter + extraClassSize);
                List<Integer> indiciesOfSet = indiciesBySet.getOrDefault(set, new ArrayList<>());
                indiciesOfSet.addAll(extraIndicies);
                indiciesBySet.put(set, indiciesOfSet);
                classCounters.put(xClass, counter + extraClassSize);
            }
            classCountersBySet.put(set, classCounters);
        }

        Map<String, int[]> indiciesBySetArrays = indiciesBySet.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().stream().mapToInt(i -> i).toArray()
        ));

        return indiciesBySetArrays;
    }

    public Map<String, int[]> randomUniformSampleTrainValidationTest(double trainRatio, double validationRatio, double testRatio) {
        Map<String, Double> setRatios = new HashMap<>();
        setRatios.put("train", trainRatio);
        setRatios.put("validation", validationRatio);
        setRatios.put("test", testRatio);
        if (((int) (setRatios.get("train") * X.length)) == 0) {
            System.out.println("Not enough data");
            return new HashMap<>();
        }

        Random r = new Random(0);
        List<Integer> indices = IntStream.range(0, X.length).boxed().collect(Collectors.toList());
        Collections.shuffle(indices, r);

        Map<String, List<Integer>> indiciesBySet = new HashMap<>();
        int counter = 0;
        for (String set : setRatios.keySet()) {
            int begin = counter;
            int end = counter + (int) (X.length * setRatios.get(set));
            if (end > indices.size()) {
                end = indices.size();
            }
            indiciesBySet.put(set, indices.subList(begin, end));
            counter += (int) (X.length * setRatios.get(set));
        }

        Map<String, int[]> indiciesBySetArrays = indiciesBySet.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().stream().mapToInt(i -> i).toArray()
        ));

        return indiciesBySetArrays;
    }

    public int[] stratifiedSampling(double sampleRatio) {
        Map<Integer, List<Integer>> indiciesByClass = new HashMap<>();
        for (int i = 0; i < Yint.length; ++i) {
            List<Integer> indicies = indiciesByClass.getOrDefault(Yint[i], new ArrayList<>());
            indicies.add(i);
            indiciesByClass.put(Yint[i], indicies);
        }

        Random r = new Random(0);
        indiciesByClass.keySet().stream().sorted().forEach(k -> {
            List<Integer> l = indiciesByClass.get(k);
            Collections.shuffle(l,r);
            indiciesByClass.put(k, l);
        });

        List<Integer> finalIndicies = new ArrayList<>();
        for (Integer xClass : indiciesByClass.keySet()) {
            finalIndicies.addAll(indiciesByClass.get(xClass).subList(0, (int) (indiciesByClass.get(xClass).size() * sampleRatio)));
        }

        return finalIndicies.stream().mapToInt(x -> x).toArray();
    }

}
