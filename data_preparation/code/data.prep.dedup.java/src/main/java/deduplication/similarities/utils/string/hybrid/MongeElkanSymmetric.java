package deduplication.similarities.utils.string.hybrid;

import deduplication.similarities.utils.StringSimilarityMeasure;
import deduplication.similarities.utils.string.edit.DamerauLevenshtein;
import deduplication.similarities.utils.string.edit.JaroWinkler;
import deduplication.similarities.utils.string.edit.Levenshtein;
import deduplication.similarities.utils.string.hybrid.utils.Commons;
import org.apache.commons.lang3.math.NumberUtils;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Triplet;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jokoum on 10/9/16.
 */
public class MongeElkanSymmetric extends StringSimilarityMeasure implements Serializable {
    private Commons.MergingType mergingType;
    private String mongeElkanType;

    private StringSimilarityMeasure tokenSimilarityMeasure;

    public MongeElkanSymmetric(String id, Boolean enabledCache, Map<String, Object> parameters, StringSimilarityMeasure tokenSimilarityMeasure) {
        this(id, enabledCache, parameters, SimilarityMeasureType.MongeElkanSymmetric, tokenSimilarityMeasure);
    }

    public MongeElkanSymmetric(String id, Boolean enabledCache, Map<String, Object> parameters, SimilarityMeasureType type, StringSimilarityMeasure tokenSimilarityMeasure) {
        super(id, enabledCache, type, parameters);
        mergingType = parameters.containsKey("mergingType") ? Commons.MergingType.valueOf((String) parameters.get("mergingType")) : Commons.MergingType.BOTH_UNION;
        mongeElkanType = parameters.containsKey("mongeElkanType") ? (String) parameters.get("mongeElkanType") : "custom";//"inPhases";

        this.tokenSimilarityMeasure = tokenSimilarityMeasure;
    }

    private Pair<HashMap<Integer, Integer>, ArrayList<Double>> calculateAllTypes(PriorityQueue<Quartet<Boolean, Double, Integer, Integer>> pq,
                                                                                 ArrayList<Commons.TokenType> s1Types, ArrayList<Commons.TokenType> s2Types) {

        HashMap<Integer, Integer> s12Maps = new HashMap<>();
        ArrayList<Double> s12Sims = new ArrayList<>(s1Types.size());
        for( int i = 0 ; i < s1Types.size(); ++i) {
            s12Sims.add(-1.0);
        }

        ArrayList<Boolean> isMapped2 = new ArrayList<>();
        for( int i = 0 ; i < s2Types.size(); ++i) {
            isMapped2.add(Boolean.FALSE);
        }

        /* Given by importance [??]*/
        Integer toks1StillToMap = s1Types.size() - s12Maps.size();
        while (toks1StillToMap > 0 && pq.size() > 0) {
            Quartet<Boolean, Double, Integer, Integer> quartet = pq.poll();
            if (quartet == null) {
                break;
            }
            double similarity = quartet.getValue1();
            int i = quartet.getValue2(), j = quartet.getValue3();

            if (!s12Maps.containsKey(i) && !isMapped2.get(j)) {
                s12Maps.put(i, j);
                s12Sims.set(i, similarity);
                isMapped2.set(j, true);
                --toks1StillToMap;
            }
        }

        return new Pair<>(s12Maps, s12Sims);
    }

    private HashMap<Pair<Integer, Integer>, Double> calculateAllTypesBothWays(PriorityQueue<Quartet<Boolean, Double, Integer, Integer>> pq, Integer s1Toks, Integer s2Toks) {

        HashMap<Pair<Integer, Integer>, Double> pairSims = new HashMap<>();

        Boolean[] isMapped1 = new Boolean[s1Toks];
        Arrays.fill(isMapped1, Boolean.FALSE);
        Boolean[] isMapped2 = new Boolean[s2Toks];
        Arrays.fill(isMapped2, Boolean.FALSE);


        /* Given by importance [??]*/
        while (pq.size() > 0 && pairSims.size() < (s1Toks + s2Toks)) {
            Quartet<Boolean, Double, Integer, Integer> quartet = pq.poll();
            if (quartet == null) {
                break;
            }
            double similarity = quartet.getValue1();
            int i = quartet.getValue2(), j = quartet.getValue3();

            if (!isMapped1[i] && !isMapped2[j]) {
                isMapped1[i] = Boolean.TRUE;
                isMapped2[j] = Boolean.TRUE;

                pairSims.put(new Pair<>(i,j), similarity);
            }
        }

        return pairSims;
    }

    private Double mongeElkanSymmetric(String s1, String s2) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        // String 1
        ArrayList<String> s1Toks = new ArrayList<>(Arrays.asList(s1.split(" ")));
        ArrayList<Commons.TokenType> s1Types = s1Toks.stream().map(Commons::getTokenType).collect(Collectors.toCollection(ArrayList::new));

        // String 2
        ArrayList<String> s2Toks = new ArrayList<>(Arrays.asList(s2.split(" ")));
        ArrayList<Commons.TokenType> s2Types = s2Toks.stream().map(Commons::getTokenType).collect(Collectors.toCollection(ArrayList::new));

        PriorityQueue<Quartet<Boolean, Double, Integer, Integer>> pq = new PriorityQueue<>(quartetComparator);

        for (int i = 0; i < s1Toks.size(); ++i) {
            for (int j = 0; j < s2Toks.size(); ++j) {
                pq.add(new Quartet<>(s1Types.get(i).equals(s2Types.get(j)), tokenSimilarityMeasure.
                        similarity(s1Toks.get(i), s2Toks.get(j)), i, j));
            }
        }

        ArrayList<Double> similarities = new ArrayList<>();
        ArrayList<Double> weights = new ArrayList<>();
        switch (mergingType) {
            case ONCE_SMALLER_BIGGER:
                if (s1Toks.size() < s2Toks.size()) {
                    ArrayList<String> tmp = s1Toks;
                    s1Toks = s2Toks;
                    s2Toks = tmp;
                }
                Pair<HashMap<Integer, Integer>, ArrayList<Double>> pair = calculateAllTypes(pq, s1Types, s2Types);
                HashMap<Integer, Integer> s12Maps = pair.getValue0();
                ArrayList<Double> s12Sims = pair.getValue1();

                for (int i = 0; i < s1Types.size(); ++i) {
                    if (!s12Maps.containsKey(i)) {
                        continue;
                    }
                    double weight = s1Types.get(i).getWeight();
                    weights.add(weight);
                    similarities.add(s12Sims.get(i) * weight);
                }
                break;
            case BOTH_ALL:
                /* We inverse the first execution, so that for the second we just give the original PQ. */
                PriorityQueue<Quartet<Boolean, Double, Integer, Integer>> pqInv = new PriorityQueue<>(quartetComparator);
                pqInv.addAll(pq.stream().map(x -> new Quartet<>(x.getValue0(), x.getValue1(),
                        x.getValue3(), x.getValue2())).collect(Collectors.toCollection(PriorityQueue::new)));
                Pair<HashMap<Integer, Integer>, ArrayList<Double>> pair21 = calculateAllTypes(pqInv, s2Types, s1Types);
                Pair<HashMap<Integer, Integer>, ArrayList<Double>> pair12 = calculateAllTypes(pq, s1Types, s2Types);

                s12Maps = pair12.getValue0();
                s12Sims = pair12.getValue1();
                HashMap<Integer, Integer> s21Maps = pair21.getValue0();
                ArrayList<Double> s21Sims = pair21.getValue1();

                s12Maps.entrySet().forEach(x -> {
                    double weight = s1Types.get(x.getKey()).getWeight();
                    weights.add(weight);
                    similarities.add(s12Sims.get(x.getKey()) * weight);
                });
                s21Maps.entrySet().forEach(x -> {
                    double weight = s2Types.get(x.getKey()).getWeight();
                    weights.add(weight);
                    similarities.add(s21Sims.get(x.getKey()) * weight);
                });
                break;
            case BOTH_UNION:
//                pqInv = new PriorityQueue<>(quartetComparator);
//                pqInv.addAll(pq.stream().map(x -> new Quartet<>(x.getValue0(), x.getValue1(),
//                        x.getValue3(), x.getValue2())).collect(Collectors.toCollection(PriorityQueue::new)));
//                pair21 = calculateAllTypes(pqInv, s2Types, s1Types);
//                pair12 = calculateAllTypes(pq, s1Types, s2Types);
                HashMap<Pair<Integer, Integer>, Double> pairSims = calculateAllTypesBothWays(pq, s1Toks.size(), s2Toks.size());

//                s12Maps = pair12.getValue0();
//                s12Sims = pair12.getValue1();
//                s21Maps = pair21.getValue0();
//                s21Sims = pair21.getValue1();

                pairSims.forEach((p, sim) -> {
                    int i = p.getValue0();
                    int j = p.getValue1();
                    double weight = 1.0;
                    if (s1Types.get(i) == s2Types.get(j)) {
                        weight = s1Types.get(i).getWeight();
                    }
                    similarities.add(sim);
                    weights.add(weight);
                });

//                Set<DedupPair<Integer, Integer>> assignedTo12Union = new HashSet<>();
//                s12Maps.entrySet().forEach(x -> {
//                    assignedTo12Union.addScore(new DedupPair(x.getKey(), x.getValue()));
//                });
//
//                s21Maps.entrySet().forEach(x -> {
//                    assignedTo12Union.addScore(new DedupPair(x.getValue(), x.getKey()));  // Inverse
//                });
//
//                assignedTo12Union.forEach(x -> {
//                    double weight = s1Types.get(x.getValue0()).getWeight();
//                    weights.addScore(weight);
//
//                    /* If the pair was added because of s12 map then return similarity from s12Sims, otherwise from s21Sims */
//                    Double sim = s12Maps.containsKey(x.getValue0()) && s12Maps.get(x.getValue0()).equals(x.getValue1()) ?
//                            s12Sims.get(x.getValue0()) :
//                            s21Sims.get(x.getValue1());
//                    similarities.addScore(sim * weight);
//                });
                break;
            case BOTH_INTERSECTION:
                pqInv = new PriorityQueue<>(quartetComparator);
                pqInv.addAll(pq.stream().map(x -> new Quartet<>(x.getValue0(), x.getValue1(),
                                x.getValue3(), x.getValue2())).collect(Collectors.toCollection(PriorityQueue::new)));
                pair21 = calculateAllTypes(pqInv, s2Types, s1Types);
                pair12 = calculateAllTypes(pq, s1Types, s2Types);

                s12Maps = pair12.getValue0();
                s12Sims = pair12.getValue1();
                s21Maps = pair21.getValue0();
                s21Sims = pair21.getValue1();

                Set<Pair<Integer, Integer>> assignedTo12Intersection = new HashSet<>();

                s12Maps.entrySet().forEach(x -> {
                    assignedTo12Intersection.add(new Pair(x.getKey(), x.getValue()));
                });

                /* Inverse and intersection */
                assignedTo12Intersection.retainAll(s21Maps.entrySet().stream().map(x -> new Pair(x.getValue(), x.getKey())).collect(Collectors.toSet()));

                assignedTo12Intersection.forEach(x -> {
                    double weight = s1Types.get(x.getValue0()).getWeight();
                    weights.add(weight);

                    Double sim = s12Sims.get(x.getValue0());
                    similarities.add(sim * weight);
                });

                break;
            case BOTH_DIFFERENCE:
                pqInv = new PriorityQueue<>(quartetComparator);
                pqInv.addAll(pq.stream().map(x -> new Quartet<>(x.getValue0(), x.getValue1(),
                        x.getValue3(), x.getValue2())).collect(Collectors.toCollection(PriorityQueue::new)));
                pair21 = calculateAllTypes(pqInv, s2Types, s1Types);
                pair12 = calculateAllTypes(pq, s1Types, s2Types);

                s12Maps = pair12.getValue0();
                s12Sims = pair12.getValue1();
                s21Maps = pair21.getValue0();
                s21Sims = pair21.getValue1();

                Set<Pair<Integer, Integer>> assignedTo12Union;
                assignedTo12Union = new HashSet<>();
                s12Maps.entrySet().forEach(x -> {
                    assignedTo12Union.add(new Pair(x.getKey(), x.getValue()));
                });

                s21Maps.entrySet().forEach(x -> {
                    assignedTo12Union.add(new Pair(x.getValue(), x.getKey()));  // Inverse
                });

                assignedTo12Intersection = new HashSet<>();

                s12Maps.entrySet().forEach(x -> {
                    assignedTo12Intersection.add(new Pair(x.getKey(), x.getValue()));
                });

                /* Inverse and intersection */
                assignedTo12Intersection.retainAll(s21Maps.entrySet().stream().map(x -> new Pair(x.getValue(), x.getKey())).collect(Collectors.toSet()));

                Set<Pair<Integer, Integer>> assignedTo12Difference = assignedTo12Union;
                assignedTo12Difference.removeAll(assignedTo12Intersection);

                assignedTo12Difference.forEach(x -> {
                    double weight = s1Types.get(x.getValue0()).getWeight();
                    weights.add(weight);

                    /* If the pair was added because of s12 map then return similarity from s12Sims, otherwise from s21Sims */
                    Double sim = s12Maps.containsKey(x.getValue0()) && s12Maps.get(x.getValue0()).equals(x.getValue1()) ?
                            s12Sims.get(x.getValue0()) :
                            s21Sims.get(x.getValue1());
                    similarities.add(sim * weight);
                });
                break;
        }

        if (similarities.size() == 0) {
            return 0.0;
        } else {
            Double similarity = similarities.stream().mapToDouble(Double::doubleValue).sum() / weights.stream().mapToDouble(x -> x).sum();
            return similarity;
        }
    }

    transient Comparator quartetComparator = new Comparator<Quartet<Boolean, Double, Integer, Integer>> () {
        @Override
            /* WARNING: Notice the '-' before lvl 1,2,3 returns. We reverse the orders from a min PQ to a max PQ. */
        public int compare(Quartet<Boolean, Double, Integer, Integer> o1, Quartet<Boolean, Double, Integer, Integer> o2) {
            Integer x;
//            if((o1.getValue0().equals(o2.getValue0()))) {
//                x = 0;
//            } else if (o1.getValue0()) {
//                x = -1;
//            } else {
//                x = 1;
//            }
            if((o1.getValue0().equals(o2.getValue0()))) {
                x = 0;
            } else if (o1.getValue0()){
                x = -1;
            } else {
                x = 1;
            }
            if (x != 0) {return x;}

            x = o1.getValue1().compareTo(o2.getValue1());
            return -x;
//            if (x != 0) {return -x;}

//            x = o1.getValue2().compareTo(o2.getValue2());
//            if (x != 0) {return -x;}
//
//            x = o1.getValue3().compareTo(o2.getValue3());
//            if (x != 0) {return -x;}
//            return -x;
        }
    };

    transient Comparator tripletComparator = new Comparator<Triplet<Double, Integer, Integer>>() {
        @Override
        public int compare(Triplet<Double, Integer, Integer> o1, Triplet<Double, Integer, Integer> o2) {
            Integer x;
            x = o1.getValue0().compareTo(o2.getValue0());
            if (x != 0) {return -x;}

            x = o1.getValue1().compareTo(o2.getValue1());
            if (x != 0) {return -x;}

            x = o1.getValue2().compareTo(o2.getValue2());
//            if (x != 0) {return -x;}
            return -x;
        }
    };

    private Double mongeElkanSymmetricWeighted1Way(String s1, String s2) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        // String 1
        ArrayList<String> s1Toks = new ArrayList<>(Arrays.asList(s1.split(" ")));
        ArrayList<Boolean> isDigit1 = s1Toks.stream().map(NumberUtils::isNumber).collect(Collectors.toCollection(ArrayList::new));

        ArrayList<Integer> mapS1ToksToS2Toks = new ArrayList<>();
        ArrayList<Double> simS1ToksToS2Toks = new ArrayList<>();
        for (int i = 0 ; i < s1Toks.size(); ++i) {
            mapS1ToksToS2Toks.add(-1);
            simS1ToksToS2Toks.add(-1.0);
        }

        // String 2
        ArrayList<String> s2Toks = new ArrayList<>(Arrays.asList(s2.split(" ")));
        ArrayList<Boolean> isDigit2 = s2Toks.stream().map(NumberUtils::isNumber).collect(Collectors.toCollection(ArrayList::new));

        ArrayList<Boolean> isMapped2 = new ArrayList<>();
        for (int i = 0 ; i < s2Toks.size(); ++i) {
            isMapped2.add(Boolean.FALSE);
        }

        PriorityQueue<Triplet<Double, Integer, Integer>> pq = new PriorityQueue<>(tripletComparator);

        List<Pair<Integer, Integer>> pairsToCalculate = new ArrayList<>();
        for (int i = 0; i < s1Toks.size(); ++i) {
            for (int j = 0; j < s2Toks.size(); ++j) {
                if (!isDigit1.get(i).equals(isDigit2.get(j))) {
                    continue;
                }
                pairsToCalculate.add(new Pair(i, j));
            }
        }

        pq.addAll(pairsToCalculate.stream().map(x -> {
            int i = x.getValue0(), j = x.getValue1();
            double similarityIJ = 0;
            try {
                similarityIJ = tokenSimilarityMeasure.similarity(s1Toks.get(i), s2Toks.get(j));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            return new Triplet<>(similarityIJ, i, j);
        }).collect(Collectors.toList()));

        boolean calculatedDigitsStrings = false;
        // toks1_remaining_to_be_mapped
        Integer toks1StillToMap = mapS1ToksToS2Toks.stream().mapToInt(x -> x >= 0?0:1).sum();
        while (toks1StillToMap > 0) {
            if (pq.size() == 0) {
                if (!calculatedDigitsStrings) { // Last try to calculate de.hpi.is.datamatching.similarity between opposite type of tokens. Digits and non-digits.
                    if (toks1StillToMap > 0) {
                        calculatedDigitsStrings = true;
                        // Need to recompute distances for the remaining tokens
                        for (int i = 0; i < s1Toks.size(); ++i) {
                            if (mapS1ToksToS2Toks.get(i) != -1) {
                                continue;
                            }

                            for (int j = 0; j < s2Toks.size(); ++j) {
                                if (isMapped2.get(j)) {
                                    continue;
                                }

                                // Without checking this time if both are numbers or words
                                double similarityIJ = tokenSimilarityMeasure.similarity(s1Toks.get(i), s2Toks.get(j));

                                pq.add(new Triplet(similarityIJ, i, j));
                            }
                        }
                    }
                } else {
                    break;
                }
            }

            Triplet<Double, Integer, Integer> triplet = pq.poll();
            if (triplet == null) {
                continue;
            }
            double similarity = triplet.getValue0();
            int i = triplet.getValue1(), j = triplet.getValue2();

            if (mapS1ToksToS2Toks.get(i) == -1 && !isMapped2.get(j)) {
                mapS1ToksToS2Toks.set(i, j);
                simS1ToksToS2Toks.set(i, similarity);
                isMapped2.set(j, true);
                --toks1StillToMap;
            }
        }

        double sumSim = 0.0;
        double sumWeights = 0.0;

        double weightWords = 1.0;
        double weightNumbers = 1.0;

        for (int i = 0; i < s1Toks.size(); ++i) {
            double weight = isDigit1.get(i) ? weightNumbers : weightWords;
            sumWeights += weight;
            if (mapS1ToksToS2Toks.get(i) == -1) {
                continue;
            }
            sumSim += simS1ToksToS2Toks.get(i) * weight;
        }

        if (sumWeights == 0.0) {
            return 0.0;
        }

        return sumSim / sumWeights;
    }


    @Override
    protected Double _similarity(String s1, String s2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        switch (mongeElkanType) {
            case "inPhases":
                Double s1s2 = mongeElkanSymmetricWeighted1Way(s1, s2);
                Double s2s1 = mongeElkanSymmetricWeighted1Way(s2, s1);
                return (s1s2 + s2s1) / 2.0;
//                break;
            case "custom":
                return mongeElkanSymmetric(s1, s2);
//                break;
            default:
                return 0.0;
//                break;
        }
    }

    public static void main(String[] args) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {
        MongeElkanSymmetricDamerauLevenshtein sm = new MongeElkanSymmetricDamerauLevenshtein("", Boolean.TRUE);

        String s1 = "PRUS ELS LLC";
        String s2 = "PROMUS HOTELS LLCASD";
        System.out.println(sm.similarity(s2, s1));
//        System.out.println(sm.mongeElkanSymmetricWeighted1Way("hello world! How are you?", "hello wld", extra));
    }

    public static class MongeElkanSymmetricLevenshtein extends MongeElkanSymmetric {
        public MongeElkanSymmetricLevenshtein(String id, Boolean enabledCache) {
            super(id, enabledCache, new HashMap<>(), SimilarityMeasureType.MongeElkanSymmetricLevenshtein, new Levenshtein("token_measure", Boolean.FALSE, Collections.emptyMap()));
        }
    }

    public static class MongeElkanSymmetricDamerauLevenshtein extends MongeElkanSymmetric {
        public MongeElkanSymmetricDamerauLevenshtein(String id, Boolean enabledCache) {

            super(id, enabledCache, new HashMap<>(), SimilarityMeasureType.MongeElkanSymmetricDamerauLevenshtein, new DamerauLevenshtein("token_measure", Boolean.FALSE, Collections.emptyMap()));
        }
    }

    public static class MongeElkanSymmetricJaroWinkler extends MongeElkanSymmetric {
        public MongeElkanSymmetricJaroWinkler(String id, Boolean enabledCache) {

            super(id, enabledCache, new HashMap<>(), SimilarityMeasureType.MongeElkanSymmetricJaroWinkler, new JaroWinkler("token_measure", Boolean.FALSE, Collections.emptyMap()));
        }
    }
}
