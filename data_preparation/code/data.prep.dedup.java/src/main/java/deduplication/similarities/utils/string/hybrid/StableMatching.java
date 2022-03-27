package deduplication.similarities.utils.string.hybrid;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import deduplication.similarities.utils.StringSimilarityMeasure;
import deduplication.similarities.utils.string.edit.DamerauLevenshtein;
import deduplication.similarities.utils.string.edit.JaroWinkler;
import deduplication.similarities.utils.string.edit.Levenshtein;
import deduplication.similarities.utils.string.hybrid.stablematching.GaleShapleySimple;
import deduplication.similarities.utils.string.hybrid.utils.Commons;
import org.apache.commons.lang3.math.NumberUtils;
import org.javatuples.Pair;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

import static deduplication.similarities.utils.string.hybrid.utils.Commons.MergingType.BOTH_ALL_AVERAGE;

/**
 * Created by jokoum on 11/22/16.
 */
public class StableMatching extends StringSimilarityMeasure {
    protected final static Gson gson = new GsonBuilder().enableComplexMapKeySerialization().setPrettyPrinting().create();

    private Commons.MergingType mergingType;
    private StringSimilarityMeasure tokenSimilarityMeasure;

    public static class TokenPreference implements Comparable<TokenPreference>{
        Integer index;
        Commons.TokenType type;
        Double similarity;

        public TokenPreference(Integer index, Commons.TokenType type, Double similarity) {
            this.index = index;
            this.type = type;
            this.similarity = similarity;
        }

        @Override
        public int compareTo(TokenPreference tokPref) {
            Integer x;
            x = type.equals(tokPref.type) ? 0 : 1;
            if (x != 0) {
                return x;
            }
            x = similarity.compareTo(tokPref.similarity);
            return x;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + index;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TokenPreference other = (TokenPreference) obj;
            if (index.compareTo(other.index) != 0)
                return false;
            return true;
        }

        public Integer getIndex() {
            return index;
        }

        public Double getSimilarity() {
            return similarity;
        }
    }


    public Pair<Map<Integer, ArrayList<TokenPreference>>, Map<Integer, ArrayList<TokenPreference>>>
    getTokensWithSimilaritesCalculatedAndSorted(String s1, String s2) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

        //        /* Ensure no same tokens */
//        List<String> toks1 = new ArrayList<>(new HashSet<>(Arrays.asList(s1.split(" "))));
//        List<String> toks2 = new ArrayList<>(new HashSet<>(Arrays.asList(s2.split(" "))));

        ArrayList<String> toksI = new ArrayList<>(Arrays.asList(s1.split(" ")));
        ArrayList<String> toksJ = new ArrayList<>(Arrays.asList(s2.split(" ")));

        ArrayList<Commons.TokenType> toksIType = toksI.stream().map(x -> NumberUtils.isNumber(x) ? Commons.TokenType.NUMBER : Commons.TokenType.TEXT).collect(Collectors.toCollection(ArrayList::new));
        ArrayList<Commons.TokenType> toksJType = toksJ.stream().map(x -> NumberUtils.isNumber(x) ? Commons.TokenType.NUMBER : Commons.TokenType.TEXT).collect(Collectors.toCollection(ArrayList::new));

        Map<Integer, ArrayList<TokenPreference>> toksIPreferences = new HashMap<>();
        Map<Integer, ArrayList<TokenPreference>> toksJPreferences = new HashMap<>();

//        StringSimilarityMeasure tokenSimilarityMeasure = (StringSimilarityMeasure) parameters.get("token_measure");

        for (int i = 0 ; i < toksI.size() ; ++i) {
            for (int j = 0 ; j < toksJ.size() ; ++j) {
                double similarityIJ = tokenSimilarityMeasure.similarity(toksI.get(i), toksJ.get(j));
                ArrayList<TokenPreference> iPref = toksIPreferences.containsKey(i) ? toksIPreferences.get(i) : new ArrayList<>();
                ArrayList<TokenPreference> jPref = toksJPreferences.containsKey(j) ? toksJPreferences.get(j) : new ArrayList<>();
                TokenPreference tpI = new TokenPreference(j, toksJType.get(j), similarityIJ);
                TokenPreference tpJ = new TokenPreference(i, toksIType.get(i), similarityIJ);
                iPref.add(tpI);
                jPref.add(tpJ);
                toksIPreferences.put(i, iPref);
                toksJPreferences.put(j, jPref);
            }
        }

        toksIPreferences.forEach((k, v) -> v.sort((p1, p2) -> -p1.similarity.compareTo(p2.similarity)));
        toksJPreferences.forEach((k, v) -> v.sort((p1, p2) -> -p1.similarity.compareTo(p2.similarity)));

        return new Pair(toksIPreferences, toksJPreferences);
    }

    public StableMatching(String id, Boolean enabledCache, Map<String, Object> parameters, StringSimilarityMeasure tokenSimilarityMeasure) {
        this(id, enabledCache, parameters, SimilarityMeasureType.StableMatching, tokenSimilarityMeasure);
    }

    public StableMatching(String id, Boolean enabledCache, Map<String, Object> parameters, SimilarityMeasureType type, StringSimilarityMeasure tokenSimilarityMeasure) {
        super(id, enabledCache, type, parameters);
        mergingType = parameters.containsKey("mergingType") ? Commons.MergingType.valueOf((String) parameters.get("mergingType")) : BOTH_ALL_AVERAGE;
        this.tokenSimilarityMeasure = tokenSimilarityMeasure;
    }


    public List<Double> getSimilaritiesOfAssignments(Set<Pair<Integer, Integer>> assignedTo, Map<Integer, ArrayList<TokenPreference>> toksIPreferences) {
        List<Double> similarities = new ArrayList<>();

        for (Pair<Integer, Integer> pair : assignedTo) {
            Integer i = pair.getValue1(), j = pair.getValue0();  // XXX: Be aware that we assign them in this way in StableMatching (i, j)
            List<TokenPreference> iPref = toksIPreferences.get(i);
            TokenPreference jPref = GaleShapleySimple.makeAPreference(j);
            Integer jPrefIndex = iPref.indexOf(jPref);
            TokenPreference ijPref = iPref.get(jPrefIndex);
            similarities.add(ijPref.getSimilarity());
        }
        return similarities;
    }

    public Set<Pair<Integer, Integer>> mapToSetPair(Map<Integer, Integer> m) {
        return m.entrySet().stream().map(x -> new Pair<>(x.getKey(), x.getValue())).collect(Collectors.toSet());
    }

    public Double similarity(String s1, String s2, Commons.MergingType mergingType) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Pair<Map<Integer, ArrayList<TokenPreference>>, Map<Integer, ArrayList<TokenPreference>>> pair = getTokensWithSimilaritesCalculatedAndSorted(s1, s2);

        Map<Integer, ArrayList<TokenPreference>> toksIPreferences = pair.getValue0();
        Map<Integer, ArrayList<TokenPreference>> toksJPreferences = pair.getValue1();

        List<Double> similarities = new ArrayList<>();
        switch (mergingType) {
            case ONCE_SMALLER_BIGGER:
                if (toksIPreferences.size() > toksJPreferences.size()) {
                    Map<Integer, ArrayList<TokenPreference>> tmp = toksIPreferences;
                    toksIPreferences = toksJPreferences;
                    toksJPreferences = tmp;
                }
                Set<Pair<Integer, Integer>> assignedToIJ = GaleShapleySimple.match(toksIPreferences, toksJPreferences).entrySet().stream().map(x -> new Pair<>(x.getKey(), x.getValue())).collect(Collectors.toSet());

                /* One time in favor of I */
                similarities.addAll(getSimilaritiesOfAssignments(assignedToIJ, toksIPreferences));
                break;
            case BOTH_ALL:
                assignedToIJ = mapToSetPair(GaleShapleySimple.match(toksIPreferences, toksJPreferences));
                Set<Pair<Integer, Integer>> assignedToJI = mapToSetPair(GaleShapleySimple.match(toksJPreferences, toksIPreferences));

                /* One time in favor of I */
                similarities.addAll(getSimilaritiesOfAssignments(assignedToIJ, toksIPreferences));
                /* One time in favor of J*/
                similarities.addAll(getSimilaritiesOfAssignments(assignedToJI, toksJPreferences));
                break;
            case BOTH_ALL_AVERAGE:
                assignedToIJ = mapToSetPair(GaleShapleySimple.match(toksIPreferences, toksJPreferences));
                assignedToJI = mapToSetPair(GaleShapleySimple.match(toksJPreferences, toksIPreferences));

                List<Double> simIJList = getSimilaritiesOfAssignments(assignedToIJ, toksIPreferences);
                List<Double> simJIList = getSimilaritiesOfAssignments(assignedToJI, toksJPreferences);

                double simIJ = simIJList.stream().mapToDouble(Double::doubleValue).sum() / (double) simIJList.size();
                double simJI = simJIList.stream().mapToDouble(Double::doubleValue).sum() / (double) simJIList.size();

                similarities.add(simIJ);
                similarities.add(simJI);

                break;
            case BOTH_UNION:
                assignedToIJ = mapToSetPair(GaleShapleySimple.match(toksIPreferences, toksJPreferences));
                assignedToJI = mapToSetPair(GaleShapleySimple.match(toksJPreferences, toksIPreferences));

                Set<Pair<Integer, Integer>> assignedToUnion = new HashSet<>(assignedToIJ);
                assignedToUnion.addAll(assignedToJI.stream().map(x -> new Pair<>(x.getValue1(), x.getValue0())).collect(Collectors.toSet())); // Inverse

                similarities.addAll(getSimilaritiesOfAssignments(assignedToUnion, toksIPreferences));
                break;
            case BOTH_INTERSECTION:
                assignedToIJ = mapToSetPair(GaleShapleySimple.match(toksIPreferences, toksJPreferences));
                assignedToJI = mapToSetPair(GaleShapleySimple.match(toksJPreferences, toksIPreferences));

                Set<Pair<Integer, Integer>> assignedToIntersection = new HashSet<>(assignedToIJ);
                assignedToIntersection.retainAll(assignedToJI.stream().map(x -> new Pair(x.getValue1(), x.getValue0())).collect(Collectors.toSet())); // Inverse

                similarities.addAll(getSimilaritiesOfAssignments(assignedToIntersection, toksIPreferences));
                break;
            case BOTH_DIFFERENCE:
                assignedToIJ = mapToSetPair(GaleShapleySimple.match(toksIPreferences, toksJPreferences));
                assignedToJI = mapToSetPair(GaleShapleySimple.match(toksJPreferences, toksIPreferences));

                assignedToUnion = new HashSet<>(assignedToIJ);
                assignedToUnion.addAll(assignedToJI.stream().map(x -> new Pair<>(x.getValue1(), x.getValue0())).collect(Collectors.toSet())); // Inverse

                assignedToIntersection = new HashSet<>(assignedToIJ);
                assignedToIntersection.retainAll(assignedToJI.stream().map(x -> new Pair<>(x.getValue1(), x.getValue0())).collect(Collectors.toSet())); // Inverse

                assignedToUnion.removeAll(assignedToIntersection);

                similarities.addAll(getSimilaritiesOfAssignments(assignedToUnion, toksIPreferences));

                break;
        }
        Double similarity = similarities.stream().mapToDouble(Double::doubleValue).sum() / (double) similarities.size();
//        System.out.println("OVERALL("+ toksI + ", " + toksJ +"): " + similarity + "\n");
        return similarity;
    }

    @Override
    protected Double _similarity(String s1, String s2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        return similarity(s1, s2, mergingType);
    }

    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Map<String, Object> measureParameters = new HashMap<>();
        measureParameters.put("moduleType", "StableMatching");
        measureParameters.put("mergingType", BOTH_ALL_AVERAGE.toString());

        Map<String, Object> additionalParameters = new HashMap<>();
//        additionalParameters.put("mergingType", BOTH_ALL_AVERAGE.toString());

        System.out.println(measureParameters);

        StableMatching sm = new StableMatching("", Boolean.TRUE, measureParameters, new Levenshtein("token_measure", Boolean.FALSE, additionalParameters));

        String s1 = "Hello world oh hi he";
        String s2 = "Hello world hoo hie";

        System.out.println(sm.similarity(s1, s2, sm.mergingType));
    }

    public static class StableMatchingLevenshtein extends StableMatching {
        public StableMatchingLevenshtein(String id, Boolean enabledCache, Map<String, Object> parameters) {
            super(id, enabledCache, parameters, SimilarityMeasureType.StableMatchingLevenshtein, new Levenshtein("token_measure", Boolean.FALSE, Collections.emptyMap()));
        }
    }

    public static class StableMatchingDamerauLevenshtein extends StableMatching {
        public StableMatchingDamerauLevenshtein(String id, Boolean enabledCache, Map<String, Object> parameters) {
            super(id, enabledCache, parameters, SimilarityMeasureType.StableMatchingDamerauLevenshtein, new DamerauLevenshtein("token_measure", Boolean.FALSE, Collections.emptyMap()));
        }
    }

    public static class StableMatchingJaroWinkler extends StableMatching {
        public StableMatchingJaroWinkler(String id, Boolean enabledCache, Map<String, Object> parameters) {
            super(id, enabledCache, parameters, SimilarityMeasureType.StableMatchingJaroWinkler, new JaroWinkler("token_measure", Boolean.FALSE, Collections.emptyMap()));
        }
    }
}
