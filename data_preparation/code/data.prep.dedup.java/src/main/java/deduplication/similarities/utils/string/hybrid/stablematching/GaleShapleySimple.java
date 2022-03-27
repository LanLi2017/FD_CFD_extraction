package deduplication.similarities.utils.string.hybrid.stablematching;


import deduplication.similarities.utils.string.hybrid.StableMatching;
import deduplication.similarities.utils.string.hybrid.utils.Commons;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by jokoum on 11/22/16.
 *
 * Based on: https://rosettacode.org/wiki/Stable_marriage_problem#Java
 */
public class GaleShapleySimple {

    public static StableMatching.TokenPreference makeAPreference(Integer i) {
        return new StableMatching.TokenPreference(i, Commons.TokenType.TEXT, 0.0);
    }

    public static Map<Integer, Integer> match(Map<Integer, ArrayList<StableMatching.TokenPreference>> toksIPreferences,
                                             Map<Integer, ArrayList<StableMatching.TokenPreference>> toksJPreferences){

        Map<Integer, Integer> assignedTo = new TreeMap<>();
        ArrayList<Integer> freeSI = IntStream.range(0, toksIPreferences.size()).boxed().collect(Collectors.toCollection(ArrayList::new));

        while(!freeSI.isEmpty()){
            Integer i = freeSI.remove(0); //get a load of THIS tokI
            ArrayList<StableMatching.TokenPreference> thisS1Prefers = toksIPreferences.get(i);
            for(StableMatching.TokenPreference iPref : thisS1Prefers){
                Integer j  = iPref.getIndex();
                if(assignedTo.get(j) == null){//tokJ is free
                    assignedTo.put(j, i); //awww
//                    assignedTo.put(i, j); //awww
                    break;
                }else{
                    Integer ji = assignedTo.get(j);
                    ArrayList<StableMatching.TokenPreference> jPref = toksJPreferences.get(j);
//                    Integer ji = jPref.
                    if(jPref.indexOf(i) < jPref.indexOf(ji)){
                        //this tokJ prefers this tokI to the tokI it's assigned to
                        assignedTo.put(j, i);
//                        assignedTo.put(j, i);
                        freeSI.add(ji);
                        break;
                    }//else no change...keep looking for this tokI
                }
            }
        }
        return assignedTo;
    }

//    private static boolean checkMatches(Map<String, String> matches,
//                                        Map<String, List<String>> guyPrefers,
//                                        Map<String, List<String>> girlPrefers) {
//        if(!matches.keySet().containsAll(girls)){
//            return false;
//        }
//
//        if(!matches.values().containsAll(guys)){
//            return false;
//        }
//
//        Map<String, String> invertedMatches = new TreeMap<String, String>();
//        for(Map.Entry<String, String> couple:matches.entrySet()){
//            invertedMatches.put(couple.getValue(), couple.getKey());
//        }
//
//        for(Map.Entry<String, String> couple:matches.entrySet()){
//            List<String> shePrefers = girlPrefers.get(couple.getKey());
//            List<String> sheLikesBetter = new LinkedList<String>();
//            sheLikesBetter.addAll(shePrefers.subList(0, shePrefers.indexOf(couple.getValue())));
//            List<String> hePrefers = guyPrefers.get(couple.getValue());
//            List<String> heLikesBetter = new LinkedList<String>();
//            heLikesBetter.addAll(hePrefers.subList(0, hePrefers.indexOf(couple.getKey())));
//
//            for(String guy : sheLikesBetter){
//                String guysFinace = invertedMatches.get(guy);
//                List<String> thisGuyPrefers = guyPrefers.get(guy);
//                if(thisGuyPrefers.indexOf(guysFinace) >
//                        thisGuyPrefers.indexOf(couple.getKey())){
//                    System.out.printf("%s likes %s better than %s and %s"
//                                    + " likes %s better than their current partner\n",
//                            couple.getKey(), guy, couple.getValue(),
//                            guy, couple.getKey());
//                    return false;
//                }
//            }
//
//            for(String girl : heLikesBetter){
//                String girlsFinace = matches.get(girl);
//                List<String> thisGirlPrefers = girlPrefers.get(girl);
//                if(thisGirlPrefers.indexOf(girlsFinace) >
//                        thisGirlPrefers.indexOf(couple.getValue())){
//                    System.out.printf("%s likes %s better than %s and %s"
//                                    + " likes %s better than their current partner\n",
//                            couple.getValue(), girl, couple.getKey(),
//                            girl, couple.getValue());
//                    return false;
//                }
//            }
//        }
//        return true;
//    }

//    private static Map<String, String> match(List<String> guys,
//                                             Map<String, List<String>> guyPrefers,
//                                             Map<String, List<String>> girlPrefers){
//        Map<String, String> engagedTo = new TreeMap<String, String>();
//        List<String> freeGuys = new LinkedList<String>();
//        freeGuys.addAll(guys);
//        while(!freeGuys.isEmpty()){
//            String thisGuy = freeGuys.remove(0); //get a load of THIS guy
//            List<String> thisGuyPrefers = guyPrefers.get(thisGuy);
//            for(String girl:thisGuyPrefers){
//                if(engagedTo.get(girl) == null){//girl is free
//                    engagedTo.put(girl, thisGuy); //awww
//                    break;
//                }else{
//                    String otherGuy = engagedTo.get(girl);
//                    List<String> thisGirlPrefers = girlPrefers.get(girl);
//                    if(thisGirlPrefers.indexOf(thisGuy) <
//                            thisGirlPrefers.indexOf(otherGuy)){
//                        //this girl prefers this guy to the guy she's engaged to
//                        engagedTo.put(girl, thisGuy);
//                        freeGuys.addScore(otherGuy);
//                        break;
//                    }//else no change...keep looking for this guy
//                }
//            }
//        }
//        return engagedTo;
//    }
//
//    private static boolean checkMatches(List<String> guys, List<String> girls,
//                                        Map<String, String> matches, Map<String, List<String>> guyPrefers,
//                                        Map<String, List<String>> girlPrefers) {
//        if(!matches.keySet().containsAll(girls)){
//            return false;
//        }
//
//        if(!matches.values().containsAll(guys)){
//            return false;
//        }
//
//        Map<String, String> invertedMatches = new TreeMap<String, String>();
//        for(Map.Entry<String, String> couple:matches.entrySet()){
//            invertedMatches.put(couple.getValue(), couple.getKey());
//        }
//
//        for(Map.Entry<String, String> couple:matches.entrySet()){
//            List<String> shePrefers = girlPrefers.get(couple.getKey());
//            List<String> sheLikesBetter = new LinkedList<String>();
//            sheLikesBetter.addAll(shePrefers.subList(0, shePrefers.indexOf(couple.getValue())));
//            List<String> hePrefers = guyPrefers.get(couple.getValue());
//            List<String> heLikesBetter = new LinkedList<String>();
//            heLikesBetter.addAll(hePrefers.subList(0, hePrefers.indexOf(couple.getKey())));
//
//            for(String guy : sheLikesBetter){
//                String guysFinace = invertedMatches.get(guy);
//                List<String> thisGuyPrefers = guyPrefers.get(guy);
//                if(thisGuyPrefers.indexOf(guysFinace) >
//                        thisGuyPrefers.indexOf(couple.getKey())){
//                    System.out.printf("%s likes %s better than %s and %s"
//                                    + " likes %s better than their current partner\n",
//                            couple.getKey(), guy, couple.getValue(),
//                            guy, couple.getKey());
//                    return false;
//                }
//            }
//
//            for(String girl : heLikesBetter){
//                String girlsFinace = matches.get(girl);
//                List<String> thisGirlPrefers = girlPrefers.get(girl);
//                if(thisGirlPrefers.indexOf(girlsFinace) >
//                        thisGirlPrefers.indexOf(couple.getValue())){
//                    System.out.printf("%s likes %s better than %s and %s"
//                                    + " likes %s better than their current partner\n",
//                            couple.getValue(), girl, couple.getKey(),
//                            girl, couple.getValue());
//                    return false;
//                }
//            }
//        }
//        return true;
//    }
}
