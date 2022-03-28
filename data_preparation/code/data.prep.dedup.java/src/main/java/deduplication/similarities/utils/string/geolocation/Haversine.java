package deduplication.similarities.utils.string.geolocation;

import deduplication.similarities.utils.StringSimilarityMeasure;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;

import static java.lang.Boolean.FALSE;

/**
 *
 * Based on https://github.com/jasonwinn/haversine/blob/master/Haversine.java
 *
 * Jason Winn
 * http://jasonwinn.org
 * Created July 10, 2013
 *
 * Description: Small class that provides approximate distance between
 * two points using the Haversine formula.
 *
 * Call in a static context:
 * Haversine.distance(47.6788206, -122.3271205,
 *                    47.6788206, -122.5271205)
 * --> 14.973190481586224 [km]
 *
 */
public class Haversine extends StringSimilarityMeasure implements Serializable {

    /* Based on: https://github.com/jasonwinn/haversine/blob/master/Haversine.java */

    private static final int EARTH_RADIUS = 6371; // Approx Earth radius in KM
    private Double MAX_DISTANCE_RADIUS = 3.31186186517602; // Empirically chosen for the dataset Hotels.
    private Boolean IN_DISTANCE = Boolean.FALSE;

    public Haversine(String id, Boolean enabledCache, Map<String, Object> parameters) {
        super(id, enabledCache, FALSE, SimilarityMeasureType.Haversine, parameters);
        if (parameters.containsKey("IN_DISTANCE")) {
            IN_DISTANCE = Boolean.TRUE;
        }
        if (parameters.containsKey("MAX_DISTANCE_RADIUS")) {
            MAX_DISTANCE_RADIUS = (Double) parameters.get("MAX_DISTANCE_RADIUS");
        }
    }

    /* Returns distance in KiloMetres */
    public static double distance(double startLat, double startLong,
                                  double endLat, double endLong) {

        double dLat  = Math.toRadians((endLat - startLat));
        double dLong = Math.toRadians((endLong - startLong));

        startLat = Math.toRadians(startLat);
        endLat   = Math.toRadians(endLat);

        double a = haversin(dLat) + Math.cos(startLat) * Math.cos(endLat) * haversin(dLong);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return EARTH_RADIUS * c; // <-- d
    }

    public static double haversin(double val) {
        return Math.pow(Math.sin(val / 2), 2);
    }

    @Override
    protected Double _similarity(String s1, String s2) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        if ((s1.equals("") && s2.equals("")) || (s1.equals("_") && s2.equals("_"))) {
            return 0.0;
//            return 1.0;
        }
        String[] toks1 = s1.split("_");
        String[] toks2 = s2.split("_");

        double lat1 = Double.valueOf(toks1[0]);
        double long1 = Double.valueOf(toks1[1]);

        double lat2 = Double.valueOf(toks2[0]);
        double long2 = Double.valueOf(toks2[1]);

        Double dst = distance(lat1, long1, lat2, long2);
        if (dst == 0.0) {
            return 1.0;
        } else {
            Integer distanceInMeters = (int)(dst * 1000);
            return 1.0/Math.pow(distanceInMeters, 2.0);
        }

//        if (IN_DISTANCE) {
//            return dst;
//        } else {
//            if (dst > MAX_DISTANCE_RADIUS) {
//                return 0.0;
//            } else {
//    //            return 1.0 - (dst / MAX_DISTANCE_RADIUS);
//                return Math.pow((1.0 - (dst / MAX_DISTANCE_RADIUS)), 2.0);
//            }
//        }
//        return dst;
    }

    public static void main(String[] args) {
        Haversine h = new Haversine("hvrs", FALSE, Collections.emptyMap());

        String s1 = "47.6788206_-122.4271205";
        String s2 = "47.6788206_-122.5271205";

        try {
            System.out.println(h.similarity(s1, s2));
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}
