package deduplication.preparation.addressnormalization;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.util.*;

/**
 * Created by jokoum on 2/23/17.
 */
public class NominatimResponse {
    protected static Gson gson = new GsonBuilder().enableComplexMapKeySerialization().setPrettyPrinting().create();

    String jsonStr = null;

    String place_id = null;
    String licence = null;
    String osm_type = null;
    String osm_id = null;
    List<Double> boundingbox = null;
    String lat = null;
    String lon = null;
    String display_name = null;

    @SerializedName("class")
    String osm_class = null;

    String type = null;
    Double importance = null;
    String icon = null;

    AddressResponse address = null;

    public NominatimResponse(String jsonStr) {
        this.jsonStr = jsonStr;

        Map<String, String> m = gson.fromJson(jsonStr, Map.class);
        this.lat = m.get("lat");
        this.lon = m.get("lon");
    }

//    private Map<String, String> extractPartsFromGeocoding(String geocodingJson) {
//        Map<String, String> geocodingParts = new HashMap<>();
//
//        if (geocodingJson.equals("")) {
//            return geocodingParts;
//        }
//
//        String sanitizedGeocodingJsonString = geocodingJson.replaceAll("'", "\"");
//
//        JSONObject geocoding = new JSONObject(sanitizedGeocodingJsonString);
//
//        geocodingParts.put("lat", geocoding.getString("lat")); // XXX: Remove any specific attribute access, even lat, lon etc.
//        geocodingParts.put("lon", geocoding.getString("lon"));
//
//        return geocodingParts;
//    }

//    public static NominatimResponse valueOf(String jsonStr) {
//        return gson.fromJson(jsonStr, NominatimResponse.class);
//    }
    public static List<NominatimResponse> valueOf(String jsonStr) {
        String escapedStr = escapeChars(jsonStr);
    //        String escapedStr = StringEscapeUtils.escapeJava(jsonStr);
    //        String unescapedStr = StringEscapeUtils.unescapeJava(escapedStr);
        Object[] nrsStr = null;
        try {
            nrsStr = gson.fromJson(escapedStr, Object[].class);
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
        List<NominatimResponse> nrsList = new ArrayList<>();
//        NominatimResponse[] nrs = gson.fromJson(escapedStr, NominatimResponse[].class);
        if (nrsStr != null && nrsStr.length > 0) {
            for (int i = 0 ; i <nrsStr.length; ++i) {
//                nrsList.addScore(new NominatimResponse(gson.toJson(nrsStr[i])));
                String rspStr = gson.toJson(nrsStr[i]);
                NominatimResponse rsp = gson.fromJson(rspStr, NominatimResponse.class);
                rsp.setJsonStr(rspStr);
                nrsList.add(rsp);
//                locationSanitizer.sanitizeNominatimResponse(nrs[i]);
            }

        }
        return nrsList;
    }

    /* http://stackoverflow.com/questions/6230190/convert-international-string-to-u-codes-in-java */
    private static String escapeChars(String str) {
        StringBuilder b = new StringBuilder();
        for (char c : str.toCharArray()) {
            if (c >= 128)
                b.append("\\u").append(String.format("%04X", (int) c));
            else
                b.append(c);
        }
        return b.toString();
    }

    public String toString() {
        return gson.toJson(toMap());
    }

    public Map<String, Object> mapFromString(String mapGsonStr) {
        return gson.fromJson(mapGsonStr, Map.class);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new HashMap<>();

        m.put("jsonStr", jsonStr);
        m.put("place_id", place_id);
        m.put("licence", licence);
        m.put("osm_type", osm_type);
        m.put("osm_id", osm_id);
        m.put("boundingbox", boundingbox);
        m.put("lat", lat);
        m.put("lon", lon);
        m.put("display_name", display_name);
        m.put("osm_class", osm_class);
        m.put("type", type);
        m.put("importance", importance);
        m.put("address", address);

        return m;
    }

    public static class AddressResponse {
        String house_number;
        String road;
        String neighbourhood;
        String suburb;
        String city_district;
        String city;
        String state;
        String postcode;
        String county;

        public String getCounty() {
            return county;
        }

        public void setCounty(String county) {
            this.county = county;
        }

        String country;
        String country_code;

        public String getHouse_number() {
            return house_number;
        }

        public void setHouse_number(String house_number) {
            this.house_number = house_number;
        }

        public String getNeighbourhood() {
            return neighbourhood;
        }

        public void setNeighbourhood(String neighbourhood) {
            this.neighbourhood = neighbourhood;
        }

        public String getRoad() {
            return road;
        }

        public void setRoad(String road) {
            this.road = road;
        }

        public String getSuburb() {
            return suburb;
        }

        public void setSuburb(String suburb) {
            this.suburb = suburb;
        }

        public String getCity_district() {
            return city_district;
        }

        public void setCity_district(String city_district) {
            this.city_district = city_district;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public String getPostcode() {
            return postcode;
        }

        public void setPostcode(String postcode) {
            this.postcode = postcode;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getCountry_code() {
            return country_code;
        }

        public void setCountry_code(String country_code) {
            this.country_code = country_code;
        }
    }

    public String getIcon() {
        return icon;
    }

    public void setIcon(String icon) {
        this.icon = icon;
    }

    public String getPlace_id() {
        return place_id;
    }

    public void setPlace_id(String place_id) {
        this.place_id = place_id;
    }

    public String getLicence() {
        return licence;
    }

    public void setLicence(String licence) {
        this.licence = licence;
    }

    public String getOsm_type() {
        return osm_type;
    }

    public void setOsm_type(String osm_type) {
        this.osm_type = osm_type;
    }

    public String getOsm_id() {
        return osm_id;
    }

    public void setOsm_id(String osm_id) {
        this.osm_id = osm_id;
    }

    public List<Double> getBoundingbox() {
        return boundingbox;
    }

    public void setBoundingbox(List<Double> boundingbox) {
        this.boundingbox = boundingbox;
    }

    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public String getLon() {
        return lon;
    }

    public void setLon(String lon) {
        this.lon = lon;
    }

    public String getDisplay_name() {
        return display_name;
    }

    public void setDisplay_name(String display_name) {
        this.display_name = display_name;
    }

    public String getOsm_class() {
        return osm_class;
    }

    public void setOsm_class(String osm_class) {
        this.osm_class = osm_class;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Double getImportance() {
        return importance;
    }

    public void setImportance(Double importance) {
        this.importance = importance;
    }

    public AddressResponse getAddress() {
        return address;
    }

    public void setAddress(AddressResponse address) {
        this.address = address;
    }

    public String getJsonStr() {
        return jsonStr;
    }

    public void setJsonStr(String jsonStr) {
        this.jsonStr = jsonStr;
    }

}
