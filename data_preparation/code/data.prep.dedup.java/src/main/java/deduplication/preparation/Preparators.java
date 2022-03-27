package deduplication.preparation;

//import ch.sbs.jhyphen.Hyphenator;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import de.mfietz.jhyphenator.HyphenationPattern;
import de.mfietz.jhyphenator.Hyphenator;
import deduplication.nonduplicates.indexing.entities.Record;
import deduplication.nonduplicates.indexing.utils.RecordUtils;
import deduplication.preparation.addressnormalization.NominatimResponse;
import deduplication.preparation.addressnormalization.NominatimSanitizer;
import deduplication.similarities.utils.string.hybrid.MongeElkanSymmetric;
import net.gcardone.junidecode.Junidecode;
import opennlp.tools.stemmer.PorterStemmer;
import org.apache.commons.codec.language.Metaphone;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

//import gcardone.junidecode.Junidecode;

/**
 * Includes all preparators. Every preparator extends the class "Preparator".
 *
 * @author Ioannis Koumarelas
 */
public class Preparators {

    /**
     * A generic class that all preparators have to extend. It provides two methods that need to be overriden by
     * descendant classes. However, some preparators need special treatment and do not conform to this interface
     * completely.
     */
    public static abstract class Preparator {
        protected String name;

        public Preparator(String name) {
            this.name = name;
        }

        /**
         * Applies the corresponding preparator on the record. This version is used when more attributes are needed
         * for the application of the preparator.
         *
         * @param r: Record in a HashMap version.
         * @param params: Any further parameters needed for the application of this preparator on this record.
         * @return the changed record, also in a HashMap version.
         */
        public HashMap<String, String> apply(HashMap<String, String> r, HashMap<String, String> params) {
            return null;
        }

        /**
         * Similar to the previous method, but more straightforward. Given a String value returns the prepared String
         * value.
         *
         * @param v: The input String value.
         * @return the prepared String value.
         */
        public String apply(String v) {
            return null;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * @return The String names of all preparators.
     */
    public static List<String> getAllPreparatorNames() {
        Config config = ConfigFactory.load();
        List<String> allPreparators = new ArrayList<>(config.getStringList("all_preparators"));
        Collections.sort(allPreparators);
        return allPreparators;
    }

    public static class PreparatorRemoveSpecialCharacters extends Preparator {
        /**
         * This list could be extended, but represents the most common special characters. Turning this around and
         * keeping just the regex [a-zA-Z0-9] is not possible as we are in UTF-8 and non-ASCII characters are also
         * included.
         */

        protected static Pattern specialCharacters = Pattern.compile ("[,.:;\"^'/\\\\!@#$%&*()_+=|<>?{}\\[\\]~-]");

        public PreparatorRemoveSpecialCharacters() {
            super("remove_special_characters");
        }

        public String apply(String v) {
            return v.replaceAll(specialCharacters.pattern(), "");
        }
    }

    /**
     * At this point this preparator is mostly specialized for the Hotels dataset, which was the most interesting for
     * our experiments. However, since Nominatim execution is possible within this preparator, it could be used also for
     * other datasets.
     */
    public static class PreparatorNormalizeAddress extends Preparator {

        protected Set<String> addressAttributes = new HashSet<>(Arrays.asList(
                "street_address1", "city", "zip", "state_code", "state_name", "country_code", "country_name"
        ));

        protected Map<String, Record> preNormalizedRecords = null;
        protected Map<String, Record> cachedRecords = null;

        NominatimSanitizer nominatim;

        public PreparatorNormalizeAddress(Boolean cached) {
            this("normalize_address", cached);
        }

        public PreparatorNormalizeAddress(String id, Boolean cached) {
            super(id);
            try {
                if (cached) {
                    importNormalizedDatasetHotels();
                }
                nominatim = new NominatimSanitizer(
                        new MongeElkanSymmetric.MongeElkanSymmetricLevenshtein("mes-lev", Boolean.TRUE)
                );
                cachedRecords = new HashMap<>();
            } catch (SQLException | IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        /**
         * To reduce the execution time we have prenormalized all addresses for the Hotels dataset.
         *
         * @throws SQLException
         * @throws IOException
         * @throws ClassNotFoundException
         */
        private void importNormalizedDatasetHotels() throws SQLException, IOException, ClassNotFoundException {
            Config conf = ConfigFactory.load();
            String normalizedFilePath = conf.getString("data_base_dir") + "hotels_nominatim_addressnormalized.sqlite";

            RecordUtils ru = new RecordUtils();
            preNormalizedRecords = ru.importRecords(Paths.get(normalizedFilePath),
                    new TreeSet<>(Arrays.asList("id", "city", "country_code", "country_name", "county_name", "hotel_name", "id",
                            "latitude", "longitude", "latitude_longitude", "state_code", "state_name", "street_address1",
                            "zip", "zip5", "geocoded"))
                    , "hotels");
        }

        public String apply(String id, String address, String attribute) {
            String v = null;
            if (preNormalizedRecords != null) {
                /**
                 * In case prenormalizedRecords are available (currently only for Hotels), we retrieve the attribute for
                 * which we ask for the normalized version.
                 */
                String geocodedJSON = preNormalizedRecords.get(id).getAttribute("geocoded");
                JsonElement root = new JsonParser().parse(geocodedJSON);
                v = preNormalizedRecords.get(id).getAttribute(attribute);
                if (root.getAsJsonObject().has(attribute)) {
                    v = root.getAsJsonObject().get(attribute).getAsString();
                }
            } else {
                /**
                 * In case of a new dataset, where prenormalized records are not available, Nominatim is executed and
                 * the normalized version is returned and saved in the cachedRecords.
                 */
                if (!cachedRecords.containsKey(id)) {
                    Record r = new Record();
                    try {
                        NominatimResponse nr = nominatim.normalizeAddress(address, null);
                        r.setAttribute("street_address1", nr.getAddress().getRoad() + " " + nr.getAddress().getHouse_number());
                        r.setAttribute("city", nr.getAddress().getCity());
                        r.setAttribute("zip5", nr.getAddress().getPostcode());
                        r.setAttribute("country_code", nr.getAddress().getCountry_code());
                        r.setAttribute("country_name", nr.getAddress().getCountry());
//                        r.setAttribute("state_code", nr.getAddress().getState());
                        r.setAttribute("latitude_longitude", nr.getLat() + "_" + nr.getLon());
                        r.setAttribute("state_name", nr.getAddress().getState());
                        cachedRecords.put(id, r);
                    } catch (IOException | URISyntaxException | ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                Record r = cachedRecords.get(id);
                if (r.containsAttribute(attribute)) {
                    v = r.getAttribute(attribute);
                }
            }

            return v;
        }

        public Boolean isAttributeApplicable(String attribute) {
            return addressAttributes.contains(attribute);
        }
    }

    /**
     * Essentially it returns the normalized geolocation using the PreparatorNormalizeAddress implementation.
     */
    public static class PreparatorGeocode extends PreparatorNormalizeAddress {
        public PreparatorGeocode () {
            super("geocode", true);
            this.addressAttributes = new HashSet<>(Arrays.asList("latitude_longitude"));
        }
    }

    /**
     * This preparator is currently implemented specifically for the dataset Census. It splits the attribute given
     * predefined ranges of the included attributes.
     */
    public static class PreparatorSplitAttribute extends Preparator {

        protected Map<Integer, String> fieldBorderToAttribute = new HashMap<>();
        protected List<Integer> fieldBorders = new ArrayList<>();

        public PreparatorSplitAttribute () {
            super("split_attribute");
            fieldBorderToAttribute.put(0, "last_name");
            fieldBorderToAttribute.put(15, "first_name");
            fieldBorderToAttribute.put(28, "middle_name");
            fieldBorderToAttribute.put(30, "zip_code");
            fieldBorderToAttribute.put(43, "street_address");
            fieldBorders.addAll(fieldBorderToAttribute.keySet());
            fieldBorders.add(64);
            Collections.sort(fieldBorders);
        }

        @Override
        public HashMap<String, String> apply(HashMap<String, String> r, HashMap<String, String> params) {
            // Standardizing text. Adding extra white space if it is lesser.
            String text = " " + r.get("text");
            Integer extraWhiteSpace = (64 - text.length());
            String whiteSpaces = String.format("%"+extraWhiteSpace+"s", "");
            text += whiteSpaces;

            // Adding every attribute, using its borders, to the HashMap.
            HashMap<String, String> m = new HashMap<>();
            for (int i = 0 ; i < fieldBorders.size() - 1; ++i) {
                int start = fieldBorders.get(i);
                int end = fieldBorders.get(i + 1);

                String v = text.substring(start, end);
                String vCleaned = cleanStringToken(v);
                m.put(fieldBorderToAttribute.get(fieldBorders.get(i)), vCleaned);
            }
            return m;
        }

        /**
         * Removing any carriage return and newline characters.
         *
         * @param s: the string to remove \r and \n characters.
         * @return
         */
        private String cleanStringToken(String s) {
            String v = s;
            v = v.replaceAll("\r\n", "\n");
            v = v.replaceAll("\r", "\n");
            v = v.replaceAll("\n", "");
            v = v.replaceAll("\"", "");
            v = v.trim().replaceAll(" +", " ").trim();
            return v;
        }
    }

    /**
     * Merging all alphanumeric values into a single one.
     */
    public static class PreparatorMergeAttributes extends Preparator {
        protected String mergedValue = null;

        public PreparatorMergeAttributes() {
            super("merge_attributes");
        }

        @Override
        public HashMap<String, String> apply(HashMap<String, String> r, HashMap<String, String> params) {
            List<String> l = new ArrayList<>();
            for (String attr: new TreeSet<>(r.keySet())) {
                l.add(r.get(attr));
            }
            mergedValue = String.join(" ", l);
            return r;
        }

        @Override
        public String apply(String v) {
            return mergedValue;
        }
    }


    /**
     * Applies phonetic encoding to a given alphanumeric value.
     */
    public static class PreparatorPhoneticEncode extends Preparator {
        Metaphone pe = null; // We selected Metaphone for our phonetic encoding.
        public PreparatorPhoneticEncode() {
            super("phonetic_encode");
            pe = new Metaphone();
            pe.setMaxCodeLen(6);
        }

        public String apply(String v) {
            return pe.encode(v);
        }
    }

    /**
     * Implements capitalization by lower-casing alphanumerics.
     */
    public static class PreparatorCapitalize extends Preparator {

        public PreparatorCapitalize() {
            super("capitalize");
        }

        public String apply(String v) {
            return v.toLowerCase();
        }
    }

    /**
     * Applies stemming to alphanumerics. It uses the Porter stemmer, which uses English rules to apply the stemming.
     */
    public static class PreparatorStem extends Preparator {

        PorterStemmer stemmer = new PorterStemmer();

        public PreparatorStem() {
            super("stem");
        }

        public synchronized String apply(String v) {
            StringBuilder sb = new StringBuilder();
            String[] toks = v.split(" ");
            stemmer.reset();
            for (String tok: toks) {
                Boolean isLowerCase = tok.equals(tok.toLowerCase()) ? Boolean.TRUE : Boolean.FALSE;
                String tokStemmed = stemmer.stem(isLowerCase ? tok.toLowerCase() : tok );
                sb.append((isLowerCase ? tokStemmed.toLowerCase() : tokStemmed) + " ");
            }
            return sb.toString().trim();
        }
    }

    /** Applies transliteration.
     *
     */
    public static class PreparatorTransliterate extends Preparator {

        public PreparatorTransliterate() {
            super("transliterate");
        }

        public synchronized String apply(String v) {
            return Junidecode.unidecode(v);
        }
    }

    /**
     * Applies the syllabify preparator, based on Frank Liang's algorithm.
     *
     * Using: https://github.com/mfietz/JHyphenator
     * Alternative solution using: https://nedbatchelder.com/code/modules/hyphenate.py (Python script)
     */
    public static class PreparatorSyllabify extends Preparator {
        // We apply American English hyphenation rules.
        HyphenationPattern us = HyphenationPattern.lookup("en_us");
        Hyphenator h = Hyphenator.getInstance(us);

        public PreparatorSyllabify() {
            super("syllabify");
        }

        public synchronized String apply(String v) {
            return String.join(" ", h.hyphenate(v))
                    .trim().replaceAll(" +", " "); // Replaces 2 or more spaces with 1.
        }
    }

    /**
     * Acronymizes alphanumerics. This is done by keeping the first character out of every token.
     */
    public static class PreparatorAcronymize extends Preparator {

        public PreparatorAcronymize() {
            super("acronymize");
        }

        public synchronized String apply(String v) {
            if (v.trim().equals("")) {
                return "";
            }
            StringBuilder sb = new StringBuilder();

            for (String tok: v.split(" ")) { // Split by white space.
                if (tok.trim().equals("")) {
                    continue;
                }
                sb.append(tok.charAt(0));
            }

            return sb.toString().toUpperCase();
        }
    }
}
