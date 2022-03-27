package deduplication.preparation;

import deduplication.preparation.utils.Preparation;
import deduplication.utils.DataPrepDedupController;
import deduplication.utils.DataPrepDedupIO;
import me.tongfei.progressbar.ProgressBar;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This controller class is responsible for applying the preparations. The preparations are applied and exported
 * progressively.
 */
public class DataPreparationController extends DataPrepDedupController {
    final static Logger logger = Logger.getLogger(DataPreparationController.class);

    private Map<String, Preparators.Preparator> preparators = null;

    public static void main(String[] args) throws SQLException {
        logger.removeAllAppenders();
        logger.addAppender(new NullAppender());

        Map<String, String> m = new HashMap<>();
        if (args.length == 0) {
            System.out.println("No arguments provided. Bye!");
            System.exit(-1);
        }
        // Converting the arguments into a <key, value> HashMap.
        for(String arg: args) {
            String[] toks = arg.split("==");
            m.put(toks[0], toks[1]);
        }
        DataPreparationController dp = new DataPreparationController(m);
        dp.applyPreparationsProgressivelyExport();

    }

    public DataPreparationController(Map<String, String> m) {
        super(m);
        loadPreparators(dataset);
    }

    public void applyPreparationsProgressivelyExport() throws SQLException {
        DataPrepDedupIO dpdIO = new DataPrepDedupIO(dbDriver, dbConnectionURL, dbUsername, dbPassword);
        Map<String, Map<String, String>> R = dpdIO.importTable("select * from " + dbTablename);

        Connection  conn = dpdIO.getConn();

        Boolean containsSplitAttributeCensus = containsPreparator(preparations, "split_attribute") && dataset.equalsIgnoreCase("census");
        Boolean containsMergeAttributes = containsPreparator(preparations, "merge_attributes");

        dpdIO.createTableDataCube();

        /* If the table already existed just clear these preparations. This is mostly useful during experimentation,
          where we might try the some preparations again and again. */
        chainedPreparations.forEach((key, attributePreparations) -> {
            String attribute = attributePreparations.get(0).getAttribute();
            Set<String> attributePreparators = attributePreparations.stream().map(Preparation::getPreparator).collect(Collectors.toSet());

            try {
                dpdIO.clearTableDataCubePreparation(dataset, xstandard, attribute, attributePreparators);
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        });

        final int BATCH_SIZE = 2000; // How many records to send each time to MySQL server.
        List<String> header = dpdIO.getDataCubeHeader();
        header.remove("id");

        String query = " insert into preparation_data_cube" +
                " (" + String.join(", ", header) + ")"
                + " values (" + header.stream().map(c -> "?")
                .collect(Collectors.joining(", "))+")";
        PreparedStatement stmt = conn.prepareStatement(query);

        try (ProgressBar pb = new ProgressBar("Data preparation for " + dataset, R.size())) { // name, initial max
            R.forEach((key, r) -> {  // For every record.
                pb.step();

                String recordID = key;

                Map<String, String> splitAttributeResult = null;

                /* Preparator split attribute needs special treatment. This is the case as we have to load the split
                * attribute values before the preparations begin. In theory the record could also be cached in the
                * preparator to avoid this, but for now we keep it as it is. */
                if (containsSplitAttributeCensus) {
                    Preparators.PreparatorSplitAttribute splitAttribute = (Preparators.PreparatorSplitAttribute) preparators.get("split_attribute");
                    HashMap<String, String> rHM = (HashMap) r;
                    splitAttributeResult = splitAttribute.apply(rHM, new HashMap<>());
                }

                Map<String, String> finalSplitAttributeResult = splitAttributeResult;
                // Do not apply parallelStream. We want to preseve the given orders.
                chainedPreparations.entrySet().forEach(e -> {
                    String attributeChain = e.getKey();  // preparations for a given attribute.
                    List<Preparation> attributePreparations = e.getValue(); //list of preparations for a given attribute.
                    String attribute = attributePreparations.get(0).getAttribute();

                    // Cloning the initial values. Any changes will be stored in this record.
                    Map<String, String> rPrepared = new HashMap<>(r);

                    String v = rPrepared.get(attributePreparations.get(0).getAttribute());  // value for this attribute.
                    for (Preparation prp : attributePreparations) {
                        String vPr = v;  // previous value.
                        try {
                            switch (prp.getPreparator()) {
                                case "split_attribute":
                                    if (prp.getAttribute().equals("text")) {
//                                        v = "";
                                        v = v;
                                    } else {
                                        v = finalSplitAttributeResult.get(prp.getAttribute());
                                    }
                                    break;
                                case "normalize_address":
                                    List<String> addressValues = new ArrayList<>();
                                    for (String addressAttribute : Arrays.asList("street_address1", "city", "zip5", "state_code", "country_code")) {
                                        if (rPrepared.containsKey(addressAttribute)) {
                                            addressValues.add(rPrepared.get(addressAttribute));
                                        }
                                    }
                                    // all the available address information
                                    String addressQuery = String.join(", ", addressValues);

                                    Preparators.PreparatorNormalizeAddress prAddr = (Preparators.PreparatorNormalizeAddress) preparators.get("normalize_address");
                                    if (prAddr.isAttributeApplicable(prp.getAttribute())) {
                                        v = prAddr.apply(key, addressQuery, prp.getAttribute()).toUpperCase();
                                    }
                                    break;
                                case "geocode":
                                    addressValues = new ArrayList<>();
                                    for (String addressAttribute : Arrays.asList("street_address1", "city", "zip5", "state_code", "country_code")) {
                                        if (rPrepared.containsKey(addressAttribute)) {
                                            addressValues.add(rPrepared.get(addressAttribute));
                                        }
                                    }
                                    // all the available address information
                                    addressQuery = String.join(", ", addressValues);

                                    Preparators.PreparatorGeocode prGcd = (Preparators.PreparatorGeocode) preparators.get("geocode");
                                    if (prGcd.isAttributeApplicable(prp.getAttribute())) {
                                        v = prGcd.apply(key, addressQuery, prp.getAttribute()).toUpperCase();
                                    }
                                    break;
                                case "merge_attributes":
                                    if (prp.getAttribute().equals("merged_values")) {
                                        Preparators.PreparatorMergeAttributes mergeAttributes = (Preparators.PreparatorMergeAttributes) preparators.get("merge_attributes");
                                        HashMap<String, String> rHM = (HashMap) r;
                                        rHM.remove("id");
                                        mergeAttributes.apply(rHM, new HashMap<>());
                                        v = mergeAttributes.apply("");
                                    } else {
                                        v = rPrepared.get(prp.getAttribute());
                                    }
                                    break;
                                default:
                                    v = preparators.containsKey(prp.getPreparator()) ? preparators.get(prp.getPreparator()).apply(v) : v;
                            }
                            if (v == null && vPr != null) {
                                v = vPr;
                            }
                        } catch (Exception xc) {
                            logger.warn("Issues while applying: " + prp.getPreparator() + " on value: " + vPr);
                            v = vPr;
                        }
                        rPrepared.put(prp.getAttribute(), v);
                    }
                    int j = 0;

                    Set<String> attributePreparators = attributePreparations.stream().map(Preparation::getPreparator).collect(Collectors.toSet());
                    Map<String, Object> valueToPreparationCube = getValueToDataPreparationCube(dataset, recordID, attribute, attributePreparators, rPrepared.get(attribute));
                    try {
                        for (String h : header) {
                            stmt.setObject(++j, valueToPreparationCube.get(h));
                        }
                        stmt.addBatch();
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }

                    if (pb.getCurrent() % BATCH_SIZE == BATCH_SIZE - 1) {
                        try {
                            stmt.executeBatch();
                            conn.commit();
                        } catch (SQLException ex) {
                            ex.printStackTrace();
                        }
                    }
                });
            });
            stmt.executeBatch();
            conn.commit();
        }
        dpdIO.close();
    }

    protected Map<String, Object> getValueToDataPreparationCube(String dataset, String recordID, String attribute, Set<String> preparators, String value) {
        Map<String, Object> m = new HashMap<>();
        m.put("dataset", dataset);
        m.put("xstandard", xstandard);
        m.put("attribute", attribute);
        m.put("value", value);
        m.put("record_id", recordID);
        List<String> allPreparators = Preparators.getAllPreparatorNames();
        for (String preparator: allPreparators) {
            m.put(preparator, preparators.contains(preparator));
        }
        return m;
    }

    protected Boolean containsPreparator(List<Preparation> preparations, String preparator) {
        Boolean containsPreparator = Boolean.FALSE;
        for (Preparation prp: preparations) {
            if (prp.getPreparator().equals(preparator)) {
                containsPreparator = Boolean.TRUE;
            }
        }
        return containsPreparator;
    }

    private void loadPreparators(String dataset) {
        preparators = new HashMap<>();

        Preparators.PreparatorRemoveSpecialCharacters removeSpecialCharacter = new Preparators.PreparatorRemoveSpecialCharacters();
        preparators.put(removeSpecialCharacter.getName(), removeSpecialCharacter);
        if (dataset.equalsIgnoreCase("hotels")) {
            Preparators.PreparatorNormalizeAddress normalizeAddress = new Preparators.PreparatorNormalizeAddress(true);
            preparators.put(normalizeAddress.getName(), normalizeAddress);
            Preparators.PreparatorGeocode geocode = new Preparators.PreparatorGeocode();
            preparators.put(geocode.getName(), geocode);
        }
        Preparators.PreparatorSplitAttribute splitAttribute = new Preparators.PreparatorSplitAttribute();
        preparators.put(splitAttribute.getName(), splitAttribute);
        Preparators.PreparatorPhoneticEncode phoneticEncode = new Preparators.PreparatorPhoneticEncode();
        preparators.put(phoneticEncode.getName(), phoneticEncode);
        Preparators.PreparatorCapitalize capitalize = new Preparators.PreparatorCapitalize();
        preparators.put(capitalize.getName(), capitalize);
        Preparators.PreparatorStem stem = new Preparators.PreparatorStem();
        preparators.put(stem.getName(), stem);
        Preparators.PreparatorTransliterate transliterate = new Preparators.PreparatorTransliterate();
        preparators.put(transliterate.getName(), transliterate);
        Preparators.PreparatorSyllabify syllabify = new Preparators.PreparatorSyllabify();
        preparators.put(syllabify.getName(), syllabify);
        Preparators.PreparatorMergeAttributes mergeAttributes = new Preparators.PreparatorMergeAttributes();
        preparators.put(mergeAttributes.getName(), mergeAttributes);
        Preparators.PreparatorAcronymize acronymize = new Preparators.PreparatorAcronymize();
        preparators.put(acronymize.getName(), acronymize);
    }

}

