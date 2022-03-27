package deduplication.utils;

import deduplication.preparation.utils.Preparation;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataPrepDedupController {

    protected String dataset;
    protected Set<String> attributes;
    protected List<Preparation> preparations;

    protected String dbUsername;
    protected String dbPassword;
    protected String dbSchemaname;
    protected String dbTablename;

    protected String dbDriver;

    protected String dbConnectionURL;

    protected Boolean isChainPreparators;
    protected Map<String, List<Preparation>> chainedPreparations;

    protected String xstandard;

    protected Map<String, String> parameters;

    public DataPrepDedupController(Map<String, String> m) {
        this.parameters = m;
        preparations = getPreparations(m);
        dataset = m.get("dataset");
        attributes = Stream.of(m.get("attributes").split("-")).collect(Collectors.toSet());

        dbUsername = m.get("db_username");
        dbPassword = m.get("db_password");
        dbSchemaname = m.get("db_schemaname");
        dbTablename = m.get("db_tablename");
        dbDriver = m.get("db_driver");
        dbConnectionURL = m.get("db_connection_url");
        if (dbConnectionURL.startsWith("'") && dbConnectionURL.endsWith("'")) {
            dbConnectionURL = dbConnectionURL.substring(1, dbConnectionURL.length() - 1);
        }
        xstandard = m.get("xstandard");
        isChainPreparators = Boolean.valueOf(m.get("is_chain_preparators"));

        // Chain preparations as discussed at the end of phase of the paper. This means that cleaning preparators will
        // be available for all lossy ones.
        chainedPreparations = chainPreparations(preparations, isChainPreparators);
    }

    public static List<Preparation> getPreparations(Map<String, String> m) {
        List<Preparation> preparations = new ArrayList<>();
        for (int i = 0; i < Integer.valueOf(m.get("total_preparations")); ++i) {
            String preparator = m.get("preparator_" + i);
            String attribute = m.get("attribute_" + i);
            Integer order = Integer.valueOf(m.get("order_" + i));
            Boolean deduplicationSpecific = Boolean.valueOf(m.get("deduplication_specific_" + i));

            Preparation prp = new Preparation(
                    m.get("dataset"),
                    preparator,
                    attribute,
                    order,
                    deduplicationSpecific
            );
            preparations.add(prp);
        }
        return preparations;
    }

    protected Map<String, List<Preparation>> chainPreparations(List<Preparation> preparations, Boolean isChainPreparators) {
        Map<String, List<Preparation>> chainedPreparations = new HashMap<>();
        if (isChainPreparators) {
            // Group preparations per attribute -
            // but the non-deduplication specific preparators (such as remove special characters) are part of all
            // deduplication specific preparators (such as phonetic encoding).

            // First collect the attributes
            Set<String> attributes = new HashSet<>();
            for (Preparation prp: preparations) {
                attributes.add(prp.getAttribute());
            }

            Map<String, List<Preparation>> attributeToNondeduplicationPreps = new HashMap<>();
            Map<String, List<Preparation>> attributeToDeduplicationPreps = new HashMap<>();
            for (Preparation prp: preparations) {
                if (!prp.getDeduplicationSpecific()) {
                    List<Preparation> nondeduplicationPreps = attributeToNondeduplicationPreps.getOrDefault(prp.getAttribute(), new ArrayList<>());
                    nondeduplicationPreps.add(prp);
                    attributeToNondeduplicationPreps.put(prp.getAttribute(), nondeduplicationPreps);
                } else {
                    List<Preparation> deduplicationPreps = attributeToDeduplicationPreps.getOrDefault(prp.getAttribute(), new ArrayList<>());
                    deduplicationPreps.add(prp);
                    attributeToDeduplicationPreps.put(prp.getAttribute(), deduplicationPreps);
                }
            }
            // Now generate chainedPreparations
            for (String attribute : attributes) {

                List<Preparation> chainedPreparationsForAttribute = new ArrayList<>();

                if (attributeToNondeduplicationPreps.containsKey(attribute) && attributeToNondeduplicationPreps.get(attribute).size() > 0) {
                    attributeToNondeduplicationPreps.get(attribute).sort(Comparator.comparing(Preparation::getOrder));
                    chainedPreparationsForAttribute.addAll(attributeToNondeduplicationPreps.get(attribute));
                }

                // Add once without any deduplication specific preparator.
                if (chainedPreparationsForAttribute.size() > 0) {
                    chainedPreparations.put(attribute, chainedPreparationsForAttribute);
                }

                // And now let's add chained preps where the basic (non-deduplication specific) preparators are always there.
                if (attributeToDeduplicationPreps.containsKey(attribute) && attributeToDeduplicationPreps.get(attribute).size() > 0) {
                    int counter = 0;
                    for (Preparation prp: attributeToDeduplicationPreps.get(attribute)) {
                        List<Preparation> chainedPreparationsForAttributeDeduplicationPrep = new ArrayList<>(chainedPreparationsForAttribute);
                        chainedPreparationsForAttributeDeduplicationPrep.add(prp);
                        chainedPreparations.put(attribute + "@" + ++counter, chainedPreparationsForAttributeDeduplicationPrep);
                    }
                }
            }
        } else {
            // Every preparator is applied separately
            for (Preparation prp: preparations) {
                chainedPreparations.put(
                        prp.getAttribute() + "@" + prp.getPreparator(),
                        new ArrayList<>(Arrays.asList(prp))
                );
            }
        }

        return chainedPreparations;
    }
}
