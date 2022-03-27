package deduplication.nonduplicates.indexing.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import deduplication.nonduplicates.indexing.entities.Attributes;
import deduplication.similarities.utils.ConfigurationUtils;
import deduplication.similarities.utils.SimilarityMeasure;
import deduplication.similarities.utils.StringSimilarityMeasure;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.*;

public class DomainUtils {
    protected String domain = null;

    protected Attributes attributes = null;

    protected ConfigurationUtils.SimilarityMeasureCfg defaultDomainSimilarity = null;
    protected Map<String, BlockingKeyGenerator> attributeCombiners = null;

    protected Config config;

    public DomainUtils(String domain, ConfigurationUtils.SimilarityMeasureCfg defaultDomainSimilarity, Config config) {
        this.domain = domain;
        this.defaultDomainSimilarity = defaultDomainSimilarity;
        if (defaultDomainSimilarity == null) {
            this.defaultDomainSimilarity = new ConfigurationUtils.SimilarityMeasureCfg(
                    "", SimilarityMeasure.SimilarityMeasureType.MongeElkanLevenshtein, new HashMap<>(), Boolean.FALSE
            );
        }
        attributeCombiners = new HashMap<>();
        this.config = config;

        setBlockingAttributeCombiner();

        attributes = new Attributes(Paths.get(config.getString("base_dir") + "/" + domain + ".tsv"));
    }

    public void setBlockingAttributeCombiner() {
        Config datasetConfig = config.getConfig("datasets").getConfig(domain);
        for (Config attr : datasetConfig.getConfigList("attributes")) {
            String attribute = attr.getString("attribute");
            if (attr.hasPath("blocking")) {
                if (attr.hasPath("combiner")) {
                    Map<String, Object> parameters = new HashMap<>();

                    for (Map.Entry<String, ConfigValue> e : attr.entrySet()) {
                        parameters.put(e.getKey(), e.getValue().unwrapped());
                    }
                    BlockingKeyGenerator bkg = new BlockingKeyGenerator(attribute + "_" + attr.getString("combiner"),
                            Arrays.asList(attribute),
                            BlockingKeyGenerator.BlockingKeyGeneratorType.valueOf(attr.getString("combiner").toUpperCase()),
                            parameters
                    );
                    attributeCombiners.put(
                            bkg.id,
                            bkg
                    );
                } else {
                    attributeCombiners.put(
                            attribute,
                            new BlockingKeyGenerator(
                                    attribute,
                                    Arrays.asList(attribute),
                                    BlockingKeyGenerator.BlockingKeyGeneratorType.CONCATENATE,
                                    Collections.emptyMap()
                            )
                    );
                }
            }
        }
    }

    public String getDatasetFilename() {
        return domain + ".tsv";
    }

    public String getGoldstandardFilename() {
        return domain + "_DPL.tsv";
    }


    public Map<String, StringSimilarityMeasure> getAttributeSimilarityMap() {
        List<String> attributesToBeCompared = new ArrayList<>();
        Config datasetConfig = config.getConfig("datasets").getConfig(domain);
        for (Config attr : datasetConfig.getConfigList("attributes")) {
            String attribute = attr.getString("attribute");
            attributesToBeCompared.add(attribute);
        }
        return getAttributeSimilarityMap(attributesToBeCompared);
    }

    protected Map<String, StringSimilarityMeasure> getAttributeSimilarityMap(List<String> attributesToBeCompared) {
        Map<String, StringSimilarityMeasure> attributeSimilarityMap = new HashMap<>();

        for (String attr: attributesToBeCompared) {
            ConfigurationUtils.SimilarityMeasureCfg simCfg = new ConfigurationUtils.SimilarityMeasureCfg(
                    attr, defaultDomainSimilarity.similarityMeasureType,
                    defaultDomainSimilarity.parameters, defaultDomainSimilarity.enabled_cache);

            StringSimilarityMeasure attrSim = null;
            try {
                attrSim = (StringSimilarityMeasure) SimilarityMeasure.valueOf(simCfg);
            } catch (NoSuchMethodException | IllegalAccessException | ClassNotFoundException | InstantiationException | InvocationTargetException e) {
                e.printStackTrace();
            }
            attributeSimilarityMap.put(attr, attrSim);
        }

        return attributeSimilarityMap;
    }

    public String getDatasetRelationname() {
        return domain;
    }

    public Attributes getAttributes() {
        return attributes;
    }

    public String getDomain() {
        return domain;
    }

    public Map<String, BlockingKeyGenerator> getAttributeCombiners() {
        return attributeCombiners;
    }
}
