package deduplication.similarities.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import deduplication.nonduplicates.indexing.utils.BlockingKeyGenerator;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by jokoum on 12/3/16.
 */
public class ConfigurationUtils {
    protected static transient Gson gson = new GsonBuilder().enableComplexMapKeySerialization().setPrettyPrinting().create();

    /** NOT USED CURRENTLY WHILE EXPORTING JSON **/
    public static class SimilarityMeasureCfg {
        public String id = "";
        public Map<String, Object> parameters = Collections.emptyMap();

        /* String Similarity */
        public SimilarityMeasure.SimilarityMeasureType similarityMeasureType = null;
        public Boolean enabled_cache = Boolean.FALSE;

        public SimilarityMeasureCfg(String id,
                                    SimilarityMeasure.SimilarityMeasureType similarityMeasureType,
                                    Map<String, Object> parameters,
                                    Boolean enabled_cache) {

            this.id = id;
            this.parameters = parameters;

            this.similarityMeasureType = similarityMeasureType;
            this.enabled_cache = enabled_cache;
        }
    }

    public static class AttributesCombinerCfg {
        public String id = "";
        public BlockingKeyGenerator.BlockingKeyGeneratorType type = null;
        public List<String> selected_attributes = Collections.emptyList();
        public Map<String, Object> parameters = Collections.emptyMap();

        public AttributesCombinerCfg(String id, BlockingKeyGenerator.BlockingKeyGeneratorType type, List<String> selected_attributes, Map<String, Object> parameters) {
            this.id = id;
            this.type = type;
            this.selected_attributes = selected_attributes;
            this.parameters = parameters;
        }
    }
}
