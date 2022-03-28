package deduplication.nonduplicates.indexing.utils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;

/**
 * Created by jokoum on 10/23/16.
 */
public class IdMapper<K, V> extends BiMapCustom<K,V> {

    public IdMapper(Path dir, Boolean purge, Function<String, K> convertStringToK, Function<String, V> convertStringToV) throws IOException {
        super(dir, purge, convertStringToK, convertStringToV);
    }

    public V getIdFromTo(K from) {
        return kv.getOrDefault(from, null);
    }

    public K getIdToFrom(V to) {
        return vk.getOrDefault(to, null);
    }

    public void putIDFromTo(K from, V to) {
        kv.put(from, to);
        vk.put(to, from);
    }

    public Map<K, V> getIdFromTo() {
        return kv;
    }

    public Map<V, K> getIdToFrom() {
        return vk;
    }
}
