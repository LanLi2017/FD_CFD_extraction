package deduplication.nonduplicates.indexing.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Created by jokoum on 10/23/16.
 */
public class BiMapCustom<K, V> {
    final static Logger logger = Logger.getLogger(BiMapCustom.class);
    protected static CSVFormat csvFormat = CSVFormat.TDF.withFirstRecordAsHeader().withIgnoreEmptyLines().withDelimiter('\t');

    private Path dir;
    private Path keyValuesPath;

    protected Map<K, V> kv;
    protected Map<V, K> vk;

    protected Function<String, K> convertStringToK;
    protected Function<String, V> convertStringToV;

    public void clear() {
        kv.clear();
        vk.clear();
    }

    public void putKeyValue(K k, V v) {
        kv.put(k, v);
        vk.put(v, k);
    }



    public V getKV(K k) {
        return kv.getOrDefault(k, null);
    }

    public K getVK(V v) {
        return vk.getOrDefault(v, null);
    }

    public BiMapCustom(Path dir, Boolean purge,
                       Function<String, K> convertStringToK,
//                    Function<K, String> convertKToString,  // Obviously has to implement toString
                       Function<String, V> convertStringToV
//                    Function<V, String> convertVToString   // Obviously has to implement toString
    )
            throws IOException {
        this.dir = dir;
        this.convertStringToK = convertStringToK;
        this.convertStringToV = convertStringToV;

        keyValuesPath = dir.resolve("biMapDump.tsv");
        File keyValuesFile = new File(keyValuesPath.toString());

        kv = new HashMap<K, V>();
        vk = new HashMap<V, K>();

        File dirFile = new File(dir.toString());
        if (purge && dir != null && dirFile.isDirectory()) {
            logger.info("Deleting BiMapCustom directory: " + dir);
            FileUtils.deleteDirectory(dirFile);
        }

        /* Import */
        if (dirFile.exists() && keyValuesFile.exists()) {
            try {
                Reader csvReader = new FileReader(keyValuesFile);
                CSVParser parser = new CSVParser(csvReader, csvFormat);

                for (CSVRecord rec : parser) {
                    vk.put(convertStringToV.apply(rec.get("key")), convertStringToK.apply(rec.get("value")));
                }

                parser.close();
            } catch (IOException e) {
                logger.fatal("Did not find file at" + keyValuesFile);
                System.exit(137);
            }
        }
    }


    public void close() throws IOException {
        File dirFile = new File(dir.toString());
        if (!dirFile.isDirectory()) {
            dirFile.mkdirs();
        }

        File keyValuesFile = new File(keyValuesPath.toString());
        String[] header = {  "key","value"};
        try (CSVPrinter printer = new CSVPrinter(new FileWriter(keyValuesFile), csvFormat)) {
            printer.printRecord(header);

            for (Map.Entry<V, K> entry : vk.entrySet()) {
                printer.print(Arrays.asList(entry.getKey(),entry.getValue()));
                printer.println();
            }
        }

        kv.clear();
        kv = null;
        vk.clear();
        vk = null;
    }

    public Map<K, V> getKv() {
        return kv;
    }

    public Map<V, K> getVk() {
        return vk;
    }

//    private File getIdFromIdToFile() {
//        return new File(storageDir + File.separator + "id_to_id_from.tsv");
//    }
}
