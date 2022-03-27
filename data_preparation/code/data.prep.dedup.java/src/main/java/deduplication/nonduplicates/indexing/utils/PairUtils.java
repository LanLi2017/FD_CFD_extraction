package deduplication.nonduplicates.indexing.utils;

import deduplication.nonduplicates.indexing.entities.Attributes;
import deduplication.nonduplicates.indexing.entities.Record;
import deduplication.similarities.utils.SymmetricTable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class PairUtils {

    protected static Logger logger = Logger.getLogger(PairUtils.class);



    public enum NDPLGenerationStrategy {
        RANDOMLY, BLOCKING
    }

    // Step 1. Create pairs, by using the blocking technique. This way we ensure that the non-duplicates are more similar.
    protected Map<String, Record> records = null;
    protected Attributes attributes = null;
    public void setAttributes(Attributes attributes) {
        this.attributes = attributes;
    }

    public static class PairsDPLAndNDPL {
        protected PairsWithInfo pairsDPL;
        protected PairsWithInfo pairsNDPL;

        public PairsDPLAndNDPL(PairsWithInfo pairsDPL, PairsWithInfo pairsNDPL) {
            this.pairsDPL = pairsDPL;
            this.pairsNDPL = pairsNDPL;
        }

        public PairsDPLAndNDPL() {
            this.pairsDPL = new PairsWithInfo("DPL", new TreeSet<>());
            this.pairsNDPL = new PairsWithInfo("NDPL", new TreeSet<>());
        }

        public PairsWithInfo getPairsDPL() {
            return pairsDPL;
        }

        public PairsWithInfo getPairsNDPL() {
            return pairsNDPL;
        }

        public void setPairsDPL(PairsWithInfo pairsDPL) {
            this.pairsDPL = pairsDPL;
        }

        public void setPairsNDPL(PairsWithInfo pairsNDPL) {
            this.pairsNDPL = pairsNDPL;
        }
    }

    public static class PairsWithInfo {
        private String id = null;
        public Set<Pair<String, String>> pairs = null;
        private IdMapper<Pair<String, String>, String> pairToID = null;
        private Map<String, Map<String, String>> pairIDToInfo = null;
        public Set<String> infoHeld = null;
        public UnionFind<String> uf = null;
        private Integer counter = null;


        public PairsWithInfo(String id, Set<Pair<String, String>> pairs) {
            this.id = id;
            this.pairs = pairs;
            try {
                pairToID = new IdMapper<>(
                        Paths.get("/tmp/idMapperBlocking/" + id),
                        Boolean.TRUE,
                        (x -> {
                            String[] toks = x.split("_");
                            Pair<String, String> p = new Pair<>(toks[0], toks[1]);
                            return p;
                        }),
                        (x -> x)
                );
                uf = new UnionFind<>("[PairUtils - PairsWithInfo] - " + id, new File("/tmp/uf_"+id+".sqlite"), Boolean.FALSE);
            } catch (IOException e) {
                e.printStackTrace();
            }
            counter = 0;
            infoHeld = new HashSet<>();
            pairIDToInfo = new HashMap<>();
        }

        public void addPair(Pair<String, String> p) {
            if (!pairs.contains(p)) {
                pairs.add(p);
                pairToID.putIDFromTo(p, (++counter).toString());
                uf.union(p.getValue0(), p.getValue1());
            }
        }

        public void addInfoToPair(Pair<String, String> p, String key, String value) {
            String id = pairToID.getIdFromTo(p);
            Map<String, String> info = pairIDToInfo.getOrDefault(id, new HashMap<>());
            info.put(key, value);
            pairIDToInfo.put(id, info);
            infoHeld.add(key);
        }

        public Map<String, String> getInfoFromPair(Pair<String, String> p) {
            String id = pairToID.getIdFromTo(p);
            Map<String, String> info = pairIDToInfo.getOrDefault(id, new HashMap<>());
            return info;
        }

        public String getInfoFromPair(Pair<String, String> p, String key) {
            return getInfoFromPair(p).getOrDefault(key, null);
        }

        public Set<Pair<String, String>> getPairs() {
            return pairs;
        }

        public String getPairToID(Pair<String, String> p) {
            return pairToID.getIdFromTo(p);
        }

        public Pair<String, String> getIDToPair(String id) {
            return pairToID.getIdToFrom(id);
        }

        public UnionFind<String> getUf() {
            return uf;
        }

    }

    public PairUtils(Map<String, Record> records, Attributes attributes) {
        this.records = records;
        this.attributes = attributes;
    }

    public void setRecords(Map<String, Record> records) {
        this.records = records;
    }


    public static SymmetricTable<String, String> importPairs(File pairsFile, CSVFormat csvFormat, Set<String> limitToIDs, String limitRecordsUsage) {
        logger.info("Importing pairs");
        SymmetricTable<String, String> pairs = new SymmetricTable<>();
        try (CSVParser parser = new CSVParser(new FileReader(pairsFile), csvFormat)) {
            for (CSVRecord rec : parser) {
                Map<String, String> recMap = rec.toMap();
                String id1 = recMap.get("id1");
                String id2 = recMap.get("id2");

                if (limitToIDs.size() > 0) {
                    if (limitRecordsUsage.equals("or")) {
                        if (!(limitToIDs.contains(id1) || limitToIDs.contains(id2))) {
                            continue;
                        }
                    } else if (limitRecordsUsage.equals("and")) {
                        if (!(limitToIDs.contains(id1) && limitToIDs.contains(id2))) {
                            continue;
                        }
                    }
                }

                String note = recMap.getOrDefault("note", "");
                pairs.put(id1, id2, note);
            }
        } catch (java.io.IOException e) {
            System.out.println("Could maybe not findNode mappings file in: " + pairsFile);
            e.printStackTrace();
            System.exit(1);
        }
        logger.info("Imported " + pairs.cellSet().size() + " pairs");
        return pairs;
    }

    public PairUtils.PairsWithInfo importPairsAndExpand(Path pairsFile, String pairsID) {
        logger.info("Reading " + pairsID + " pairs");
        SymmetricTable<String, String> st = importPairs(new File(pairsFile.toString()), CSVFormat.TDF.withHeader(), Collections.emptySet(), "NOLIMIT");

        logger.info("Expanding " + pairsID + " pairs");
        PairUtils.PairsWithInfo pairs = expandPairsWithTransitiveClosure(pairsID, st);

        return pairs;
    }

    private PairUtils.PairsWithInfo expandPairsWithTransitiveClosure(String pairsID, SymmetricTable<String, String> st) {
        PairUtils.PairsWithInfo pairs = new PairUtils.PairsWithInfo(pairsID, new HashSet<>());

        st.cellSet().forEach(x -> pairs.getUf().union(x.getRowKey(), x.getColumnKey()));

        logger.info("Getting components from UnionFinds");
        Collection<Set<String>> componentsAll = pairs.getUf().getComponentsParallel();
        List<List<String>> components = componentsAll.parallelStream().map(ArrayList::new).collect(Collectors.toList());

        /* pairsDPL with Transitive Closure */
        logger.info("Retrieving transitive pairs from Union Find of Gold Standard, from " + components.size() + " components.");
        for (List<String> component : components) {
            for (int i = 0; i < component.size(); ++i) {
                String idI = component.get(i);
                for (int j = i+1 ; j < component.size(); ++j) {
                    String idJ = component.get(j);
                    if (idI.equals("") || idJ.equals("")) {
                        continue;
                    }
                    pairs.addPair(new Pair<>(idI, idJ));
                }
            }
        }
        logger.info("Retrieved " + pairs.pairs.size() + " " + pairsID + " pairs.");

        return pairs;
    }
}
