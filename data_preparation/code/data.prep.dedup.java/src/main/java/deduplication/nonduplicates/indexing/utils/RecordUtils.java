package deduplication.nonduplicates.indexing.utils;

import deduplication.nonduplicates.indexing.entities.Record;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

public class RecordUtils {

    final static Logger logger = Logger.getLogger(RecordUtils.class);

    public Map<String, Record> importRecords(Path dataset, Set<String> attributesNeeded, String relationName) throws SQLException,
            IOException, ClassNotFoundException {

        Map<String, Record> id2rec = new HashMap<>();

        String extension = FilenameUtils.getExtension(dataset.toString());

        if (extension.equals("sqlite")) {
            logger.info("Loading SQLite file: " + dataset);
            JDBCWrapper jdbcWrp = new JDBCWrapper();
            Connection c = jdbcWrp.initiateConnection(dataset);
            c.setAutoCommit(false);
            id2rec = fetchAllRecords(null, relationName, attributesNeeded, c);
            logger.info("Loaded SQLite file: " + dataset);
        } else {
            logger.info("Loading CSV/TSV file: " + dataset);
            InputStreamReader inStream = new InputStreamReader(new FileInputStream(dataset.toFile()), "UTF-8");
//            try (CSVParser parser = new CSVParser(inStream, CSVFormat.TDF.withHeader().withQuote(null))) {
            try (CSVParser parser = new CSVParser(inStream, CSVFormat.TDF.withHeader().withQuote(null))) {
                int counter = 0;
                for (CSVRecord rec : parser) {
                    if (++counter % 1000 == 0) {
                        logger.info("Loaded " + counter + " records.");
                    }
                    Map<String, String> rMap = rec.toMap();

                    Record r = new Record(rMap);

                    id2rec.put(r.getID(), r);
                }
            } catch (java.io.IOException e) {
                logger.fatal("Could maybe not findNode mappings file in: " + dataset);
                e.printStackTrace();
                System.exit(1);
            }
            logger.info("Loaded CSV/TSV file: " + dataset);
        }

        return id2rec;
    }

    private Map<String, Record> fetchAllRecords(Collection<String> recordIDs, String relationName, Set<String> attributesNeeded, Connection c) throws SQLException {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT * FROM " + relationName);
        if (recordIDs != null) {
            sb.append(" WHERE name IN (" +
                    recordIDs.parallelStream()
                            .map(x -> "\"" + x + "\"")
                            .collect(Collectors.joining(", "))
                    + ")");
        }
        sb.append(";");

        Statement stmt = c.createStatement();
        ResultSet rs = stmt.executeQuery(sb.toString());

        Map<String, Record> result = new HashMap<>();

        while (rs.next()) {
            Record r = new Record();
            Set<String> attributesOfRec = new HashSet<>();
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i) {
                attributesOfRec.add(rs.getMetaData().getColumnName(i));
            }
            attributesOfRec.retainAll(attributesNeeded);
            for (String attrName : attributesOfRec) {
                r.setAttribute(attrName, rs.getString(attrName));
            }

            String id = rs.getString("id");
            r.setAttribute("id", id);

            result.put(id, r);
        }

        return result;
    }
}
