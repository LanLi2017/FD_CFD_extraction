package deduplication.nonduplicates.indexing.entities;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import deduplication.nonduplicates.indexing.utils.JDBCWrapper;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by koumarelas on 9/9/16.
 */
public class Attributes implements Serializable {
    protected Logger logger = Logger.getLogger(Attributes.class);
    protected static Gson gson = new GsonBuilder().enableComplexMapKeySerialization().setPrettyPrinting().create();

    Path datasetFile;

    Set<String> attributes;
    String relationName;

    public Attributes(Set<String> attributes, String relationName) {
        this.datasetFile = null;
        this.relationName = relationName;
        this.attributes = attributes;
    }

    public Attributes(Path datasetFile) {
        this(datasetFile, FilenameUtils.getBaseName(datasetFile.toString()));
    }

    public Attributes(Path datasetFile, String relationName) {
        this.datasetFile = datasetFile;
        this.relationName = relationName;
        attributes = new TreeSet<>();

        try {
            importAttributes(datasetFile, relationName);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    private void importAttributes(Path datasetFile, String relationName) throws ClassNotFoundException, SQLException {
        Class.forName("org.sqlite.JDBC");
        JDBCWrapper jdbcwrp = new JDBCWrapper();
        Connection c = jdbcwrp.initiateConnection(datasetFile);
        this.attributes = new TreeSet<>(jdbcwrp.getDatabaseAttributeNames(c, relationName, relationName));
    }

    public static void main(String[] args) {
        Path datasetFile = Paths.get("/data/projects/datamatching/data/cd/datasets/sources/cd_v2.tsv");
        String relationName = "cd_v2.";
        Attributes attributes = new Attributes(datasetFile, relationName);
        try {
            attributes.importAttributes(datasetFile, relationName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(attributes.getAttributes());
    }
}
