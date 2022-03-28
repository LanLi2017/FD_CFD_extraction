package deduplication.nonduplicates.indexing.utils;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import java.nio.file.Path;
import java.sql.*;
import java.util.Collection;
import java.util.Properties;
import java.util.TreeSet;

public class JDBCWrapper {
    protected Logger logger = Logger.getLogger(JDBCWrapper.class);

    public Connection initiateConnection(Path datasetFile) throws SQLException {
        //        Class.forName("org.sqlite.JDBC");
        String extension = FilenameUtils.getExtension(datasetFile.toString());
        Connection c = null;
        switch (extension) {
            case "sqlite":
                c = DriverManager.getConnection("jdbc:sqlite:" + datasetFile);
                break;
            case "tsv":
            case "csv":
                Properties props = new Properties();
                if (extension.equals("tsv")) {
                    props.put("separator", "\t");
                } else if (extension.equals("csv")) {
                    props.put("separator", ",");
                }
                props.put("fileExtension", extension);
                try {
                    Class.forName("org.relique.jdbc.csv.CsvDriver");
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                // Load the driver.
//                Class.forName("org.relique.jdbc.csv.CsvDriver");
//                c = DriverManager.getConnection("jdbc:hsqldb:file:" + dbFile, "SA", "");
//                c = DriverManager.getConnection("jdbc:relique:csv:" + datasetFile.getParent(), props);
//                c = DriverManager.getConnection("jdbc:relique:csv:" + datasetFile.getParent(), props);
//                c = DriverManager.getConnection("jdbc:relique:csv:" + datasetFile.getParent(), props);
                // Create a connection to directory given as first command line
                // parameter. Driver properties are passed in URL format
                // (or alternatively in a java.utils.Properties object).
                //
                // A single connection is thread-safe for use by several threads.
                String url = "jdbc:relique:csv:" + datasetFile.getParent() + "?" +
                        "separator=\t" + "&" + "fileExtension=.tsv";
                c = DriverManager.getConnection(url);
                break;
        }
        return c;
    }

    public Collection<String> getDatabaseSchemaNames(Connection c) throws SQLException {
        // --- LISTING DATABASE SCHEMA NAMES ---
        ResultSet resultSet = c.getMetaData().getCatalogs();
        while (resultSet.next()) {
            logger.info("Schema Name = " + resultSet.getString("TABLE_CAT"));
        }
        resultSet.close();

        return null;
    }

    public Collection<String> getDatabaseRelationNames(Connection c, String databaseName) throws SQLException {
        // --- LISTING DATABASE TABLE NAMES ---
        String[] types = { "TABLE" };
        ResultSet resultSet = c.getMetaData()
                .getTables(databaseName, null, "%", types);
        String tableName = "";
        while (resultSet.next()) {
            tableName = resultSet.getString(3);
            logger.info("Table Name = " + tableName);
        }
        resultSet.close();
        return null;
    }

    public Collection<String> getDatabaseAttributeNames(Connection c, String databaseName, String relationName) throws SQLException {
        Collection<String> databaseAttributeNames = new TreeSet<>();
        // --- LISTING DATABASE COLUMN NAMES ---
        DatabaseMetaData meta = c.getMetaData();
        ResultSet resultSet = meta.getColumns(databaseName, null, relationName, "%");
        while (resultSet.next()) {
//            logger.info("Column Name of table " + relationName + " = " + resultSet.getString(4));
            databaseAttributeNames.add(resultSet.getString(4));
        }
        return databaseAttributeNames;
    }
}
