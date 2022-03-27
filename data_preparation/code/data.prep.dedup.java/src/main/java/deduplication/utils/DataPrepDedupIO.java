package deduplication.utils;

import deduplication.preparation.Preparators;
import deduplication.preparation.utils.Preparation;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataPrepDedupIO {

    protected Connection conn;

    protected void resetTable() throws SQLException {

    }

    protected void dropTable() throws SQLException {

    }

    public DataPrepDedupIO(String dbDriver, String dbConnection, String dbUser, String dbPassword) {
        try {
            Class.forName(dbDriver);
            conn = DriverManager.getConnection(dbConnection, dbUser, dbPassword);

            conn.setAutoCommit(false);
        } catch (Exception e) {
            System.err.println("Got an exception!");
            System.err.println(e.getMessage());
        }
    }

    public Connection getConn() {
        return conn;
    }

    public DataPrepDedupIO(Connection conn) {
        this.conn = conn;
    }

    public void close() {
        try {
            if (conn != null) {
                conn.close();
                conn = null;
            }
        } catch (Exception e) {
            System.err.println("Got an exception!");
            System.err.println(e.getMessage());
        }
    }


    public static Map<String, Map<String, String>> importTable(Connection conn, String sql, List<String> columns) {
        Map<String, Map<String, String>> records = new HashMap<>();
        try {
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery(sql);
            ResultSetMetaData rsmd = rs.getMetaData();

            if (columns == null) {
                columns = IntStream.range(1, rsmd.getColumnCount() + 1).mapToObj(i -> {
                    try {
                        return rsmd.getColumnName(i);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    return null;
                }).filter(Objects::nonNull).collect(Collectors.toList());
            }
            int counter = 0;
            boolean containsID = columns.contains("id");
            while (rs.next()) {
                String id = containsID ? rs.getString("id") : String.valueOf(counter++);

                Map<String, String> record = columns.stream().collect(HashMap::new, (m, c) ->
                {
                    try {
                        m.put(c, rs.getString(c));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }, HashMap::putAll);
                records.put(id, record);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return records;
    }

    public Map<String, Map<String, String>> importTable(String sql) {
        return importTable(conn, sql, null);
    }

    public Map<String, Map<String, String>> importSinglePreparationCubeTable(String dataset, String xstandard, String attribute, Set<String> preparators, String cubeType) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM preparation_" + cubeType + "_cube WHERE `dataset` = '" + dataset + "'");
        sb.append(" and `xstandard` = '" + xstandard + "'");
        if (attribute != null) {
            sb.append(" and `attribute` = '" + attribute + "'");
        }

        List<String> allPreparators = Preparators.getAllPreparatorNames();

        for (String prp : allPreparators) {
            sb.append(" and `" + prp + "` = " + (preparators.contains(prp)) + "");
        }
        sb.append(";");

        String sql = sb.toString();

        Map<String, Map<String, String>> records = new HashMap<>();
        try {
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                String rid = null;
                Map<String, String> record = null;
                switch (cubeType) {
                    case "data":
                        rid = rs.getString("record_id");
                        record = records.getOrDefault(rid, new HashMap<>());
                        record.put(rs.getString("attribute"), String.valueOf(rs.getObject("value")));
//                        record.put("attribute", rs.getString("attribute"));
//                        record.put("similarity", rs.getString("similarity"));
                        break;
                    case "similarities":
                        rid = rs.getString("record_id1") + "_" + rs.getString("record_id2");
                        record = records.getOrDefault(rid, new HashMap<>());
                        record.put(rs.getString("attribute"), String.valueOf(rs.getObject("similarity")));
//                        record.put("attribute", rs.getString("attribute"));
//                        record.put("similarity", rs.getString("similarity"));
                        record.put("pair_class", String.valueOf(rs.getObject("pair_class")));
                        break;
                }

                records.put(rid, record);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return records;
    }

    public Map<String, Map<String, String>> importTotalPreparationCubeTable(
            Map<String, List<Preparation>> chainedPreparations, String xstandard, String dataset, String cubeType) {

        Map<String, Map<String, String>> records = new HashMap<>();
        if (chainedPreparations == null) {
            chainedPreparations.forEach((key, attributePreparations) -> {
                String attribute = attributePreparations.get(0).getAttribute();
                Set<String> attributePreparators = attributePreparations.stream().map(Preparation::getPreparator).collect(Collectors.toSet());

                Map<String, Map<String, String>> preparationRecords = importSinglePreparationCubeTable(dataset, xstandard, attribute, attributePreparators, cubeType);

                for (Map.Entry<String, Map<String, String>> e : preparationRecords.entrySet()) {
                    Map<String, String> record = records.getOrDefault(e.getKey(), new HashMap<>());
                    record.putAll(e.getValue());
                    records.put(e.getKey(), record);
                }
            });
        } else {
            Map<String, Map<String, String>> preparationRecords = importSinglePreparationCubeTable(dataset, xstandard, null, new HashSet<>(), cubeType);
            records.putAll(preparationRecords);
        }

        return records;
    }


    public void clearTableDataCubePreparation(String dataset, String xstandard, String attribute, Set<String> preparators) throws SQLException {

        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM `preparation_data_cube` WHERE `dataset` = '" + dataset + "'");
        sb.append(" and `xstandard` = '" + xstandard + "'");
        sb.append(" and `attribute` = '" + attribute + "'");

        List<String> allPreparators = Preparators.getAllPreparatorNames();

        for (String prp : allPreparators) {
            sb.append(" and `" + prp + "` = " + (preparators.contains(prp)) + "");
        }
        sb.append(";");
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(1);
        stmt.execute(sb.toString());
        conn.commit();
    }

    public void clearTableSimilaritiesCubePreparation(String dataset, String xstandard, String attribute, Set<String> preparators) {

        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM `preparation_similarities_cube` WHERE `dataset` = '" + dataset + "'");
        sb.append(" and `xstandard` = '" + xstandard + "'");
        sb.append(" and `attribute` = '" + attribute + "'");

        List<String> allPreparators = Preparators.getAllPreparatorNames();

        for (String prp : allPreparators) {
            sb.append(" and `" + prp + "` = " + (preparators.contains(prp)) + "");
        }
        sb.append(";");
        try {
            Statement stmt = conn.createStatement();
            stmt.setQueryTimeout(1);
            stmt.execute(sb.toString());
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void clearTableSimilaritiesDifferencesCubePreparation(String dataset, String xstandard, String attribute, Set<String> preparators) {

        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM `preparation_similarities_differences_cube` WHERE `dataset` = '" + dataset + "'");
        sb.append(" and `xstandard` = '" + xstandard + "'");
        sb.append(" and `attribute` = '" + attribute + "'");

        List<String> allPreparators = Preparators.getAllPreparatorNames();

        for (String prp : allPreparators) {
            sb.append(" and `" + prp + "` = " + (preparators.contains(prp)) + "");
        }
        sb.append(";");
        try {
            Statement stmt = conn.createStatement();
            stmt.setQueryTimeout(1);
            stmt.execute(sb.toString());
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void clearTableCandidatePreparations(String dataset, String xstandard) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM `candidate_preparations` WHERE `dataset` = '" + dataset + "'");
        sb.append(" and `xstandard` = '" + xstandard + "'");
        sb.append(";");
        try {
            Statement stmt = conn.createStatement();
            stmt.setQueryTimeout(1);
            stmt.execute(sb.toString());
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void clearTableClassification(String dataset, String xstandard, String preparations, String classifier) {

        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM `classification` WHERE `dataset` = '" + dataset + "'");
        sb.append(" and `xstandard` = '" + xstandard + "'");
        sb.append(" and `preparations` = '" + preparations + "'");
        sb.append(" and `classifier` = '" + classifier + "'");
        sb.append(";");
        try {
            Statement stmt = conn.createStatement();
//            stmt.setQueryTimeout(3600);
            stmt.setQueryTimeout(5);
            stmt.execute(sb.toString());
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createTableDataCube() throws SQLException {
        List<String> allPreparators = Preparators.getAllPreparatorNames();

        StringBuilder sb = new StringBuilder();
        StringBuilder sbIndex = new StringBuilder();
        for (String prp : allPreparators) {
            sb.append("`" + prp + "` boolean DEFAULT FALSE,");
            sbIndex.append("`" + prp + "`, ");
        }

        String sql = "CREATE TABLE IF NOT EXISTS `preparation_data_cube` (" +
                "`id` int(11) NOT NULL AUTO_INCREMENT," +
                "`dataset` varchar(45) DEFAULT NULL," +
                "`xstandard` varchar(45) DEFAULT NULL," +
//            "`preparators` varchar(45) DEFAULT NULL," +
                sb.toString() +
                "`record_id` varchar(45) DEFAULT NULL," +
                "`attribute` varchar(45) DEFAULT NULL," +
                "`value` longtext DEFAULT NULL," +
                "PRIMARY KEY (`id`), " +
//            "INDEX name (`dataset`, " + sbIndex.toString()+ "`record_id`, `attribute`)" +
                "INDEX name (`dataset`, `record_id`, `attribute`)" +
                ") ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;";

        System.out.println(sql);
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(5);
        stmt.execute(sql);
        conn.commit();
    }

    public void createTableSimilaritiesCube() throws SQLException {
        List<String> allPreparators = Preparators.getAllPreparatorNames();

        StringBuilder sb = new StringBuilder();
        StringBuilder sbIndex = new StringBuilder();
        for (String prp : allPreparators) {
            sb.append("`" + prp + "` boolean DEFAULT FALSE,");
            sbIndex.append("`" + prp + "`, ");
        }

        String sql = "CREATE TABLE IF NOT EXISTS `preparation_similarities_cube` (" +
                "`id` int(11) NOT NULL AUTO_INCREMENT," +
                "`dataset` varchar(45) DEFAULT NULL," +
                "`xstandard` varchar(45) DEFAULT NULL," +
//            "`preparators` varchar(45) DEFAULT NULL," +
                sb.toString() +
                "`record_id1` varchar(45) DEFAULT NULL," +
                "`record_id2` varchar(45) DEFAULT NULL," +
                "`pair_class` varchar(45) DEFAULT NULL," +
                "`attribute` varchar(45) DEFAULT NULL," +
                "`similarity` double DEFAULT NULL," +
                "PRIMARY KEY (`id`), " +
//            "INDEX name (`dataset`, " + sbIndex.toString()+ "`record_id`, `attribute`)" +
                "INDEX name (`dataset`, `record_id1`, `record_id2`, `attribute`)" +
                ") ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;";

        System.out.println(sql);
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(5);
        stmt.execute(sql);
        conn.commit();
    }

    public void createTableSimilaritiesDifferencesCube() throws SQLException {
        List<String> allPreparators = Preparators.getAllPreparatorNames();

        StringBuilder sb = new StringBuilder();
        StringBuilder sbIndex = new StringBuilder();
        for (String prp : allPreparators) {
            sb.append("`" + prp + "` boolean DEFAULT FALSE,");
            sbIndex.append("`" + prp + "`, ");
        }

        String sql = "CREATE TABLE IF NOT EXISTS `preparation_similarities_differences_cube` (" +
                "`id` int(11) NOT NULL AUTO_INCREMENT," +
                "`dataset` varchar(45) DEFAULT NULL," +
                "`xstandard` varchar(45) DEFAULT NULL," +
//            "`preparators` varchar(45) DEFAULT NULL," +
                sb.toString() +
                "`record_id1` varchar(45) DEFAULT NULL," +
                "`record_id2` varchar(45) DEFAULT NULL," +
                "`pair_class` varchar(45) DEFAULT NULL," +
                "`attribute` varchar(45) DEFAULT NULL," +
                "`similarity_before` double DEFAULT NULL," +
                "`similarity_after` double DEFAULT NULL," +
                "`difference` double DEFAULT NULL," +
                "PRIMARY KEY (`id`), " +
//            "INDEX name (`dataset`, " + sbIndex.toString()+ "`record_id`, `attribute`)" +
                "INDEX name (`dataset`, `record_id1`, `record_id2`, `attribute`)" +
                ") ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;";

        System.out.println(sql);
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(5);
        stmt.execute(sql);
        conn.commit();
    }

    public void createTableCandidatePreparations() throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS `candidate_preparations` (" +
            "`id` int(11) NOT NULL AUTO_INCREMENT," +
            "`dataset` varchar(45) DEFAULT NULL," +
            "`xstandard` varchar(45) DEFAULT NULL," +
            "`candidate_preparations` longtext DEFAULT NULL," +
            "PRIMARY KEY (`id`), " +
            "INDEX name (`dataset`, `xstandard`)" +
            ") ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;";

        System.out.println(sql);
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(5);
        stmt.execute(sql);
        conn.commit();
    }

    public void createTableClassification() throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS `classification` (" +
                "`id` int(11) NOT NULL AUTO_INCREMENT," +

                "`dataset` varchar(45) DEFAULT NULL," +
                "`xstandard` varchar(45) DEFAULT NULL," +
                "`preparations` longtext DEFAULT NULL," +
                "`classifier` varchar(45) DEFAULT NULL," +
                "`classifier_f1` double DEFAULT NULL," +
                "`classifier_precision` double DEFAULT NULL," +
                "`classifier_recall` double DEFAULT NULL," +
                "`auc_pr` double DEFAULT NULL," +
                "`auc_pr_best_f1` double DEFAULT NULL," +
                "`auc_pr_best_precision` double DEFAULT NULL," +
                "`auc_pr_best_recall` double DEFAULT NULL," +
                "`auc_pr_precisions` longtext DEFAULT NULL," +
                "`auc_pr_recalls` longtext DEFAULT NULL," +
                "`auc_pr_thresholds` longtext DEFAULT NULL," +

                "PRIMARY KEY (`id`)" +
                ") ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;";

        System.out.println(sql);
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(5);
        stmt.execute(sql);
        conn.commit();
    }

    public List<String> getDataCubeHeader() {
        List<String> header = new ArrayList<>(
                Arrays.asList(
                        "id",
                        "dataset",
                        "xstandard"
//                    "preparators"
                )
        );
        header.addAll(Preparators.getAllPreparatorNames());
        header.addAll(
                Arrays.asList(
                        "record_id",
                        "attribute",
                        "value"
                )
        );
        return header;
    }

    public List<String> getSimilarityCubeHeader() {
        List<String> header = new ArrayList<>(
                Arrays.asList(
                        "id",
                        "dataset",
                        "xstandard"
//                    "preparators"
                )
        );
        header.addAll(Preparators.getAllPreparatorNames());
        header.addAll(
                Arrays.asList(
                        "record_id1",
                        "record_id2",
                        "pair_class",
                        "attribute",
                        "similarity"
                )
        );
        return header;
    }

    public List<String> getSimilarityDifferencesCubeHeader() {
        List<String> header = new ArrayList<>(
                Arrays.asList(
                        "id",
                        "dataset",
                        "xstandard"
//                    "preparators"
                )
        );
        header.addAll(Preparators.getAllPreparatorNames());
        header.addAll(
                Arrays.asList(
                        "record_id1",
                        "record_id2",
                        "pair_class",
                        "attribute",
                        "similarity_before",
                        "similarity_after",
                        "difference"
                )
        );
        return header;
    }

    public List<String> getCandidatePreparationsHeader() {
        return new ArrayList<>(
                Arrays.asList(
                        "id",
                        "dataset",
                        "xstandard",
                        "candidate_preparations"
                )
        );
    }

    public List<String> getClassificationHeader() {
        return new ArrayList<>(
                Arrays.asList(
                        "id",
                        "dataset",
                        "xstandard",
                        "preparations",
                        "classifier",
                        "classifier_f1",
                        "classifier_precision",
                        "classifier_recall",
                        "auc_pr",
                        "auc_pr_best_f1",
                        "auc_pr_best_precision",
                        "auc_pr_best_recall",
                        "auc_pr_precisions",
                        "auc_pr_recalls",
                        "auc_pr_thresholds"
                )

        );
    }

}
