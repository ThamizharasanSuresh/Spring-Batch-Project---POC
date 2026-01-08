package com.student.springbatchproject;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;

public class DynamicItemWriter implements ItemWriter<String[]> {

    private static final Logger logger = LoggerFactory.getLogger(DynamicItemWriter.class);

    private final String inputFilePath;
    private final JdbcTemplate jdbcTemplate;
    private final HeaderHolder headerHolder;

    private Connection globalConnection;

    private String tableName;
    private String dbProduct;

    public DynamicItemWriter(JdbcTemplate jdbcTemplate,
                             HeaderHolder headerHolder,
                             @Value("#{jobParameters['input.file']}") String inputFilePath) {
        this.jdbcTemplate = jdbcTemplate;
        this.headerHolder = headerHolder;
        this.inputFilePath = inputFilePath;
    }


    @PostConstruct
    public void initializeWriter() {
        try {

            if (inputFilePath == null || inputFilePath.isBlank()) {
                throw new IllegalStateException("'app.input-file' not found!");
            }
            File file = new File(inputFilePath);
            String baseName = file.getName().replaceFirst("[.][^.]+$", "");
            this.tableName = sanitizeTableName(baseName + "_table");
            try (Connection metaConn = jdbcTemplate.getDataSource().getConnection()) {
                this.dbProduct = metaConn.getMetaData().getDatabaseProductName().toLowerCase();
            }
            globalConnection = jdbcTemplate.getDataSource().getConnection();
            globalConnection.setAutoCommit(false);
            logger.info("Writer initialized.");
            logger.info("DB Detected: {}", dbProduct);
            logger.info("CSV File Path: {}", inputFilePath);
            logger.info("Derived Table Name: {}", tableName);
        } catch (Exception e) {
            logger.error("Initialization error: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
    @PreDestroy
    public void closeGlobalConnection() {
        try {
            globalConnection.commit();
            logger.info("Global transaction committed successfully.");
        } catch (Exception e) {
            try {
                globalConnection.rollback();
            } catch (Exception ignored) {}
            logger.error("Global transaction rolled back due to error: {}", e.getMessage());
        } finally {
            try { globalConnection.close(); } catch (Exception ignored) {}
        }
    }
    private boolean firstRunCompleted = false;
    @Override
    public void write(Chunk<? extends String[]> chunk) throws Exception {
        List<? extends String[]> rows = chunk.getItems();
        if (rows == null || rows.isEmpty()) return;
        String[] headers = headerHolder.getHeaders();
        if (firstRunCompleted) {
            insertRows(tableName, headers, rows);
            return;
        }
        boolean exists = checkTableExists(tableName);
        if (!exists) {
            createTable(tableName, headers);
            insertRows(tableName, headers, rows);
            firstRunCompleted = true;
            return;
        }
        if (!validateColumns(tableName, headers)) {
            String newTable = getNextVersionedTableName(tableName);
            createTable(newTable, headers);
            insertRows(newTable, headers, rows);
            tableName = newTable;
            firstRunCompleted = true;
            return;
        }
        List<String[]> unique = filterDuplicatesFromDB(tableName, headers, rows);
        if (!unique.isEmpty()) {
            insertRows(tableName, headers, unique);
        }
        firstRunCompleted = true;
    }


    private boolean checkTableExists(String tableName) {
        try {
            String sql;

            if (dbProduct.contains("oracle")) {
                sql = "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = UPPER(?)";
            }
            else if (dbProduct.contains("postgresql")) {
                sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = LOWER(?)";
            }
            else {
                sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?";
            }

            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, tableName);
            return count != null && count > 0;

        } catch (Exception e) {
            logger.error("Error checking table: {}", e.getMessage());
            return false;
        }
    }

    private boolean validateColumns(String tableName, String[] headers) {
        try {
            String dbProduct = this.dbProduct;

            List<String> existingColumns;

            if (dbProduct.contains("oracle")) {
                existingColumns = jdbcTemplate.queryForList(
                        "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS " +
                                "WHERE TABLE_NAME = UPPER(?) AND COLUMN_NAME <> 'ID' ORDER BY COLUMN_ID",
                        String.class, tableName
                );
            }
            else {
                existingColumns = jdbcTemplate.queryForList(
                        "SELECT column_name FROM information_schema.columns " +
                                "WHERE table_name = ? AND column_name <> 'id' ORDER BY ordinal_position",
                        String.class, tableName.toLowerCase()
                );
            }

            List<String> csvColumns = new ArrayList<>();
            for (String h : headers)
                csvColumns.add(sanitizeColumnName(h));

            List<String> normalizedExisting = new ArrayList<>();
            for (String col : existingColumns)
                normalizedExisting.add(col.toLowerCase());

            return normalizedExisting.equals(csvColumns);

        } catch (Exception e) {
            logger.error("Column validation error: {}", e.getMessage());
            return false;
        }
    }


    private void createTable(String tableName, String[] headers) {
        try {

            String dbProduct = this.dbProduct;


            StringBuilder sql = new StringBuilder();

            if (dbProduct.contains("oracle")) {
                sql.append("CREATE TABLE ").append(tableName.toUpperCase())
                        .append(" (ID NUMBER PRIMARY KEY");

                for (String header : headers) {
                    String col = sanitizeColumnName(header).toUpperCase();
                    sql.append(", ").append(col).append(" VARCHAR2(4000)");
                }
                sql.append(")");
            }
            else if (dbProduct.contains("postgresql")) {

                sql.append("CREATE TABLE ").append(tableName)
                        .append(" (id SERIAL PRIMARY KEY");

                for (String header : headers) {
                    sql.append(", ")
                            .append(sanitizeColumnName(header))
                            .append(" TEXT");
                }
                sql.append(")");
            }
            else {
                sql.append("CREATE TABLE `").append(tableName).append("`")
                        .append(" (id INT AUTO_INCREMENT PRIMARY KEY");

                for (String header : headers) {
                    String col = sanitizeColumnName(header);
                    if (col.equalsIgnoreCase("index")) {
                        col = "`index`";
                    } else {
                        col = "`" + col + "`";
                    }
                    sql.append(", ")
                            .append(col)
                            .append(" TEXT");
                }
                sql.append(")");
            }

            jdbcTemplate.execute(sql.toString());
            logger.info("Created table '{}' for DB: {}", tableName, dbProduct);
            if (dbProduct.contains("oracle")) {
                jdbcTemplate.execute("CREATE SEQUENCE " + tableName + "_seq START WITH 1 INCREMENT BY 1");
                jdbcTemplate.execute(
                        "CREATE OR REPLACE TRIGGER " + tableName + "_trg " +
                                "BEFORE INSERT ON " + tableName + " " +
                                "FOR EACH ROW " +
                                "BEGIN " +
                                "IF :NEW.id IS NULL THEN " +
                                "SELECT " + tableName + "_seq.NEXTVAL INTO :NEW.id FROM dual; " +
                                "END IF; " +
                                "END;"
                );
            }

        } catch (Exception e) {
            logger.error("Error creating table {}: {}", tableName, e.getMessage());
            throw new RuntimeException(e);
        }
    }


    private List<String[]> filterDuplicatesFromDB(String tableName, String[] headers, List<? extends String[]> newRows) {
        try {

            String concat;

            if (dbProduct.contains("oracle")) {
                StringJoiner joiner = new StringJoiner(" || '|' || ");
                for (String h : headers)
                    joiner.add("NVL(" + sanitizeColumnName(h) + ", '')");
                concat = joiner.toString();
            }
            else if (dbProduct.contains("postgresql")) {
                StringJoiner joiner = new StringJoiner(" || '|' || ");
                for (String h : headers)
                    joiner.add("COALESCE(" + sanitizeColumnName(h) + ", '')");
                concat = joiner.toString();
            }
            else {
                StringBuilder sb = new StringBuilder("CONCAT(");
                for (int i = 0; i < headers.length; i++) {
                    if (i > 0) sb.append(", '|', ");
                    sb.append("COALESCE(`").append(sanitizeColumnName(headers[i])).append("`, '')");
                }
                sb.append(")");
                concat = sb.toString();
            }

            String sql = "SELECT " + concat + " AS row_signature FROM " + tableName;

            List<String> existing = jdbcTemplate.query(sql, (rs, n) ->
                    rs.getString("row_signature").trim().toLowerCase()
            );

            Set<String> existingSet = new HashSet<>(existing);
            List<String[]> unique = new ArrayList<>();

            for (String[] row : newRows) {
                String key = String.join("|", row).trim().toLowerCase();
                if (!existingSet.contains(key)) unique.add(row);
            }

            return unique;

        } catch (Exception e) {
            logger.error("Duplicate filter failed: {}", e.getMessage());
            return new ArrayList<>(newRows);
        }
    }

    private final Map<String, String> sqlCache = new HashMap<>();

    private void insertRows(String tableName, String[] headers, List<? extends String[]> rows) throws Exception {

        String sql = sqlCache.computeIfAbsent(tableName, t -> buildInsertQuery(tableName, headers));
        logger.info("Using SQL: {}", sql);

        try (PreparedStatement ps = globalConnection.prepareStatement(sql)) {

            for (String[] row : rows) {
                for (int i = 0; i < headers.length; i++) {
                    ps.setString(i + 1, row[i]);
                }
                ps.addBatch();
            }
            ps.executeBatch();

        } catch (Exception e) {
            logger.error("Batch insert failed: {}", e.getMessage(), e);
            throw e;
        }

    }



    private String buildInsertQuery(String tableName, String[] headers){
        StringBuilder sql = new StringBuilder();
        if (dbProduct.contains("oracle")) {
            String seq = tableName.toUpperCase() + "_SEQ";
            sql.append("INSERT INTO ").append(tableName.toUpperCase()).append(" (ID");

            for (String h : headers) {
                sql.append(", ").append(sanitizeColumnName(h).toUpperCase());
            }

            sql.append(") VALUES (").append(seq).append(".NEXTVAL");

            for (int i = 0; i < headers.length; i++) {
                sql.append(", ?");
            }
            sql.append(")");

        } else if (dbProduct.contains("postgresql")) {
            StringJoiner cols = new StringJoiner(", ");
            StringJoiner placeholders = new StringJoiner(", ");
            for (String header : headers) {
                String col = sanitizeColumnName(header);
                if (col.equalsIgnoreCase("index")) {
                    col = "\"index\"";
                } else {
                    col = "\"" + col + "\"";
                }
                cols.add(col);
                placeholders.add("?");
            }
            sql.append("INSERT INTO \"")
                    .append(tableName)
                    .append("\" (")
                    .append(cols)
                    .append(") VALUES (")
                    .append(placeholders)
                    .append(")");

        } else {
            StringJoiner cols = new StringJoiner(", ");
            StringJoiner placeholders = new StringJoiner(", ");
            for (String header : headers) {
                String col = sanitizeColumnName(header);

                if (col.equalsIgnoreCase("index")) {
                    col = "`index`";
                } else {
                    col = "`" + col + "`";
                }
                cols.add(col);
                placeholders.add("?");
            }
            sql.append("INSERT INTO `")
                    .append(tableName)
                    .append("` (")
                    .append(cols)
                    .append(") VALUES (")
                    .append(placeholders)
                    .append(")");
        }
        return sql.toString();
    }

    private String getNextVersionedTableName(String base) {
        int v = 2;
        while (true) {
            String name = base + "_v" + v;
            if (!checkTableExists(name)) return name;
            v++;
        }
    }

//    private String sanitizeColumnName(String name) {
//        return name.trim().replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
//    }
private static final Set<String> RESERVED_WORDS = Set.of(
        "INDEX","ORDER","DATE","NUMBER","ROWNUM",
        "SELECT","INSERT","UPDATE","DELETE","WHERE","FROM","GROUP","USER",
        "TABLE","VIEW","KEY","PRIMARY","FOREIGN","CHECK","LONG","LEVEL"
);

    private String sanitizeColumnName(String name) {
        String col = name.trim().replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
        if (!col.matches("^[a-zA-Z].*")) col = "col_" + col;
        col = col.replaceAll("_+", "_").replaceAll("_$", "");
        if (RESERVED_WORDS.contains(col.toUpperCase())) col = col + "_col";
        return col;
    }

    private String sanitizeTableName(String name) {
        return name.trim().replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
    }
}
