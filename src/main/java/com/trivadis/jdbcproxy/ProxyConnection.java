package com.trivadis.jdbcproxy;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class ProxyConnection implements Connection {
    final Connection target;

    ProxyConnection(Connection connection) {
        target = connection;
    }

    private boolean isSnowflake() throws SQLException {
        return target.getMetaData().getURL().contains("snowflake");
    }

    @Override
    public Statement createStatement() throws SQLException {
        return target.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (isSnowflake()) {
            String patchedSql;
            if ("show databases".equals(sql)) {
                // Snowflake returns created_on as the first column, but
                // SQL Developer expects the database name as first column.
                patchedSql = "SELECT * FROM information_schema.databases";
            } else {
                // make dictionary queries by SQL Developer targeting MySQL compatible with Snowflake
                // changes must be specific enough to not affect user queries
                final String INDEX_DETAILS_FROM = "select INDEX_NAME, INDEX_TYPE, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE, COLLATION, CARDINALITY, SUB_PART, \n" +
                        "PACKED, NULLABLE, COMMENT FROM INFORMATION_SCHEMA.STATISTICS \n" +
                        "WHERE (COALESCE(COLLATION(?), 'x') NOT LIKE '%chinese%' \n" +
                        "or COALESCE(COLLATION(?), 'x') NOT LIKE '%japanese%' \n" +
                        "or COALESCE(COLLATION(?), 'x') NOT LIKE '%korean%') \n" +
                        "and TABLE_NAME = ? AND TABLE_SCHEMA = ? \n" +
                        "UNION \n" +
                        "select INDEX_NAME, INDEX_TYPE, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE, COLLATION, CARDINALITY, SUB_PART, \n" +
                        "PACKED, NULLABLE, COMMENT FROM INFORMATION_SCHEMA.STATISTICS \n" +
                        "WHERE (COALESCE(COLLATION(?), 'x') LIKE '%chinese%' \n" +
                        "or COALESCE(COLLATION(?), 'x') LIKE '%japanese%' \n" +
                        "or COALESCE(COLLATION(?), 'x') LIKE '%korean%') \n" +
                        "and TABLE_NAME = ? AND TABLE_SCHEMA = ? \n" +
                        "ORDER BY INDEX_NAME, SEQ_IN_INDEX";
                final String INDEX_DETAILS_TO = "SELECT NULL  AS index_name,\n" +
                        "       NULL  AS index_type,\n" +
                        "       NULL  AS column_name,\n" +
                        "       NULL  AS seq_in_index,\n" +
                        "       NULL  AS non_unique,\n" +
                        "       NULL  AS collation,\n" +
                        "       NULL  AS cardinality,\n" +
                        "       NULL  AS sub_part,\n" +
                        "       NULL  AS packed,\n" +
                        "       NULL  AS nullable,\n" +
                        "       NULL  AS comment\n" +
                        " WHERE 'x' IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                final String CHECK_CONSTRAINTS_FROM = "SELECT t.table_schema, \n" +
                        "  t.table_name, \n" +
                        "  t.constraint_name, \n" +
                        "  t.constraint_type, \n" +
                        "  t.is_deferrable, \n" +
                        "  t.initially_deferred, \n" +
                        "  c.check_clause \n" +
                        "FROM information_schema.check_constraints c, \n" +
                        "  information_schema.table_constraints t \n" +
                        "WHERE t.table_schema    = ? \n" +
                        "AND t.table_name        = ? \n" +
                        "AND t.constraint_type   = 'CHECK' \n" +
                        "AND c.constraint_name   = t.constraint_name \n" +
                        "AND c.constraint_schema = t.constraint_schema";
                final String CHECK_CONSTRAINTS_TO = "SELECT NULL  AS table_schema,\n" +
                        "       NULL  AS table_name,\n" +
                        "       NULL  AS constraint_name,\n" +
                        "       NULL  AS constraint_type,\n" +
                        "       NULL  AS is_deferrable,\n" +
                        "       NULL  AS initially_deferred,\n" +
                        "       NULL  AS check_clause\n" +
                        " WHERE 'x' IN (?, ?)";
                final String TABLE_CONSTRAINTS_FROM = "select a.owner,a.table_name, a.constraint_name, a.delete_rule,a.r_constraint_name, a.deferred, a.deferrable from sys.all_constraints a, (select owner,constraint_name from sys.all_constraints where owner = ? and table_name = ? and constraint_type in ('P','U')) b where a.constraint_type = 'R' and a.r_constraint_name = b.constraint_name and a.r_owner = b.owner";
                final String TABLE_CONSTRAINTS_TO = "SELECT NULL  AS owner,\n" +
                        "       NULL  AS table_name,\n" +
                        "       NULL  AS constraint_name,\n" +
                        "       NULL  AS delete_rule,\n" +
                        "       NULL  AS r_constraint_name,\n" +
                        "       NULL  AS deferred,\n" +
                        "       NULL  AS deferrable\n" +
                        "WHERE 'x' IN (?, ?);";
                patchedSql = sql
                        .replace("cast(TABLE_SCHEMA as binary)", "TABLE_SCHEMA")
                        .replace("cast(TABLE_NAME as binary)", "TABLE_NAME")
                        .replace("DATA_TYPE , NUMERIC_PRECISION , NUMERIC_SCALE , COLUMN_COMMENT","DATA_TYPE , NUMERIC_PRECISION , NUMERIC_SCALE , COMMENT AS COLUMN_COMMENT")
                        .replace("binary TABLE_NAME", "TABLE_NAME")
                        .replace("`", "\"")
                        .replace("COLLATION(?)", "COALESCE(COLLATION(?), 'x')")
                        .replace("SELECT DISTINCT(CONCAT(INDEX_NAME,' (',TABLE_NAME,')')) IND_NAME, INDEX_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ?", "SELECT null AS IND_NAME, null AS index_name, null AS table_name WHERE 'x' = ?")
                        .replace(INDEX_DETAILS_FROM, INDEX_DETAILS_TO) // not supported by Snowflake
                        .replace(CHECK_CONSTRAINTS_FROM, CHECK_CONSTRAINTS_TO) // not supported by Snowflake
                        .replace(TABLE_CONSTRAINTS_FROM, TABLE_CONSTRAINTS_TO) // not needed, all relevant data are queried via DatabaseMetaData
                        .replace("SELECT VIEW_DEFINITION, CHECK_OPTION, IS_UPDATABLE, DEFINER, SECURITY_TYPE", "SELECT view_definition, check_option, is_updatable, insertable_into, is_secure, created, last_altered, comment")
                        .replace("select SPECIFIC_NAME from information_schema.routines where ROUTINE_TYPE = 'PROCEDURE' and cast(ROUTINE_SCHEMA as binary) = ?", "select procedure_name AS specific_name FROM information_schema.procedures WHERE procedure_schema = ?")
                        .replace("select SPECIFIC_NAME from information_schema.routines where ROUTINE_TYPE = 'FUNCTION' and cast(ROUTINE_SCHEMA as binary) = ?", "select function_name AS specific_name FROM information_schema.functions WHERE function_schema = ?")
                        .replace("select TRIGGER_NAME from information_schema.triggers  where trigger_schema = ?", "SELECT null AS trigger_name WHERE 'x' = ?");
            }
            return target.prepareStatement(patchedSql);
        }
        return target.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return target.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return target.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        target.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return target.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        target.commit();
    }

    @Override
    public void rollback() throws SQLException {
        target.rollback();
    }

    @Override
    public void close() throws SQLException {
        target.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return target.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new ProxyDatabaseMetaData(target.getMetaData());
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        target.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return target.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        target.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return target.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        target.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return target.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return target.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        target.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return target.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return target.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return target.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return target.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        target.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        target.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return target.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return target.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return target.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        target.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        target.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return target.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return target.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return target.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return target.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return target.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return target.prepareStatement(sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        return target.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return target.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return target.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return target.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return target.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        target.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        target.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return target.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return target.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return target.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return target.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        target.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return target.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        target.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        target.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return target.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return target.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return target.isWrapperFor(iface);
    }
}
