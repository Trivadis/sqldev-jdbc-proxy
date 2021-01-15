/*
 * Copyright 2021 Philipp Salvisberg <philipp.salvisberg@trivadis.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.trivadis.jdbcproxy.rewrite;


import com.trivadis.jdbcproxy.rewrite.SQLRewriter.SQLRewrite;

/**
 * Class rewrites SQL statements from SQL Developer written for MySQL 5.0.
 * It contains methods annotated with @SQLRewrite. A rewrite must match the source
 * statement 100%. For partial rewrites use @SQLPatch(full=false).
 * Full rewrites are executed first. Beside that rule the order is undefined.
 */
public class SQLDevNavigatorSQLRewriter {
    private final static String MYSQL = "MySQL";
    private final static String POSTGRES = "PostgreSQL";
    private final static String SNOWFLAKE = "Snowflake";
    private final static String SQLITE = "SQLite";
    private final static String H2 = "H2";

    @SQLRewrite(full=false)
    public String backtickWithQuote(String sql, String product) {
        if (MYSQL.equals(product) || SQLITE.equals(product)) {
            return sql;
        }
        return sql.replace('`', '"');
    }

    @SQLRewrite
    public String showDatabases(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("show databases")) {
            if (POSTGRES.equals(product)) {
                // databases not available in information_schema
                return "SELECT datname\n" +
                        "  FROM pg_database\n" +
                        " WHERE datistemplate = false\n" +
                        " ORDER BY datname";
            } else if (SNOWFLAKE.equals(product)) {
                return "SELECT database_name\n" +
                        "  FROM information_schema.databases\n" +
                        " ORDER BY database_name";
            } else if (H2.equals(product)) {
                return "SELECT 'PUBLIC' AS database_name";
            } else if (SQLITE.equals(product)) {
                return "SELECT name AS database_name FROM pragma_database_list()";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showSchemas(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("select SCHEMA_NAME from information_schema.schemata")) {
            // SQL Developer expects the column to be in upper case
            if (POSTGRES.equals(product) || SNOWFLAKE.equals(product) || H2.equals(product)) {
                return "SELECT schema_name AS \"SCHEMA_NAME\"\n" +
                        "  FROM information_schema.schemata\n" +
                        " ORDER BY schema_name";
            } else if (SQLITE.equals(product)) {
                return "SELECT 'main' AS \"SCHEMA_NAME\"";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showTables(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("select TABLE_NAME from information_schema.Tables where cast(TABLE_SCHEMA as binary) = ?  \n" +
                "\t\t\t\t\t\t\t\tand (TABLE_TYPE = 'BASE TABLE' OR table_schema='information_schema')\n" +
                "                        ")) {
            if (POSTGRES.equals(product)) {
                // case-sensitive schema name
                return "SELECT table_name AS \"TABLE_NAME\"\n" +
                        "  FROM information_schema.tables\n" +
                        " WHERE table_schema = ?\n" +
                        "   AND ( table_type = 'BASE TABLE'\n" +
                        "    OR table_schema = 'information_schema' )\n" +
                        " ORDER BY table_name";
            } else if (SNOWFLAKE.equals(product)) {
                return "SELECT table_name\n" +
                        "  FROM information_schema.tables\n" +
                        " WHERE table_schema = ?\n" +
                        "   AND ( table_type = 'BASE TABLE'\n" +
                        "    OR table_schema = 'INFORMATION_SCHEMA' )\n" +
                        " ORDER BY table_name";
            } else if (H2.equals(product)) {
                return "SELECT table_name\n" +
                        "  FROM information_schema.tables\n" +
                        " WHERE table_schema = ?\n" +
                        "   AND ( table_type = 'TABLE'\n" +
                        "    OR table_schema = 'INFORMATION_SCHEMA' )\n" +
                        " ORDER BY table_name";
            } else if (SQLITE.equals(product)) {
                return "SELECT name AS \"TABLE_NAME\"\n" +
                        "  FROM sqlite_schema\n" +
                        " WHERE type = 'table'\n" +
                        "   AND ? IS NOT NULL\n" +
                        " ORDER BY name";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showTableColumns(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("select  COLUMN_NAME , ORDINAL_POSITION , COLUMN_DEFAULT , IS_NULLABLE ,\n" +
                " DATA_TYPE , NUMERIC_PRECISION , NUMERIC_SCALE , COLUMN_COMMENT\n" +
                "from information_schema.Columns where\n" +
                "COLLATION(?) NOT LIKE '%chinese%' \n" +
                "and COLLATION(?) NOT LIKE '%japanese%' \n" +
                "and COLLATION(?) NOT LIKE '%korean%'\n" +
                "  and binary TABLE_NAME = ?\n" +
                " AND cast(TABLE_SCHEMA as binary)=? \n" +
                " UNION\n" +
                " select  COLUMN_NAME , ORDINAL_POSITION , COLUMN_DEFAULT , IS_NULLABLE ,\n" +
                " DATA_TYPE , NUMERIC_PRECISION , NUMERIC_SCALE , COLUMN_COMMENT\n" +
                "from information_schema.Columns where\n" +
                "(COLLATION(?) LIKE '%chinese%' \n" +
                "or COLLATION(?) LIKE '%japanese%' \n" +
                "or COLLATION(?) LIKE '%korean%' )\n" +
                " and TABLE_NAME = ?\n" +
                " AND cast(TABLE_SCHEMA as binary)=?")) {
            if (POSTGRES.equals(product) || H2.equals(product)) {
                // no column comments
                return "SELECT column_name,\n" +
                        "       ordinal_position,\n" +
                        "       column_default,\n" +
                        "       is_nullable,\n" +
                        "       data_type,\n" +
                        "       numeric_precision,\n" +
                        "       numeric_scale,\n" +
                        "       NULL AS column_comment\n" +
                        "  FROM information_schema.columns\n" +
                        " WHERE coalesce(?, ?, ?, 'x') IS NOT NULL\n" +
                        "   AND table_name = ?\n" +
                        "   AND table_schema = ?\n" +
                        "   AND coalesce(?, ?, ?, ?, ?, 'x') IS NOT NULL\n" +
                        " ORDER BY ordinal_position";
            } else if (SNOWFLAKE.equals(product)) {
                return "SELECT column_name,\n" +
                        "       ordinal_position,\n" +
                        "       column_default,\n" +
                        "       is_nullable,\n" +
                        "       data_type,\n" +
                        "       numeric_precision,\n" +
                        "       numeric_scale,\n" +
                        "       comment AS column_comment\n" +
                        "  FROM information_schema.columns\n" +
                        " WHERE coalesce(?, ?, ?, 'x') IS NOT NULL\n" +
                        "   AND table_name = ?\n" +
                        "   AND table_schema = ?\n" +
                        "   AND coalesce(?, ?, ?, ?, ?, 'x') IS NOT NULL\n" +
                        " ORDER BY ordinal_position";
            } else if (SQLITE.equals(product)) {
                return "SELECT name        AS column_name,\n" +
                        "       type        AS data_type,\n" +
                        "       CASE `notnull`\n" +
                        "          WHEN 0 THEN\n" +
                        "             'YES'\n" +
                        "          WHEN 42 THEN\n" +
                        "             coalesce(?, ?, ?)\n" +
                        "          ELSE\n" +
                        "             'NO'\n" +
                        "       END         AS is_nullable,\n" +
                        "       dflt_value  AS column_default\n" +
                        "  FROM pragma_table_info (?)\n" +
                        " WHERE coalesce(?, ?, ?, ?, ?, ?, 'x') IS NOT NULL\n" +
                        " ORDER BY cid";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showIndexes(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("\n" +
                "                        SELECT DISTINCT(CONCAT(INDEX_NAME,' (',TABLE_NAME,')')) IND_NAME, INDEX_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE cast(TABLE_SCHEMA as binary) = ?")) {
            if (POSTGRES.equals(product)) {
                // no dictionary view in postgres for index columns
                return "SELECT (concat(indexname, ' (', tablename, ')')) AS \"IND_NAME\",\n" +
                        "       indexname                                 AS \"INDEX_NAME\",\n" +
                        "       tablename                                 AS \"TABLE_NAME\"\n" +
                        "  FROM pg_indexes\n" +
                        " WHERE schemaname = ?\n" +
                        "  ORDER BY 1";
            } else if (SNOWFLAKE.equals(product)) {
                // no indexes in Snowflake
                return "SELECT NULL  AS ind_name,\n" +
                        "       NULL  AS index_name,\n" +
                        "       NULL  AS table_name\n" +
                        " WHERE 'x' = ?";
            } else if (H2.equals(product)) {
                return "SELECT (concat(index_name, ' (', table_name, ')')) AS \"IND_NAME\",\n" +
                        "       index_name                                  AS \"INDEX_NAME\",\n" +
                        "       table_name                                  AS \"TABLE_NAME\"\n" +
                        "  FROM information_schema.indexes\n" +
                        " WHERE table_schema = ?\n" +
                        " ORDER BY 1";
            } else if (SQLITE.equals(product)) {
                return "SELECT name || ' (' || tbl_name || ')' AS \"IND_NAME\",\n" +
                        "       name                            AS \"INDEX_NAME\",\n" +
                        "       tbl_name                        AS \"TABLE_NAME\"\n" +
                        "  FROM sqlite_schema\n" +
                        " WHERE type = 'index' \n" +
                        "   AND ? IS NOT NULL\n" +
                        " ORDER BY 1";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showIndexDetails(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("select INDEX_TYPE, TABLE_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE, COLLATION, CARDINALITY, SUB_PART, \n" +
                "\t\t\t\tPACKED, NULLABLE, COMMENT\n" +
                "\t\t\t\tFROM INFORMATION_SCHEMA.STATISTICS WHERE INDEX_NAME = ? AND cast(TABLE_SCHEMA as binary) = ?\n" +
                "\t\t\t\tORDER BY INDEX_NAME, SEQ_IN_INDEX")) {
            if (POSTGRES.equals(product)) {
                // no dictionary view in postgres for index columns
                return "SELECT i.relname    AS index_name,\n" +
                        "       a.attname    AS column_name,\n" +
                        "       a.attnum     AS seq_in_index\n" +
                        "  FROM pg_namespace  s\n" +
                        "  JOIN pg_class      t  ON t.relnamespace = s.oid\n" +
                        "  JOIN pg_index      ix ON ix.indrelid = t.oid\n" +
                        "  JOIN pg_class      i  ON i.oid = ix.indexrelid\n" +
                        "  JOIN pg_attribute  a  ON a.attrelid = t.oid AND a.attnum = ANY (ix.indkey)\n" +
                        " WHERE t.relkind = 'r'\n" +
                        "   AND i.relname = ?\n" +
                        "   AND s.nspname = ?\n" +
                        " ORDER BY i.relname, a.attnum";
            } else if (SNOWFLAKE.equals(product)) {
                // no indexes in Snowflake
                return "SELECT NULL  AS index_name,\n" +
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
                        " WHERE 'x' IN (?, ?)";
            } else if (H2.equals(product)) {
                return "SELECT index_name,\n" +
                        "       index_type_name,\n" +
                        "       column_name,\n" +
                        "       sql\n" +
                        "  FROM information_schema.indexes\n" +
                        " WHERE index_name = ?\n" +
                        "   AND table_schema = ?";
            } else if (SQLITE.equals(product)) {
                return "SELECT s.name    AS index_name,\n" +
                        "       i.name    AS column_name,\n" +
                        "       cid       AS seq_in_index\n" +
                        "  FROM sqlite_schema s,\n" +
                        "       pragma_index_info (?) i\n" +
                        " WHERE type = 'index'\n" +
                        "   AND ? IS NOT NULL\n" +
                        " ORDER BY i.cid";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showTableIndexColumns(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("select INDEX_NAME, INDEX_TYPE, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE, COLLATION, CARDINALITY, SUB_PART, \n" +
                "PACKED, NULLABLE, COMMENT FROM INFORMATION_SCHEMA.STATISTICS \n" +
                "WHERE (COLLATION(?) NOT LIKE '%chinese%' \n" +
                "or COLLATION(?) NOT LIKE '%japanese%' \n" +
                "or COLLATION(?) NOT LIKE '%korean%') \n" +
                "and cast(TABLE_NAME as binary) = ? AND cast(TABLE_SCHEMA as binary) = ? \n" +
                "UNION \n" +
                "select INDEX_NAME, INDEX_TYPE, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE, COLLATION, CARDINALITY, SUB_PART, \n" +
                "PACKED, NULLABLE, COMMENT FROM INFORMATION_SCHEMA.STATISTICS \n" +
                "WHERE (COLLATION(?) LIKE '%chinese%' \n" +
                "or COLLATION(?) LIKE '%japanese%' \n" +
                "or COLLATION(?) LIKE '%korean%') \n" +
                "and TABLE_NAME = ? AND TABLE_SCHEMA = ? \n" +
                "ORDER BY INDEX_NAME, SEQ_IN_INDEX")) {
            if (POSTGRES.equals(product)) {
                // no dictionary view in postgres for index columns
                return "SELECT i.relname    AS index_name,\n" +
                        "       a.attname    AS column_name,\n" +
                        "       a.attnum     AS seq_in_index\n" +
                        "  FROM pg_namespace  s\n" +
                        "  JOIN pg_class      t  ON t.relnamespace = s.oid\n" +
                        "  JOIN pg_index      ix ON ix.indrelid = t.oid\n" +
                        "  JOIN pg_class      i  ON i.oid = ix.indexrelid\n" +
                        "  JOIN pg_attribute  a  ON a.attrelid = t.oid AND a.attnum = ANY (ix.indkey)\n" +
                        " WHERE t.relkind = 'r'\n" +
                        "   AND coalesce(?, ?, ?, 'x') IS NOT NULL\n" +
                        "   AND t.relname = ?\n" +
                        "   AND s.nspname = ?\n" +
                        "   AND coalesce(?, ?, ?, ?, ?, 'x') IS NOT NULL\n" +
                        " ORDER BY i.relname, a.attnum";
            } else if (SNOWFLAKE.equals(product)) {
                // no indexes in Snowflake
                return "SELECT NULL  AS index_name,\n" +
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
            } else if (H2.equals(product)) {
                return "SELECT index_name,\n" +
                        "       column_name,\n" +
                        "       ordinal_position,\n" +
                        "       sql\n" +
                        "  FROM information_schema.indexes\n" +
                        " WHERE coalesce(?, ?, ?, 'x') IS NOT NULL\n" +
                        "   AND table_name = ?\n" +
                        "   AND table_schema = ?\n" +
                        "   AND coalesce(?, ?, ?, ?, ?, 'x') IS NOT NULL\n" +
                        " ORDER BY index_name,\n" +
                        "          ordinal_position";
            } else if (SQLITE.equals(product)) {
                return "SELECT DISTINCT\n" +
                        "       i.name     AS index_name,\n" +
                        "       c.name     AS column_name,\n" +
                        "       c.seqno    AS seq_in_index,\n" +
                        "       CASE i.`unique`\n" +
                        "          WHEN 0   THEN\n" +
                        "             'NO'\n" +
                        "          WHEN 42  THEN\n" +
                        "             coalesce(?, ?, ?)\n" +
                        "          ELSE\n" +
                        "             'YES'\n" +
                        "       END        AS is_unique\n" +
                        "  FROM sqlite_schema s,\n" +
                        "       pragma_index_list (?) i,\n" +
                        "       pragma_index_info (i.name) c\n" +
                        " WHERE s.type = 'table'\n" +
                        "   AND coalesce(?, ?, ?, ?, ?, ?, 'x') IS NOT NULL\n" +
                        " ORDER BY c.seqno";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showTableConstraints(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS\n" +
                "\t    \t\tWHERE TABLE_NAME = ? AND cast(TABLE_SCHEMA as binary) = ?")) {
            if (SQLITE.equals(product)) {
                return "SELECT constraint_name, constraint_type \n" +
                        "  FROM (\n" +
                        "         SELECT s.tbl_name    AS table_name,\n" +
                        "                i.name        AS constraint_name,\n" +
                        "                i.origin      AS constraint_type\n" +
                        "           FROM sqlite_schema s,\n" +
                        "                pragma_index_list (s.tbl_name) i\n" +
                        "          WHERE s.type = 'index'\n" +
                        "         UNION\n" +
                        "         SELECT s.name           AS table_name,\n" +
                        "                f.`table`\n" +
                        "                || '_fk_'\n" +
                        "                || ( f.seq + 1 ) AS constraint_name,\n" +
                        "                'fk'             AS constraint_type\n" +
                        "           FROM sqlite_schema s,\n" +
                        "                pragma_foreign_key_list (s.name) f\n" +
                        "          WHERE s.type = 'table'\n" +
                        "       )\n" +
                        "  WHERE table_name = ?\n" +
                        "    AND ? IS NOT NULL";
            } else {
                return "SELECT constraint_name,\n" +
                        "       constraint_type\n" +
                        "  FROM information_schema.table_constraints\n" +
                        " WHERE table_name = ?\n" +
                        "   AND table_schema = ?";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showCheckConstraints(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("SELECT t.table_schema, \n" +
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
                "AND c.constraint_schema = t.constraint_schema")) {
            if (SNOWFLAKE.equals(product) || SQLITE.equals(product) || H2.equals(product)) {
                // no check constraints view
                return "SELECT NULL  AS table_schema,\n" +
                        "       NULL  AS table_name,\n" +
                        "       NULL  AS constraint_name,\n" +
                        "       NULL  AS constraint_type,\n" +
                        "       NULL  AS is_deferrable,\n" +
                        "       NULL  AS initially_deferred,\n" +
                        "       NULL  AS check_clause\n" +
                        " WHERE 'x' IN (?, ?)";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showViews(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("select TABLE_NAME from information_schema.views where cast(TABLE_SCHEMA as binary) = ?")) {
            if (SQLITE.equals(product)) {
                return "SELECT name AS \"TABLE_NAME\"\n" +
                        "  FROM sqlite_schema\n" +
                        " WHERE type = 'view'\n" +
                        "   AND ? IS NOT NULL\n" +
                        " ORDER BY name";
            } else {
                return "SELECT table_name AS \"TABLE_NAME\"\n" +
                        "  FROM information_schema.views\n" +
                        " WHERE table_schema = ?\n" +
                        " ORDER BY table_name";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showViewColumnsShort(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("select COLUMN_NAME from information_schema.Columns where cast(TABLE_SCHEMA as binary) = ? and cast(TABLE_NAME as binary) = ?")) {
            if (SQLITE.equals(product)) {
                return "SELECT name AS \"COLUMN_NAME\",\n" +
                        "      ?    AS schema_name\n" +
                        "  FROM pragma_table_info (?)\n" +
                        " ORDER BY cid";
            } else {
                return "SELECT column_name AS \"COLUMN_NAME\"\n" +
                        "  FROM information_schema.columns\n" +
                        " WHERE table_schema = ?\n" +
                        "   AND table_name = ?\n" +
                        " ORDER BY ordinal_position";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showViewColumns(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("select COLUMN_NAME , ORDINAL_POSITION , COLUMN_DEFAULT , IS_NULLABLE , \n" +
                "\t\t\t\t\t\t\t\tDATA_TYPE , NUMERIC_PRECISION , NUMERIC_SCALE , COLUMN_COMMENT \n" +
                "\t\t\t\t\t\t\t\tfrom information_schema.Columns where \n" +
                "\t\t\t\t\t\t\t\t(COLLATION(?) NOT LIKE '%chinese%' \n" +
                "                                and COLLATION(?) NOT LIKE '%japanese%' \n" +
                "                                and COLLATION(?) NOT LIKE '%korean%')\n" +
                "                                and cast(TABLE_NAME as binary) = ?\n" +
                "                                AND cast(TABLE_SCHEMA as binary)= ? \n" +
                "                         UNION\n" +
                "                         select COLUMN_NAME , ORDINAL_POSITION , COLUMN_DEFAULT , IS_NULLABLE ,\n" +
                "                                DATA_TYPE , NUMERIC_PRECISION , NUMERIC_SCALE , COLUMN_COMMENT\n" +
                "                                from information_schema.Columns where\n" +
                "                                (COLLATION(?) LIKE '%chinese%' \n" +
                "                                or COLLATION(?) LIKE '%japanese%' \n" +
                "                                or COLLATION(?) LIKE '%korean%')\n" +
                "                                and TABLE_NAME = ?\n" +
                "                                AND TABLE_SCHEMA = ?")) {
            if (POSTGRES.equals(product)) {
                // no column comments
                return "SELECT column_name,\n" +
                        "       ordinal_position,\n" +
                        "       column_default,\n" +
                        "       is_nullable,\n" +
                        "       data_type,\n" +
                        "       numeric_precision,\n" +
                        "       numeric_scale,\n" +
                        "       NULL AS column_comment\n" +
                        "  FROM information_schema.columns\n" +
                        " WHERE coalesce(?, ?, ?, 'x') IS NOT NULL\n" +
                        "   AND table_name = ?\n" +
                        "   AND table_schema = ?\n" +
                        "   AND coalesce(?, ?, ?, ?, ?, 'x') IS NOT NULL\n" +
                        " ORDER BY ordinal_position";
            } else if (SNOWFLAKE.equals(product)) {
                return "SELECT column_name,\n" +
                        "       ordinal_position,\n" +
                        "       column_default,\n" +
                        "       is_nullable,\n" +
                        "       data_type,\n" +
                        "       numeric_precision,\n" +
                        "       numeric_scale,\n" +
                        "       comment AS column_comment\n" +
                        "  FROM information_schema.columns\n" +
                        " WHERE coalesce(?, ?, ?, 'x') IS NOT NULL\n" +
                        "   AND table_name = ?\n" +
                        "   AND table_schema = ?\n" +
                        "   AND coalesce(?, ?, ?, ?, ?, 'x') IS NOT NULL\n" +
                        " ORDER BY ordinal_position";
            } else if (H2.equals(product)) {
                return "SELECT column_name,\n" +
                        "       ordinal_position,\n" +
                        "       column_default,\n" +
                        "       is_nullable,\n" +
                        "       data_type,\n" +
                        "       numeric_precision,\n" +
                        "       numeric_scale,\n" +
                        "       remarks\n" +
                        "  FROM information_schema.columns\n" +
                        " WHERE coalesce(?, ?, ?, 'x') IS NOT NULL\n" +
                        "   AND table_name = ?\n" +
                        "   AND table_schema = ?\n" +
                        "   AND coalesce(?, ?, ?, ?, ?, 'x') IS NOT NULL\n" +
                        " ORDER BY ordinal_position";
            } else if (SQLITE.equals(product)) {
                return "SELECT name        AS column_name,\n" +
                        "       type        AS data_type,\n" +
                        "       CASE `notnull`\n" +
                        "          WHEN 0 THEN\n" +
                        "             'YES'\n" +
                        "          WHEN 42 THEN\n" +
                        "             coalesce(?, ?, ?)\n" +
                        "          ELSE\n" +
                        "             'NO'\n" +
                        "       END         AS is_nullable,\n" +
                        "       dflt_value  AS column_default\n" +
                        "  FROM pragma_table_info (?)\n" +
                        " WHERE coalesce(?, ?, ?, ?, ?, ?, 'x') IS NOT NULL\n" +
                        " ORDER BY cid";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showViewDetails(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("SELECT VIEW_DEFINITION, CHECK_OPTION, IS_UPDATABLE, DEFINER, SECURITY_TYPE FROM\n" +
                "    \t\t\t           INFORMATION_SCHEMA.VIEWS WHERE (COLLATION(?) NOT LIKE '%chinese%' \n" +
                "                           and COLLATION(?) NOT LIKE '%japanese%' \n" +
                "                           and COLLATION(?) NOT LIKE '%korean%')\n" +
                "                           and cast(TABLE_NAME as binary) = ? AND cast(TABLE_SCHEMA as binary)=?\n" +
                "                         UNION\n" +
                "                         SELECT VIEW_DEFINITION, CHECK_OPTION, IS_UPDATABLE, DEFINER, SECURITY_TYPE FROM\n" +
                "    \t\t\t           INFORMATION_SCHEMA.VIEWS WHERE (COLLATION(?) LIKE '%chinese%' \n" +
                "                           or COLLATION(?) NOT LIKE '%japanese%' \n" +
                "                           or COLLATION(?) NOT LIKE '%korean%')\n" +
                "                           and TABLE_NAME = ? AND TABLE_SCHEMA = ?")) {
            if (POSTGRES.equals(product)) {
                return "SELECT view_definition,\n" +
                        "       check_option,\n" +
                        "       is_updatable,\n" +
                        "       is_insertable_into,\n" +
                        "       is_trigger_updatable,\n" +
                        "       is_trigger_deletable,\n" +
                        "       is_trigger_insertable_into\n" +
                        "  FROM information_schema.views\n" +
                        " WHERE coalesce(?, ?, ?, 'x') IS NOT NULL\n" +
                        "   AND table_name = ?\n" +
                        "   AND table_schema = ?\n" +
                        "   AND coalesce(?, ?, ?, ?, ?, 'x') IS NOT NULL";
            } else if (SNOWFLAKE.equals(product)) {
                return "SELECT view_definition,\n" +
                        "       check_option,\n" +
                        "       is_updatable,\n" +
                        "       insertable_into,\n" +
                        "       is_secure,\n" +
                        "       created,\n" +
                        "       last_altered,\n" +
                        "       comment\n" +
                        "  FROM information_schema.views\n" +
                        " WHERE coalesce(?, ?, ?, 'x') IS NOT NULL\n" +
                        "   AND table_name = ?\n" +
                        "   AND table_schema = ?\n" +
                        "   AND coalesce(?, ?, ?, ?, ?, 'x') IS NOT NULL";
            } else if (H2.equals(product)) {
                return "SELECT view_definition,\n" +
                        "       check_option,\n" +
                        "       is_updatable,\n" +
                        "       status,\n" +
                        "       remarks" +
                        "  FROM information_schema.views\n" +
                        " WHERE coalesce(?, ?, ?, 'x') IS NOT NULL\n" +
                        "   AND table_name = ?\n" +
                        "   AND table_schema = ?\n" +
                        "   AND coalesce(?, ?, ?, ?, ?, 'x') IS NOT NULL";
            } else if (SQLITE.equals(product)) {
                return "SELECT sql AS view_definition\n" +
                        "  FROM sqlite_schema\n" +
                        " WHERE type = 'view'\n" +
                        "   AND name = ?";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showProcedures(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("select SPECIFIC_NAME from information_schema.routines where ROUTINE_TYPE = 'PROCEDURE' and cast(ROUTINE_SCHEMA as binary) = ?")) {
            if (POSTGRES.equals(product)) {
                return "SELECT p.proname AS \"SPECIFIC_NAME\"\n" +
                        "  FROM pg_proc       p\n" +
                        "  JOIN pg_namespace  n\n" +
                        "    ON p.pronamespace = n.oid\n" +
                        " WHERE p.prokind = 'p'\n" +
                        "   AND n.nspname = ?";
            } else if (SNOWFLAKE.equals(product)) {
                return "SELECT procedure_name AS specific_name\n" +
                        "  FROM information_schema.procedures\n" +
                        " WHERE procedure_schema = ?\n" +
                        " ORDER BY procedure_name";
            } else if (H2.equals(product) || SQLITE.equals(product)) {
                return "SELECT NULL AS specific_name WHERE 'x' = ?";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showFunctions(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("select SPECIFIC_NAME from information_schema.routines where ROUTINE_TYPE = 'FUNCTION' and cast(ROUTINE_SCHEMA as binary) = ?")) {
            if (POSTGRES.equals(product)) {
                return "SELECT routine_name AS \"SPECIFIC_NAME\"\n" +
                        "  FROM information_schema.routines\n" +
                        " WHERE routine_type = 'FUNCTION'\n" +
                        "   AND routine_schema = ?\n" +
                        " ORDER BY specific_name";
            } else if (SNOWFLAKE.equals(product)) {
                return "SELECT function_name AS specific_name\n" +
                        "  FROM information_schema.functions\n" +
                        " WHERE function_schema = ?" +
                        " ORDER BY function_name\n";
            } else if (H2.equals(product)) {
                return "SELECT alias_name AS specific_name\n" +
                        "  FROM information_schema.function_aliases\n" +
                        " WHERE alias_schema = ?";
            } else if (SQLITE.equals(product)) {
                return "SELECT NULL AS specific_name WHERE 'x' = ?";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showFunctionOrProcedureDetail(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        // the very same query is issued for function and procedure details
        if (sql.equals("select routine_definition from information_schema.routines where cast(routine_schema as binary) = ? and routine_name = ?")) {
            if (POSTGRES.equals(product)) {
                // one view for functions and procedures in PostgreSQL as in MySQL
                return "SELECT routine_definition" +
                        "  FROM information_schema.routines\n" +
                        " WHERE routine_schema = ?\n" +
                        "   AND routine_name = ?\n";
            } else if (SNOWFLAKE.equals(product)) {
                // dedicated views for functions and procedures in Snowflake
                return "SELECT routine_definition\n" +
                        "  FROM (\n" +
                        "          SELECT function_schema      AS routine_schema,\n" +
                        "                 function_name        AS routine_name,\n" +
                        "                 function_definition  AS routine_definition\n" +
                        "            FROM information_schema.functions\n" +
                        "          UNION ALL\n" +
                        "          SELECT procedure_schema      AS routine_schema,\n" +
                        "                 procedure_name        AS routine_name,\n" +
                        "                 procedure_definition  AS routine_definition\n" +
                        "            FROM information_schema.procedures\n" +
                        "       )\n" +
                        " WHERE routine_schema = ?\n" +
                        "   AND routine_name = ?";
            } else if (H2.equals(product)) {
                // H2 supports only functions, no procedures
                return "SELECT source\n" +
                        "  FROM information_schema.function_aliases\n" +
                        " WHERE alias_schema = ?\n" +
                        "   AND alias_name = ?";
            } else if (SQLITE.equals(product)) {
                return "SELECT NULL AS specific_name WHERE 'x' = ?";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showTriggers(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("select TRIGGER_NAME from information_schema.triggers  where trigger_schema = ?  ")) {
            if (POSTGRES.equals(product) || H2.equals(product)) {
                return "SELECT trigger_name AS \"TRIGGER_NAME\"\n" +
                        "  FROM information_schema.triggers\n" +
                        " WHERE trigger_schema = ?\n" +
                        " ORDER BY trigger_name";
            } else if (SNOWFLAKE.equals(product)) {
                // no triggers in Snowflake
                return "SELECT null AS trigger_name WHERE 'x' = ?";
            } else if (SQLITE.equals(product)) {
                return "SELECT name AS \"TRIGGER_NAME\"\n" +
                        "  FROM sqlite_schema\n" +
                        " WHERE type = 'trigger'\n" +
                        "   AND ? IS NOT NULL";
            }
        }
        return sql;
    }

    @SQLRewrite
    public String showTriggerDetails(String sql, String product) {
        if (MYSQL.equals(product)) {
            return sql;
        }
        if (sql.equals("select action_statement from information_schema.triggers where cast(trigger_schema as binary) = ? and trigger_name = ?")) {
            if (POSTGRES.equals(product)) {
                return "SELECT action_statement\n" +
                        "  FROM information_schema.triggers\n" +
                        " WHERE trigger_schema = ?\n" +
                        "   AND trigger_name = ?";
            } else if (SNOWFLAKE.equals(product)) {
                // no triggers in Snowflake
                return "SELECT null AS action_statement WHERE 'x' = ?";
            } else if (H2.equals(product)) {
                return "SELECT sql\n" +
                        "  FROM information_schema.triggers\n" +
                        " WHERE trigger_schema = ?\n" +
                        "   AND trigger_name = ?";
            } else if (SQLITE.equals(product)) {
                return "SELECT sql\n" +
                        "  FROM sqlite_schema\n" +
                        " WHERE type = 'trigger'\n" +
                        "   AND ? IS NOT NULL\n" +
                        "   AND name = ?";
            }
        }
        return sql;
    }


}
