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

package com.trivadis.jdbcproxy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseMetaDataTests {

    private void deployModel(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute(
                "CREATE TABLE dept (\n" +
                "   deptno   NUMERIC(2,0)  CONSTRAINT pk_dept PRIMARY KEY,\n" +
                "   dname    VARCHAR2(14)  NOT NULL,\n" +
                "   loc      VARCHAR2(13)  NOT NULL \n" +
                ")");
        jdbcTemplate.execute(
            "CREATE TABLE emp (\n" +
                "   empno    NUMERIC(4,0)   CONSTRAINT pk_emp PRIMARY KEY,\n" +
                "   ename    VARCHAR(10)    NOT NULL,\n" +
                "   job      VARCHAR(9)     NOT NULL,\n" +
                "   mgr      NUMERIC(4,0),\n" +
                "   hiredate DATE           NOT NULL,\n" +
                "   sal      NUMERIC(7,2)   NOT NULL,\n" +
                "   comm     NUMERIC(7,2),\n" +
                "   deptno   NUMERIC(2,0)   NOT NULL CONSTRAINT fk_deptno REFERENCES dept,\n" +
                "   CONSTRAINT fk_mgr FOREIGN KEY (mgr) REFERENCES emp\n" +
                ")");
    }

    @Nested
    @DisplayName("when using SQLite via proxy")
    class SQLiteWithProxy {
        @Test
        void getImportedKeys() throws SQLException {
            SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
            dataSource.setUrl("jdbc:proxy:jdbc:sqlite::memory:");
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            deployModel(jdbcTemplate);
            ResultSet rs = dataSource.getConnection().getMetaData().getImportedKeys(null, "main", "emp");
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals("dept", rs.getString("PKTABLE_NAME"));
            Assertions.assertEquals("deptno", rs.getString("PKCOLUMN_NAME"));
            Assertions.assertEquals("deptno", rs.getString("FKCOLUMN_NAME"));
            Assertions.assertTrue(rs.getString("FK_NAME").startsWith("emp_fk"));
            Assertions.assertEquals("dept__IDX", rs.getString("PK_NAME"));
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals("emp", rs.getString("PKTABLE_NAME"));
            Assertions.assertEquals("empno", rs.getString("PKCOLUMN_NAME"));
            Assertions.assertEquals("mgr", rs.getString("FKCOLUMN_NAME"));
            Assertions.assertTrue(rs.getString("FK_NAME").startsWith("emp_fk"));
            Assertions.assertEquals("emp__IDX", rs.getString("PK_NAME"));
        }
    }

    @Nested
    @DisplayName("when using SQLite without proxy")
    class SQLiteWithoutProxy {
        @Test
        void getImportedKeys() throws SQLException {
            SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
            dataSource.setUrl("jdbc:sqlite::memory:");
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            deployModel(jdbcTemplate);
            ResultSet rs = dataSource.getConnection().getMetaData().getImportedKeys(null, "main", "emp");
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals("dept", rs.getString("PKTABLE_NAME"));
            Assertions.assertEquals("deptno", rs.getString("PKCOLUMN_NAME"));
            Assertions.assertEquals("deptno", rs.getString("FKCOLUMN_NAME"));
            Assertions.assertTrue(rs.getString("FK_NAME").isEmpty()); // a problem for SQLDev
            Assertions.assertTrue(rs.getString("PK_NAME").isEmpty()); // a problem for SQLDev
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals("emp", rs.getString("PKTABLE_NAME"));
            Assertions.assertEquals("empno", rs.getString("PKCOLUMN_NAME"));
            Assertions.assertEquals("mgr", rs.getString("FKCOLUMN_NAME"));
            Assertions.assertEquals("fk_mgr", rs.getString("FK_NAME"));
            Assertions.assertTrue(rs.getString("PK_NAME").isEmpty()); // a problem for SQLDev
        }
    }

}
