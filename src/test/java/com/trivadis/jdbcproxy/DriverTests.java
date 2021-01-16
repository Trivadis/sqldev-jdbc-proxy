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

import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

public class DriverTests {

    private String getTargetProductName(DataSource dataSource) throws SQLException {
        return ((ProxyDatabaseMetaData)dataSource.getConnection().getMetaData()).getTargetDatabaseProductName();
    }

    private Driver getMysqlDriverSQLDevStyle() {
        try {
            return (Driver) Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    @Nested
    @DisplayName("when using proxy URL (jdbc:proxy)")
    class WhenUsingJdbcProxy {
        @Test
        void to_PostgreSQL() throws SQLException {
            SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
            dataSource.setUrl("jdbc:proxy:jdbc:postgresql://localhost:5432/postgres");
            dataSource.setUsername("postgres");
            dataSource.setPassword("postgres");
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            String actual = jdbcTemplate.queryForObject("select 'Hello World!' as test", String.class);
            Assertions.assertEquals("Hello World!", actual);
            Assertions.assertEquals("MySQL", dataSource.getConnection().getMetaData().getDatabaseProductName());
            Assertions.assertEquals("PostgreSQL", getTargetProductName(dataSource));
        }

        @Test
        void to_MySQL() throws SQLException {
            SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
            dataSource.setUrl("jdbc:proxy:jdbc:mysql://localhost:3306/mysql");
            dataSource.setUsername("root");
            dataSource.setPassword("mysql");
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            String actual = jdbcTemplate.queryForObject("select 'Hello World!' as test", String.class);
            Assertions.assertEquals("Hello World!", actual);
            Assertions.assertEquals("MySQL", dataSource.getConnection().getMetaData().getDatabaseProductName());
            Assertions.assertEquals("MySQL", getTargetProductName(dataSource));
        }

        @Test
        void to_Snowflake() throws SQLException, IOException {
            try {
                FileInputStream fis = new FileInputStream(System.getProperty("user.home") + File.separator + "snowflake-test.properties");
                Properties props = new Properties();
                props.load(fis);
                if (Boolean.parseBoolean(props.getProperty("enabled", "true"))) {
                    SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
                    dataSource.setUrl("jdbc:proxy:jdbc:snowflake://" + props.getProperty("account") + ".snowflakecomputing.com:443");
                    dataSource.setUsername(props.getProperty("username"));
                    dataSource.setPassword(props.getProperty("password"));
                    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
                    String actual = jdbcTemplate.queryForObject("select 'Hello World!' as test", String.class);
                    Assertions.assertEquals("Hello World!", actual);
                    Assertions.assertEquals("MySQL", dataSource.getConnection().getMetaData().getDatabaseProductName());
                    Assertions.assertEquals("Snowflake", getTargetProductName(dataSource));
                }
            } catch (FileNotFoundException e) {
                // ignore test, when snowflake-test.properties are not available.
            }
        }
    }

    @Nested
    @DisplayName("when using direct URL (jdbc:mysql://localhost)")
    class WhenUsingWithoutProxyURL {
        /**
         * This method uses the first registered driver that accepts the JDBC URL.
         * Drivers are registered based on the JAR's service configuration (META-INF/services).
         * In the test environment this is always the original MySQL driver "com.mysql.cj.jdbc.Driver",
         * In the production environment it's the proxy driver (based on position in class path).
         */
        @Test
        void with_default_Driver_without_Proxy() throws SQLException {
            SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
            dataSource.setUrl("jdbc:mysql://localhost:3306/mysql");
            dataSource.setUsername("root");
            dataSource.setPassword("mysql");
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            String actual = jdbcTemplate.queryForObject("select 'Hello World!' as test", String.class);
            Assertions.assertEquals("Hello World!", actual);
            Assertions.assertEquals("MySQL", dataSource.getConnection().getMetaData().getDatabaseProductName());
            ClassCastException ex = Assertions.assertThrows(ClassCastException.class, () -> getTargetProductName(dataSource));
            Assertions.assertTrue(ex.getMessage().contains("com.mysql.cj.jdbc.DatabaseMetaDataUsingInfoSchema cannot be cast to"));
        }

        /**
         * In this case we load the driver similarly to SQL Developer. We load the class ourselves,
         * therefore service configuration is irrelevant and the proxy driver is loaded.
         */
        @Test
        void with_chosen_Driver_with_Proxy() throws SQLException {
            SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
            dataSource.setDriver(getMysqlDriverSQLDevStyle());
            dataSource.setUrl("jdbc:mysql://localhost:3306/mysql");
            dataSource.setUsername("root");
            dataSource.setPassword("mysql");
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            String actual = jdbcTemplate.queryForObject("select 'Hello World!' as test", String.class);
            Assertions.assertEquals("Hello World!", actual);
            Assertions.assertEquals("MySQL", dataSource.getConnection().getMetaData().getDatabaseProductName());
            Assertions.assertTrue(dataSource.getDriver() instanceof ProxyDriver);
            Assertions.assertEquals("MySQL", getTargetProductName(dataSource));
        }
    }

    @Nested
    @DisplayName("when using proxy URL (jdbc:mysql://jdbc:)")
    class WhenUsingWithProxyURL {
        @Test
        void to_MySQL() throws SQLException {
            SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
            dataSource.setDriver(getMysqlDriverSQLDevStyle());
            dataSource.setUrl("jdbc:mysql://jdbc:mysql://localhost:3306/mysql:/");
            dataSource.setUsername("root");
            dataSource.setPassword("mysql");
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            String actual = jdbcTemplate.queryForObject("select 'Hello World!' as test", String.class);
            Assertions.assertEquals("Hello World!", actual);
            Assertions.assertEquals("MySQL", dataSource.getConnection().getMetaData().getDatabaseProductName());
            Assertions.assertEquals("MySQL", getTargetProductName(dataSource));
        }

        @Test
        void to_PostgreSQL() throws SQLException {
            SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
            dataSource.setDriver(getMysqlDriverSQLDevStyle());
            dataSource.setUrl("jdbc:mysql://jdbc:postgresql://localhost:5432/postgres:/SomeDbToRemove");
            dataSource.setUsername("postgres");
            dataSource.setPassword("postgres");
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            String actual = jdbcTemplate.queryForObject("select 'Hello World!' as test", String.class);
            Assertions.assertEquals("Hello World!", actual);
            Assertions.assertEquals("MySQL", dataSource.getConnection().getMetaData().getDatabaseProductName());
            Assertions.assertEquals("PostgreSQL", getTargetProductName(dataSource));
        }

        @Test
        void to_SQLite() throws SQLException {
            SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
            dataSource.setDriver(getMysqlDriverSQLDevStyle());
            dataSource.setUrl("jdbc:mysql://jdbc:sqlite::memory::/SomeDbToRemove");
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            String actual = jdbcTemplate.queryForObject("select 'Hello World!' as test", String.class);
            Assertions.assertEquals("Hello World!", actual);
            Assertions.assertEquals("MySQL", dataSource.getConnection().getMetaData().getDatabaseProductName());
            Assertions.assertEquals("SQLite", getTargetProductName(dataSource));
        }

        @Test
        void to_H2() throws IOException, SQLException {
            SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
            dataSource.setDriver(getMysqlDriverSQLDevStyle());
            File file = File.createTempFile("h2-test-", "");
            dataSource.setUrl("jdbc:mysql://jdbc:h2:" + file.getAbsolutePath() + ":/SomeDbToRemove");
            dataSource.setUsername("sa");
            dataSource.setPassword("sa");
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            String actual = jdbcTemplate.queryForObject("select 'Hello World!' as test", String.class);
            Assertions.assertEquals("Hello World!", actual);
            Assertions.assertEquals("MySQL", dataSource.getConnection().getMetaData().getDatabaseProductName());
            Assertions.assertEquals("H2", getTargetProductName(dataSource));
        }
    }
}
