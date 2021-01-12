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
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

public class DriverTests {

    @Test
    void jdbcProxy_to_PostgreSQL() {
        SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
        dataSource.setUrl("jdbc:proxy:jdbc:postgresql://localhost:5432/postgres");
        dataSource.setUsername("postgres");
        dataSource.setPassword("postgres");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String actual = jdbcTemplate.queryForObject("select 'Hello World!' as test", String.class);
        Assertions.assertEquals("Hello World!", actual);
    }

    @Test
    void jdbcProxy_to_MySQL() {
        SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
        dataSource.setUrl("jdbc:proxy:jdbc:mysql://localhost:3306/mysql");
        dataSource.setUsername("root");
        dataSource.setPassword("mysql");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String actual = jdbcTemplate.queryForObject("select 'Hello World!' as test", String.class);
        Assertions.assertEquals("Hello World!", actual);
    }

    @Test
    void jdbcProxy_to_MySQL_with_original_URL() {
        SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
        dataSource.setUrl("jdbc:mysql://localhost:3306/mysql");
        dataSource.setUsername("root");
        dataSource.setPassword("mysql");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String actual = jdbcTemplate.queryForObject("select 'Hello World!' as test", String.class);
        Assertions.assertEquals("Hello World!", actual);
    }

    @Test
    void mySQL_with_old_Driver() {
        SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://localhost:3306/mysql");
        dataSource.setUsername("root");
        dataSource.setPassword("mysql");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String actual = jdbcTemplate.queryForObject("select 'Hello World!' as test", String.class);
        Assertions.assertEquals("Hello World!", actual);
    }

    @Test
    void mySQL_with_old_Driver_to_PostgreSQL() {
        SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://jdbc:postgresql://localhost:5432/postgres:/SomeDbToRemove");
        dataSource.setUsername("postgres");
        dataSource.setPassword("postgres");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String actual = jdbcTemplate.queryForObject("select 'Hello World!' as test", String.class);
        Assertions.assertEquals("Hello World!", actual);
    }

    //@Test
    @SuppressWarnings("unused")
    void jdbcProxy_to_Snowflake() {
        SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
        dataSource.setUrl("jdbc:proxy:jdbc:snowflake://<account>.snowflakecomputing.com:443");
        dataSource.setUsername("<username>");
        dataSource.setPassword("<password>");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String actual = jdbcTemplate.queryForObject("select 'Hello World!' as test", String.class);
        Assertions.assertEquals("Hello World!", actual);
    }

}
