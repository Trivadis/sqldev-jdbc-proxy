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

public class UrlUtilTests {

    @Test
    public void mysql_proxy_with_port_and_db() {
        String actual = UrlUtil.extractTargetUrl("jdbc:mysql://jdbc:mysql://localhost:3306/mysql:3306/mysql");
        Assertions.assertEquals("jdbc:mysql://localhost:3306/mysql", actual);
    }

    @Test
    public void mysql_proxy_without_port_and_with_db() {
        String actual = UrlUtil.extractTargetUrl("jdbc:mysql://jdbc:mysql://localhost/mysql:/mysql");
        Assertions.assertEquals("jdbc:mysql://localhost/mysql", actual);
    }

    @Test
    public void mysql_proxy_without_port_but_with_dot_and_with_db() {
        String actual = UrlUtil.extractTargetUrl("jdbc:mysql://jdbc:mysql://localhost/mysql:/mysql");
        Assertions.assertEquals("jdbc:mysql://localhost/mysql", actual);
    }

    @Test
    public void mysql_proxy_without_db_leads_to_invalid_target() {
        String actual = UrlUtil.extractTargetUrl("jdbc:mysql://jdbc:mysql://localhost:3306/mysql");
        Assertions.assertEquals("jdbc:mysql://localhost", actual);
    }

    @Test
    public void wrong_url() {
        AssertionError error = Assertions.assertThrows(AssertionError.class,
                () -> UrlUtil.extractTargetUrl("jdbc:mysql2://jdbc:mysql://localhost:3306/mysql:3306:/mysql"));
        Assertions.assertEquals(UrlUtil.INVALID_FORMAT, error.getMessage());
    }

}
