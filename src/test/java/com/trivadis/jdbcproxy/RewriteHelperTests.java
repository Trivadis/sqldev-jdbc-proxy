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

import com.trivadis.jdbcproxy.rewrite.RewriteHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class RewriteHelperTests {

    @Nested
    class WhenRewrite {
        @Test
        public void replace_backtick_for_Snowflake() {
            RewriteHelper helper = new RewriteHelper();
            Assertions.assertEquals("select \"Test\"", helper.rewrite("select `Test`", "Snowflake"));
        }

        @Test
        public void dont_replace_backtick_for_SQLite() {
            RewriteHelper helper = new RewriteHelper();
            Assertions.assertEquals("select `Test`", helper.rewrite("select `Test`", "SQLite"));
        }

        @Test
        public void replace_show_databases_for_Snowflake() {
            RewriteHelper helper = new RewriteHelper();
            Assertions.assertTrue(helper.rewrite("show databases", "Snowflake").startsWith("SELECT database_name"));
        }
    }

}
