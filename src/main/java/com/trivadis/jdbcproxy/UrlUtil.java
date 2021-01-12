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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UrlUtil {
    public static final String INVALID_FORMAT = "Invalid proxy URL. Expected format: jdbc:mysql://<targetUrl>:[<port>]/[<database>]";

    /**
     * Extracts the target part of a JDBC URL constructed by SQL Developer.
     * Example: "jdbc:mysql:jdbc:mysql://localhost:3306/mysql:3306/mysql"
     * returns "jdbc:mysql://localhost:3306/mysql".
     */
    private UrlUtil() {
        // do not instantiate
    }

    public static String extractTargetUrl(String url) {
        final Pattern p = Pattern.compile("^(jdbc:mysql:\\/\\/)(.+?)(:([0-9]+)?\\/([^\\/:]+)?)$");
        final Matcher m = p.matcher(url);
        final boolean found = m.find();
        assert found:INVALID_FORMAT;
        return m.group(2);
    }
}
