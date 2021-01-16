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


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.trivadis.jdbcproxy.rewrite.SQLRewriter.SQLRewrite;

public class RewriteHelper {
    private final SQLDevNavigatorSQLRewriter rewriter = new SQLDevNavigatorSQLRewriter();
    private final List<Method> fullRewriterMethods = new ArrayList<>();
    private final List<Method> partialRewriterMethods = new ArrayList<>();

    public RewriteHelper() {
        super();
        populateRewriterMethods();
    }

    private void populateRewriterMethods() {
        for (Method method : rewriter.getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(SQLRewrite.class)) {
                SQLRewrite annotation = method.getAnnotation(SQLRewrite.class);
                if (annotation.full()) {
                    fullRewriterMethods.add(method);
                } else {
                    partialRewriterMethods.add(method);
                }
            }
        }
    }

    /**
     * Rewrites a sql in MySQL dialect to the target dialect (based on product).
     */
    public String rewrite(String sql, String product) {
        String result = rewrite(fullRewriterMethods, sql, product);
        return rewrite(partialRewriterMethods, result, product);
    }

    /**
     * Rewrites a call statement written for Oracle Databases to a
     * generic target (independent of the product).
     */
    public String rewriteCall(String sql) {
        final Pattern p = Pattern.compile("(?i)^\\s*BEGIN\\s+(.+?)\\s*;\\s*END\\s*;\\s*$");
        final Matcher m = p.matcher(sql);
        if (m.find()) {
            return "CALL " + m.group(1)
                    .replaceAll("(?i)\\s*TO_NUMBER\\s*\\(\\s*\\?\\s*\\)\\s*", "?")
                    .replaceAll("(?i)\\s*TO_CHAR\\s*\\(\\s*\\?\\s*\\)\\s*", "?");
        }
        return sql;
    }

    private String rewrite(List<Method> methods, String sql, String product) {
        String result = sql;
        for (Method method : methods) {
            try {
                result = (String) method.invoke(rewriter, result, product);
            } catch (IllegalAccessException | InvocationTargetException e ) {
                throw new RuntimeException("Cannot rewrite SQL statement for " + product + ".");
            }
        }
        return result;
    }

}
