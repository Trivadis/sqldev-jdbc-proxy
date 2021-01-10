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

import java.sql.*;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver {
    public Driver() throws SQLException {
        super();
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (acceptsURL(url)) {
            if (url.startsWith("jdbc:proxy:")) {
                String targetUrl = url.substring(11);
                if (targetUrl.startsWith("jdbc:mysql:")) {
                    return connect(targetUrl, info);
                } else {
                    java.sql.Driver targetDriver;
                    targetDriver = DriverManager.getDriver(targetUrl);
                    return targetDriver.connect(targetUrl, info);
                }
            } else {
                // original MySQL JDBC URL
                Enumeration<java.sql.Driver> drivers = DriverManager.getDrivers();
                while (drivers.hasMoreElements()) {
                    java.sql.Driver targetDriver = drivers.nextElement();
                    if (targetDriver instanceof com.mysql.cj.jdbc.Driver) {
                        return targetDriver.connect(url, info);
                    }
                }
                throw new SQLException("Cannot connect. Cannot find MySQL driver.");
            }
        } else {
            throw new SQLException("Cannot connect. JDBC URL " + url + " is not supported.");
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        if (url != null) {
            return url.startsWith("jdbc:proxy:") || url.startsWith("jdbc:mysql:");
        }
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            throw new RuntimeException("Driver cannot be registered.");
        }
    }
}