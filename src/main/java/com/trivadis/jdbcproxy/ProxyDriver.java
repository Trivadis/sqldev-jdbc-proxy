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

public class ProxyDriver implements Driver {
    public ProxyDriver() {
        super();
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (acceptsURL(url)) {
            if (url.startsWith("jdbc:proxy:")) {
                String targetUrl = url.substring("jdbc:proxy:".length());
                if (targetUrl.startsWith("jdbc:mysql:")) {
                    return connect(targetUrl, info);
                } else {
                    Driver targetDriver;
                    targetDriver = DriverManager.getDriver(targetUrl);
                    return new ProxyConnection(targetDriver.connect(targetUrl, info));
                }
            } else {
                // original MySQL JDBC URL
                Enumeration<Driver> drivers = DriverManager.getDrivers();
                while (drivers.hasMoreElements()) {
                    Driver targetDriver = drivers.nextElement();
                    if (targetDriver instanceof com.mysql.cj.jdbc.Driver) {
                        if (url.startsWith("jdbc:mysql://jdbc:")) {
                            // make proxy URL, remove superfluous parameters (port, database)
                            String targetUrl = "jdbc:proxy:" + UrlUtil.extractTargetUrl(url);
                            return connect(targetUrl, info);
                        } else {
                            return new ProxyConnection(targetDriver.connect(url, info));
                        }
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
            DriverManager.registerDriver(new ProxyDriver());
            // ensure all target drivers are registered upfront
            // this might lead to duplicate registered drivers, but that should not be a big problem
            DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
            DriverManager.registerDriver(new net.snowflake.client.jdbc.SnowflakeDriver());
            DriverManager.registerDriver(new org.postgresql.Driver());
            DriverManager.registerDriver(new org.sqlite.JDBC());
            DriverManager.registerDriver(new org.h2.Driver());
        } catch (SQLException e) {
            throw new RuntimeException("Drivers cannot be registered.");
        }
    }
}
