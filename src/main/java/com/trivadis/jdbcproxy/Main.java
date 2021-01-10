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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

public class Main {
    private String implementationDate = "0000-00-00 00:00:00";
    private String implementationVersion = "x.y.z";

    Main() {
        super();
    }

    private void print(String msg) {
        System.out.println(msg);
        setManifestProperties();
    }

    private void setManifestProperties() {
        File file;
        try {
            // file in IDE: classes directory; file in runtime environment: jar file
            file = new File(getClass().getProtectionDomain().getCodeSource().getLocation().toURI());
        } catch (URISyntaxException e) {
           return;
        }
        if (file.exists()) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
            implementationDate = dateFormat.format(file.lastModified());
            if (file.getName().endsWith(".jar")) {
                FileInputStream fis;
                try {
                    fis = new FileInputStream(file);
                } catch (FileNotFoundException e) {
                    return;
                }
                JarInputStream jis;
                try {
                    jis = new JarInputStream(fis);
                } catch (IOException e) {
                    return;
                }
                Manifest mf = jis.getManifest();
                implementationVersion = mf.getMainAttributes().getValue("Implementation-Version");
                try {
                    jis.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    private void printInfo() {
        print("");
        print("Trivadis JDBC Proxy version " + implementationVersion + " built on " + implementationDate + ".");
        print("");
    }

    public static void main(String[] args) {
        Main instance = new Main();
        instance.printInfo();
    }

}
