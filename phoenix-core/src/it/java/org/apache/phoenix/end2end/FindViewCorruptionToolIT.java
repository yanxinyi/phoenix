/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.mapreduce.FindViewCorruptionTool;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FindViewCorruptionToolIT extends ParallelStatsEnabledIT {

    private static final String filePath = "/tmp/";
    String CREATE_TABLE_QUERY = "CREATE TABLE %s (A BIGINT PRIMARY KEY, B VARCHAR)";
    String CREATE_VIEW_QUERY = "CREATE VIEW %s AS SELECT * FROM %s";

    @Test
    public void testTableAndView() throws Exception {
        String tableName = generateUniqueName();
        String viewName1 = generateUniqueName();
        String viewName2 = generateUniqueName();
        String viewName3 = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(String.format(CREATE_TABLE_QUERY, tableName));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, viewName1, tableName));

        // without modify STSTEM.CATALOG, it should not have any corrupted view
        assertEquals(0, runFindCorruptedViewTool(true, false));

        conn.createStatement().execute("UPSERT INTO SYSTEM.CATALOG " +
                "(TABLE_NAME, TABLE_TYPE, COLUMN_COUNT) VALUES('" + viewName1 + "','v',1) ");
        conn.commit();
        assertEquals(1, runFindCorruptedViewTool(true, false));


        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, viewName2, tableName));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, viewName3, viewName2));
        assertEquals(1, runFindCorruptedViewTool(true, false));


        conn.createStatement().execute("UPSERT INTO SYSTEM.CATALOG " +
                "(TABLE_NAME, TABLE_TYPE, COLUMN_COUNT) VALUES('" + viewName2 + "','v',10) ");
        assertEquals(2, runFindCorruptedViewTool(true, false));

    }

    public static String[] getArgValues(boolean getCorruptedViewCount, boolean outputPath) {
        final List<String> args = Lists.newArrayList();
        if (outputPath) {
            args.add("-op");
            args.add(filePath);
        }
        if (getCorruptedViewCount) {
            args.add("-c");
        }
        return args.toArray(new String[0]);
    }

    public static int runFindCorruptedViewTool(boolean getCorruptedViewCount, boolean outputPath)
            throws Exception {
        FindViewCorruptionTool tool = new FindViewCorruptionTool();
        Configuration conf = new Configuration(getUtility().getConfiguration());
        tool.setConf(conf);
        final String[] cmdArgs = getArgValues(getCorruptedViewCount, outputPath);
        return tool.run(cmdArgs);
    }
}
