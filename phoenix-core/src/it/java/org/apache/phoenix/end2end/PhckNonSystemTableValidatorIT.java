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
import org.apache.phoenix.mapreduce.PhckNonSystemTableValidator;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PhckNonSystemTableValidatorIT extends ParallelStatsEnabledIT {
    String CREATE_SCHEM_QUERY = "CREATE SCHEMA %s";
    String CREATE_TABLE_QUERY = "CREATE TABLE %s (A BIGINT PRIMARY KEY, B BIGINT, C BIGINT)";
    String CREATE_VIEW_QUERY = "CREATE VIEW %s AS SELECT * FROM %s";
    String CREATE_MULTI_TENANT_TABLE_QUERY = "CREATE TABLE %s (TENANT_ID VARCHAR NOT NULL, " +
            "A VARCHAR NOT NULL, B BIGINT, CONSTRAINT pk PRIMARY KEY(TENANT_ID, A)) MULTI_TENANT=true";


    @Test
    public void test() throws Exception {
        String schema1 = generateUniqueName();
        String schema2 = generateUniqueName();
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        String viewName1 = generateUniqueName();
        String viewName2 = generateUniqueName();
        String viewName3 = generateUniqueName();

        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(String.format(CREATE_SCHEM_QUERY, schema1));
        conn.createStatement().execute(String.format(CREATE_SCHEM_QUERY, schema2));

        conn.createStatement().execute(String.format(CREATE_TABLE_QUERY, SchemaUtil.getTableName(schema1, tableName1)));
        conn.createStatement().execute(String.format(CREATE_TABLE_QUERY, SchemaUtil.getTableName(schema2, tableName2)));

        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, SchemaUtil.getTableName(schema1, viewName1), SchemaUtil.getTableName(schema1, tableName1)));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, SchemaUtil.getTableName(schema1, viewName2), SchemaUtil.getTableName(schema1, viewName1)));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, SchemaUtil.getTableName(schema1, viewName3), SchemaUtil.getTableName(schema1, viewName2)));

        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, SchemaUtil.getTableName(schema2, viewName1), SchemaUtil.getTableName(schema2, tableName2)));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, SchemaUtil.getTableName(schema2, viewName2), SchemaUtil.getTableName(schema2, tableName2)));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, SchemaUtil.getTableName(schema2, viewName3), SchemaUtil.getTableName(schema2, viewName2)));

        assertEquals(0, runPhckNonSystemTableValidator(true, false));
    }

    private static String[] getArgValues(boolean invalidCount, boolean outputPath) {
        final List<String> args = Lists.newArrayList();
        if (outputPath) {
            args.add("-op");
            args.add("/tmp");
        }
        if (invalidCount) {
            args.add("-c");
        }

        return args.toArray(new String[0]);
    }


    public static int runPhckNonSystemTableValidator(boolean getCorruptedViewCount, boolean outputPath)
            throws Exception {
        PhckNonSystemTableValidator tool = new PhckNonSystemTableValidator();
        final String[] cmdArgs =
                getArgValues(getCorruptedViewCount, outputPath);
        return tool.run(cmdArgs);
    }
}
