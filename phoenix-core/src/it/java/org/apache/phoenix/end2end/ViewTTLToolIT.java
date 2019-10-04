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
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.mapreduce.ViewTTLTool;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

import javax.swing.text.View;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE;
import static org.junit.Assert.assertEquals;

/**
 * Test that our ViewTTLToolIT as expected
 */
public class ViewTTLToolIT extends ParallelStatsDisabledIT {

    private final int NUMBER_OF_UPSERT_ROWS = 1000;

    private void createBaseTable(Connection conn, String ddl) throws SQLException {

        conn.createStatement().execute(ddl);
    }

    private void createTenantViewAndUpsertData(Connection conn, String tableName,String viewName, String tenantId) throws SQLException {
        String ddl = "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName;
        conn.createStatement().execute(ddl);

        String pk2 = generateUniqueName();
        for (int i = 0; i < NUMBER_OF_UPSERT_ROWS; i++) {
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + viewName + " (TENANT_ID, ID, NUM) VALUES(?,?,?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, pk2);
            stmt.setInt(3, i);
            stmt.execute();
        }

        pk2 = generateUniqueName();
        for (int i = 0; i < NUMBER_OF_UPSERT_ROWS; i++) {
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + viewName + " (TENANT_ID, ID, NUM) VALUES(?,?,?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, pk2);
            stmt.setInt(3, i);
            stmt.execute();
        }
    }

    private void createTenantPkViewAndUpsertData(Connection conn, String ddl, String viewName) throws Exception {
        conn.createStatement().execute(ddl);
        String id = generateUniqueName();
        for (int i = 0; i < NUMBER_OF_UPSERT_ROWS; i++) {
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + viewName +
                    " (ID, pk1, pk2, col1, col2) VALUES (?,?,?,?,?)");
            stmt.setString(1, id);
            stmt.setInt(2, i);
            stmt.setInt(3, i);
            stmt.setString(4, generateUniqueName());
            stmt.setString(5, generateUniqueName());
            stmt.execute();
        }

        id = generateUniqueName();
        for (int i = 0; i < NUMBER_OF_UPSERT_ROWS; i++) {
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + viewName +
                    " (ID, pk1, pk2, col1, col2) VALUES (?,?,?,?,?)");
            stmt.setString(1, id);
            stmt.setInt(2, i);
            stmt.setInt(3, i);
            stmt.setString(4, generateUniqueName());
            stmt.setString(5, generateUniqueName());
            stmt.execute();
        }

        id = generateUniqueName();
        for (int i = 0; i < NUMBER_OF_UPSERT_ROWS; i++) {
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + viewName +
                    " (ID, pk1, pk2, col1, col2) VALUES (?,?,?,?,?)");
            stmt.setString(1, id);
            stmt.setInt(2, i);
            stmt.setInt(3, i);
            stmt.setString(4, generateUniqueName());
            stmt.setString(5, generateUniqueName());
            stmt.execute();
        }

        id = generateUniqueName();
        for (int i = 0; i < NUMBER_OF_UPSERT_ROWS; i++) {
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + viewName +
                    " (ID, pk1, pk2, col1, col2) VALUES (?,?,?,?,?)");
            stmt.setString(1, id);
            stmt.setInt(2, i);
            stmt.setInt(3, i);
            stmt.setString(4, generateUniqueName());
            stmt.setString(5, generateUniqueName());
            stmt.execute();
        }

    }

    @Test
    public void testTenantCreatedViews() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTable = generateUniqueName();
            String ddl = "CREATE TABLE " + baseTable + "(TENANT_ID CHAR(10) NOT NULL, ID CHAR(10) NOT NULL, " +
                    "NUM BIGINT CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true";
            createBaseTable(conn, ddl);

            String tenant1 = generateUniqueName();
            String tenant2 = generateUniqueName();
            String tenant3 = generateUniqueName();

            Connection tenant1Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant1);
            Connection tenant2Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant2);
            Connection tenant3Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant3);

            String tenant1ViewName = generateUniqueName();
            String tenant2ViewName = generateUniqueName();
            String tenant3ViewName = generateUniqueName();

            createTenantViewAndUpsertData(tenant1Connection, baseTable, tenant1ViewName,tenant1);
            createTenantViewAndUpsertData(tenant2Connection, baseTable, tenant2ViewName,tenant2);

            ddl = "CREATE VIEW " + tenant3ViewName +
                    " (pk1 BIGINT NOT NULL, pk2 BIGINT NOT NULL, col1 VARCHAR, col2 VARCHAR " +
                    "CONSTRAINT PK PRIMARY KEY (pk1, pk2)) " +
                    "AS SELECT * FROM " + baseTable;
            createTenantPkViewAndUpsertData(tenant3Connection, ddl, tenant3ViewName);
        }

        final List<String> args = Lists.newArrayList();

        ViewTTLTool viewTTLTool = new ViewTTLTool();
        Configuration conf = new Configuration(getUtility().getConfiguration());
        viewTTLTool.setConf(conf);

        int status = viewTTLTool.run(args.toArray(new String[0]));

        assertEquals(0, status);
    }

    @Test
    public void testGlobalViewWithRegionSplits() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTable = generateUniqueName();
            String viewName = generateUniqueName();
            String ddl = "CREATE TABLE " + baseTable + "(ID INTEGER not null,NUM INTEGER," +
                    "col2 VARCHAR constraint pk primary key(ID)) split on (400,800,1200,1600)";
            createBaseTable(conn, ddl);
            conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s " +
                    "WHERE ID < 250 " +
                    "OR ID > 500 AND ID < 799 " +
                    "OR ID > 1200 AND ID < 1300 " +
                    "OR ID > 1350 AND ID < 1380", viewName,baseTable));

            for (int i = 0; i < 2000; i++) {
                PreparedStatement stmt = conn.prepareStatement(
                        "UPSERT INTO "+ baseTable + " (ID, NUM, col2) values(?,?,?)");
                stmt.setInt(1, i);
                stmt.setInt(2, 1);
                stmt.setString(3, generateUniqueName());
                stmt.execute();
            }

            for (int i = 0; i < 2000; i++) {
                PreparedStatement stmt = conn.prepareStatement(
                        "UPSERT INTO "+ baseTable + " (ID, NUM, col2) values(?,?,?)");
                stmt.setInt(1, i);
                stmt.setInt(2, 2);
                stmt.setString(3, generateUniqueName());
                stmt.execute();
            }
        }

        final List<String> args = Lists.newArrayList();

        ViewTTLTool viewTTLTool = new ViewTTLTool();
        Configuration conf = new Configuration(getUtility().getConfiguration());
        viewTTLTool.setConf(conf);

        int status = viewTTLTool.run(args.toArray(new String[0]));

        assertEquals(0, status);
    }
}
