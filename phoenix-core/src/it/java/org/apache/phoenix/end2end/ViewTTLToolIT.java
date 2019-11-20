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
import org.apache.phoenix.query.QueryServices;
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
import static org.junit.Assert.*;

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

        for (int i = 0; i < NUMBER_OF_UPSERT_ROWS; i++) {
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + viewName + " (ID, NUM) VALUES(?,?)");
            stmt.setString(1, generateUniqueName());
            stmt.setInt(2, i);
            stmt.execute();
        }
        conn.commit();
//        conn.close();
    }

    private void createTenantPkViewAndUpsertData(Connection conn, String ddl, String viewName) throws Exception {
        conn.createStatement().execute(ddl);
        for (int i = 0; i < NUMBER_OF_UPSERT_ROWS; i++) {
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + viewName +
                    " (ID, pk1, pk2, col1, col2) VALUES (?,?,?,?,?)");
            stmt.setString(1, generateUniqueName());
            stmt.setInt(2, i);
            stmt.setInt(3, i);
            stmt.setString(4, generateUniqueName());
            stmt.setString(5, generateUniqueName());
            stmt.execute();
        }
        conn.commit();
        conn.close();
    }

    @Test
    public void testTenantCreatedViews() throws Exception {
        String baseTable = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {

            String ddl = "CREATE TABLE " + baseTable + "(TENANT_ID CHAR(10) NOT NULL, ID CHAR(10) NOT NULL, " +
                    "NUM BIGINT CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true";
            createBaseTable(conn, ddl);

            String tenant1 = "T" + generateUniqueName();
            String tenant2 = "T" + generateUniqueName();
            String tenant3 = "T" + generateUniqueName();
            String tenant4 = "T" + generateUniqueName();
            String tenant5 = "T" + generateUniqueName();
            String tenant6 = "T" + generateUniqueName();
            String tenant7 = "T" + generateUniqueName();
            String tenant8 = "T" + generateUniqueName();
            String tenant9 = "T" + generateUniqueName();
            String tenant10 = "T" + generateUniqueName();
            String tenant11 = "T" + generateUniqueName();
            String tenant12 = "T" + generateUniqueName();

            Connection tenant1Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant1);
            Connection tenant2Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant2);
            Connection tenant3Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant3);
            Connection tenant4Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant4);
            Connection tenant5Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant5);
            Connection tenant6Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant6);
            Connection tenant7Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant7);
            Connection tenant8Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant8);
            Connection tenant9Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant9);
            Connection tenant10Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant10);
            Connection tenant11Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant11);
            Connection tenant12Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant12);

            String tenant1ViewName = generateUniqueName();
            String tenant2ViewName = generateUniqueName();
            String tenant3ViewName = generateUniqueName();
            String tenant4ViewName = generateUniqueName();
            String tenant5ViewName = generateUniqueName();
            String tenant6ViewName = generateUniqueName();
            String tenant7ViewName = generateUniqueName();
            String tenant8ViewName = generateUniqueName();
            String tenant9ViewName = generateUniqueName();
            String tenant10ViewName = generateUniqueName();
            String tenant11ViewName = generateUniqueName();
            String tenant12ViewName = generateUniqueName();

            createTenantViewAndUpsertData(tenant1Connection, baseTable, tenant1ViewName,tenant1);
            createTenantViewAndUpsertData(tenant2Connection, baseTable, tenant2ViewName,tenant2);

            createTenantViewAndUpsertData(tenant4Connection, baseTable, tenant4ViewName,tenant4);
            createTenantViewAndUpsertData(tenant5Connection, baseTable, tenant5ViewName,tenant5);
            createTenantViewAndUpsertData(tenant6Connection, baseTable, tenant6ViewName,tenant6);
            createTenantViewAndUpsertData(tenant7Connection, baseTable, tenant7ViewName,tenant7);
            createTenantViewAndUpsertData(tenant8Connection, baseTable, tenant8ViewName,tenant8);
            createTenantViewAndUpsertData(tenant9Connection, baseTable, tenant9ViewName,tenant9);
            createTenantViewAndUpsertData(tenant10Connection, baseTable, tenant10ViewName,tenant10);
            createTenantViewAndUpsertData(tenant11Connection, baseTable, tenant11ViewName,tenant11);
            createTenantViewAndUpsertData(tenant12Connection, baseTable, tenant12ViewName,tenant12);


            ddl = "CREATE VIEW " + tenant3ViewName +
                    " (pk1 BIGINT NOT NULL, pk2 BIGINT NOT NULL, col1 VARCHAR, col2 VARCHAR " +
                    "CONSTRAINT PK PRIMARY KEY (pk1, pk2)) " +
                    "AS SELECT * FROM " + baseTable;
            createTenantPkViewAndUpsertData(tenant3Connection, ddl, tenant3ViewName);
        }

        ViewTTLTool viewTTLTool = new ViewTTLTool();
        Configuration conf = new Configuration(getUtility().getConfiguration());
        viewTTLTool.setConf(conf);

        int status = viewTTLTool.run(new String[] {"-t", baseTable, "-runfg"});

        assertEquals(0, status);
    }

    @Test
    public void testGlobalViewWithRegionSplits() throws Exception {
        String baseTable = generateUniqueName();
        String viewName1 = generateUniqueName();
        String viewName2 = generateUniqueName();
        String viewName3 = generateUniqueName();
        String viewName4 = generateUniqueName();
        String viewName5 = generateUniqueName();
        String viewName6 = generateUniqueName();
        String viewName7 = generateUniqueName();
        String viewName8 = generateUniqueName();
        String viewName9 = generateUniqueName();
        String viewName10 = generateUniqueName();
        String viewName11 = generateUniqueName();
        String viewName12 = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE " + baseTable + "(ID INTEGER not null,NUM INTEGER," +
                    "col2 VARCHAR constraint pk primary key(ID)) split on (400,800,1200,1600)";
            createBaseTable(conn, ddl);

            conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s " +
                    "WHERE ID < 25 ", viewName1,baseTable));
            conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s " +
                    "WHERE ID > 50 AND ID < 80 " +
                    "OR ID > 1900", viewName2,baseTable));
            conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s " +
                    "WHERE ID > 100 AND ID < 130 " +
                    "OR ID > 1350 AND ID < 1380 " +
                    "OR ID > 1680 AND ID < 1690", viewName3,baseTable));


            conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s " +
                    "WHERE ID > 150 AND ID < 180", viewName4,baseTable));
            conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s " +
                    "WHERE ID > 200 AND ID < 280", viewName5,baseTable));
            conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s " +
                    "WHERE ID > 300 AND ID < 380 ", viewName6,baseTable));
            conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s " +
                    "WHERE ID > 450 AND ID < 580 ", viewName7,baseTable));
            conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s " +
                    "WHERE ID > 650 AND ID < 780 ", viewName8,baseTable));
            conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s " +
                    "WHERE ID > 950 AND ID < 1080 ", viewName9,baseTable));
            conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s " +
                    "WHERE ID > 1200 AND ID < 1210 ", viewName10,baseTable));
            conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s " +
                    "WHERE ID > 1220 AND ID < 1230 ", viewName11,baseTable));
            conn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s " +
                    "WHERE ID > 1230 AND ID < 1340 ", viewName12,baseTable));

            for (int i = 0; i < 2000; i++) {
                PreparedStatement stmt = conn.prepareStatement(
                        "UPSERT INTO "+ baseTable + " (ID, NUM, col2) values(?,?,?)");
                stmt.setInt(1, i);
                stmt.setInt(2, 1);
                stmt.setString(3, generateUniqueName());
                stmt.execute();
            }
            conn.commit();

            for (int i = 0; i < 2000; i++) {
                PreparedStatement stmt = conn.prepareStatement(
                        "UPSERT INTO "+ baseTable + " (ID, NUM, col2) values(?,?,?)");
                stmt.setInt(1, i);
                stmt.setInt(2, 2);
                stmt.setString(3, generateUniqueName());
                stmt.execute();
            }
            conn.commit();
        }

        ViewTTLTool viewTTLTool = new ViewTTLTool();
        Configuration conf = new Configuration(getUtility().getConfiguration());
        viewTTLTool.setConf(conf);

        int status = viewTTLTool.run(new String[] {"-t", baseTable, "-runfg"});
        assertEquals(0, status);
    }


    private Connection createSchemaAndGetConnection(String schema) throws Exception {
        Properties props = new Properties();
        props.setProperty(QueryServices.SCHEMA_ATTRIB, schema);
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        String ddl = "CREATE SCHEMA IF NOT EXISTS " + schema;
        conn.createStatement().execute(ddl);

        return conn;
    }

    private void createMultiTenantTable(Connection conn, String tableName) throws Exception {
        String ddl = "CREATE TABLE " + tableName + "(TENANT_ID CHAR(10) NOT NULL, ID CHAR(10) NOT NULL, " +
                "NUM BIGINT CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true";

        conn.createStatement().execute(ddl);
    }

    @Test
    public void testAllViewCases() throws Exception {
        String schema1 = generateUniqueName();
        String schema2 = generateUniqueName();
        Connection conn1 = createSchemaAndGetConnection(schema1);
        Connection conn2 = createSchemaAndGetConnection(schema2);

        String baseTable1 = generateUniqueName();
        String baseTable2 = generateUniqueName();

        String fullTable11 = schema1 + "." + baseTable1;
        String fullTable12 = schema1 + "." + baseTable2;
        createMultiTenantTable(conn1, fullTable11);
        createMultiTenantTable(conn1, fullTable12);

        String fullTable21 = schema2 + "." + baseTable1;
        String fullTable22 = schema2 + "." + baseTable2;
        createMultiTenantTable(conn2, fullTable21);
        createMultiTenantTable(conn2, fullTable22);


        String tenant1 = "T" + generateUniqueName();
        String tenant2 = "T" + generateUniqueName();
        String tenant3 = "T" + generateUniqueName();
        String tenant4 = "T" + generateUniqueName();
        Connection tenant1Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant1);
        Connection tenant2Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant2);
        Connection tenant3Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant3);
        Connection tenant4Connection = ViewTTLTool.buildTenantConnection(getUrl(), tenant4);

        String viewName1 = generateUniqueName();
        String viewName2 = generateUniqueName();

        createTenantViewAndUpsertData(tenant1Connection, fullTable11, schema1 + "." +viewName1, tenant1);
        createTenantViewAndUpsertData(tenant2Connection, fullTable11, schema1 + "." +viewName1, tenant2);
        createTenantViewAndUpsertData(tenant3Connection, fullTable11, schema1 + "." +viewName1, tenant3);
        createTenantViewAndUpsertData(tenant4Connection, fullTable11, schema1 + "." +viewName1, tenant4);

        createTenantViewAndUpsertData(tenant1Connection, fullTable12, schema1 + "." +viewName2, tenant1);
        createTenantViewAndUpsertData(tenant2Connection, fullTable12, schema1 + "." +viewName2, tenant2);
        createTenantViewAndUpsertData(tenant3Connection, fullTable12, schema1 + "." +viewName2, tenant3);
        createTenantViewAndUpsertData(tenant4Connection, fullTable12, schema1 + "." +viewName2, tenant4);

        createTenantViewAndUpsertData(tenant1Connection, fullTable21, schema2 + "." + viewName1, tenant1);
        createTenantViewAndUpsertData(tenant2Connection, fullTable21, schema2 + "." + viewName1, tenant2);
        createTenantViewAndUpsertData(tenant3Connection, fullTable21, schema2 + "." + viewName1, tenant3);
        createTenantViewAndUpsertData(tenant4Connection, fullTable21, schema2 + "." + viewName1, tenant4);

        createTenantViewAndUpsertData(tenant1Connection, fullTable22, schema2 + "." + viewName2, tenant1);
        createTenantViewAndUpsertData(tenant2Connection, fullTable22, schema2 + "." + viewName2, tenant2);
        createTenantViewAndUpsertData(tenant3Connection, fullTable22, schema2 + "." + viewName2, tenant3);
        createTenantViewAndUpsertData(tenant4Connection, fullTable22, schema2 + "." + viewName2, tenant4);


//        try (Connection conn = DriverManager.getConnection(getUrl())) {
//            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM SYSTEM.CHILD_LINK");
//            while (rs.next()) {
//                System.out.println("\n ************");
//                System.out.println("TENANT_ID:" + rs.getString(1));
//                System.out.println("TABLE_SCHEM:" + rs.getString(2));
//                System.out.println("TABLE_NAME:" + rs.getString(3));
//                System.out.println("COLUMN_NAME:" + rs.getString(4));
//                System.out.println("COLUMN_FAMILY:" + rs.getString(5));
//                System.out.println("LINK_TYPE:" + rs.getString(6));
//                System.out.println("************  \n\n");
//            }
//        }
//
//
//        System.out.println("************  \n\n");
//        System.out.println("************  \n\n");
//        System.out.println("************  \n\n");
//        System.out.println("************  \n\n");
//        System.out.println("************  \n\n");
//        System.out.println("************  \n\n");
//        System.out.println("************  \n\n");

//        try (Connection conn = DriverManager.getConnection(getUrl())) {
//            ResultSet rs = conn.createStatement().executeQuery("SELECT TENANT_ID,TABLE_SCHEM,TABLE_NAME" +
//                    ",COLUMN_COUNT FROM SYSTEM.CATALOG WHERE COLUMN_COUNT IS NOT NULL");
//            while (rs.next()) {
//                System.out.println("\n ************");
//                System.out.println("TENANT_ID:" + rs.getString(1));
//                System.out.println("TABLE_SCHEM:" + rs.getString(2));
//                System.out.println("TABLE_NAME:" + rs.getString(3));
//                System.out.println("COLUMN_COUNT:" + rs.getString(4));
//                System.out.println("************  \n\n");
//            }
//        }


        ViewTTLTool viewTTLTool = new ViewTTLTool();
        Configuration conf = new Configuration(getUtility().getConfiguration());
        viewTTLTool.setConf(conf);

        int status = viewTTLTool.run(new String[] {"-runfg"});

        assertEquals(0                                                                                                                                    , status);
    }

}
