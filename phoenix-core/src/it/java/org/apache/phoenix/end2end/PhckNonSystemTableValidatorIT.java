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
import org.apache.phoenix.mapreduce.PhckNonSystemTableValidator;
import org.apache.phoenix.mapreduce.util.PhckRow;
import org.apache.phoenix.mapreduce.util.PhckTable;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(NeedsOwnMiniClusterTest.class)
public class PhckNonSystemTableValidatorIT extends ParallelStatsEnabledIT {
    String CREATE_SCHEM_QUERY = "CREATE SCHEMA %s";
    String CREATE_TABLE_QUERY = "CREATE TABLE %s (A BIGINT PRIMARY KEY, B BIGINT, C BIGINT)";
    String CREATE_VIEW_QUERY = "CREATE VIEW %s AS SELECT * FROM %s";
    String CREATE_INDEX_QUERY = "CREATE INDEX %s ON %s (B) INCLUDE(A,C)";
    String CREATE_MULTI_TENANT_TABLE_QUERY = "CREATE TABLE %s (TENANT_ID VARCHAR NOT NULL, " +
            "A VARCHAR NOT NULL, B BIGINT, CONSTRAINT pk PRIMARY KEY(TENANT_ID, A)) MULTI_TENANT=true";
    String CORRUPT_HEAD_ROW_COLUMN_COUNT_QUERY = "UPSERT INTO SYSTEM.CATALOG " +
            "(TABLE_SCHEM,TABLE_NAME, TABLE_TYPE,COLUMN_COUNT) VALUES('%s', '%s','%s', 100)";
    String DELETE_HEAD_ROW_COLUMN_COUNT_QUERY = "DELETE FROM SYSTEM.CATALOG WHERE " +
            "TABLE_SCHEM = '%s' AND TABLE_NAME = '%s' AND TABLE_TYPE ='%s'";
    String UPSERT_ORPHAN_ROW_QUERY = "UPSERT INTO SYSTEM.CATALOG " +
            "(TABLE_NAME, COLUMN_NAME,COLUMN_FAMILY) VALUES(?,'ORPHAN_ROW_COLUMN_NAME', '0')";
    String UPSERT_OREPHAN_VIEW_HEAD_ROW_QUERY = "UPSERT INTO SYSTEM.CATALOG " +
            "(TABLE_NAME, TABLE_TYPE,COLUMN_COUNT) VALUES(?,'v', 1)";
    String UPSERT_OREPHAN_VIEW_COLUMN_ROW_QUERY = "UPSERT INTO SYSTEM.CATALOG " +
            "(TABLE_NAME, COLUMN_NAME,COLUMN_FAMILY) VALUES(?,'ORPHAN_VIEW_COLUMN_NAME', '0')";

    @Test
    public void test() throws Exception {
        String schema1 = generateUniqueName() + "_SCHEMA";
        String schema2 = generateUniqueName() + "_SCHEMA";
        String schema3 = generateUniqueName() + "_SCHEMA";
        String schema4 = generateUniqueName() + "_SCHEMA";

        String tableName1 = generateUniqueName() + "_TABLE";
        String noSchemaTableName1 = generateUniqueName() + "_TABLE";

        String indexName1 = generateUniqueName() + "_INDEX";
        String indexName2 = generateUniqueName() + "_INDEX";
        String noSchemaIndexName1 = generateUniqueName() + "_INDEX";
        String noSchemaIndexViewName1 = generateUniqueName() + "_INDEX";

        String viewName1 = generateUniqueName() + "_VIEW";
        String viewName2 = generateUniqueName() + "_VIEW";
        String viewName3 = generateUniqueName() + "_VIEW";
        String noSchemaViewName1 = generateUniqueName() + "_VIEW";
        String noSchemaViewName2 = generateUniqueName() + "_VIEW";
        String noSchemaViewName3 = generateUniqueName() + "_VIEW";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        Connection conn = DriverManager.getConnection(getUrl(), props);

        conn.createStatement().execute(String.format(CREATE_SCHEM_QUERY, schema1));
        conn.createStatement().execute(String.format(CREATE_SCHEM_QUERY, schema2));
        conn.createStatement().execute(String.format(CREATE_SCHEM_QUERY, schema3));
        conn.createStatement().execute(String.format(CREATE_SCHEM_QUERY, schema4));

        String baseTableName1 = SchemaUtil.getTableName(schema1, tableName1);
        String baseTableName2 = SchemaUtil.getTableName(schema2, tableName1);
        String baseTableName3 = SchemaUtil.getTableName(schema3, tableName1);
        String baseTableName4 = SchemaUtil.getTableName(schema4, tableName1);
        conn.createStatement().execute(String.format(CREATE_TABLE_QUERY, baseTableName1));
        conn.createStatement().execute(String.format(CREATE_TABLE_QUERY, baseTableName2));
        conn.createStatement().execute(String.format(CREATE_TABLE_QUERY, baseTableName3));
        conn.createStatement().execute(String.format(CREATE_TABLE_QUERY, baseTableName4));
        conn.createStatement().execute(String.format(CREATE_TABLE_QUERY, noSchemaTableName1));

        //happy path
        String schema1ViewName1 = SchemaUtil.getTableName(schema1, viewName1);
        String schema1ViewName2 = SchemaUtil.getTableName(schema1, viewName2);
        String schema1ViewName3 = SchemaUtil.getTableName(schema1, viewName3);
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema1ViewName1, baseTableName1));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema1ViewName2, baseTableName1));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema1ViewName3, schema1ViewName2));

        String schema2ViewName1 = SchemaUtil.getTableName(schema2, viewName1);
        String schema2ViewName2 = SchemaUtil.getTableName(schema2, viewName2);
        String schema2ViewName3 = SchemaUtil.getTableName(schema2, viewName3);
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema2ViewName1, baseTableName2));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema2ViewName2, baseTableName2));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema2ViewName3, schema2ViewName2));

        String schema3ViewName1 = SchemaUtil.getTableName(schema3, viewName1);
        String schema3ViewName2 = SchemaUtil.getTableName(schema3, viewName2);
        String schema3ViewName3 = SchemaUtil.getTableName(schema3, viewName3);
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema3ViewName1, baseTableName3));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema3ViewName2, baseTableName3));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema3ViewName3, schema3ViewName2));

        String schema4ViewName1 = SchemaUtil.getTableName(schema4, viewName1);
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema4ViewName1, baseTableName4));

        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, noSchemaViewName1, noSchemaTableName1));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, noSchemaViewName2, noSchemaTableName1));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, noSchemaViewName3, noSchemaViewName2));

        // create index on base/physical table
        conn.createStatement().execute(String.format(CREATE_INDEX_QUERY, indexName1, baseTableName1));
        conn.createStatement().execute(String.format(CREATE_INDEX_QUERY, indexName1, baseTableName2));
        conn.createStatement().execute(String.format(CREATE_INDEX_QUERY, indexName1, baseTableName3));
        conn.createStatement().execute(String.format(CREATE_INDEX_QUERY, noSchemaIndexName1, noSchemaTableName1));

        // create index on view
        conn.createStatement().execute(String.format(CREATE_INDEX_QUERY, indexName2 ,schema1ViewName1));
        conn.createStatement().execute(String.format(CREATE_INDEX_QUERY, indexName2, schema2ViewName1));
        conn.createStatement().execute(String.format(CREATE_INDEX_QUERY, indexName2, schema3ViewName1));
        conn.createStatement().execute(String.format(CREATE_INDEX_QUERY, indexName2, schema4ViewName1));
        conn.createStatement().execute(String.format(CREATE_INDEX_QUERY, noSchemaIndexViewName1, noSchemaTableName1));

        PhckNonSystemTableValidator tool = new PhckNonSystemTableValidator();
        runPhckNonSystemTableValidator(true, false, tool);

        assertEquals(0, tool.getInvalidRowSet().size());
        assertEquals(0, tool.getInvalidTableSet().size());



        // incorrect column count with nest condition.
        conn.createStatement().execute("UPSERT INTO SYSTEM.CATALOG " +
                "(TABLE_SCHEM,TABLE_NAME, TABLE_TYPE,COLUMN_COUNT) VALUES('"+ schema1 + "', '" + tableName1 + "','u', -1)");
        conn.commit();
        conn.createStatement().execute("UPSERT INTO SYSTEM.CATALOG " +
                "(TABLE_SCHEM,TABLE_NAME, TABLE_TYPE,COLUMN_COUNT) VALUES('"+ schema2 + "', '" + viewName2 + "','v', -1)");
        conn.commit();

        // delete head row
        conn.createStatement().execute("DELETE FROM SYSTEM.CATALOG WHERE " +
                "TABLE_SCHEM = '" + schema3 + "' AND TABLE_NAME = '" + viewName3 + "' AND TABLE_TYPE ='v'");
        conn.commit();

        // adding orphan row
        String orphanRowTableName = generateUniqueName() + "_ORPHAN_ROW";
        conn.createStatement().execute("UPSERT INTO SYSTEM.CATALOG " +
                "(TABLE_NAME, COLUMN_NAME,COLUMN_FAMILY) VALUES('" + orphanRowTableName + "','ORPHAN_ROW_COLUMN_NAME', '0')");
        conn.commit();

        // adding orphan view
        String orphanViewName = generateUniqueName() + "_ORPHAN_VIEW";
        conn.createStatement().execute("UPSERT INTO SYSTEM.CATALOG " +
                "(TABLE_NAME, TABLE_TYPE,COLUMN_COUNT) VALUES('"+ orphanViewName +"','v', 1)");
        conn.commit();
        conn.createStatement().execute("UPSERT INTO SYSTEM.CATALOG " +
                "(TABLE_NAME, COLUMN_NAME,COLUMN_FAMILY) VALUES('" + orphanViewName + "','ORPHAN_VIEW_COLUMN_NAME', '0')");
        conn.commit();

        // running phck tool
        PhckNonSystemTableValidator phckTool = new PhckNonSystemTableValidator();
        runPhckNonSystemTableValidator(true, false, phckTool);

        // showing report
        assertEquals(4, phckTool.getInvalidRowSet().size());
        assertEquals(7, phckTool.getInvalidTableSet().size());

        // try to running query on corrupted table
        ResultSet rs;
        try {
            rs = conn.createStatement().executeQuery("SELECT * FROM " + schema1 + "." + tableName1);
            fail();
        } catch (Exception e) {

        }

        // calling phck tool to fix the incorrect column count
        phckTool.fix(conn);

        // should running query successfully
        try {
            rs = conn.createStatement().executeQuery("SELECT * FROM " + schema1 + "." + tableName1);
        } catch (Exception e) {
            fail();
        }
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

    public static int runPhckNonSystemTableValidator(boolean getCorruptedViewCount, boolean outputPath, PhckNonSystemTableValidator tool)
            throws Exception {
        if (tool == null) {
            tool = new PhckNonSystemTableValidator();
        }
        Configuration conf = new Configuration(getUtility().getConfiguration());
        tool.setConf(conf);
        final String[] cmdArgs =
                getArgValues(getCorruptedViewCount, outputPath);
        return tool.run(cmdArgs);
    }
}
