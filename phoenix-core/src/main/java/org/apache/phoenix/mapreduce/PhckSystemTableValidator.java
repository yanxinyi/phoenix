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

package org.apache.phoenix.mapreduce;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhckUtil;
import org.apache.phoenix.query.QueryConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;

public class PhckSystemTableValidator extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhckSystemTableValidator.class);
    private HashMap<String,String> allSystemTables = new HashMap<>();
    private List<String> currentSystemTables = new ArrayList<>();

    //available for testing
    public void populateSystemTables() {
        allSystemTables.put(SYSTEM_CATALOG_TABLE, QueryConstants.CREATE_TABLE_METADATA);
        allSystemTables.put(SYSTEM_STATS_TABLE,QueryConstants.CREATE_STATS_TABLE_METADATA);
        allSystemTables.put(TYPE_SEQUENCE,QueryConstants.CREATE_SEQUENCE_METADATA);
        allSystemTables.put(SYSTEM_FUNCTION_TABLE,QueryConstants.CREATE_FUNCTION_METADATA);
        allSystemTables.put(SYSTEM_LOG_TABLE,QueryConstants.CREATE_LOG_METADATA);
        allSystemTables.put(SYSTEM_CHILD_LINK_TABLE,QueryConstants.CREATE_CHILD_LINK_METADATA);
        allSystemTables.put(SYSTEM_MUTEX_TABLE_NAME,QueryConstants.CREATE_MUTEX_METADTA);
        allSystemTables.put(SYSTEM_TASK_TABLE,QueryConstants.CREATE_TASK_METADATA);
    }

    private static final String SELECT_QUERY = "SELECT " + TABLE_NAME +  " FROM " + SYSTEM_CATALOG_NAME
            + " WHERE " + TABLE_SCHEM + " = '" + SYSTEM_SCHEMA_NAME + "'" + " AND " + TABLE_TYPE + " = 's'";

    public void fetchAllRows(PhoenixConnection phoenixConnection) throws Exception {
        ResultSet viewRS = phoenixConnection.createStatement().executeQuery(SELECT_QUERY);
        while (viewRS.next()) {
            currentSystemTables.add(viewRS.getString(1));
        }
    }

    public void processSystemLevelCheck(PhoenixConnection phoenixConnection) throws Exception {
        populateSystemTables();
        fetchAllRows(phoenixConnection);
        Admin admin = phoenixConnection.getQueryServices().getAdmin();
        for (String table:currentSystemTables){
            TableName tableName = TableName.valueOf(SYSTEM_CATALOG_SCHEMA+ "." + table);
            if(admin.tableExists(tableName)) {
                if(admin.isTableDisabled(tableName)){
                    admin.enableTable(tableName);
                    LOGGER.warn("Table was disabled. PHCK ENABLED Table: "+ tableName.toString());
                }
            } else {
                phoenixConnection.createStatement().executeUpdate(allSystemTables.get(table));
                LOGGER.warn("Table was dropped. PHCK CREATED Table: "+ tableName.toString());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Connection connection = null;
        try {
            Configuration configuration = HBaseConfiguration.addHbaseResources(getConf());

            Properties props = new Properties();
            try {
                parseOptions(args);
            } catch (IllegalStateException e) {
                PhckUtil.printHelpAndExit(e.getMessage(), PhckUtil.getOptions());
            }
            connection = ConnectionUtil.getInputConnection(configuration, props);
            PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
            processSystemLevelCheck(phoenixConnection);

        } catch (Exception e) {
            LOGGER.error("Phck : An exception occurred "
                    + ExceptionUtils.getMessage(e) + " at:\n" +
                    ExceptionUtils.getStackTrace(e));
            return -1;
        } finally {
            closeConnection(connection);
        }

        return 0;
    }

    public void parseOptions (String[] args) {

    }

    private void closeConnection(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to close connection: ", e);
            throw new RuntimeException("Failed to close connection with exception: ", e);
        }
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new PhckNonSystemTableValidator(), args);
        System.exit(result);
    }
}