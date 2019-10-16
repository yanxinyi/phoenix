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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhckRow;
import org.apache.phoenix.mapreduce.util.PhckTable;
import org.apache.phoenix.mapreduce.util.PhckUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;

public class PhckSystemTableValidator extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhckSystemTableValidator.class);
    private HashSet<PhckRow> orphanRowSet = new HashSet<>();
    private HashSet<PhckRow> invalidSystemTableName = new HashSet<>();
    private HashSet<PhckTable> invalidSystemTableCount = new HashSet<>();
    private HashSet<PhckRow> invalidSystemTableLink = new HashSet<>();
    private HashMap<String, PhckTable> allTables = new HashMap<>();
    private HashSet<PhckTable> validRows = new HashSet<>();

    private static final String SELECT_QUERY = PhckUtil.BASE_SELECT_QUERY
            + " WHERE " + TABLE_SCHEM + " = '" + SYSTEM_SCHEMA_NAME + "'";

    public void fetchAllRows(PhoenixConnection phoenixConnection) throws Exception {
        ResultSet viewRS = phoenixConnection.createStatement().executeQuery(SELECT_QUERY);
        while (viewRS.next()) {
            PhckRow row = new PhckRow(viewRS, PhckUtil.PHCK_ROW_RESOURCE.CATALOG);
            PhckTable phckTable;
            String tableName = row.getFullName();
            if (row.isHeadRow()) {
                phckTable = new PhckTable(row.getTenantId(), row.getTableSchema(),
                        row.getTableName(),row.getTableType(),row.getColumnCount(),
                        row.getIndexState());
                if (!phckTable.isSystemTable()) {
                    invalidSystemTableName.add(row);
                } else {
                    allTables.put(tableName,phckTable);
                }
            } else if (row.isLinkRow()) {
                invalidSystemTableLink.add(row);
            } else if (row.isColumnRow()){
                if (allTables.containsKey(tableName)) {
                    allTables.get(tableName).incrementColumnCount();
                } else {
                    orphanRowSet.add(row);
                }
            } else {
                LOGGER.warn("Unknown row type : "+row.toString());
            }
        }
    }

    public void processSystemLevelCheck(PhoenixConnection phoenixConnection) throws Exception {
        fetchAllRows(phoenixConnection);
        for (PhckTable table : allTables.values()) {
            if (!table.isColumnCountMatches()) {
                invalidSystemTableCount.add(table);
            }
            else {
                validRows.add(table);
            }
        }
        if(!invalidSystemTableCount.isEmpty()) {
            throw new AssertionError("Invalid Row count detected!!!");
        }
        if(!invalidSystemTableLink.isEmpty()) {
            throw new AssertionError("Invalid System table links detected!!!");
        }
        if(!invalidSystemTableName.isEmpty()) {
            throw new AssertionError("Invalid tables with System schema detected!!!");
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