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

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhckRow;
import org.apache.phoenix.mapreduce.util.PhckTable;
import org.apache.phoenix.mapreduce.util.PhckUtil;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import static com.sun.javaws.Globals.parseOptions;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_TYPE;

public class PhoenixIntegrityCheckAndRepairTool extends Configured implements Tool {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PhoenixIntegrityCheckAndRepairTool.class);

    private HashSet<PhckRow> orphanRowSet = new HashSet<>();
    private HashMap<String, PhckTable> allTables = new HashMap<>();

    private static final String SELECT_QUERY = PhckUtil.BASE_SELECT_QUERY +
            " WHERE " + TABLE_SCHEM + " NOT LIKE '" + SYSTEM_SCHEMA_NAME + "'";

    public void fetchAllRows(PhoenixConnection phoenixConnection) throws Exception {
        ResultSet viewRS = phoenixConnection.createStatement().executeQuery(SELECT_QUERY);

        while (viewRS.next()) {
            PhckRow row = new PhckRow(viewRS);
            PhckTable phckTable;
            //added row table name to the set.
            String fullName = row.getFullName();

            if (row.isHeadRow()) {
                phckTable = new PhckTable(row.getTenantId(), row.getTableSchema(),
                        row.getTableName(),row.getTableType(),row.getColumnCount(),
                        row.getIndexState());
                if (phckTable.isSystemTable()) {
                    // SHOULD NOT BE
                }
                allTables.put(fullName, phckTable);
                continue;
            }

            if (!allTables.containsKey(fullName)) {
                // found an orphan row
                orphanRowSet.add(row);
            }

            phckTable = allTables.get(fullName);
            if (row.isQualifierCounterRow()) {
                // for now, we don't do anything at qualifier counter row
                phckTable.incrementQualifierCount();
            } else if (row.isColumnRow()) {
                // update the column count
                phckTable.incrementColumnCount();
            } else if (row.isLinkRow()) {
                //update the linking relation
                phckTable.addLinkRow(row);
            }
        }
    }

    public void processNoneSystemLevelCheck(PhoenixConnection phoenixConnection) {

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
            processNoneSystemLevelCheck(phoenixConnection);

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
        int result = ToolRunner.run(new PhoenixIntegrityCheckAndRepairTool(), args);
        System.exit(result);
    }
}
