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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;

public class PhckNonSystemTableValidator extends Configured implements Tool {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PhckNonSystemTableValidator.class);

    private HashSet<PhckRow> invalidRowSet = new HashSet<>();
    private HashSet<PhckTable> invalidTableSet = new HashSet<>();

    private HashMap<String, PhckTable> allTables = new HashMap<>();

    private static final String SELECT_QUERY = PhckUtil.BASE_SELECT_QUERY +
            " WHERE " + TABLE_SCHEM + " NOT LIKE '" + SYSTEM_SCHEMA_NAME + "'";


    private void fetchAllRows(PhoenixConnection phoenixConnection) throws Exception {
        ResultSet viewRS = phoenixConnection.createStatement().executeQuery(SELECT_QUERY);

        while (viewRS.next()) {
            PhckRow row = new PhckRow(viewRS, PhckUtil.PHCK_ROW_RESOURCE.CATALOG);
            PhckTable phckTable;
            //added row table name to the set.
            String fullName = row.getFullName();

            if (row.isHeadRow()) {
                phckTable = new PhckTable(row.getTenantId(), row.getTableSchema(),
                        row.getTableName(),row.getTableType(),row.getColumnCount(),
                        row.getIndexState());
                allTables.put(fullName, phckTable);
                continue;
            }

            if (!allTables.containsKey(fullName)) {
                // found an orphan row
                row.setPhckState(PhckUtil.PHCK_STATE.ORPHAN_ROW);
                invalidRowSet.add(row);
            }

            phckTable = allTables.get(fullName);
            if (row.isQualifierCounterRow()) {
                // for now, we don't do anything at qualifier counter row
                phckTable.incrementQualifierCount();
            } else if (row.isColumnRow()) {
                // update the column count
                phckTable.incrementColumnCount();
            } else if (row.isLinkRow()) {
                //TODO: figure out do we need link relation here
            }
        }
    }

    private void processValidationCheck() {
        Set<String> orphanViewsName = allTables.keySet();
        Queue<PhckTable> queue = new LinkedList<>();

        //build graph tree from the base table
        for (PhckTable phckTable : allTables.values()) {
            if (phckTable.isPhysicalTable()) {
                queue.add(phckTable);
            }
        }

        while (!queue.isEmpty()) {
            PhckTable phckTable = queue.poll();
            orphanViewsName.remove(phckTable.getFullName());

            if (phckTable.isColumnCountMatches()) {
                queue.addAll(phckTable.getChildren());
            } else {
                phckTable.setPhckState(PhckUtil.PHCK_STATE.MISMATCH_COLUMN_COUNT);
                invalidTableSet.add(phckTable);
                // if base table is corrupted, all child view/index are corrupted too.
                for (PhckTable childTable : phckTable.getChildren()) {
                    childTable.setPhckState(PhckUtil.PHCK_STATE.MISMATCH_COLUMN_COUNT);
                    invalidTableSet.add(childTable);
                    orphanViewsName.remove(childTable.getFullName());
                }
            }
        }

        for (String orphanViewName : orphanViewsName) {
            PhckTable orphanView = allTables.get(orphanViewName);
            orphanView.setPhckState(PhckUtil.PHCK_STATE.ORPHAN_VIEW);
            invalidTableSet.add(orphanView);
        }
    }

    private void buildLinkGraph(PhoenixConnection phoenixConnection) throws Exception {
        ResultSet viewRS = phoenixConnection.createStatement().executeQuery(
                PhckUtil.SELECT_CHILD_LINK_QUERY);

        while (viewRS.next()) {
            PhckRow row = new PhckRow(viewRS, PhckUtil.PHCK_ROW_RESOURCE.CHILD_LINK);
            if (row.getLinkType() == null) {
                row.setPhckState(PhckUtil.PHCK_STATE.INVALID_CHILD_LINK_ROW);
                invalidRowSet.add(row);
            } else if (row.getLinkType() == PTable.LinkType.CHILD_TABLE) {
                /**
                 * 0: jdbc:phoenix:localhost:57286> SELECT * FROM SYSTEM.CHILD_LINK;
                 * +-----------+-------------+------------+-------------+---------------+-----------+
                 * | TENANT_ID | TABLE_SCHEM | TABLE_NAME | COLUMN_NAME | COLUMN_FAMILY | LINK_TYPE |
                 * +-----------+-------------+------------+-------------+---------------+-----------+
                 * |           |             | AAA        |             | AAA_V1        | 4         |
                 * |           |             | AAA_V1     |             | AAA_V2        | 4         |
                 * |           |             | AAA_V2     |             | AAA_V3        | 4         |
                 * +-----------+-------------+------------+-------------+---------------+-----------+
                 */
                String parent = row.getFullName();
                String child = row.getColumnFamily();
                //TODO: how to construct parentTableName, as well as childTableName
                if (!allTables.containsKey(parent) || !allTables.containsKey(child)) {
                    // TODO INVALID ROW
                    row.setPhckState(PhckUtil.PHCK_STATE.INVALID_CHILD_LINK_ROW);
                    invalidRowSet.add(row);
                } else {
                    PhckTable parentTable = allTables.get(parent);
                    PhckTable childTable = allTables.get(child);
                    parentTable.addChildTable(childTable);
                    childTable.addParentTable(parentTable);
                }
            } else {
                // Do we have other scenario here?
            }

        }
    }

    private void processNoneSystemLevelCheck(PhoenixConnection phoenixConnection) throws Exception {
        fetchAllRows(phoenixConnection);
        buildLinkGraph(phoenixConnection);
        processValidationCheck();
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

    public void parseOptions(String[] args) {

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
