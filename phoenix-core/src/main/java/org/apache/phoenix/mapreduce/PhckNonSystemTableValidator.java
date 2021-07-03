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

import org.apache.hadoop.hbase.client.Admin;
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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.phoenix.mapreduce.util.PhckUtil.EMPTY_COLUMN_VALUE;
import static org.apache.phoenix.mapreduce.util.PhckUtil.closeConnection;

public class PhckNonSystemTableValidator extends Configured implements Tool {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PhckNonSystemTableValidator.class);

    private HashSet<PhckRow> invalidRowSet = new HashSet<>();
    private HashSet<PhckTable> invalidTableSet = new HashSet<>();

    private ConcurrentHashMap<String, PhckTable> allTables = new ConcurrentHashMap<>();

    private void fetchAllRows(PhoenixConnection phoenixConnection) throws Exception {
        ResultSet viewRS = phoenixConnection.createStatement().executeQuery(PhckUtil.BASE_SELECT_QUERY);

        while (viewRS.next()) {
            PhckRow row = new PhckRow(viewRS, PhckUtil.PHCK_ROW_RESOURCE.CATALOG);
            PhckTable phckTable;
            //added row table name to the set.
            String fullName = row.getFullName();
            if (row.isSchemaRow()) {
                continue;
            } else if (row.isHeadRow()) {
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
                LOGGER.info("Invalid Row: " + row.getSerializedValue());
                continue;
            }

            phckTable = allTables.get(fullName);
            if (row.isQualifierCounterRow()) {
                // for now, we don't do anything at qualifier counter row
                phckTable.incrementQualifierCount();
            } else if (row.isColumnRow()) {
                // update the column count
                phckTable.incrementColumnCount();
            } else if (row.isLinkRow()) {
                phckTable.addLinkRow(row);
            }
        }
    }

    private void processValidationCheck(PhoenixConnection phoenixConnection) throws Exception {
        Set<String> orphanViewsName = allTables.keySet();
        Queue<PhckTable> processQueue = new LinkedList<>();
        Queue<PhckTable> invalidTableQueue = new LinkedList<>();
        Admin admin = phoenixConnection.getQueryServices().getAdmin();

        //build graph tree from the base table
        Iterator<PhckTable> it = allTables.values().iterator();
        while (it.hasNext()) {
            PhckTable phckTable = it.next();
            if (phckTable.isSystemTable()) {
                orphanViewsName.remove(phckTable.getFullName());
                continue;
            }
            if (phckTable.isPhysicalTable() || phckTable.isIndexTable()) {
                processQueue.add(phckTable);
            }

            if (phckTable.hasPendingRelationRowToProcess()) {
                for (PhckRow phckRow : phckTable.getRelationRows()) {
                    if (phckRow.getLinkType() == PTable.LinkType.INDEX_TABLE) {
                        String relationTableName = phckRow.getRelatedTableFullName();
                        // Link from a table to its index table
                        if (allTables.containsKey(relationTableName)) {
                            phckTable.setPhysicalTable(allTables.get(relationTableName));
                        } else {
                            phckRow.setPhckState(PhckUtil.PHCK_STATE.INVALID_SYSTEM_TABLE_LINK);
                            invalidRowSet.add(phckRow);
                            LOGGER.info("Invalid Row: " + phckRow.getSerializedValue());
                        }
                    } else if (phckRow.getLinkType() == PTable.LinkType.PARENT_TABLE) {
                        // Link from a view to its parent table
                        String relationTableName = phckRow.getRelatedTableFullName();
                        if (allTables.containsKey(relationTableName)) {
                            PhckTable childPhckTable = allTables.get(relationTableName);
                            childPhckTable.addChildTable(phckTable);
                        } else {
                            // if parent doesn't exist, it means linking is bad and current view
                            // has problem too.
                            phckTable.setPhckState(PhckUtil.PHCK_STATE.INVALID_SYSTEM_TABLE_LINK);
                            invalidTableQueue.add(phckTable);
                            phckRow.setPhckState(PhckUtil.PHCK_STATE.INVALID_SYSTEM_TABLE_LINK);
                            invalidRowSet.add(phckRow);
                            LOGGER.info("Invalid Row: " + phckRow.getSerializedValue());

                        }
                    } else if (phckRow.getLinkType() == PTable.LinkType.VIEW_INDEX_PARENT_TABLE) {
                        String relationTableName = phckRow.getRelatedTableFullName();
                        if (allTables.containsKey(relationTableName)) {
                            PhckTable parentViewPhckTable = allTables.get(relationTableName);
                            phckTable.setParent(parentViewPhckTable);
                        } else {
                            phckTable.setPhckState(PhckUtil.PHCK_STATE.INVALID_SYSTEM_TABLE_LINK);
                            invalidTableQueue.add(phckTable);
                            invalidRowSet.add(phckRow);
                            LOGGER.info("Invalid Row: " + phckRow.getSerializedValue());
                        }
                    } else if (phckRow.getLinkType() == PTable.LinkType.PHYSICAL_TABLE) {
                        String relationTableName = phckRow.getRelatedTableFullName();
                        if (allTables.containsKey(relationTableName)) {
                            PhckTable physicalPhckTable = allTables.get(relationTableName);
                            phckTable.setPhysicalTable(physicalPhckTable);
                        } else {
                            if (phckTable.getTableType() == PTableType.INDEX) {
                                phckTable.setIndexPhysicalName(relationTableName);
                            } else {
                                phckTable.setPhckState(PhckUtil.PHCK_STATE.INVALID_CHILD_LINK_ROW);
                                invalidTableQueue.add(phckTable);
                                invalidRowSet.add(phckRow);
                                LOGGER.info("Invalid Row: " + phckRow.getSerializedValue());
                            }
                        }
                    }
                }
            }
        }

        while (!processQueue.isEmpty()) {
            PhckTable phckTable = processQueue.poll();

            if (phckTable.isPhysicalTable() && !admin.tableExists(phckTable.getHBaseTableName())) {
                phckTable.setPhckState(PhckUtil.PHCK_STATE.INVALID_TABLE);
                invalidTableQueue.add(phckTable);
            } else if (phckTable.isIndexTable()) {
                if (!admin.tableExists(phckTable.getHBaseTableName()) &&
                        !admin.tableExists(phckTable.getIndexPhysicalName())) {
                    invalidTableQueue.add(phckTable);
                }
            }

            orphanViewsName.remove(phckTable.getFullName());

            if (phckTable.isColumnCountMatches()) {
                if (phckTable.getChildren() != null) {
                    processQueue.addAll(phckTable.getChildren());
                }
            } else {
                phckTable.setPhckState(PhckUtil.PHCK_STATE.MISMATCH_COLUMN_COUNT);
                invalidTableQueue.add(phckTable);
            }
        }

        for (String orphanViewName : orphanViewsName) {
            PhckTable orphanView = allTables.get(orphanViewName);
            orphanView.setPhckState(PhckUtil.PHCK_STATE.ORPHAN_VIEW);
            invalidTableQueue.add(orphanView);
        }

        while (!invalidTableQueue.isEmpty()) {
            PhckTable phckTable = invalidTableQueue.poll();
            if (phckTable.getPhckState() == PhckUtil.PHCK_STATE.VALID) {
                phckTable.setPhckState(phckTable.getParent().getPhckState());
            }
            invalidTableSet.add(phckTable);
            LOGGER.info("Invalid Table: " + phckTable.getSerializedValue());
            if (phckTable.getChildren() != null) {
                for (PhckTable childTable : phckTable.getChildren()) {
                    childTable.setPhckState(phckTable.getPhckState());
                    invalidTableQueue.add(childTable);
                }
            }
        }
    }

    private void buildLinkGraph(PhoenixConnection phoenixConnection) throws Exception {
        ResultSet viewRS = phoenixConnection.createStatement().executeQuery(
                PhckUtil.SELECT_CHILD_LINK_QUERY);

        while (viewRS.next()) {
            PhckRow row = new PhckRow(viewRS, PhckUtil.PHCK_ROW_RESOURCE.CHILD_LINK);
            if (row.getLinkType() == PTable.LinkType.CHILD_TABLE) {
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
                String child = row.getRelatedTableFullName();
                if (!allTables.containsKey(parent) || !allTables.containsKey(child)) {
                    // TODO INVALID ROW
                    row.setPhckState(PhckUtil.PHCK_STATE.INVALID_CHILD_LINK_ROW);
                    invalidRowSet.add(row);
                    LOGGER.info("Invalid Row: " + row.getSerializedValue());
                } else {
                    PhckTable parentTable = allTables.get(parent);
                    PhckTable childTable = allTables.get(child);
                    parentTable.addChildTable(childTable);
                    childTable.addParentTable(parentTable);
                }
            } else {
                // Do we have other scenario here?
                row.setPhckState(PhckUtil.PHCK_STATE.INVALID_CHILD_LINK_ROW);
                invalidRowSet.add(row);
            }
        }
    }

    private void processNoneSystemLevelCheck(PhoenixConnection phoenixConnection) throws Exception {
        fetchAllRows(phoenixConnection);
        buildLinkGraph(phoenixConnection);
        processValidationCheck(phoenixConnection);
    }

    public Set<PhckRow> getInvalidRowSet() {
        return this.invalidRowSet;
    }

    public Set<PhckTable> getInvalidTableSet() {
        return this.invalidTableSet;
    }

    public void fix(Connection connection) throws Exception {
        PreparedStatement ps = null;
        Iterator<PhckRow> it = invalidRowSet.iterator();
        while (it .hasNext()) {
            PhckRow phckRow = it.next();
            if (phckRow.isColumnRow()) {
                String query = "DELETE FROM SYSTEM.CATALOG WHERE " +
                        "TABLE_TYPE IS NULL AND COLUMN_FAMILY IS NOT NULL AND COLUMN_COUNT IS NULL AND " +
                        "LINK_TYPE IS NULL AND TABLE_NAME ='" + phckRow.getTableName() + "'";
                if (phckRow.getTableSchema() != null && phckRow.getTableSchema().length() > 0 &&
                    !phckRow.getTableSchema().equals(EMPTY_COLUMN_VALUE)) {
                    query = query + " AND TABLE_SCHEM = '" + phckRow.getTableSchema() + "'";
                }
                connection.createStatement().execute(query);
                connection.commit();
            } else if (phckRow.isQualifierCounterRow()) {
                    // todo
            } else if (phckRow.isLinkRow()){
                if (phckRow.getPhckRowResource() == PhckUtil.PHCK_ROW_RESOURCE.CATALOG) {
                    //todo
                }
            }
        }

        Iterator<PhckTable> itTable = invalidTableSet.iterator();

        while (itTable .hasNext()) {
            PhckTable phckTable = itTable.next();
            if (phckTable.getPhckState() == PhckUtil.PHCK_STATE.ORPHAN_VIEW) {
                //Todo
            } else if (phckTable.getPhckState() == PhckUtil.PHCK_STATE.MISMATCH_COLUMN_COUNT) {
                // update the column count
                String query = "UPSERT INTO SYSTEM.CATALOG (TABLE_SCHEM,TABLE_NAME, TABLE_TYPE,COLUMN_COUNT) VALUES(" +
                        "'" + phckTable.getTableSchema() + "','" +  phckTable.getTableName() + "','" +
                        phckTable.getTableType().getSerializedValue() + "'," + phckTable.getColumnCounter()+ ")";
                connection.createStatement().execute(query);
                connection.commit();
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
            processNoneSystemLevelCheck(phoenixConnection);

        } catch (Exception e) {
            LOGGER.error("Phck : An exception occurred "
                    + ExceptionUtils.getMessage(e) + " at:\n" +
                    ExceptionUtils.getStackTrace(e));
            return -1;
        } finally {
            closeConnection(connection, LOGGER);
        }

        return this.invalidRowSet.size() + this.invalidTableSet.size();
    }

    public void parseOptions(String[] args) {

    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new PhckNonSystemTableValidator(), args);
        System.exit(result);
    }
}
