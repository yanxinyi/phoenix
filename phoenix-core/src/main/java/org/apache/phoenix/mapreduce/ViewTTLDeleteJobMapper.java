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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.ViewInfoTracker;
import org.apache.phoenix.mapreduce.util.ViewInfoWritable.ViewInfoJobState;
import org.apache.phoenix.mapreduce.util.MultiViewJobStatusTracker;
import org.apache.phoenix.mapreduce.util.DefaultMultiViewJobStatusTracker;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ViewTTLDeleteJobMapper extends Mapper<NullWritable, ViewInfoTracker, NullWritable, NullWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewTTLDeleteJobMapper.class);
    private MultiViewJobStatusTracker multiViewJobStatusTracker;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_RETRY_SLEEP_TIME_IN_MS = 10000;

    private void initMultiViewJobStatusTracker(Configuration config) throws Exception {
        try {
            Class<?> defaultViewDeletionTrackerClass = DefaultMultiViewJobStatusTracker.class;
            if (config.get(PhoenixConfigurationUtil.MAPREDUCE_MULTI_INPUT_MAPPER_TRACKER_CLAZZ) != null) {
                LOGGER.info("Using customized tracker class : " +
                        config.get(PhoenixConfigurationUtil.MAPREDUCE_MULTI_INPUT_MAPPER_TRACKER_CLAZZ));
                defaultViewDeletionTrackerClass = Class.forName(
                        config.get(PhoenixConfigurationUtil.MAPREDUCE_MULTI_INPUT_MAPPER_TRACKER_CLAZZ));
            } else {
                LOGGER.info("Using default tracker class ");
            }
            this.multiViewJobStatusTracker = (MultiViewJobStatusTracker) defaultViewDeletionTrackerClass.newInstance();
        } catch (Exception e) {
            LOGGER.error("Getting exception While initializing initMultiViewJobStatusTracker with error message");
            LOGGER.error("stack trace" + e.getStackTrace().toString());
            throw e;
        }
    }

    @Override
    protected void map(NullWritable key, ViewInfoTracker value, Context context) throws IOException  {
        try {
            final Configuration config = context.getConfiguration();

            if (this.multiViewJobStatusTracker == null) {
                initMultiViewJobStatusTracker(config);
            }

            LOGGER.debug(String.format("Deleting from view %s, TenantID %s, and TTL value: %d",
                    value.getViewName(), value.getTenantId(), value.getPhoenixTtl()));

            deletingExpiredRows(value, config, context);

        } catch (SQLException e) {
            LOGGER.error("Mapper got an exception while deleting expired rows : " + e.getMessage() );
            throw new IOException(e.getMessage(), e.getCause());
        } catch (Exception e) {
            LOGGER.error("Getting IOException while running View TTL Deletion Job mapper with error : "
                    + e.getMessage());
            throw new IOException(e.getMessage(), e.getCause());
        }
    }

    private void deletingExpiredRows(ViewInfoTracker value, Configuration config, Context context) throws Exception {
        try (PhoenixConnection connection = (PhoenixConnection) ConnectionUtil.getInputConnection(config)) {
            if (value.getTenantId() != null && !value.getTenantId().equals("NULL")) {
                Properties props = new Properties();
                props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, value.getTenantId());

                try (PhoenixConnection tenantConnection = (PhoenixConnection)
                        DriverManager.getConnection(connection.getURL(), props)) {
                    deletingExpiredRows(tenantConnection, value, config, context);
                }
            } else {
                deletingExpiredRows(connection, value, config, context);
            }
        }
    }

    private void deletingExpiredRows(PhoenixConnection connection, ViewInfoTracker viewInfoTracker,
                                     Configuration config, Context context) throws Exception {
        try {
            PTable ptable = PhoenixRuntime.getTable(connection, viewInfoTracker.getViewName());
            String deleteIfExpiredStatement = "SELECT /*+ NO_INDEX */ count(*) FROM " + viewInfoTracker.getViewName();

            if (viewInfoTracker.isIndexRelation()) {
                ptable = PhoenixRuntime.getTable(connection, viewInfoTracker.getRelationName());
                deleteIfExpiredStatement = "SELECT count(*) FROM " + viewInfoTracker.getRelationName();
            }

            deletingExpiredRows(connection, ptable, deleteIfExpiredStatement, config, context, viewInfoTracker);

        } catch (Exception e) {
            LOGGER.error(String.format("Had an issue to process the view: %s, " +
                    "see error %s ", viewInfoTracker.toString(),e.getMessage()));
        }
    }

    /*
     * Each Mapper that receives a MultiPhoenixViewInputSplit will execute a DeleteMutation/Scan
     *  (With DELETE_TTL_EXPIRED attribute) per view for all the views and view indexes in the split.
     * For each DeleteMutation, it bounded by the view start and stop keys for the region and
     *  TTL attributes and Delete Hint.
     */
    private boolean deletingExpiredRows(PhoenixConnection connection, PTable pTable,
                                        String deleteIfExpiredStatement, Configuration config,
                                        Context context, ViewInfoTracker viewInfoTracker) throws Exception {

        try (PhoenixStatement pstmt = new PhoenixStatement(connection).unwrap(PhoenixStatement.class)) {
            String sourceTableName = pTable.getTableName().getString();
            this.multiViewJobStatusTracker.updateJobStatus(viewInfoTracker, 0,
                    ViewInfoJobState.PREP.getValue(), config, 0, context.getJobName(), sourceTableName);
            final QueryPlan queryPlan = pstmt.optimizeQuery(deleteIfExpiredStatement);
            final Scan scan = queryPlan.getContext().getScan();
            byte[] emptyColumnFamilyName = SchemaUtil.getEmptyColumnFamily(pTable);
            byte[] emptyColumnName =
                    pTable.getEncodingScheme() == PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS ?
                            QueryConstants.EMPTY_COLUMN_BYTES :
                            pTable.getEncodingScheme().encode(QueryConstants.ENCODED_EMPTY_COLUMN_NAME);

            scan.setAttribute(BaseScannerRegionObserver.EMPTY_COLUMN_FAMILY_NAME, emptyColumnFamilyName);
            scan.setAttribute(BaseScannerRegionObserver.EMPTY_COLUMN_QUALIFIER_NAME, emptyColumnName);
            scan.setAttribute(BaseScannerRegionObserver.DELETE_PHOENIX_TTL_EXPIRED, PDataType.TRUE_BYTES);
            scan.setAttribute(BaseScannerRegionObserver.MASK_PHOENIX_TTL_EXPIRED, PDataType.FALSE_BYTES);
            scan.setAttribute(BaseScannerRegionObserver.PHOENIX_TTL, Bytes.toBytes(Long.valueOf(viewInfoTracker.getPhoenixTtl())));
            this.multiViewJobStatusTracker.updateJobStatus(viewInfoTracker, 0,
                    ViewInfoJobState.RUNNING.getValue(), config, 0, context.getJobName(), sourceTableName);
            return addingDeletionMarkWithRetries(pstmt, viewInfoTracker, config, context, queryPlan, sourceTableName);
        }
    }

    private boolean addingDeletionMarkWithRetries(PhoenixStatement stmt, ViewInfoTracker viewInfoTracker,
                                                  Configuration config, Context context,
                                                  QueryPlan queryPlan, String sourceTableName) throws Exception {
        int retry = 0;
        long startTime = System.currentTimeMillis();
        String viewInfo = viewInfoTracker.getTenantId() == null ?
                viewInfoTracker.getViewName() : viewInfoTracker.getTenantId() + "." + viewInfoTracker.getViewName();

        while (retry < DEFAULT_MAX_RETRIES) {
            try {
                PhoenixResultSet rs = stmt.newResultSet(
                        queryPlan.iterator(), queryPlan.getProjector(), queryPlan.getContext());

                long numberOfDeletedRows = 0;
                if (rs.next()) {
                    numberOfDeletedRows = rs.getLong(1);
                }
                this.multiViewJobStatusTracker.updateJobStatus(viewInfoTracker, numberOfDeletedRows,
                        ViewInfoJobState.SUCCEEDED.getValue(), config,
                        System.currentTimeMillis() - startTime, context.getJobName(), sourceTableName);
                context.getCounter(ViewTTLTool.MR_COUNTER_METRICS.SUCCEED).increment(1);
                return true;
            } catch (Exception e) {
                if (e instanceof SQLException && ((SQLException) e).getErrorCode() == SQLExceptionCode.TABLE_UNDEFINED.getErrorCode()) {
                    LOGGER.info(viewInfo + " has been deleted : " + e.getMessage());
                    this.multiViewJobStatusTracker.updateJobStatus(viewInfoTracker, 0,
                            ViewInfoJobState.DELETED.getValue(), config, 0, context.getJobName(), sourceTableName);
                    context.getCounter(ViewTTLTool.MR_COUNTER_METRICS.SUCCEED).increment(1);
                    return false;
                }
                retry++;

                if (retry == DEFAULT_MAX_RETRIES) {
                    LOGGER.error("Deleting " + viewInfo + " expired rows has an exception for : " + e.getMessage());
                    this.multiViewJobStatusTracker.updateJobStatus(viewInfoTracker, 0,
                            ViewInfoJobState.FAILED.getValue(), config, 0, context.getJobName(), sourceTableName);
                    context.getCounter(ViewTTLTool.MR_COUNTER_METRICS.FAILED).increment(1);
                    throw e;
                } else {
                    Thread.sleep(DEFAULT_RETRY_SLEEP_TIME_IN_MS);
                }
            }
        }
        return false;
    }
}