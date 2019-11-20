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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSizeCalculator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.ViewInfo;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class PhoenixMultiViewInputFormat<T extends DBWritable> extends PhoenixInputFormat {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixMultiViewInputFormat.class);

    public PhoenixMultiViewInputFormat() {

    }
    private ConcurrentHashMap<InputSplit, QueryPlan> queryPlanConcurrentHashMap = new ConcurrentHashMap<>();

    @Override
    public RecordReader<NullWritable,T> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        String serializedViewInfo = configuration.get(split.toString());
        setConfiguration(serializedViewInfo, configuration);
        final QueryPlan queryPlan = getQueryPlan(context,configuration);
        final Class<T> inputClass = (Class<T>) PhoenixConfigurationUtil.getInputClass(configuration);
        return getPhoenixRecordReader(inputClass, configuration, queryPlan);
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        List<InputSplit> inputSplitList = new ArrayList<>();
        String serializedViewInfoStrings =
                configuration.get(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_INFO_LIST);
        String[] viewInfoList =
                serializedViewInfoStrings.split(ViewInfo.VIEW_INFO_DESERIALIZED_SPLITTER);

        // Get the RegionSizeCalculator
        try(org.apache.hadoop.hbase.client.Connection connection =
                    HBaseFactoryProvider.getHConnectionFactory().createConnection(configuration)) {
            RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(
                    configuration.get(PhoenixConfigurationUtil.PHYSICAL_TABLE_NAME)));
            RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(regionLocator, connection
                    .getAdmin());

            List<ViewInfo> viewInfos = new ArrayList<>();
            for (String serializedViewInfo : viewInfoList) {
                viewInfos.add(new ViewInfo(serializedViewInfo));
            }

            inputSplitList.add(new PhoenixMultiViewInputSplit(viewInfos, viewInfos.size(), ))

        }



//        String serializedViewInfoStrings = configuration.get(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_INFO_LIST);
//        String[] viewInfoList = serializedViewInfoStrings.split(ViewInfo.VIEW_INFO_DESERIALIZED_SPLITTER);
//        for (String serializedViewInfo : viewInfoList) {
//            setConfiguration(serializedViewInfo, configuration);
//            final QueryPlan queryPlan = getQueryPlan(context,configuration);
//            List<InputSplit> tmp = generateSplits(queryPlan, configuration);
//            for (InputSplit inputSplit:tmp) {
//                configuration.set(inputSplit.toString(), serializedViewInfo);
//            }
//            inputSplitList.addAll(tmp);
//        }
        return inputSplitList;
    }

    private void setConfiguration(String serializedViewInfo,Configuration configuration) {
        ViewInfo viewInfo = new ViewInfo(serializedViewInfo);
//            Configuration config = new Configuration(configuration);
//            if (viewInfo.getTenantId() != null) {
//                config.set(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID, viewInfo.getTenantId());
//            }
        configuration.set(PhoenixConfigurationUtil.INPUT_TABLE_NAME, viewInfo.getViewName());
        configuration.set(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL, viewInfo.getViewTtl().toString());

    }
//    @Override
//    protected QueryPlan getQueryPlan(final JobContext context, final Configuration configuration)
//            throws IOException {
//        Preconditions.checkNotNull(context);
//        try {
//            final String txnScnValue = configuration.get(PhoenixConfigurationUtil.TX_SCN_VALUE);
//            final String currentScnValue = configuration.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE);
//            final String tenantId = configuration.get(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID);
//            final Properties overridingProps = new Properties();
//            if(txnScnValue==null && currentScnValue!=null) {
//                overridingProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, currentScnValue);
//            }
//            if (tenantId != null && configuration.get(PhoenixRuntime.TENANT_ID_ATTRIB) == null){
//                overridingProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
//            }
//            try (final Connection connection = ConnectionUtil.getInputConnection(configuration, overridingProps);
//                 final Statement statement = connection.createStatement()) {
//
//                final String selectStatement = PhoenixConfigurationUtil.getSelectStatement(configuration);
//                Preconditions.checkNotNull(selectStatement);
//
//                final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
//                // Optimize the query plan so that we potentially use secondary indexes
//                final QueryPlan queryPlan = pstmt.optimizeQuery(selectStatement);
//                final Scan scan = queryPlan.getContext().getScan();
//                // since we can't set a scn on connections with txn set TX_SCN attribute so that the max time range is set by BaseScannerRegionObserver
//                if (txnScnValue != null) {
//                    scan.setAttribute(BaseScannerRegionObserver.TX_SCN, Bytes.toBytes(Long.valueOf(txnScnValue)));
//                }
//
//                // setting the snapshot configuration
//                String snapshotName = configuration.get(PhoenixConfigurationUtil.SNAPSHOT_NAME_KEY);
//                if (snapshotName != null)
//                    PhoenixConfigurationUtil.setSnapshotNameKey(queryPlan.getContext().getConnection().
//                            getQueryServices().getConfiguration(), snapshotName);
//
//                return queryPlan;
//            }
//        } catch (Exception exception) {
//            LOGGER.error(String.format("Failed to get the query plan with error [%s]",
//                    exception.getMessage()));
//            throw new RuntimeException(exception);
//        }
//    }
//
//    private List<InputSplit> generateSplits(final QueryPlan qplan, Configuration config) throws IOException {
//
//    }
}
