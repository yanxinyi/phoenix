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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.mapreduce.util.PhckUtil;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_TYPE;

public class PhoenixIntegrityCheckAndRepairTool extends Configured implements Tool {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PhoenixIntegrityCheckAndRepairTool.class);

    private HashSet<PhckUtil.Row> orphanRowSet = new HashSet<>();
    private HashMap<String, PhckUtil.Table> allTables = new HashMap<>();
    /**
     SELECT
        TENANT_ID, TABLE_SCHEM,TABLE_NAME,TABLE_TYPE,
        COLUMN_FAMILY,COLUMN_NAME,COLUMN_COUNT,LINK_TYPE,
        INDEX_STATE,INDEX_TYPE,VIEW_TYPE,QUALIFIER_COUNTER
     FROM SYSTEM.CATALOG
        WHERE TABLE_SCHEM NOT LIKE 'SYSTEM%';
     */



    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new PhoenixIntegrityCheckAndRepairTool(), args);
        System.exit(result);
//        Configuration conf = HBaseConfiguration.create();
//        HBaseAdmin admin = new HBaseAdmin(conf);
//        admin.getTableDescriptor("table");
    }
}
