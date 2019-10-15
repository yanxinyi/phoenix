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
package org.apache.phoenix.mapreduce.util;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableType;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;

public class PhckUtil {
    public enum PHCK_STATE {
        ORPHAN_ROW,
        ORPHAN_VIEW,
        MISMATCH_COLUMN_COUNT,
        INVALID_SYSTEM_TABLE_LINK,
        INVALID_SYSTEM_TABLE,
        VALID,
    }

    public enum PHCK_ROW_RESOURCE {
        CATALOG,
        CHILD_LINK,
    }

    public static final String BASE_SELECT_QUERY = "SELECT " +
            TENANT_ID + ", " + TABLE_SCHEM + "," + TABLE_NAME + "," + TABLE_TYPE + "," +
            COLUMN_FAMILY + "," + COLUMN_NAME + "," + COLUMN_COUNT + "," + TABLE_TYPE + "," +
            INDEX_STATE + "," + INDEX_TYPE + "," + VIEW_TYPE + "," + COLUMN_QUALIFIER_COUNTER +
            " FROM " + SYSTEM_CATALOG_NAME;

    public static Options getOptions() {
        final Options options = new Options();
        return options;
    }

    public static void printHelpAndExit(String errorMessage, Options options) {
        System.err.println(errorMessage);
        printHelpAndExit(options, 1);
    }

    public static void printHelpAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        System.exit(exitCode);
    }
}
