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
package org.apache.phoenix.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;

public final class ViewIndexIdRetrieveUtil {
    public static final int VIEW_INDEX_ID_BIGINT_TYPE_PTR_LEN = 9;
    public static final int VIEW_INDEX_ID_SMALLINT_TYPE_VALUE_LEN = 3;

    private ViewIndexIdRetrieveUtil() {

    }

    public static Cell getViewIndexIdKeyValueInLongDataFormat(Cell viewIndexIdCell) {
        Short valueInShort = (Short) PSmallint.INSTANCE.toObject(
                viewIndexIdCell.getValueArray(), viewIndexIdCell.getValueOffset(),
                viewIndexIdCell.getValueLength(), PSmallint.INSTANCE, SortOrder.ASC);
        byte[] valueBytesInLong = new byte[VIEW_INDEX_ID_BIGINT_TYPE_PTR_LEN];
        byte[] valueBytes = PLong.INSTANCE.toBytes(valueInShort);
        System.arraycopy(valueBytes, 0, valueBytesInLong,
                0, VIEW_INDEX_ID_BIGINT_TYPE_PTR_LEN - 1);
        return buildNewCell(viewIndexIdCell, valueBytesInLong);
    }

    public static Cell buildNewCell(Cell viewIndexIdCell, byte[] newVal) {
        KeyValue keyValue = new KeyValue(
                viewIndexIdCell.getRowArray(), viewIndexIdCell.getRowOffset(),
                viewIndexIdCell.getRowLength(),
                viewIndexIdCell.getFamilyArray(), viewIndexIdCell.getFamilyOffset(),
                viewIndexIdCell.getFamilyLength(),
                viewIndexIdCell.getQualifierArray(), viewIndexIdCell.getQualifierOffset(),
                viewIndexIdCell.getQualifierLength(),
                viewIndexIdCell.getTimestamp(),KeyValue.Type.Put,
                newVal, 0,newVal.length);
        keyValue.setSequenceId(viewIndexIdCell.getSequenceId());
        return keyValue;
    }

    public static Cell getViewIndexIdKeyValueInShortDataFormat(Cell viewIndexIdCell) {
        Long valueInShort = (Long) PLong.INSTANCE.toObject(
                viewIndexIdCell.getValueArray(), viewIndexIdCell.getValueOffset(),
                viewIndexIdCell.getValueLength(), PLong.INSTANCE, SortOrder.ASC);
        byte[] valueBytesInShort = new byte[VIEW_INDEX_ID_SMALLINT_TYPE_VALUE_LEN];
        byte[] valueBytes = PSmallint.INSTANCE.toBytes(valueInShort);
        System.arraycopy(valueBytes, 0, valueBytesInShort,
                0, VIEW_INDEX_ID_SMALLINT_TYPE_VALUE_LEN - 1);

        return buildNewCell(viewIndexIdCell, valueBytesInShort);
    }
}
