/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.glutenproject.substrait.rel;

import io.glutenproject.backendsapi.BackendsApiManager;

import com.google.protobuf.StringValue;
import io.substrait.proto.ReadRel;

import java.util.ArrayList;
import java.util.List;

public class ExtensionTableNode implements SplitInfo {
  private static final String MERGE_TREE = "MergeTree;";
  private Long minPartsNum;
  private Long maxPartsNum;
  private String database;
  private String tableName;
  private String relativePath;
  private String tableSchemaJson;
  private StringBuffer extensionTableStr = new StringBuffer(MERGE_TREE);
  private StringBuffer partPathList = new StringBuffer("");
  private final List<String> preferredLocations = new ArrayList<>();

  private String orderByKey;

  private String primaryKey;

  private List<String> partList;
  private List<Long> starts;
  private List<Long> lengths;

  ExtensionTableNode(
      Long minPartsNum,
      Long maxPartsNum,
      String database,
      String tableName,
      String relativePath,
      String orderByKey,
      String primaryKey,
      List<String> partList,
      List<Long> starts,
      List<Long> lengths,
      String tableSchemaJson,
      List<String> preferredLocations) {
    this.minPartsNum = minPartsNum;
    this.maxPartsNum = maxPartsNum;
    this.database = database;
    this.tableName = tableName;
    if (relativePath.startsWith("/")) {
      this.relativePath = relativePath.substring(1);
    } else {
      this.relativePath = relativePath;
    }
    this.tableSchemaJson = tableSchemaJson;
    this.orderByKey = orderByKey;
    this.primaryKey = primaryKey;
    this.partList = partList;
    this.starts = starts;
    this.lengths = lengths;
    this.preferredLocations.addAll(preferredLocations);

    // New: MergeTree;{database}\n{table}\n{orderByKey}\n{primaryKey}\n{relative_path}\n
    // {part_path1}\n{part_path2}\n...
    for (int i = 0; i < this.partList.size(); i++) {
      partPathList
          .append(this.partList.get(i))
          .append("\n")
          .append(this.starts.get(i))
          .append("\n")
          .append(this.lengths.get(i))
          .append("\n");
    }

    extensionTableStr
        .append(database)
        .append("\n")
        .append(tableName)
        .append("\n")
        .append(tableSchemaJson)
        .append("\n")
        .append(this.orderByKey)
        .append("\n");

    if (!this.orderByKey.isEmpty() && !this.orderByKey.equals("tuple()")) {
      extensionTableStr.append(this.primaryKey).append("\n");
    }
    extensionTableStr.append(this.relativePath).append("\n").append(partPathList);

    /* old format
    if (!this.partList.isEmpty()) {
    } else {
      // Old: MergeTree;{database}\n{table}\n{relative_path}\n{min_part}\n{max_part}\n
      extensionTableStr
          .append(database)
          .append("\n")
          .append(tableName)
          .append("\n")
          .append(relativePath)
          .append("\n")
          .append(this.minPartsNum)
          .append("\n")
          .append(this.maxPartsNum)
          .append("\n");
    } */
  }

  @Override
  public List<String> preferredLocations() {
    return this.preferredLocations;
  }

  public ReadRel.ExtensionTable toProtobuf() {
    ReadRel.ExtensionTable.Builder extensionTableBuilder = ReadRel.ExtensionTable.newBuilder();
    StringValue extensionTable =
        StringValue.newBuilder().setValue(extensionTableStr.toString()).build();
    extensionTableBuilder.setDetail(
        BackendsApiManager.getTransformerApiInstance().packPBMessage(extensionTable));
    return extensionTableBuilder.build();
  }
}
