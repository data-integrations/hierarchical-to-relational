/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.spark;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * HierarchyToRelationalUtils - utils class for {@link HierarchyToRelational} plugin
 * outputs relational data.
 */
public class HierarchyToRelationalUtils {

  private final Schema outputSchema;

  private final HierarchyToRelationalConfig config;
  private final List<String> nonMappedFields;
  private final Map<String, String> mappedFields;

  public HierarchyToRelationalUtils(HierarchyToRelationalConfig config, Schema outputSchema,
                                    List<String> nonMappedFields, Map<String, String> mappedFields) {
    this.config = config;
    this.outputSchema = outputSchema;
    this.nonMappedFields = nonMappedFields;
    this.mappedFields = mappedFields;
  }

  /**
   * Generate trees of each unique record in dataset using sql statement approach
   *
   * @param sparkExecutionPluginContext {@link SparkExecutionPluginContext}
   * @param javaRDD                     input data
   * @param inputSchema                 {@link Schema} input schema
   * @return dataset with rows of all tree records for each unique input record
   * @throws Exception on validation issues
   */
  private Dataset<Row> queryApproach(SparkExecutionPluginContext sparkExecutionPluginContext,
                                     JavaRDD<StructuredRecord> javaRDD,
                                     Schema inputSchema) throws Exception {
    JavaRDD<Row> map = javaRDD.map(
      structuredRecord -> DataFrames.toRow(structuredRecord, DataFrames.toDataType(inputSchema)));
    SQLContext sqlContext = new SQLContext(sparkExecutionPluginContext.getSparkContext());
    Dataset<Row> data = sqlContext.createDataFrame(map, DataFrames.toDataType(inputSchema));
    data.registerTempTable(SQLUtils.NAME_HIERARCHY_TABLE);
    Dataset<Row> rootRecords = sqlContext.sql(SQLUtils.sqlStatementForRoot(config, SQLUtils.NAME_HIERARCHY_TABLE));
    if (rootRecords.select(config.getParentField()).distinct().count() > 1) {
      throw new Exception("There can only be one root element in the hierarchy");
    }
    Row root = rootRecords.select(config.getParentField()).distinct().first();

    Dataset<Row> uniqueRecords = data.sqlContext().sql(
      SQLUtils.sqlForUniqueRecords(config, root, SQLUtils.NAME_HIERARCHY_TABLE));
    // Fetch on child id's from the input dataset
    Dataset<Row> leafRecords = data.sqlContext().sql(
      SQLUtils.sqlStatementForLeafRecords(config, SQLUtils.NAME_HIERARCHY_TABLE));
    leafRecords.registerTempTable(SQLUtils.NAME_LEAVES_TEMP_TABLE);
    // NOTE - required in order to be able to iterate - can't pass data and sqlContext otherwise
    List<Row> rows = uniqueRecords.collectAsList();

    // Empty Dataset to hold all the results
    Dataset<Row> results = sqlContext.createDataFrame(
      sparkExecutionPluginContext.getSparkContext().emptyRDD(), DataFrames.toDataType(outputSchema));
    // Root does not exists as separate value - this will generate it
    results = results.union(generateTreeRoot(data, config, nonMappedFields, mappedFields));
    // Generate tree for each unique row
    for (Row row : rows) {
      results = results.union(generateQuery(row, data, sqlContext));
    }
    // sets leaves indicator
    results.registerTempTable(SQLUtils.NAME_RESULTS_TEMP_TABLE);
    return sqlContext.sql(SQLUtils.sqlStatementForSettingLeavesValue(config, nonMappedFields, mappedFields));
  }

  /**
   * Generate dataset containing all subtree details for a given row
   *
   * @param parentRow  row for which to extract the subtree
   * @param data       reference to input dataset
   * @param sqlContext reference to {@link SQLContext}
   * @return dataset reference with all subtree rows for a given row
   * @throws Exception raises exception if max depth is exceeded
   */
  public Dataset<Row> generateQuery(Row parentRow, Dataset<Row> data, SQLContext sqlContext)
    throws Exception {
    long dataFrameCount = 1;
    int count = 1;

    // make parent id sql ready based on type
    String parentId;
    String parentIdValue = String.valueOf(parentRow.get(0));
    if (NumberUtils.isNumber(parentIdValue)) {
      parentId = parentIdValue;
    } else {
      parentId = String.format("'%s'", parentRow.getString(0));
    }

    mapSelfToTreeRoot(parentRow, data, config);
    List<String> childFields = new ArrayList<>(mappedFields.values());
    String otherFields = String.join(",", nonMappedFields);

    Dataset<Row> dataFrameSeed = data.sqlContext()
      .sql(SQLUtils.startQueryForRecursive(config, SQLUtils.NAME_HIERARCHY_TABLE, parentId));
    dataFrameSeed.registerTempTable(String.format("vt_%s_seed_0", parentId));

    Dataset<Row> branchRoot = data.sqlContext().sql(String.format("select %s from vt_%s_seed_0",
                                                                  String.join(",", mappedFields.keySet()),
                                                                  parentId));
    List<String> parentFields = new ArrayList<>();
    if (branchRoot.count() > 0) {
      Row branchRootParentFieldValues = branchRoot.first();
      for (String fieldName : mappedFields.keySet()) {
        int fieldIndex = branchRootParentFieldValues.fieldIndex(fieldName);
        parentFields.add(String.format("'%s' as %s", branchRootParentFieldValues.get(fieldIndex), fieldName));
      }
    } else {
      parentFields = new ArrayList<>(mappedFields.keySet());
    }

    while (dataFrameCount != 0) {
      if (count == Integer.parseInt(config.getMaxDepthField()) + 1) {
        throw new Exception(String.format("Exceeded supported max depth level of %s", config.getMaxDepthField()));
      }
      String treeTableName = String.format("vt_%s_seed_%s", parentId, count - 1);
      String nextTreeTableName = String.format("vt_%s_seed_%s", parentId, count);
      Dataset<Row> dataFrameTreeChildren = sqlContext.sql(SQLUtils.recursiveQuery(config, count + 1, treeTableName,
                                                                                  SQLUtils.NAME_HIERARCHY_TABLE));
      dataFrameCount = dataFrameTreeChildren.count();
      if (dataFrameCount != 0) {
        dataFrameTreeChildren.registerTempTable(nextTreeTableName);
      }
      count = count + 1;
    }
    String treeQuery = SQLUtils.sqlStatementForRootSelection(parentId);
    for (int index = 0; index <= (count - 2); index++) {
      treeQuery = treeQuery.concat(SQLUtils.unionSqlStatement(config, parentId, index, parentFields,
                                                              childFields, otherFields));
    }
    // This will generate a union query for each layer of the given branch
    // ex select * from vt_1_root_seed
    //  union
    //  select 1 as ParentId, ChildId, 'Groceries' as ParentProduct, ChildProduct, Supplier,Sales, levelField,
    //  topField, bottomField from vt_1_seed_0
    //  union
    //  select 1 as ParentId, ChildId, 'Groceries' as ParentProduct, ChildProduct, Supplier,Sales, levelField,
    //  topField, bottomField from vt_1_seed_1
    //  union ....
    return sqlContext.sql(treeQuery);
  }

  /**
   * Generates tree root record
   *
   * @param data            dataset with all unique records in tree
   * @param config          {@link HierarchyToRelationalConfig}
   * @param nonMappedFields list of fields not included in parent->child mapping
   * @param mappedFields    list of fields included in parent->child mapping
   * @return dataset with rows matching root record criteria
   */
  private Dataset<Row> generateTreeRoot(Dataset<Row> data, HierarchyToRelationalConfig config,
                                        List<String> nonMappedFields, Map<String, String> mappedFields) {
    String additionalFieldsForRoot = String.format("0 as %s,'%s' as %s,'%s' as %s ",
                                                   config.levelField, config.trueValueField, config.topField,
                                                   config.falseValueField, config.bottomField);
    String parentChildFieldMap = mappedFields.entrySet().stream()
      .map(mappedField -> String.format("%s as %s, %s", mappedField.getKey(), mappedField.getValue(),
                                        mappedField.getKey())).collect(Collectors.joining(","));
    String otherFields = nonMappedFields.stream().map(fieldName -> String.format("null as %s", fieldName))
      .collect(Collectors.joining(","));
    // ex query string: select ParentId as ChildId, ParentId, ParentProduct as ChildProduct, ParentProduct,
    // null as Supplier, null as Sales, 0 as Level, true as Top, false as Bottom
    String selfAsParentQuery = SQLUtils.sqlSelfParentQuery(config, additionalFieldsForRoot, parentChildFieldMap,
                                                           otherFields, SQLUtils.NAME_HIERARCHY_TABLE);
    return data.sqlContext().sql(selfAsParentQuery).limit(1);
  }

  /**
   * Creates root record for a given record
   *
   * @param parentRow row containing id of the record
   * @param data      dataset with all unique records in tree
   * @param config    {@link HierarchyToRelationalConfig}
   */
  private void mapSelfToTreeRoot(Row parentRow, Dataset<Row> data, HierarchyToRelationalConfig config) {
    String additionalFieldsForRoot = String.format("0 as %s,'%s' as %s,'%s' as %s ",
                                                   config.levelField, config.falseValueField, config.topField,
                                                   config.falseValueField, config.bottomField);
    Map<String, String> parentChildMapping = config.getParentChildMapping();
    StringBuilder mappedFields = new StringBuilder();
    for (Map.Entry<String, String> stringStringEntry : parentChildMapping.entrySet()) {
      mappedFields.append(String.format(", %s as %s, %s ", stringStringEntry.getValue(), stringStringEntry.getKey(),
                                        stringStringEntry.getValue()));
    }
    String nonMappedFieldsSql = nonMappedFields.isEmpty() ? "" : String.join(",", nonMappedFields) + ",";
    // ex query string: select ChildId as ParentId, ChildProduct as ParentProduct, ChildProduct, Supplier, Sales,
    // 0 as Level, false as Top, false as Bottom
    // ex query result: 2,2,Groceries,Groceries,,,0,false,false
    String selfAsParentQuery = String.format("select %s as %s, %s %s, %s %s from %s where %s=%s ",
                                             config.childField, config.parentField, config.childField,
                                             mappedFields.toString(), nonMappedFieldsSql,
                                             additionalFieldsForRoot, SQLUtils.NAME_HIERARCHY_TABLE, config.childField,
                                             parentRow.get(0));
    Dataset<Row> selfAsParentDf = data.sqlContext().sql(selfAsParentQuery).limit(1);
    selfAsParentDf.registerTempTable(String.format("vt_%s_root_seed", parentRow.get(0)));
  }

  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext sparkExecutionPluginContext,
                                             JavaRDD<StructuredRecord> javaRDD) throws Exception {
    Schema inputSchema = sparkExecutionPluginContext.getInputSchema();

    Dataset<Row> dataFrames = queryApproach(sparkExecutionPluginContext, javaRDD, inputSchema);
    return dataFrames.toJavaRDD().map(new RowToRecord(this.outputSchema));
  }
}
