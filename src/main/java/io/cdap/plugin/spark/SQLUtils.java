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

import com.google.common.base.Strings;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

/**
 * SQLUtils helper class for HierarchyToRelational plugin
 */
public class SQLUtils {

  public static final String NAME_HIERARCHY_TABLE = "hierarchy_table";
  public static final String NAME_LEAVES_TEMP_TABLE = "leafRecords";
  public static final String NAME_RESULTS_TEMP_TABLE = "hierarchy_results";


  /**
   * Generate SQL statement for fetching root element
   *
   * @param config {@link HierarchyToRelationalConfig}
   * @param tableName table name of the input dataset
   * @return sql string for fetching root record
   */
  public static String sqlStatementForRoot(HierarchyToRelationalConfig config, String tableName) {
    String additionalFieldsForRoot = getAdditionalFieldsForRoot(config);
    return String.format("select *, %s from %s where %s not in (select %s from %s)",
                         additionalFieldsForRoot, tableName, config.parentField, config.childField, tableName);
  }

  /**
   * Add the addition Level, Top and Bottom fields for root record
   *
   * @param config {@link HierarchyToRelationalConfig}
   * @return {@link String} sql string with additional fields and default values.
   */
  public static String getAdditionalFieldsForRoot(HierarchyToRelationalConfig config) {
    return String.format("0 as %s,'%s' as %s,'%s' as %s ",
                         config.getLevelField(), config.getTrueValueField(), config.getTopField(),
                         config.getFalseValueField(), config.getBottomField());
  }

  /**
   * Generates sql statement to fetch child ids of records from dataset and including root id
   *
   * @param config    {@link HierarchyToRelationalConfig}
   * @param root      {@link Row} root record
   * @param tableName {@link String} name of the temp table to read from
   * @return sql statement for fetching unique records
   */
  public static String sqlForUniqueRecords(HierarchyToRelationalConfig config, Row root,
                                           String tableName) {
    // parent field value of root record
    Object parentFieldValue = root.get(root.fieldIndex(config.parentField));
    return String.format("select %s from %s union select %s as %s ",
                         config.childField, tableName,
                         parentFieldValue, config.childField);
  }

  /**
   * Generates sql statement for fetching leaf records
   *
   * @param config    {@link HierarchyToRelationalConfig}
   * @param tableName name of the temp table to read from
   * @return {@link String} sql statement for fetching leaf records
   */
  public static String sqlStatementForLeafRecords(HierarchyToRelationalConfig config,
                                                  String tableName) {
    return String.format("select * from %s where %s not in (select %s from %s)", tableName, config.childField,
                         config.parentField, tableName);
  }

  /**
   * Generate sql statement for selecting leaves values
   *
   * @param config          {@link HierarchyToRelationalConfig}
   * @param nonMappedFields list of non mapped fields
   * @param mappedFields    list of mapped fields
   * @return sql string for selecting leaves in the tree
   */
  public static String sqlStatementForSettingLeavesValue(HierarchyToRelationalConfig config,
                                                         List<String> nonMappedFields,
                                                         Map<String, String> mappedFields) {
    StringBuilder leavesSql = new StringBuilder();
    leavesSql.append("select ");
    leavesSql.append(NAME_RESULTS_TEMP_TABLE).append(".").append(config.parentField).append(", ");
    leavesSql.append(NAME_RESULTS_TEMP_TABLE).append(".").append(config.childField).append(", ");
    for (Map.Entry<String, String> mappedField : mappedFields.entrySet()) {
      leavesSql.append(NAME_RESULTS_TEMP_TABLE).append(".").append(mappedField.getKey()).append(", ");
      leavesSql.append(NAME_RESULTS_TEMP_TABLE).append(".").append(mappedField.getValue()).append(", ");
    }

    leavesSql.append(NAME_RESULTS_TEMP_TABLE).append(".").append(config.getTopField()).append(", ");
    leavesSql.append(NAME_RESULTS_TEMP_TABLE).append(".").append(config.getLevelField()).append(", ");
    for (String fieldName : nonMappedFields) {
      leavesSql.append(NAME_RESULTS_TEMP_TABLE).append(".").append(fieldName).append(", ");
    }
    leavesSql.append("case when ").append(NAME_LEAVES_TEMP_TABLE).append(".").append(config.childField);
    leavesSql.append(" is null then ").append("'").append(config.getFalseValueField()).append("'").append(" else ");
    leavesSql.append("'").append(config.getTrueValueField()).append("' end as ").append(config.getBottomField());
    leavesSql.append(" from ").append(NAME_RESULTS_TEMP_TABLE).append(" left join ").append(NAME_LEAVES_TEMP_TABLE);
    leavesSql.append(" on ").append(NAME_RESULTS_TEMP_TABLE).append(".").append(config.childField).append("=");
    leavesSql.append(NAME_LEAVES_TEMP_TABLE).append(".").append(config.childField);
    return leavesSql.toString();
  }

  /**
   * Generates sql statement to include additional fields (Level, Top, Bottom) with default values
   *
   * @param config {@link HierarchyToRelationalConfig}
   * @param level  value for the level field
   * @return {@link String} returns sql string for including additional fields
   */
  private static String sqlStatementForAdditionalFields(HierarchyToRelationalConfig config, int level) {
    return String.format("%s as %s,'%s' as %s,'%s' as %s ", level, config.levelField, config.falseValueField,
                         config.topField, config.falseValueField, config.bottomField);
  }

  public static String startQueryForRecursive(HierarchyToRelationalConfig config,
                                              String tableName, String parentId) {
    String additionalFields = sqlStatementForAdditionalFields(config, 1);
    return String.format("select *, %s from %s where %s=%s", additionalFields, tableName,
                         config.parentField, parentId);
  }

  /**
   * Generates sql query for retrieving child records with parent ids that match the child id's from a temp table.
   *
   * @param conf          {@link HierarchyToRelationalConfig}
   * @param count         level of the child fields
   * @param treeTableName {@link String} name of the table containing the initial dataset (initial tree)
   * @param tableName     {@link String} name of the table containing current level of records in the tree
   * @return {@link String} sql statement for selecting child records based on given parent records
   */
  public static String recursiveQuery(HierarchyToRelationalConfig conf, int count,
                                      String treeTableName, String tableName) {
    String additionalFields = sqlStatementForAdditionalFields(conf, count);
    // example query string: select indirect.*, 1 as Level, false as Top, false as Bottom from vt_0_seed_1 direct,
    // hierarchy_table indirect where direct.ChildId = indirect.ParentId
    return String.format("select indirect.*, %s FROM %s direct, %s indirect WHERE " +
                           "direct.%s = indirect.%s", additionalFields, treeTableName, tableName, conf.childField,
                         conf.parentField);
  }

  /**
   * Generates sql statement for selecting root record from temp table
   *
   * @param parentId {@link String} root record id
   * @return {@link String} sql string for selecting root record from temp table
   */
  public static String sqlStatementForRootSelection(String parentId) {
    return String.format("select * from vt_%s_root_seed", parentId.replace("'", ""));
  }

  /**
   * Generate union select statement which will select all records and their appropriate additional fields
   * and values for a given branch
   *
   * @param config              {@link HierarchyToRelationalConfig}
   * @param parentId            {@link String} id of the parent record that will be the root of a branch
   * @param index               level of the branch
   * @param parentFields        sql string for selecting parent fields
   * @param childFields         sql string for selecting child fields
   * @param otherFields         other field of the record to include in the query
   * @return sql string to include records from a given level in the branch
   */
  public static String unionSqlStatement(HierarchyToRelationalConfig config, String parentId,
                                         int index, List<String> parentFields, List<String> childFields,
                                         String otherFields) {
    String additionalFields = String.format("%s, %s, %s", config.getLevelField(), config.getTopField(),
                                            config.getBottomField());
    String parentFieldsSql = parentFields.isEmpty() ? "" : String.join(",", parentFields) + ",";
    String childFieldsSql = childFields.isEmpty() ? "" : String.join(",", childFields) + ",";
    String otherFieldsSql = Strings.isNullOrEmpty(otherFields) ? "" : otherFields + ",";
    return String.format(" union select %s as %s, %s, %s %s %s %s from vt_%s_seed_%s",
                         parentId, config.parentField,
                         config.childField,
                         parentFieldsSql, childFieldsSql,
                         otherFieldsSql, additionalFields,
                         parentId.replace("'", ""), index);
  }

  /**
   * Generates sql statement generating tree root record (which by default is not available in dataset)
   * Note: root record will be generated from Parent Id and Parent Product of first record found with closest
   * level to root record.
   *
   * @param config                  {@link HierarchyToRelationalConfig}
   * @param additionalFieldsForRoot {@link String} sql string with additional fields to include (Level, Top, Bottom)
   * @param parentChildFieldMap     {@link String} sql string with mapping of parent->childs fields
   * @param otherFields             {@link String} other fields from the record to include
   * @param tableName               {@link String} name of the temp table containing input dataset.
   * @return sql string for generating root record of the tree
   */
  public static String sqlSelfParentQuery(HierarchyToRelationalConfig config,
                                          String additionalFieldsForRoot, String parentChildFieldMap,
                                          String otherFields, String tableName) {
    String mappedFields = Strings.isNullOrEmpty(parentChildFieldMap) ? "" : String.format("%s,", parentChildFieldMap);
    String nonMappedFields = Strings.isNullOrEmpty(otherFields) ? "" : String.format("%s, ", otherFields);
    return String.format("select %s as %s, %s, %s %s %s from %s where %s not in (select %s from %s)",
                         config.parentField, config.childField, config.parentField, mappedFields, nonMappedFields,
                         additionalFieldsForRoot, tableName, config.parentField, config.childField, tableName);
  }

}
