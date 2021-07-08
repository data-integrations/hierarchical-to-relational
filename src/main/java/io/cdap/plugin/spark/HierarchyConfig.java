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
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.KeyValueListParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Config class for HierarchyToRelational.
 */
public class HierarchyConfig extends PluginConfig {

  private static final String PARENT_FIELD = "parentField";
  private static final String CHILD_FIELD = "childField";
  private static final String LEVEL_FIELD = "levelField";
  private static final String LEVEL_FIELD_DEFAULT_VALUE = "Level";
  private static final String BOTTOM_FIELD = "bottomField";
  private static final String BOTTOM_FIELD_DEFAULT_VALUE = "Bottom";
  private static final String MAX_DEPTH_FIELD = "maxDepth";
  private static final int MAX_DEPTH_FIELD_DEFAULT_VALUE = 50;
  private static final String START_WITH_FIELD = "startWith";
  private static final String CONNECT_BY_ROOT_FIELD = "connectByRootField";
  private static final String PATH_FIELD = "pathField";
  private static final String PATH_ALIAS_FIELD = "pathAliasField";
  private static final String PATH_SEPARATOR_FIELD = "pathSeparator";
  private static final String PATH_SEPARATOR_DEFAULT_VALUE = "/";


  @Name(PARENT_FIELD)
  @Description("Specifies the field from the input schema that should be used as the parent in the " +
    "hierarchical model. Should always contain a single, non-null root element in the hierarchy.")
  @Macro
  private String parentField;

  @Name(CHILD_FIELD)
  @Description("Specifies the field from the input schema that should be used as the child in the hierarchical " +
    "model.")
  @Macro
  private String childField;

  @Name(LEVEL_FIELD)
  @Description("The name of the field that should contain the Yes level in the hierarchy starting at a particular " +
    "node in the tree. The level is calculated as a distance of a node to a particular parent node in the tree.")
  @Macro
  @Nullable
  private String levelField;

  @Name(BOTTOM_FIELD)
  @Description("The name of the field that determines whether a node is a leaf element or the bottom-most " +
    "element in the hierarchy. The input data can contain multiple leaf nodes.")
  @Macro
  @Nullable
  private String bottomField;

  @Name(MAX_DEPTH_FIELD)
  @Description("The maximum depth upto which the data should be flattened. If a node is reached at a deeper" +
    " level, an error should be thrown.")
  @Macro
  @Nullable
  private Integer maxDepth;

  @Name(START_WITH_FIELD)
  @Description("Defines a condition to identify a starting point in the input data that will be used to expand the" +
    " hierarchy.")
  @Macro
  @Nullable
  private String startWith;

  @Name(CONNECT_BY_ROOT_FIELD)
  @Description("Specifies root fields along with their aliases that are added to the output.")
  @Macro
  @Nullable
  private String connectByRootField;

  @Name(PATH_FIELD)
  @Description("Name of the field used to generate the path.")
  @Macro
  @Nullable
  private String pathField;

  @Name(PATH_ALIAS_FIELD)
  @Description("The name of the field that contains a textual representation of the path that a record denotes.")
  @Macro
  @Nullable
  private String pathAliasField;


  @Name(PATH_SEPARATOR_FIELD)
  @Description("The separator for nodes in the path.")
  @Macro
  @Nullable
  private String pathSeparator;

  public boolean requiredFieldsContainMacro() {
    return containsMacro(PARENT_FIELD) || containsMacro(CHILD_FIELD) || containsMacro(LEVEL_FIELD) ||
      containsMacro(LEVEL_FIELD) || containsMacro(BOTTOM_FIELD)
      || containsMacro(PATH_FIELD) || containsMacro(PATH_ALIAS_FIELD);
  }

  public void validate(FailureCollector collector, Schema inputSchema) {
    if (requiredFieldsContainMacro()) {
      return;
    }
    if (parentField.equals(childField)) {
      collector.addFailure("Parent field is same as child field.", "Parent field needs to be different child field.")
        .withConfigProperty(PARENT_FIELD);
    }
    if (Strings.isNullOrEmpty(parentField)) {
      collector.addFailure("Parent field is null/empty.", "Please provide valid parent field.")
        .withConfigProperty(PARENT_FIELD);
    }
    if (Strings.isNullOrEmpty(childField)) {
      collector.addFailure("Child field is null/empty.", "Please provide valid child field.")
        .withConfigProperty(CHILD_FIELD);
    }
    if (maxDepth != null && maxDepth < 1) {
      collector.addFailure("Invalid max depth.", "Max depth must be at least 1.")
        .withConfigProperty(MAX_DEPTH_FIELD);
    }

    if (!Strings.isNullOrEmpty(pathField)) {
      if (Strings.isNullOrEmpty(pathAliasField)) {
        collector.addFailure("Path alias field name is null/empty.",
                           "Path alias field name needs to be specified.")
          .withConfigProperty(PATH_ALIAS_FIELD);
      }
      if (inputSchema.getFields().stream().noneMatch(field -> field.getName().equals(pathField))) {
        collector.addFailure(String.format("Field %s not found in the input schema.", pathField),
                             "Specify a field that is present in the input schema.")
          .withConfigProperty(PATH_FIELD);
      }
    }
    if (!Strings.isNullOrEmpty(pathAliasField) && Strings.isNullOrEmpty(pathField)) {
      collector.addFailure("Path field name is null/empty.",
                           "Path field name needs to be specified.")
        .withConfigProperty(PATH_FIELD);
    }

    for (String fieldName: getConnectByRoot().keySet()) {
      if (inputSchema.getFields().stream().noneMatch(field -> field.getName().equals(fieldName))) {
        collector.addFailure(String.format("Field %s not found in the input schema.", fieldName),
                             "Specify a field that is present in the input schema.")
          .withConfigProperty(CONNECT_BY_ROOT_FIELD);
      }
    }
  }

  public String getParentField() {
    return parentField;
  }

  public String getChildField() {
    return childField;
  }

  public String getLevelField() {
    if (Strings.isNullOrEmpty(levelField)) {
      return LEVEL_FIELD_DEFAULT_VALUE;
    }
    return levelField;
  }

  public String getBottomField() {
    if (Strings.isNullOrEmpty(bottomField)) {
      return BOTTOM_FIELD_DEFAULT_VALUE;
    }
    return bottomField;
  }

  public int getMaxDepth() {
    return maxDepth == null ? MAX_DEPTH_FIELD_DEFAULT_VALUE : maxDepth;
  }

  @Nullable
  public String getStartWith() {
    return startWith;
  }

  @Nullable
  public String getPathField() {
    return pathField;
  }

  @Nullable
  public String getPathAliasField() {
    return pathAliasField;
  }

  public String getPathSeparator() {
    return Strings.isNullOrEmpty(pathSeparator) ? PATH_SEPARATOR_DEFAULT_VALUE : pathSeparator;
  }

  public Map<String, String> getConnectByRoot() {
    Map<String, String> connectByRootMap = new HashMap<>();
    if (Strings.isNullOrEmpty(connectByRootField)) {
      return connectByRootMap;
    }
    KeyValueListParser keyValueListParser = new KeyValueListParser(";", "=");
    Iterable<KeyValue<String, String>> parsedConnectByRootField = keyValueListParser
      .parse(connectByRootField);
    for (KeyValue<String, String> keyValuePair : parsedConnectByRootField) {
      connectByRootMap.put(keyValuePair.getKey(), keyValuePair.getValue());
    }
    return connectByRootMap;
  }

  /**
   * Generate output schema including additional fields from plugin configuration
   *
   * @param inputSchema {@link Schema}
   */
  public Schema generateOutputSchema(Schema inputSchema) {
    if (inputSchema == null || inputSchema.getFields() == null) {
      throw new IllegalArgumentException("Input schema is required.");
    }
    List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
    fields.add(Schema.Field.of(getLevelField(), Schema.of(Schema.Type.INT)));
    fields.add(Schema.Field.of(getBottomField(), Schema.of(Schema.Type.BOOLEAN)));
    if (!Strings.isNullOrEmpty(pathField) && !Strings.isNullOrEmpty(pathAliasField)) {
      fields.add(Schema.Field.of(getPathAliasField(), Schema.of(Schema.Type.STRING)));
    }
    for (Map.Entry<String, String> entry: getConnectByRoot().entrySet()) {
      fields.add(Schema.Field.of(entry.getValue(), getFieldSchema(inputSchema, entry.getKey())));
    }
    return Schema.recordOf(inputSchema.getRecordName() + "_flattened", fields);
  }

  private Schema getFieldSchema(Schema inputSchema, String fieldName) {
    for (Schema.Field field: inputSchema.getFields()) {
      if (field.getName().equals(fieldName)) {
        return field.getSchema();
      }
    }
    throw new IllegalArgumentException(String.format("Field %s not found in the input schema", fieldName));
  }
}
