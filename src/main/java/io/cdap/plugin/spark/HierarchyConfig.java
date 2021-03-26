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
import org.mortbay.log.Log;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Config class for HierarchyToRelational.
 */
public class HierarchyConfig extends PluginConfig {

  // Hierarchy Configuration
  private static final String PARENT_FIELD = "parentField";
  private static final String CHILD_FIELD = "childField";
  private static final String PARENT_CHILD_MAPPING_FIELD = "parentChildMappingField";
  private static final String START_WITH_FIELD = "startWith";
  private static final String START_WITH_DEFAULT_VALUE = "";

  // Advanced
  private static final String LEVEL_FIELD = "levelField";
  private static final String LEVEL_FIELD_DEFAULT_VALUE = "Level";
  private static final String TOP_FIELD = "topField";
  private static final String TOP_FIELD_DEFAULT_VALUE = "Top";
  private static final String BOTTOM_FIELD = "bottomField";
  private static final String BOTTOM_FIELD_DEFAULT_VALUE = "Bottom";
  private static final String TRUE_VALUE_FIELD = "trueValueField";
  private static final String TRUE_VALUE_FIELD_DEFAULT_VALUE = "Y";
  private static final String FALSE_VALUE_FIELD = "falseValueField";
  private static final String FALSE_VALUE_FIELD_DEFAULT_VALUE = "N";
  private static final String MAX_DEPTH_FIELD = "maxDepthField";
  private static final int MAX_DEPTH_FIELD_DEFAULT_VALUE = 50;

  private static final String SIBLING_ORDER_FIELD = "siblingOrder";
  private static final String SIBLING_ORDER_FIELD_DEFAULT_VALUE = "ASC";

  private static final String BROADCAST_JOIN_FIELD = "broadcastJoin";
  private static final Boolean BROADCAST_JOIN_FIELD_DEFAULT_VALUE = Boolean.FALSE;

  private static final String PATH_FIELDS_FIELD = "pathFields";
  private static final String CONNECT_BY_ROOT_FIELD = "connectByRoot";

  private static final String FIELDS_TO_TRANSFORM = "fieldsToTransform";

  private static final String KV_DROPDOWN = "propertyKVDropdown";

  public static final String VERTEX_FIELD_NAME = "vertexFieldName";
  public static final String PATH_SEPARATOR = "pathSeparator";
  public static final String PATH_FIELD_ALIAS = "pathFieldAlias";
  public static final String PATH_FIELD_LENGTH_ALIAS = "pathFieldLengthAlias";

  // Hierarchy Configuration
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

  @Name(PARENT_CHILD_MAPPING_FIELD)
  @Description("Specifies parent child field mapping for fields that require swapping parent fields with tree/branch" +
      " root fields. ")
  @Macro
  private String parentChildMappingField;

  @Name(START_WITH_FIELD)
  @Description("Expression defining where to start exploring the hierarchy from.")
  @Macro
  @Nullable
  private String startWith;

  // Advanced section
  @Name(LEVEL_FIELD)
  @Description("The name of the field that should contain the Yes level in the hierarchy starting at a particular " +
      "node in the tree. The level is calculated as a distance of a node to a particular parent node in the tree.")
  @Macro
  @Nullable
  private String levelField;

  @Name(TOP_FIELD)
  @Description("The name of the field that determines whether a node is the root element or the top-most element" +
      " in the hierarchy. The input data should always contain a single non-null root node. For that node, this" +
      " field is true, while it is marked false for all other nodes in the hierarchy.")
  @Macro
  @Nullable
  private String topField;

  @Name(BOTTOM_FIELD)
  @Description("The name of the field that determines whether a node is a leaf element or the bottom-most " +
      "element in the hierarchy. The input data can contain multiple leaf nodes.")
  @Macro
  @Nullable
  private String bottomField;

  @Name(TRUE_VALUE_FIELD)
  @Description("The value that denotes true in the Top and Bottom fields.")
  @Macro
  @Nullable
  private String trueValue;

  @Name(FALSE_VALUE_FIELD)
  @Description("The value that denotes false in the Top and Bottom fields")
  @Macro
  @Nullable
  private String falseValue;

  @Name(MAX_DEPTH_FIELD)
  @Description("The maximum depth up to which the data should be flattened. If a node is reached at a deeper" +
      " level, an error will be thrown.")
  @Macro
  @Nullable
  private Integer maxDepth;

  @Name(CONNECT_BY_ROOT_FIELD)
  @Description("Connect by root.")
  @Macro
  @Nullable
  private String connectByRoot;

  @Name(PATH_FIELDS_FIELD)
  @Nullable
  @Description("Fields used to build the path from the root.")
  private String pathFields;

//  @Name(SIBLING_ORDER_FIELD)
//  @Nullable
//  @Description("Sorting order for siblings")
//  private String siblingOrder;

  @Name(BROADCAST_JOIN_FIELD)
  @Nullable
  @Description("Performs an in-memory broadcast join")
  private Boolean broadcastJoin;

//  @Name(FIELDS_TO_TRANSFORM)
//  @Nullable
//  @Description("Test")
//  private String fieldsToTransform;

//  @Name(KV_DROPDOWN)
//  @Nullable
//  @Description("Test")
//  private String propertyKVDropdown;

  public boolean requiredFieldsContainMacro() {
    return containsMacro(PARENT_FIELD) || containsMacro(CHILD_FIELD) || containsMacro(LEVEL_FIELD) ||
        containsMacro(TOP_FIELD) || containsMacro(LEVEL_FIELD) || containsMacro(BOTTOM_FIELD);
  }

  public void validate(FailureCollector collector) {
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
    if (!Strings.isNullOrEmpty(PARENT_CHILD_MAPPING_FIELD)) {
      Map<String, String> parentChildMapping = getParentChildMapping();
      if (parentChildMapping.containsKey(parentField) || parentChildMapping.containsValue(parentField)) {
        collector.addFailure("Parent key field found mapping.",
            "Parent key field cannot be part of parent-> child mapping.")
            .withConfigProperty(PARENT_CHILD_MAPPING_FIELD);
      }
      if (parentChildMapping.containsKey(childField) || parentChildMapping.containsValue(childField)) {
        collector.addFailure("Child key field found mapping.",
            "Child key field cannot be part of parent-> child mapping.")
            .withConfigProperty(PARENT_CHILD_MAPPING_FIELD);
      }
    }
    if (Strings.isNullOrEmpty(childField)) {
      collector.addFailure("Child field is null/empty.", "Please provide valid child field.")
          .withConfigProperty(CHILD_FIELD);
    }
    if (maxDepth != null && maxDepth < 1) {
      collector.addFailure("Invalid max depth.", "Max depth must be at least 1.")
          .withConfigProperty(CHILD_FIELD);
    }
    collector.getOrThrowException();
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

  public String getTopField() {
    if (Strings.isNullOrEmpty(topField)) {
      return TOP_FIELD_DEFAULT_VALUE;
    }
    return topField;
  }

  public String getBottomField() {
    if (Strings.isNullOrEmpty(bottomField)) {
      return BOTTOM_FIELD_DEFAULT_VALUE;
    }
    return bottomField;
  }

  public String getTrueValue() {
    if (Strings.isNullOrEmpty(trueValue)) {
      return TRUE_VALUE_FIELD_DEFAULT_VALUE;
    }
    return trueValue;
  }

  public String getFalseValue() {
    if (Strings.isNullOrEmpty(falseValue)) {
      return FALSE_VALUE_FIELD_DEFAULT_VALUE;
    }
    return falseValue;
  }

  public int getMaxDepth() {
    return maxDepth == null ? MAX_DEPTH_FIELD_DEFAULT_VALUE : maxDepth;
  }

//  public String getSiblingOrder() {
//    return siblingOrder == null ? SIBLING_ORDER_FIELD_DEFAULT_VALUE : siblingOrder;
//  }

  public boolean isBroadcastJoin() {
    return broadcastJoin == null ? BROADCAST_JOIN_FIELD_DEFAULT_VALUE : broadcastJoin.booleanValue();
  }

  public String getRawPathFields() {
    return pathFields;
  }

  public List<Map<String, String>> getPathFields() {
    List<Map<String, String>> list = new ArrayList<>();
    if (!Strings.isNullOrEmpty(pathFields)) {
      String[] fields = pathFields.split(",");
      for (String field : fields) {
        String[] entries = field.split("=");
        if (entries.length == 4) {
          Map<String, String> map = new HashMap<>();
          map.put(VERTEX_FIELD_NAME, entries[0]);
          map.put(PATH_SEPARATOR, entries[1]);
          map.put(PATH_FIELD_ALIAS, entries[2]);
          map.put(PATH_FIELD_LENGTH_ALIAS, entries[3]);
          list.add(map);
        } else {
          Log.warn("Cannot parse the path fields from: " + field);
        }
      }
    }
    return list;
  }

  public Map<String, String> getParentChildMapping() {
    Map<String, String> parentChildMap = new HashMap<>();
    if (Strings.isNullOrEmpty(parentChildMappingField)) {
      return parentChildMap;
    }
    KeyValueListParser keyValueListParser = new KeyValueListParser(";", "=");
    Iterable<KeyValue<String, String>> parsedParentChildMappingField = keyValueListParser
        .parse(parentChildMappingField);
    for (KeyValue<String, String> keyValuePair : parsedParentChildMappingField) {
      parentChildMap.put(keyValuePair.getKey(), keyValuePair.getValue());
    }
    return parentChildMap;
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
    List<Schema.Field> fields = new ArrayList<>();
    List<String> nonMappedFields = getNonMappedFields(inputSchema);
    for (Schema.Field field : inputSchema.getFields()) {
      if (nonMappedFields.contains(field.getName())) {
        Schema.Field updatedField = Schema.Field.of(field.getName(),
            field.getSchema().isNullable() ? field.getSchema() : Schema.nullableOf(field.getSchema()));
        fields.add(updatedField);
      } else {
        fields.add(field);
      }
    }

    fields.add(Schema.Field.of(getLevelField(), Schema.of(Schema.Type.INT)));
    fields.add(Schema.Field.of(getTopField(), Schema.of(Schema.Type.STRING)));
    fields.add(Schema.Field.of(getBottomField(), Schema.of(Schema.Type.STRING)));

    List<Map<String, String>> paths = getPathFields();
    for (Map<String, String> path : paths) {
      fields.add(Schema.Field.of(path.get(PATH_FIELD_ALIAS), Schema.of(Schema.Type.STRING)));
    }

    Schema schema = Schema.recordOf(inputSchema.getRecordName() + "_flattened", fields);
    return schema;
  }

  /**
   * Generates list of fields that are in input schema but are not mapped.
   *
   * @param inputSchema {@link Schema}
   * @return list of fields not included in parent->child mapping
   */
  public List<String> getNonMappedFields(Schema inputSchema) {
    List<Schema.Field> fields = inputSchema.getFields();
    Map<String, String> parentChildMapping = getParentChildMapping();
    return fields.stream().map(Schema.Field::getName)
        .filter(fieldName -> !(parentChildMapping.containsKey(fieldName) ||
            parentChildMapping.containsValue(fieldName) ||
            fieldName.equals(parentField) || fieldName.equals(childField)))
        .collect(Collectors.toList());
  }
}
