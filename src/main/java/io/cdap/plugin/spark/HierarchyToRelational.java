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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import org.apache.spark.api.java.JavaRDD;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * HierarchyToRelational - SparkCompute plugin that converts any incoming hierarchy dataset records and
 * outputs relational data.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(HierarchyToRelational.PLUGIN_NAME)
@Description("HierarchyToRelational converts any incoming hierarchy dataset records and outputs relational data.")
public class HierarchyToRelational extends SparkCompute<StructuredRecord, StructuredRecord> {

  public static final String PLUGIN_NAME = "HierarchyToRelational";

  private final HierarchyConfig config;
  private Schema outputSchema = null;

  public HierarchyToRelational(HierarchyConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema outputSchema = config.generateOutputSchema(pipelineConfigurer.getStageConfigurer().getInputSchema());
    stageConfigurer.setOutputSchema(outputSchema);
    config.validate(stageConfigurer.getFailureCollector());
  }

  @Override
  public void prepareRun(SparkPluginContext context) {
    config.validate(context.getFailureCollector());
    recordLineage(context);
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) {
    Schema inputSchema = context.getInputSchema();
    outputSchema = config.generateOutputSchema(inputSchema);
  }

  /**
   * Generates lineage data
   *
   * @param context {@link SparkPluginContext}
   */
  private void recordLineage(SparkPluginContext context) {
    Schema inputSchema = context.getInputSchema();
    if (inputSchema == null) {
      return;
    }
    Schema outputSchema = config.generateOutputSchema(inputSchema);
    List<String> inputFields = context.getInputSchema().getFields().stream().map(Schema.Field::getName)
      .collect(Collectors.toList());
    List<String> outputFields = outputSchema.getFields().stream().map(Schema.Field::getName)
      .collect(Collectors.toList());
    String description = String.format("Flattened the dataset by using the input field '%s' as the parent " +
                                         "field and '%s' as the child field. Generated the column '%s' to " +
                                         "contain distance from parent nodes, '%s' to denote if a node is the root " +
                                         "of the hierarchy and '%s' to denote if a node is a leaf in the " +
                                         "hierarchy.", config.getParentField(), config.getChildField(),
                                       config.getLevelField(), config.getTopField(), config.getBottomField());
    FieldOperation operation = new FieldTransformOperation(PLUGIN_NAME, description, inputFields, outputFields);
    context.record(Collections.singletonList(operation));
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> javaRDD) {
    return new HierarchyFlattener(config).flatten(context, javaRDD, outputSchema);
  }

}
