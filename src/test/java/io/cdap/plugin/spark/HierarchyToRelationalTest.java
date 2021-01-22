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

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import org.apache.commons.collections.ListUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.internal.util.reflection.FieldSetter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HierarchyToRelationalTest extends HydratorTestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", true);

  private static final ArtifactSummary APP_ARTIFACT_PIPELINE =
    new ArtifactSummary("data-pipeline", "1.0.0");

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifactPipeline =
      NamespaceId.DEFAULT.artifact(APP_ARTIFACT_PIPELINE.getName(), APP_ARTIFACT_PIPELINE.getVersion());

    setupBatchArtifacts(parentArtifactPipeline, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact("hierarchy-relational-plugins", "1.0.0"),
                      parentArtifactPipeline, HierarchyToRelational.class);
  }

  private static final Schema INPUT_SCHEMA = Schema.recordOf(
    "x",
    Schema.Field.of("ParentId", Schema.of(Schema.Type.INT)),
    Schema.Field.of("ChildId", Schema.of(Schema.Type.INT)),
    Schema.Field.of("ParentProduct", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("ChildProduct", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Supplier", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("Sales", Schema.nullableOf(Schema.of(Schema.Type.INT)))
  );

  private static final List<StructuredRecord> INPUT_DATA = ImmutableList.of(

    StructuredRecord.builder(INPUT_SCHEMA).set("ParentId", 1).set("ChildId", 2).set("ParentProduct", "Groceries")
      .set("ChildProduct", "Produce").set("Supplier", "A").set("Sales", 50).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("ParentId", 1).set("ChildId", 3).set("ParentProduct", "Groceries")
      .set("ChildProduct", "Dairy").set("Supplier", "B").set("Sales", 40).build(),

    StructuredRecord.builder(INPUT_SCHEMA).set("ParentId", 2).set("ChildId", 4).set("ParentProduct", "Produce")
      .set("ChildProduct", "Vegetables").set("Supplier", "C").set("Sales", 50).build(),

    StructuredRecord.builder(INPUT_SCHEMA).set("ParentId", 4).set("ChildId", 6).set("ParentProduct", "Vegetables")
      .set("ChildProduct", "Onion").set("Supplier", "E").set("Sales", 30).build()
  );

  private static final List<StructuredRecord> INPUT_DATA_WITH_PARENT_RECORD = ListUtils.union(
    Collections.singletonList(StructuredRecord.builder(INPUT_SCHEMA).set("ParentId", 1).set("ChildId", 1)
                                .set("ParentProduct", "Groceries").set("ChildProduct", "Groceries")
                                .set("Supplier", null).set("Sales", null).build()), INPUT_DATA
  );


  private static StructuredRecord generateRecord(int parentId, int childId, String parentProduct, String childProduct,
                                                 String supplier, Integer sales, int leveField, String topField,
                                                 String bottomField) {
    return StructuredRecord.builder(generateOutputSchema())
      .set("ParentId", parentId)
      .set("ChildId", childId)
      .set("ParentProduct", parentProduct)
      .set("ChildProduct", childProduct)
      .set("Supplier", supplier)
      .set("Sales", sales)
      .set("levelField", leveField)
      .set("topField", topField)
      .set("bottomField", bottomField)
      .build();
  }

  private static final List<StructuredRecord> EXPECTED_OUTPUT = ImmutableList.of(
    generateRecord(1, 1, "Groceries", "Groceries", null, null, 0, "true", "false"),
    generateRecord(1, 2, "Groceries", "Produce", "A", 50, 1, "false", "false"),
    generateRecord(1, 3, "Groceries", "Dairy", "B", 40, 1, "false", "true"),
    generateRecord(1, 4, "Groceries", "Vegetables", "C", 50, 2, "false", "false"),
    generateRecord(1, 6, "Groceries", "Onion", "E", 30, 3, "false", "true"),
    generateRecord(2, 2, "Produce", "Produce", "A", 50, 0, "false", "false"),
    generateRecord(2, 4, "Produce", "Vegetables", "C", 50, 1, "false", "false"),
    generateRecord(2, 6, "Produce", "Onion", "E", 30, 2, "false", "true"),
    generateRecord(3, 3, "Dairy", "Dairy", "B", 40, 0, "false", "true"),
    generateRecord(4, 4, "Vegetables", "Vegetables", "C", 50, 0, "false", "false"),
    generateRecord(4, 6, "Vegetables", "Onion", "E", 30, 1, "false", "true"),
    generateRecord(6, 6, "Onion", "Onion", "E", 30, 0, "false", "true")
  );

  private static Schema generateOutputSchema() {
    List<Schema.Field> fields = new ArrayList<>(INPUT_SCHEMA.getFields());
    fields.add(Schema.Field.of("levelField", Schema.of(Schema.Type.INT)));
    fields.add(Schema.Field.of("topField", Schema.of(Schema.Type.STRING)));
    fields.add(Schema.Field.of("bottomField", Schema.of(Schema.Type.STRING)));
    return Schema.recordOf("record", fields);
  }

  private List<String> convertStructuredRecordListToJson(List<StructuredRecord> structuredRecordList)
    throws IOException {
    List<String> result = new ArrayList<>();
    for (StructuredRecord structuredRecord : structuredRecordList) {
      String s = StructuredRecordStringConverter.toJsonString(structuredRecord);
      result.add(s);
    }
    return result.stream().sorted().collect(Collectors.toList());
  }

  @Test
  public void testDatasetWithoutRootElement() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("parentField", "ParentId");
    properties.put("childField", "ChildId");
    properties.put("parentChildMappingField", "ParentProduct=ChildProduct");
    properties.put("levelField", "levelField");
    properties.put("topField", "topField");
    properties.put("bottomField", "bottomField");
    properties.put("trueValueField", "true");
    properties.put("falseValueField", "false");
    properties.put("maxDepthField", "50");
    String inputDataset = UUID.randomUUID().toString();
    String outputDateset = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(inputDataset, INPUT_SCHEMA)))
      .addStage(new ETLStage("hierarchytorelational", new ETLPlugin("HierarchyToRelational",
                                                                    SparkCompute.PLUGIN_TYPE, properties)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputDateset)))
      .addConnection("source", "hierarchytorelational")
      .addConnection("hierarchytorelational", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(
      new ArtifactSummary(APP_ARTIFACT_PIPELINE.getName(), APP_ARTIFACT_PIPELINE.getVersion()), config);
    ApplicationId appId = NamespaceId.DEFAULT.app("hierarchytorelational");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDataset);
    MockSource.writeInput(inputManager, INPUT_DATA);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDateset);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    List<String> expected = convertStructuredRecordListToJson(EXPECTED_OUTPUT);
    List<String> result = convertStructuredRecordListToJson(output);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testDatasetWithRootElement() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("parentField", "ParentId");
    properties.put("childField", "ChildId");
    properties.put("parentChildMappingField", "ParentProduct=ChildProduct");
    properties.put("levelField", "levelField");
    properties.put("topField", "topField");
    properties.put("bottomField", "bottomField");
    properties.put("trueValueField", "true");
    properties.put("falseValueField", "false");
    properties.put("maxDepthField", "50");
    String inputDataset = UUID.randomUUID().toString();
    String outputDateset = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(inputDataset, INPUT_SCHEMA)))
      .addStage(new ETLStage("hierarchytorelational", new ETLPlugin("HierarchyToRelational",
                                                                    SparkCompute.PLUGIN_TYPE, properties)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputDateset)))
      .addConnection("source", "hierarchytorelational")
      .addConnection("hierarchytorelational", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(
      new ArtifactSummary(APP_ARTIFACT_PIPELINE.getName(), APP_ARTIFACT_PIPELINE.getVersion()), config);
    ApplicationId appId = NamespaceId.DEFAULT.app("hierarchytorelational");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDataset);
    MockSource.writeInput(inputManager, INPUT_DATA_WITH_PARENT_RECORD);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDateset);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);

    List<String> expected = convertStructuredRecordListToJson(EXPECTED_OUTPUT);
    List<String> result = convertStructuredRecordListToJson(output);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testConfigWithDefaultValues() throws NoSuchFieldException {
    HierarchyToRelationalConfig config = new HierarchyToRelationalConfig();
    FieldSetter.setField(config, HierarchyToRelationalConfig.class.getDeclaredField("parentField"),"ParentId");
    FieldSetter.setField(config, HierarchyToRelationalConfig.class.getDeclaredField("childField"),"ChildId");
    FieldSetter.setField(config, HierarchyToRelationalConfig.class.getDeclaredField("parentChildMappingField"),"ParentProduct=ChildProduct");
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
    Assert.assertEquals("Y", config.getTrueValueField());
    Assert.assertEquals("N", config.getFalseValueField());
    Assert.assertEquals("Top", config.getTopField());
    Assert.assertEquals("Bottom", config.getBottomField());
    Assert.assertEquals("Level", config.getLevelField());
    Schema outputSchema = config.generateOutputSchema(INPUT_SCHEMA);
    // expected schema with default values
    List<Schema.Field> fields = new ArrayList<>(INPUT_SCHEMA.getFields());
    fields.add(Schema.Field.of("Level", Schema.of(Schema.Type.INT)));
    fields.add(Schema.Field.of("Top", Schema.of(Schema.Type.STRING)));
    fields.add(Schema.Field.of("Bottom", Schema.of(Schema.Type.STRING)));
    Schema expectedOutputSchema = Schema.recordOf("record", fields);
    Assert.assertEquals(expectedOutputSchema, outputSchema);
  }
}
