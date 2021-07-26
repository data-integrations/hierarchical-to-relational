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
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
    Schema.Field.of("Sales", Schema.of(Schema.Type.INT))
  );

  private static final List<StructuredRecord> INPUT_DATA = ImmutableList.of(

    StructuredRecord.builder(INPUT_SCHEMA).set("ParentId", 1).set("ChildId", 2).set("ParentProduct", "Groceries")
      .set("ChildProduct", "Produce").set("Supplier", "A").set("Sales", 50).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("ParentId", 1).set("ChildId", 3).set("ParentProduct", "Groceries")
      .set("ChildProduct", "Dairy").set("Supplier", "B").set("Sales", 40).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("ParentId", 2).set("ChildId", 4).set("ParentProduct", "Produce")
      .set("ChildProduct", "Vegetables").set("Supplier", "C").set("Sales", 50).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("ParentId", 4).set("ChildId", 6).set("ParentProduct", "Vegetables")
      .set("ChildProduct", "Onion").set("Supplier", "E").set("Sales", 30).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("ParentId", 2).set("ChildId", 7).set("ParentProduct", "Produce")
      .set("ChildProduct", "Fruits").set("Supplier", "F").set("Sales", 40).build(),
    StructuredRecord.builder(INPUT_SCHEMA).set("ParentId", 3).set("ChildId", 8).set("ParentProduct", "Dairy")
      .set("ChildProduct", "Milk").set("Supplier", "D").set("Sales", 60).build()
  );

  private static final List<StructuredRecord> INPUT_DATA_WITH_PARENT_RECORD = ListUtils.union(
    Collections.singletonList(StructuredRecord.builder(INPUT_SCHEMA).set("ParentId", 1).set("ChildId", 1)
                                .set("ParentProduct", "Groceries").set("ChildProduct", "Groceries")
                                .set("Supplier", null).set("Sales", 0).build()), INPUT_DATA
  );


  private static StructuredRecord generateRecord(int parentId, int childId, String parentProduct, String childProduct,
                                                 String supplier, Integer sales, int leveField,
                                                 boolean bottomField, String pathField, String rootField) {
    return StructuredRecord.builder(generateOutputSchema())
      .set("ParentId", parentId)
      .set("ChildId", childId)
      .set("ParentProduct", parentProduct)
      .set("ChildProduct", childProduct)
      .set("Supplier", supplier)
      .set("Sales", sales)
      .set("levelField", leveField)
      .set("bottomField", bottomField)
      .set("pathField", pathField)
      .set("rootField", rootField)
      .build();
  }

  private static final List<StructuredRecord> EXPECTED_OUTPUT = ImmutableList.of(
    generateRecord(1, 3, "Groceries", "Dairy", "B", 40, 1, false,
                   "/Groceries", "Groceries"),
    generateRecord(1, 2, "Groceries", "Produce", "A", 50, 1, false,
                   "/Groceries", "Groceries"),
    generateRecord(2, 4, "Produce", "Vegetables", "C", 50, 1, false,
                   "/Produce", "Produce"),
    generateRecord(2, 7, "Produce", "Fruits", "F", 40, 1, true,
                   "/Produce", "Produce"),
    generateRecord(3, 8, "Dairy", "Milk", "D", 60, 1, true,
                   "/Dairy", "Dairy"),
    generateRecord(4, 6, "Vegetables", "Onion", "E", 30, 1,  true,
                   "/Vegetables", "Vegetables"),

    generateRecord(3, 8, "Dairy", "Milk", "D", 60, 2,  true,
                   "/Groceries/Dairy", "Groceries"),
    generateRecord(2, 4, "Produce", "Vegetables", "C", 50, 2,  false,
                   "/Groceries/Produce", "Groceries"),
    generateRecord(2, 7, "Produce", "Fruits", "F", 40, 2,  true,
                   "/Groceries/Produce", "Groceries"),
    generateRecord(4, 6, "Vegetables", "Onion", "E", 30, 2,  true,
                   "/Produce/Vegetables", "Produce"),

    generateRecord(4, 6, "Vegetables", "Onion", "E", 30, 3,  true,
                   "/Groceries/Produce/Vegetables", "Groceries")
  );

  private static Schema generateOutputSchema() {
    List<Schema.Field> fields = new ArrayList<>(INPUT_SCHEMA.getFields());
    fields.add(Schema.Field.of("levelField", Schema.of(Schema.Type.INT)));
    fields.add(Schema.Field.of("bottomField", Schema.of(Schema.Type.BOOLEAN)));
    fields.add(Schema.Field.of("pathField", Schema.of(Schema.Type.STRING)));
    fields.add(Schema.Field.of("rootField", Schema.of(Schema.Type.STRING)));
    return Schema.recordOf("x_flattened", fields);
  }

  @Test
  public void testMultipleRoots() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("parentField", "parent");
    properties.put("childField", "child");
    properties.put("pathField", "child");
    properties.put("pathAliasField", "path");
    properties.put("connectByRootField", "child=root");

    Schema schema = Schema.recordOf("x",
                                    Schema.Field.of("parent", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("child", Schema.of(Schema.Type.STRING)));
    String inputDataset = UUID.randomUUID().toString();
    String outputDateset = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(inputDataset, schema)))
      .addStage(new ETLStage("flatten", new ETLPlugin("HierarchyToRelational",
                                                      SparkCompute.PLUGIN_TYPE, properties)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputDateset)))
      .addConnection("source", "flatten")
      .addConnection("flatten", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(
      new ArtifactSummary(APP_ARTIFACT_PIPELINE.getName(), APP_ARTIFACT_PIPELINE.getVersion()), config);
    ApplicationId appId = NamespaceId.DEFAULT.app("multipath");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    /*
            |--> 2 --> 3 --|
        1 --|              |--> 6
            |--> 4 --|     |
                     |-----|
                 5 --|
     */
    List<StructuredRecord> input = new ArrayList<>();
    input.add(StructuredRecord.builder(schema).set("parent", "1").set("child", "2").build());
    input.add(StructuredRecord.builder(schema).set("parent", "1").set("child", "4").build());
    input.add(StructuredRecord.builder(schema).set("parent", "2").set("child", "3").build());
    input.add(StructuredRecord.builder(schema).set("parent", "3").set("child", "6").build());
    input.add(StructuredRecord.builder(schema).set("parent", "4").set("child", "6").build());
    input.add(StructuredRecord.builder(schema).set("parent", "5").set("child", "6").build());
    DataSetManager<Table> inputManager = getDataset(inputDataset);
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDateset);
    Set<StructuredRecord> output = new HashSet<>(MockSink.readOutput(outputManager));

    Schema expectedSchema = Schema.recordOf("x_flattened",
                                            Schema.Field.of("parent", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("child", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("Level", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("Bottom", Schema.of(Schema.Type.BOOLEAN)),
                                            Schema.Field.of("path", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("root", Schema.of(Schema.Type.STRING)));
    /*
            |--> 2 --> 3 --|
        1 --|              |--> 6
            |--> 4 --|     |
                     |-----|
                 5 --|

        should result in:

        1->1, 1->2, 1->3, 1->4, 1->6
        2->2, 2->3, 2->6
        3->3, 3->6
        4->4, 4->6
        5->5, 5->6
        6->6

        There are two paths from 1->6, they should be deduped with the minimum level chosen
     */
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", "1").set("child", "2").set("Level", 1).set("Bottom", false)
                   .set("path", "/2").set("root", "2").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", "1").set("child", "4").set("Level", 1).set("Bottom", false)
                   .set("path", "/4").set("root", "4").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", "2").set("child", "3").set("Level", 1).set("Bottom", false)
                   .set("path", "/3").set("root", "3").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", "3").set("child", "6").set("Level", 1).set("Bottom", true)
                   .set("path", "/6").set("root", "6").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", "4").set("child", "6").set("Level", 1).set("Bottom", true)
                   .set("path", "/6").set("root", "6").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", "5").set("child", "6").set("Level", 1).set("Bottom", true)
                   .set("path", "/6").set("root", "6").build());

    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", "2").set("child", "3").set("Level", 2).set("Bottom", false)
                   .set("path", "/2/3").set("root", "2").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", "4").set("child", "6").set("Level", 2).set("Bottom", true)
                   .set("path", "/4/6").set("root", "4").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", "3").set("child", "6").set("Level", 2).set("Bottom", true)
                   .set("path", "/3/6").set("root", "3").build());

    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", "3").set("child", "6").set("Level", 3).set("Bottom", true)
                   .set("path", "/2/3/6").set("root", "2").build());

    Assert.assertEquals(expected, output);
  }

  @Test
  public void testStartWith() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("parentField", "parent");
    properties.put("childField", "child");
    properties.put("pathField", "parent");
    properties.put("pathAliasField", "path");
    properties.put("pathSeparator", "|");
    properties.put("startWith", "parent == 2 OR parent == 5");
    Schema schema = Schema.recordOf("x",
                                    Schema.Field.of("parent", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("child", Schema.of(Schema.Type.STRING)));
    String inputDataset = UUID.randomUUID().toString();
    String outputDateset = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(inputDataset, schema)))
      .addStage(new ETLStage("flatten", new ETLPlugin("HierarchyToRelational",
                                                      SparkCompute.PLUGIN_TYPE, properties)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputDateset)))
      .addConnection("source", "flatten")
      .addConnection("flatten", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(
      new ArtifactSummary(APP_ARTIFACT_PIPELINE.getName(), APP_ARTIFACT_PIPELINE.getVersion()), config);
    ApplicationId appId = NamespaceId.DEFAULT.app("multipath");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    /*
            |--> 2 --> 3 --|
        1 --|              |--> 6
            |--> 4 --|     |
                     |-----|
                 5 --|
     */
    List<StructuredRecord> input = new ArrayList<>();
    input.add(StructuredRecord.builder(schema).set("parent", "1").set("child", "2").build());
    input.add(StructuredRecord.builder(schema).set("parent", "1").set("child", "4").build());
    input.add(StructuredRecord.builder(schema).set("parent", "2").set("child", "3").build());
    input.add(StructuredRecord.builder(schema).set("parent", "3").set("child", "6").build());
    input.add(StructuredRecord.builder(schema).set("parent", "4").set("child", "6").build());
    input.add(StructuredRecord.builder(schema).set("parent", "5").set("child", "6").build());
    DataSetManager<Table> inputManager = getDataset(inputDataset);
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDateset);
    Set<StructuredRecord> output = new HashSet<>(MockSink.readOutput(outputManager));

    Schema expectedSchema = Schema.recordOf("x_flattened",
                                            Schema.Field.of("parent", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("child", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("Level", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("Bottom", Schema.of(Schema.Type.BOOLEAN)),
                                            Schema.Field.of("path", Schema.of(Schema.Type.STRING)));
    /*
            |--> 2 --> 3 --|
        1 --|              |--> 6
            |--> 4 --|     |
                     |-----|
                 5 --|

        start with parent == 2 OR parent == 5 should result in:

        2->3, 5->6 (level 1),
        2->6 (level 2)

     */
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", "2").set("child", "3").set("Level", 1).set("Bottom", false)
                   .set("path", "|2").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", "5").set("child", "6").set("Level", 1).set("Bottom", true)
                   .set("path", "|5").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", "3").set("child", "6").set("Level", 2).set("Bottom", true)
                   .set("path", "|2|3").build());
    Assert.assertEquals(expected, output);
  }

  @Test
  public void testDatasetWithoutRootElement() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("parentField", "ParentId");
    properties.put("childField", "ChildId");
    properties.put("levelField", "levelField");
    properties.put("bottomField", "bottomField");
    properties.put("maxDepthField", "50");
    properties.put("pathField", "ParentProduct");
    properties.put("pathAliasField", "pathField");
    properties.put("connectByRootField", "ParentProduct=rootField");

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

    Assert.assertEquals(new HashSet<>(EXPECTED_OUTPUT), new HashSet<>(output));
  }

  @Test
  public void testDatasetWithRootElement() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("parentField", "ParentId");
    properties.put("childField", "ChildId");
    properties.put("levelField", "levelField");
    properties.put("bottomField", "bottomField");
    properties.put("maxDepthField", "50");
    properties.put("pathField", "ParentProduct");
    properties.put("pathAliasField", "pathField");
    properties.put("connectByRootField", "ParentProduct=rootField");
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

    Assert.assertEquals(new HashSet<>(EXPECTED_OUTPUT), new HashSet<>(output));
  }

  @Test
  public void testDisjointHierarchies() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("parentField", "parent");
    properties.put("childField", "child");
    properties.put("pathField", "category");
    properties.put("pathAliasField", "path");

    Schema schema = Schema.recordOf("x",
                                    Schema.Field.of("parent", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("child", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("category", Schema.of(Schema.Type.STRING))
    );
    String inputDataset = UUID.randomUUID().toString();
    String outputDateset = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(inputDataset, schema)))
      .addStage(new ETLStage("flatten", new ETLPlugin("HierarchyToRelational",
                                                      SparkCompute.PLUGIN_TYPE, properties)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputDateset)))
      .addConnection("source", "flatten")
      .addConnection("flatten", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(
      new ArtifactSummary(APP_ARTIFACT_PIPELINE.getName(), APP_ARTIFACT_PIPELINE.getVersion()), config);
    ApplicationId appId = NamespaceId.DEFAULT.app("multipath");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    /*
            |--> 3                                7 ---> 8
        1 --|
            |        |--> 5       4
            |--> 2 --|            |
                     |--> 6  <----|

     */
    List<StructuredRecord> input = new ArrayList<>();
    input.add(StructuredRecord.builder(schema).set("parent", 1).set("child", 2)
                .set("category", "vegetable").build());
    input.add(StructuredRecord.builder(schema).set("parent", 1).set("child", 3)
                .set("category", "dairy").build());
    input.add(StructuredRecord.builder(schema).set("parent", 2).set("child", 5)
                .set("category", "lettuce").build());
    input.add(StructuredRecord.builder(schema).set("parent", 2).set("child", 6)
                .set("category", "tomato").build());
    input.add(StructuredRecord.builder(schema).set("parent", 4).set("child", 6).set("category", "tomato").build());
    input.add(StructuredRecord.builder(schema).set("parent", 7).set("child", 8)
                .set("category", "water").build());
    DataSetManager<Table> inputManager = getDataset(inputDataset);
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDateset);
    Set<StructuredRecord> output = new HashSet<>(MockSink.readOutput(outputManager));

    Schema expectedSchema = Schema.recordOf("x_flattened",
                                            Schema.Field.of("parent", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("child", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("category", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("Level", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("Bottom", Schema.of(Schema.Type.BOOLEAN)),
                                            Schema.Field.of("path", Schema.of(Schema.Type.STRING)));

    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", 1).set("child", 2).set("Level", 1).set("Bottom", false)
                   .set("category", "vegetable").set("path", "/vegetable").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", 1).set("child", 3).set("Level", 1).set("Bottom", true)
                   .set("category", "dairy").set("path", "/dairy").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", 2).set("child", 5).set("Level", 1).set("Bottom", true)
                   .set("category", "lettuce").set("path", "/lettuce").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", 2).set("child", 6).set("Level", 1).set("Bottom", true)
                   .set("category", "tomato").set("path", "/tomato").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", 4).set("child", 6).set("Level", 1).set("Bottom", true)
                   .set("category", "tomato").set("path", "/tomato").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", 7).set("child", 8).set("Level", 1).set("Bottom", true)
                   .set("category", "water").set("path", "/water").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", 2).set("child", 5).set("Level", 2).set("Bottom", true)
                   .set("category", "lettuce").set("path", "/vegetable/lettuce").build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", 2).set("child", 6).set("Level", 2).set("Bottom", true)
                   .set("category", "tomato").set("path", "/vegetable/tomato").build());

    Assert.assertEquals(expected, output);
  }

  @Test
  public void testOneLevelHierarchy() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("parentField", "parent");
    properties.put("childField", "child");
    properties.put("maxDepth", "1");
    Schema schema = Schema.recordOf("x",
                                    Schema.Field.of("parent", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("child", Schema.of(Schema.Type.INT)));
    String inputDataset = UUID.randomUUID().toString();
    String outputDateset = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(inputDataset, schema)))
      .addStage(new ETLStage("flatten", new ETLPlugin("HierarchyToRelational",
                                                      SparkCompute.PLUGIN_TYPE, properties)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputDateset)))
      .addConnection("source", "flatten")
      .addConnection("flatten", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(
      new ArtifactSummary(APP_ARTIFACT_PIPELINE.getName(), APP_ARTIFACT_PIPELINE.getVersion()), config);
    ApplicationId appId = NamespaceId.DEFAULT.app("multipath");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    List<StructuredRecord> input = new ArrayList<>();
    input.add(StructuredRecord.builder(schema).set("parent", 1).set("child", 2).build());
    DataSetManager<Table> inputManager = getDataset(inputDataset);
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDateset);
    Set<StructuredRecord> output = new HashSet<>(MockSink.readOutput(outputManager));

    Schema expectedSchema = Schema.recordOf("x_flattened",
                                            Schema.Field.of("parent", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("child", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("Level", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("Bottom", Schema.of(Schema.Type.BOOLEAN)));
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", 1).set("child", 2).set("Level", 1).set("Bottom", true).build());

    Assert.assertEquals(expected, output);
  }

  @Test
  public void testMultipleConnectByRoots() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("parentField", "parent");
    properties.put("childField", "child");
    properties.put("connectByRootField", "parent=root1;child=root2");

    Schema schema = Schema.recordOf("x",
                                    Schema.Field.of("parent", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("child", Schema.of(Schema.Type.INT)));
    String inputDataset = UUID.randomUUID().toString();
    String outputDateset = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
      .addStage(new ETLStage("source", MockSource.getPlugin(inputDataset, schema)))
      .addStage(new ETLStage("flatten", new ETLPlugin("HierarchyToRelational",
                                                      SparkCompute.PLUGIN_TYPE, properties)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputDateset)))
      .addConnection("source", "flatten")
      .addConnection("flatten", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(
      new ArtifactSummary(APP_ARTIFACT_PIPELINE.getName(), APP_ARTIFACT_PIPELINE.getVersion()), config);
    ApplicationId appId = NamespaceId.DEFAULT.app("multipath");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    List<StructuredRecord> input = new ArrayList<>();
    input.add(StructuredRecord.builder(schema).set("parent", 1).set("child", 2).build());
    input.add(StructuredRecord.builder(schema).set("parent", 2).set("child", 3).build());

    DataSetManager<Table> inputManager = getDataset(inputDataset);
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDateset);
    Set<StructuredRecord> output = new HashSet<>(MockSink.readOutput(outputManager));

    Schema expectedSchema = Schema.recordOf("x_flattened",
                                            Schema.Field.of("parent", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("child", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("Level", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("Bottom", Schema.of(Schema.Type.BOOLEAN)),
                                            Schema.Field.of("root1", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("root2", Schema.of(Schema.Type.INT)));
    Set<StructuredRecord> expected = new HashSet<>();
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", 1).set("child", 2).set("Level", 1).set("Bottom", false).set("root1", 1)
                   .set("root2", 2).build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", 2).set("child", 3).set("Level", 1).set("Bottom", true).set("root1", 2)
                   .set("root2", 3).build());
    expected.add(StructuredRecord.builder(expectedSchema)
                   .set("parent", 2).set("child", 3).set("Level", 2).set("Bottom", true).set("root1", 1)
                   .set("root2", 2).build());

    Assert.assertEquals(expected, output);
  }
}
