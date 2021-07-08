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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.FieldSetter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HierarchyConfigTest {
  private static final Schema INPUT_SCHEMA = Schema.recordOf(
    "x",
    Schema.Field.of("ParentId", Schema.of(Schema.Type.INT)),
    Schema.Field.of("ChildId", Schema.of(Schema.Type.INT)),
    Schema.Field.of("ParentProduct", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("ChildProduct", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Supplier", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("Sales", Schema.nullableOf(Schema.of(Schema.Type.INT)))
  );

  @Test
  public void testValidateEmptyPath() throws NoSuchFieldException {
    HierarchyConfig config = new HierarchyConfig();
    FieldSetter.setField(config, HierarchyConfig.class.getDeclaredField("parentField"), "ParentId");
    FieldSetter.setField(config, HierarchyConfig.class.getDeclaredField("childField"), "ChildId");
    FieldSetter.setField(config, HierarchyConfig.class.getDeclaredField("pathField"), "Sales");
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(collector, INPUT_SCHEMA);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Path alias field name is null/empty.",
                        collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateInvalidPath() throws NoSuchFieldException {
    HierarchyConfig config = new HierarchyConfig();
    FieldSetter.setField(config, HierarchyConfig.class.getDeclaredField("parentField"), "ParentId");
    FieldSetter.setField(config, HierarchyConfig.class.getDeclaredField("childField"), "ChildId");
    FieldSetter.setField(config, HierarchyConfig.class.getDeclaredField("pathField"), "foo");
    FieldSetter.setField(config, HierarchyConfig.class.getDeclaredField("pathAliasField"), "PathAlias");
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(collector, INPUT_SCHEMA);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Field foo not found in the input schema.",
                        collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidatePathAlias() throws NoSuchFieldException {
    HierarchyConfig config = new HierarchyConfig();
    FieldSetter.setField(config, HierarchyConfig.class.getDeclaredField("parentField"), "ParentId");
    FieldSetter.setField(config, HierarchyConfig.class.getDeclaredField("childField"), "ChildId");
    FieldSetter.setField(config, HierarchyConfig.class.getDeclaredField("pathAliasField"), "PathAlias");
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(collector, INPUT_SCHEMA);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Path field name is null/empty.",
                        collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testConnectByRoot() throws NoSuchFieldException {
    HierarchyConfig config = new HierarchyConfig();
    FieldSetter.setField(config, HierarchyConfig.class.getDeclaredField("parentField"), "ParentId");
    FieldSetter.setField(config, HierarchyConfig.class.getDeclaredField("childField"), "ChildId");
    FieldSetter.setField(config, HierarchyConfig.class.getDeclaredField("connectByRootField"), "foo=root");
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(collector, INPUT_SCHEMA);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("Field foo not found in the input schema.",
                        collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testConfigWithDefaultValues() throws NoSuchFieldException {
    HierarchyConfig config = new HierarchyConfig();
    FieldSetter.setField(config, HierarchyConfig.class.getDeclaredField("parentField"), "ParentId");
    FieldSetter.setField(config, HierarchyConfig.class.getDeclaredField("childField"), "ChildId");
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(collector, INPUT_SCHEMA);
    Assert.assertEquals(0, collector.getValidationFailures().size());
    Assert.assertEquals("Bottom", config.getBottomField());
    Assert.assertEquals("Level", config.getLevelField());
    Assert.assertEquals(50, config.getMaxDepth());
    Assert.assertNull(config.getStartWith());
    Assert.assertEquals(Collections.emptyMap(), config.getConnectByRoot());
    Assert.assertNull(config.getPathField());
    Assert.assertNull(config.getPathAliasField());
    Assert.assertEquals("/", config.getPathSeparator());
    Schema outputSchema = config.generateOutputSchema(INPUT_SCHEMA);
    // expected schema with default values
    List<Schema.Field> fields = new ArrayList<>(INPUT_SCHEMA.getFields());
    fields.add(Schema.Field.of("Level", Schema.of(Schema.Type.INT)));
    fields.add(Schema.Field.of("Bottom", Schema.of(Schema.Type.BOOLEAN)));
    Schema expectedOutputSchema = Schema.recordOf("record", fields);
    Assert.assertEquals(expectedOutputSchema, outputSchema);
  }
}
