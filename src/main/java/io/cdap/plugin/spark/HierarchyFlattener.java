/*
 * Copyright © 2021 Cask Data, Inc.
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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Takes an RDD that represents a hierarchical structure and flattens it.
 *
 * The input RDD is expected to edges in a tree, with each record containing an id for that node and the id of its
 * child. The plugin will flatten hierarchies rooted at all nodes in the tree, if START is *not* configured. If
 * START WITH is configured, it will flatten hierarchies starting at nodes returned as a result of the START WITH
 * query. The flattened RDD will contain rows denoting each node -> child relationship, along with
 * additional information about that relationship.
 *
 * For example, if the input is:
 *
 * parent:A, child:B     (A -> B)
 * parent:B, child:C     (B -> C)
 * parent:B, child:D     (B -> D)
 *
 * The output will be:
 *
 * parent:A, child:B, level:1, isBot:false
 * parent:B, child:C, level:1, isBot:true
 * parent:B, child:D, level:1, isBot:true
 * parent:B, child:C, level:2, isBot:true
 * parent:B, child:D, level:2, isBot:true
 */
public class HierarchyFlattener {
  private static final Logger LOG = LoggerFactory.getLogger(HierarchyFlattener.class);
  private final String parentCol;
  private final String childCol;
  private final String levelCol;
  private final String botCol;
  private final int maxLevel;
  @Nullable
  private final String startsWith;
  private final Map<String, String> connectByRoot;
  private final String pathField;
  private final String pathAliasField;
  private final String pathSeparator;

  public HierarchyFlattener(HierarchyConfig config) {
    this.parentCol = config.getParentField();
    this.childCol = config.getChildField();
    this.levelCol = config.getLevelField();
    this.botCol = config.getBottomField();
    this.maxLevel = config.getMaxDepth();
    this.startsWith = config.getStartWith();
    this.connectByRoot = config.getConnectByRoot();
    this.pathField = config.getPathField();
    this.pathAliasField = config.getPathAliasField();
    this.pathSeparator = config.getPathSeparator();
  }

  /**
   * Takes an input RDD and flattens it so that every possible ancestor to child relationship is present in the output.
   * Each output record also is annotated with whether the whether the child is a leaf node and the distance from the
   * ancestor to the child.
   *
   * Suppose the input data represents the hierarchy
   *
   *                      |--> 5
   *             |--> 2 --|
   *         1 --|        |--|            7-->8
   *             |--> 3      |--> 6
   *                     4 --|
   *
   * This would be represented with the following rows:
   *
   *   [1 -> 2]
   *   [1 -> 3]
   *   [2 -> 5]
   *   [2 -> 6]
   *   [4 -> 6]
   *   [7 -> 8]
   *
   * and would generate the following output:
   *
   *   [1 -> 2, level:1, leaf:no]
   *   [1 -> 3, level:1, leaf:yes]
   *   [2 -> 5, level:1, leaf:yes]
   *   [2 -> 6, level:1, leaf:yes]
   *   [4 -> 6, level:1, leaf:yes]
   *   [7 -> 8, level:1, leaf:yes]
   *   [2 -> 5, level:2, leaf:yes]
   *   [2 -> 6, level:2, leaf:yes]
   *
   * @param context spark plugin context
   * @param rdd input rdd representing a hierarchy
   * @return flattened hierarchy with level and leaf information
   */
  public JavaRDD<StructuredRecord> flatten(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> rdd,
                                           Schema outputSchema) {
    Schema schema = context.getInputSchema();
    SQLContext sqlContext = new SQLContext(context.getSparkContext());
    StructType sparkSchema = DataFrames.toDataType(schema);
    Dataset<Row> input = sqlContext.createDataFrame(rdd.map(record -> DataFrames.toRow(record, sparkSchema)).rdd(),
                                                    sparkSchema);
    // cache the input so that the previous stages don't get re-processed
    input = input.persist(StorageLevel.DISK_ONLY());

    // field names without the parent and child fields.
    List<String> dataFieldNames = schema.getFields().stream()
      .map(Schema.Field::getName)
      .filter(name -> !name.equals(parentCol) && !name.equals(childCol))
      .collect(Collectors.toList());

    /*
       The approach is to take N passes through the hierarchy, where N is the maximum depth of the tree.
       In iteration i, paths of length i - 1 are found.

       The first step is to generate paths with length 1.

       With input rows:

         [parent:1, child:2, category:vegetable]
         [parent:1, child:3, category:dairy]
         [parent:2, child:5, category:lettuce]
         [parent:2, child:6, category:tomato]
         [parent:4, child:6, category:tomato]
         [parent:7, child:8, category:water]

      connectByRoot: category=root

      pathField: category, pathAliasField: path, pathSeparator:"/"

       the starting points are:

         [parent:1, child:2, level: 1, leaf:0, category:vegetable root:vegetable, path:/vegetable]
         [parent:1, child:3, level: 1, leaf:0, category:dairy, root:dairy, path:/dairy]
         [parent:2, child:5, level: 1, leaf:0, category:lettuce, root:lettuce path:/lettuce]
         [parent:2, child:6, level: 1, leaf:0, category:tomato, root:tomato, path:/tomato]
         [parent:4, child:6, level: 1, leaf:0, category:tomato, root:tomato, path:/tomato]
         [parent:7, child:8, level: 1, leaf:0, category:water, root:water, path:/water]

     */
    LOG.info("Starting computation for level 1");
    Dataset<Row> currentLevel = getStartingPoints(input, dataFieldNames, startsWith);
    // Remove self referencing nodes to avoid loops.
    currentLevel = currentLevel.where(input.col(parentCol).notEqual(input.col(childCol)));

    // Add path and connect by root columns to the dataset
    if (hasPath()) {
      currentLevel = currentLevel.withColumn(pathAliasField,
                                             functions.format_string("%s%s", functions.lit(pathSeparator),
                                                                     new Column(pathField)));
    }
    for (Map.Entry<String, String> entry : connectByRoot.entrySet()) {
      currentLevel = currentLevel.withColumn(entry.getValue(), new Column(entry.getKey()));
    }
    Dataset<Row> flattened = currentLevel;

    /*
       Each time through the loop, we find paths for the next level and identify leaf rows for the current level
       This is done by performing a left outer join on the current level with the original input.

       For example, the first time through, currentLevel is:

         [parent:1, child:2, level: 1, leaf:0, category:vegetable root:vegetable, path:/vegetable]
         [parent:1, child:3, level: 1, leaf:0, category:dairy, root:dairy, path:/dairy]
         [parent:2, child:5, level: 1, leaf:0, category:lettuce, root:lettuce path:/lettuce]
         [parent:2, child:6, level: 1, leaf:0, category:tomato, root:tomato, path:/tomato]
         [parent:4, child:6, level: 1, leaf:0, category:tomato, root:tomato, path:/tomato]
         [parent:7, child:8, level: 1, leaf:0, category:water, root:water, path:/water]

       When joined, this produces:

         [parent:1, child:3, level: 1, leaf:0, category:dairy, root:dairy, path:/dairy]
         [parent:2, child:5, level: 1, leaf:0, category:lettuce, root:lettuce path:/lettuce]
         [parent:2, child:6, level: 1, leaf:0, category:tomato, root:tomato, path:/tomato]
         [parent:4, child:6, level: 1, leaf:0, category:tomato, root:tomato, path:/tomato]
         [parent:7, child:8, level: 1, leaf:0, category:water, root:water, path:/water]
         [parent:2, child:6, level: 2, leaf:0, category:tomato, root:vegetable, path:/vegetable/tomato]
         [parent:2, child:5, level: 2, leaf:0, category:lettuce, root:vegetable path:/vegetable/lettuce]

       These result get added to the flattened output. All non-leaf rows become the next currentLevel.
       This continues until the max depth is reach, or until there are no more non-leaf rows.
     */
    int level = 1;
    while (!isEmpty(currentLevel)) {
      if (level > maxLevel) {
        throw new IllegalStateException(
          String.format("Exceeded maximum depth of %d. " +
                          "Ensure there are no cycles in the hierarchy, or increase the max depth.", maxLevel));
      }
      LOG.info("Starting computation for level {}", level + 1);

      /*
         select
           current.parent as parent,
           input.child == null ? current.child : input.child as child,
           current.level + 1 as level,
           input.child == null ? 1 : 0 as leaf,
           input.child == null ? current.datafield1 : input.datafield1,
           ...,
           input.child == null ? current.datafieldN : input.datafieldN,
           input.child == null ? pathAlias : pathAlias + pathSeparator + input.path  as pathAlias,
           level == 0 ? input.col1 : current.Root1 as Root1,
           ...
           level == 0 ? input.colN : current.RootN as RootN,
         from current left outer join input on current.child = input.parent
       */

      // + 2 is for levelCol and botCol that are not part of the input schema.
      int numColumns = schema.getFields().size() + 2 + connectByRoot.size();
      if (hasPath()) {
        numColumns++;
      }
      Column[] columns = new Column[numColumns];
      // currentLevel is aliased as "current" and input is aliased as "input"
      // to remove ambiguity between common column names.
      columns[0] = functions.when(new Column("input." + parentCol).isNull(), new Column("current." + parentCol))
        .otherwise(new Column("current." + childCol)).as(parentCol);
      columns[1] = functions.when(new Column("input." + childCol).isNull(), new Column("current." + childCol))
        .otherwise(new Column("input." + childCol)).as(childCol);
      columns[2] = functions.when(new Column("input." + childCol).isNull(), new Column(levelCol))
        .otherwise(new Column(levelCol).plus(1))
        .as(levelCol);
      columns[3] = functions.when(new Column("input." + childCol).isNull(), 1).otherwise(0).as(botCol);
      int i = 4;
      for (String fieldName : dataFieldNames) {
        columns[i++] = functions.when(new Column("input." + childCol).isNull(), new Column("current." + fieldName))
          .otherwise(new Column("input." + fieldName)).as(fieldName);
      }

      if (hasPath()) {
        columns[i++] = functions.when(new Column("input." + childCol).isNull(),
                                      new Column(pathAliasField))
          .otherwise(functions.concat_ws(pathSeparator, new Column(pathAliasField),
                                         new Column("input." + pathField)))
          .as(pathAliasField);
      }

      // Get the root field from the input in level 0, carry it forward in subsequent levels.
      for (Map.Entry<String, String> entry : connectByRoot.entrySet()) {
        columns[i++] = new Column("current." + entry.getValue()).as(entry.getValue());
      }

      Dataset<Row> nextLevel = currentLevel.alias("current")
        .join(input.alias("input"),
              new Column("current." + childCol).equalTo(new Column("input." + parentCol)),
              "leftouter")
        .select(columns);
      flattened = flattened.union(nextLevel);

      // remove all leaf nodes from next iteration, since they don't have any children.
      currentLevel = nextLevel.where(nextLevel.col(botCol).notEqual(1));
      level++;
    }

    /*
        At this point, the flattened dataset contains duplicates for some leaf rows.
        With the previously mentioned input hierarchy, it will be:

         [1 -> 2, level:1, leaf:0]
         [1 -> 3, level:1, leaf:0]
         [1 -> 3, level:1, leaf:1]
         [2 -> 5, level:1, leaf:0]
         [2 -> 5, level:1, leaf:1]
         [2 -> 6, level:1, leaf:0]
         [2 -> 6, level:1, leaf:1]
         [4 -> 6, level:1, leaf:0]
         [4 -> 6, level:1, leaf:1]
         [7 -> 8, level:1, leaf:0]
         [7 -> 8, level:1, leaf:1]
         [2 -> 5, level:2, leaf:0]
         [2 -> 5, level:2, leaf:1]
         [2 -> 6, level:2, leaf:0]
         [2 -> 6, level:2, leaf:1]

       Note that for each leaf:1 row, there is a duplicate except it has leaf:0.
       These dupes are removed by grouping on [parent, child, level] and summing the leaf values.
       This is also where the leaf values are translated to their final strings instead of a 0 or 1.

         select
           parent,
           child,
           min(level),
           max(leaf) == 0 ? false : true as leaf,
           first(datafield1), first(datafield2), ..., first(datafieldN),
           min(pathAliasField),
           first(connectByRoot1), first(connectByroot2) ,..., first(connectByrootN)
         from flattened
         group by parent, child, level
     */

    // numColumns = number of schema fields - 2 (parentCol, childCol which are part of schema)  + connectByRoots + path
    int numColumns = schema.getFields().size() + connectByRoot.size() - 2;
    if (hasPath()) {
      numColumns++;
    }
    Column[] columns = new Column[numColumns];
    int i = 0;
    for (String fieldName : dataFieldNames) {
      columns[i++] = functions.first(new Column(fieldName)).as(fieldName);
    }
    if (hasPath()) {
      columns[i++] = functions.min(new Column(pathAliasField)).as(pathAliasField);
    }
    for (String field : connectByRoot.values()) {
      columns[i++] = functions.first(new Column(field)).as(field);
    }


    flattened = flattened.groupBy(new Column(parentCol), new Column(childCol), new Column(levelCol))
      .agg(functions.when(functions.max(new Column(botCol)).equalTo(0), false)
             .otherwise(true).as(botCol), columns);

    // perform a final select to make sure fields are in the same order as expected.
    Column[] finalOutputColumns = outputSchema.getFields().stream()
      .map(Schema.Field::getName)
      .map(Column::new)
      .collect(Collectors.toList()).toArray(new Column[outputSchema.getFields().size()]);
    flattened = flattened.select(finalOutputColumns);
    return flattened.javaRDD().map(row -> DataFrames.fromRow(row, outputSchema));
  }

  private Dataset<Row> getStartingPoints(Dataset<Row> input, List<String> dataFieldNames,
                                         @Nullable String condition) {
    /*
       Roots are found by running the following query:

       select
         A.parent as parent,
         A.parent as child,
         0 as level,
         false as leaf,
         A.datafield1 as datafield1,
         ...,
         null as datafieldN
       from input
       if condition != null : where <condition>

    */
    Column[] columns = getLevel1Columns(dataFieldNames);
    if (condition != null) {
      return input.alias("A").where(condition).select(columns);
    }
    return input.alias("A").select(columns);
  }

  private Column[] getLevel1Columns(List<String> dataFieldNames) {
    Column[] columns = new Column[dataFieldNames.size() + 4];
    columns[0] = new Column("A." + parentCol).as(parentCol);
    columns[1] = new Column("A." + childCol).as(childCol);
    columns[2] = functions.lit(1).as(levelCol);
    columns[3] = functions.lit(0).as(botCol);
    int i = 4;
    for (String fieldName : dataFieldNames) {
      columns[i++] = new Column("A." + fieldName).as(fieldName);
    }
    return columns;
  }

  private boolean hasPath() {
    return !Strings.isNullOrEmpty(pathField) && !Strings.isNullOrEmpty(pathAliasField);
  }

  private boolean isEmpty(Dataset<Row> dataset) {
    // unfortunately there is no way to check if a dataset is empty without running a spark job
    // this, however, is much more performant than performing a count(), which would require a pass over all the data.
    return dataset.takeAsList(1).isEmpty();
  }

}
