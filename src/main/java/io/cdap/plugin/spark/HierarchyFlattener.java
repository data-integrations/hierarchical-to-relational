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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Takes an RDD that represents a hierarchical structure and flattens it.
 *
 * The input RDD is expected to edges in a tree, with each record containing an id for that node and the id of its
 * parent. The flattened RDD will contain rows denoting each node -> accessible child relationship, along with
 * additional information about that relationship. For example, if the input is:
 *
 * parent:null, id:A  (A as root)
 * parent:A, id:B     (A -> B)
 * parent:B, id:C     (B -> C)
 * parent:B, id:D     (B -> D)
 *
 * The output will be:
 *
 * parent:A, id:A, depth:0, isTop:true, isBot:false
 * parent:A, id:B, depth:1, isTop:false, isBot:false
 * parent:A, id:C, depth:2, isTop:false, isBot:true
 * parent:A, id:D, depth:2, isTop:false, isBot:true
 * parent:B, id:B, depth:0, isTop:false, isBot:false
 * parent:B, id:C, depth:1, isTop:false, isBot:true
 * parent:B, id:D, depth:1, isTop:false, isBot:true
 * parent:C, id:C, depth:0, isTop:false, isBot:true
 * parent:D, id:D, depth:0, isTop:false, isBot:true
 */
public class HierarchyFlattener {
  private static final Logger LOG = LoggerFactory.getLogger(HierarchyFlattener.class);
  private final String parentCol;
  private final String childCol;
  private final String levelCol;
  private final String topCol;
  private final String botCol;
  private final String trueStr;
  private final String falseStr;
  private final int maxLevel;
  private final Map<String, String> parentChildMapping;

  public HierarchyFlattener(HierarchyConfig config) {
    this.parentCol = config.getParentField();
    this.childCol = config.getChildField();
    this.levelCol = config.getLevelField();
    this.topCol = config.getTopField();
    this.botCol = config.getBottomField();
    this.trueStr = config.getTrueValue();
    this.falseStr = config.getFalseValue();
    this.maxLevel = config.getMaxDepth();
    this.parentChildMapping = config.getParentChildMapping();
  }

  /**
   * Takes an input RDD and flattens it so that every possible ancestor to child relationship is present in the output.
   * Each output record also is annotated with whether the ancestor is a root node, whether the child is a leaf node,
   * and the distance from the ancestor to the child.
   *
   * Suppose the input data represents the hierarchy
   *
   *                      |--> 5
   *             |--> 2 --|
   *         1 --|        |--|
   *             |--> 3      |--> 6
   *                     4 --|
   *
   * This would be represented with the following rows:
   *
   *   [1 -> 1]
   *   [1 -> 2]
   *   [1 -> 3]
   *   [2 -> 5]
   *   [2 -> 6]
   *   [4 -> 4]
   *   [4 -> 6]
   *
   * and would generate the following output:
   *
   *   [1 -> 1, level:0, root:yes, leaf:no]
   *   [1 -> 2, level:1, root:yes, leaf:no]
   *   [1 -> 3, level:1, root:yes, leaf:no]
   *   [1 -> 5, level:2, root:yes, leaf:yes]
   *   [1 -> 6, level:2, root:yes, leaf:yes]
   *   [2 -> 2, level:0, root:no, leaf:no]
   *   [2 -> 5, level:1, root:no, leaf:yes]
   *   [2 -> 6, level:1, root:no, leaf:yes]
   *   [3 -> 3, level:0, root:no, leaf:yes]
   *   [4 -> 4, level:0, root:yes, leaf:no]
   *   [4 -> 6, level:1, root:yes, leaf:yes]
   *   [5 -> 5, level:0, root:no, leaf:yes]
   *   [6 -> 6, level:0, root:no, leaf:yes]
   *
   * @param context spark plugin context
   * @param rdd input rdd representing a hierarchy
   * @return flattened hierarchy with level, root, and leaf information
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

       The first step is to generate the 0 distance paths.

       With input rows:

         [parent:1, child:1, category:grocery]
         [parent:1, child:2, category:vegetable]
         [parent:1, child:3, category:dairy]
         [parent:2, child:5, category:lettuce]
         [parent:2, child:6, category:tomato]
         [parent:4, child:4, category:fruit]
         [parent:4, child:6, category:tomato]

       the starting points are:

         [parent:1, child:1, level:0, root:true, leaf:0, category:grocery]
         [parent:2, child:2, level:0, root:false, leaf:0, category:vegetable]
         [parent:3, child:3, level:0, root:false, leaf:0, category:dairy]
         [parent:4, child:4, level:0, root:true, leaf:0, category:fruit]
         [parent:5, child:5, level:0, root:false, leaf:0, category:lettuce]
         [parent:6, child:6, level:0, root:false, leaf:0, category:tomato]
     */
    LOG.info("Starting computation for level 0");
    Dataset<Row> currentLevel = getStartingPoints(input, dataFieldNames);
    Dataset<Row> flattened = currentLevel;

    /*
       Each time through the loop, we find paths for the next level and identify leaf rows for the current level
       This is done by performing a left outer join on the current level with the original input.

         select
           current.parent as parent,
           input.child == null ? current.child as child,
           current.level + 1 as level,
           current.root as root,
           input.child == null ? true : false as leaf,
           input.datafield1, input.datafield2, ..., input.datafieldN
         from current left outer join input on current.child = input.parent
         where parent <> child or leaf = 1

       For example, the first time through, currentLevel is:

         [parent:1, child:1, level:0, root:true, leaf:0, category:grocery]
         [parent:2, child:2, level:0, root:false, leaf:0, category:vegetable]
         [parent:3, child:3, level:0, root:false, leaf:0, category:dairy]
         [parent:4, child:4, level:0, root:true, leaf:0, category:fruit]
         [parent:5, child:5, level:0, root:false, leaf:0, category:lettuce]
         [parent:6, child:6, level:0, root:false, leaf:0, category:tomato]

       When joined, this produces:

         [parent:1, child:2, level:1, root:true, leaf:0, category:vegetable]
         [parent:1, child:3, level:1, root:true, leaf:0, category:dairy]
         [parent:2, child:5, level:1, root:false, leaf:0, category:lettuce]
         [parent:2, child:6, level:1, root:false, leaf:0, category:tomato]
         [parent:3, child:3, level:0, root:false, leaf:1, category:dairy]
         [parent:4, child:6, level:1, root:true, leaf:0, category:tomato]
         [parent:5, child:5, level:0, root:false, leaf:1, category:lettuce]
         [parent:6, child:6, level:0, root:false, leaf:1, category:tomato]

       These result get added to the flattened output. All non-leaf rows become the next currentLevel.
       This continues until the max depth is reach, or until there are no more non-leaf rows.
     */
    int level = 0;
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
           false as root,
           input.child == null ? 1 : 0 as leaf,
           input.child == null ? current.datafield1 : input.datafield1,
           ...,
           input.child == null ? current.datafieldN : input.datafieldN
         from current left outer join input on current.child = input.parent
       */
      Column[] columns = new Column[schema.getFields().size() + 3];
      // currentLevel is aliased as "current" and input is aliased as "input"
      // to remove ambiguity between common column names.
      columns[0] = new Column("current." + parentCol).as(parentCol);
      columns[1] = functions.when(new Column("input." + childCol).isNull(), new Column("current." + childCol))
        .otherwise(new Column("input." + childCol)).as(childCol);
      columns[2] = new Column(levelCol).plus(1).as(levelCol);
      columns[3] = functions.lit(falseStr).as(topCol);
      columns[4] = functions.when(new Column("input." + childCol).isNull(), 1).otherwise(0).as(botCol);
      int i = 5;
      for (String fieldName : dataFieldNames) {
        if (parentChildMapping.containsKey(fieldName)) {
          // if there is a field mapping, this means the data field is an attribute of the parent and not of the child.
          // that means it should be taken from the current side and not the input side.
          columns[i++] = new Column("current." + fieldName);
        } else {
          columns[i++] = functions.when(new Column("input." + childCol).isNull(), new Column("current." + fieldName))
            .otherwise(new Column("input." + fieldName)).as(fieldName);
        }
      }
      Dataset<Row> nextLevel = currentLevel.alias("current")
        .join(input.alias("input"),
              new Column("current." + childCol).equalTo(new Column("input." + parentCol)),
              "leftouter")
        .select(columns);
      if (level == 0) {
        /*
           The first level, all nodes as x -> x, which will always join to themselves.
           So the very first time through, we need to filter these out to prevent an infinite loop.
           However, we *do* want to keep them if they have now been identified as a leaf node.
           For example, with the previous example, nextLevel at this point is:

             x [parent:1, child:1, level:0, root:true, leaf:0, category:grocery]
             o [parent:1, child:2, level:1, root:true, leaf:0, category:vegetable]
             o [parent:1, child:2, level:1, root:true, leaf:0, category:vegetable]
             o [parent:1, child:3, level:1, root:true, leaf:0, category:dairy]
             x [parent:2, child:2, level:0, root:false, leaf:0, category:vegetable]
             o [parent:2, child:5, level:1, root:false, leaf:0, category:lettuce]
             o [parent:2, child:6, level:1, root:false, leaf:0, category:tomato]
             o [parent:3, child:3, level:0, root:false, leaf:1, category:dairy]
             x [parent:4, child:4, level:0, root:true, leaf:0, category:fruit]
             o [parent:4, child:6, level:1, root:true, leaf:0, category:tomato]
             o [parent:5, child:5, level:0, root:false, leaf:1, category:lettuce]
             o [parent:6, child:6, level:0, root:false, leaf:1, category:tomato]

           Where the x indicates a row that needs to be filtered out.
         */
        nextLevel = nextLevel.where(nextLevel.col(parentCol).notEqual(nextLevel.col(childCol))
                                      .or(nextLevel.col(botCol).equalTo(1)));
      }
      flattened = flattened.union(nextLevel);

      // remove all leaf nodes from next iteration, since they don't have any children.
      currentLevel = nextLevel.where(nextLevel.col(botCol).notEqual(1));
      level++;
    }

    /*
        At this point, the flattened dataset contains duplicates for some leaf rows.
        With the previously mentioned input hierarchy, it will be:

         [1 -> 1, level:0, root:yes, leaf:0]
         [1 -> 2, level:1, root:yes, leaf:0]
         [1 -> 3, level:1, root:yes, leaf:0]
         [1 -> 5, level:2, root:yes, leaf:0]
         [1 -> 5, level:2, root:yes, leaf:1]
         [1 -> 6, level:2, root:yes, leaf:0]
         [1 -> 6, level:2, root:yes, leaf:1]
         [2 -> 2, level:0, root:no, leaf:0]
         [2 -> 5, level:1, root:no, leaf:0]
         [2 -> 5, level:1, root:no, leaf:1]
         [2 -> 6, level:1, root:no, leaf:0]
         [2 -> 6, level:1, root:no, leaf:1]
         [3 -> 3, level:0, root:no, leaf:0]
         [4 -> 4, level:0, root:yes, leaf:0]
         [4 -> 6, level:1, root:yes, leaf:0]
         [4 -> 6, level:1, root:yes, leaf:1]
         [5 -> 5, level:0, root:no, leaf:1]
         [6 -> 6, level:0, root:no, leaf:1]

       Note that for each leaf:1 row, there is a duplicate except it has leaf:0.
       These dupes are removed by grouping on [parent, child] and summing the leaf values.
       This is also where the leaf values are translated to their final strings instead of a 0 or 1.
       If there are multiple paths from one node to another, they will also be deduplicated down to just a single
       path here, where the level is the minimum level.

         select
           parent,
           child,
           min(level),
           first(root),
           max(leaf) == 0 ? false : true as leaf,
           first(datafield1), first(datafield2), ..., first(datafieldN)
         from flattened
         group by parent, child
     */
    Column[] columns = new Column[schema.getFields().size()];
    columns[0] = functions.first(new Column(topCol)).as(topCol);
    columns[1] = functions.when(functions.max(new Column(botCol)).equalTo(0), falseStr)
      .otherwise(trueStr).as(botCol);
    int i = 2;
    for (String fieldName : dataFieldNames) {
      columns[i++] = functions.first(new Column(fieldName)).as(fieldName);
    }

    flattened = flattened.groupBy(new Column(parentCol), new Column(childCol))
      .agg(functions.min(new Column(levelCol)).as(levelCol), columns);

    // perform a final select to make sure fields are in the same order as expected.
    Column[] finalOutputColumns = outputSchema.getFields().stream()
      .map(Schema.Field::getName)
      .map(Column::new)
      .collect(Collectors.toList()).toArray(new Column[outputSchema.getFields().size()]);
    flattened = flattened.select(finalOutputColumns);
    return flattened.javaRDD().map(row -> DataFrames.fromRow(row, outputSchema));
  }

  /*
     The hierarchy starting points are self referencing paths of level 0:

     With input rows:

         [parent:1, child:2, category:vegetable]
         [parent:1, child:3, category:dairy]
         [parent:2, child:5, category:lettuce]
         [parent:2, child:6, category:tomato]
         [parent:4, child:4, category:fruit]
         [parent:4, child:6, category:tomato]

     The 0 distance paths are:

         [parent:1, child:1, level:0, root:true, leaf:0, category:null]
         [parent:2, child:2, level:0, root:false, leaf:0, category:vegetable]
         [parent:3, child:3, level:0, root:false, leaf:0, category:dairy]
         [parent:4, child:4, level:0, root:true, leaf:0, category:fruit]
         [parent:5, child:5, level:0, root:false, leaf:0, category:lettuce]
         [parent:6, child:6, level:0, root:false, leaf:0, category:tomato]
   */
  private Dataset<Row> getStartingPoints(Dataset<Row> input, List<String> dataFieldNames) {
    // This is all rows where the parent never shows up as a child
    Dataset<Row> levelZero = getNonSelfReferencingRoots(input, dataFieldNames);

    /*
       The rest of level 0 is generated with another pass through the data.

         select
           A.child as parent,
           A.child as child,
           0 as level,
           (A.parent == A.child) as root,
           false as leaf,
           A.datafield1,
           ...,
           A.datafieldN

        A distinct is run at the end to remove duplicates, since there can be multiple paths to the same child.
     */
    Column[] columns = new Column[dataFieldNames.size() + 5];
    columns[0] = input.col(childCol).as(parentCol);
    columns[1] = input.col(childCol).as(childCol);
    columns[2] = functions.lit(0).as(levelCol);
    Column isRoot = input.col(parentCol).equalTo(input.col(childCol));
    columns[3] = functions.when(isRoot, trueStr).otherwise(falseStr).as(topCol);
    columns[4] = functions.lit(0).as(botCol);
    int i = 5;
    for (String fieldName : dataFieldNames) {
      /*
         If this is a mapped field, the child field should be used as both the parent and child.
         For example, suppose the input data looks like:

           [parent:1, child:1, parentProduct:groceries, childProduct:groceries, supplier:A]
           [parent:1, child:2, parentProduct:groceries, childProduct:produce, supplier:A]

         if the parent child mapping is parentProduct -> childProduct, that means the starting points should be:

           [parent:1, child:1, parentProduct:groceries, childProduct:groceries, supplier:A]
           [parent:2, child:2, parentProduct:produce, childProduct:produce, supplier:A]
       */
      String mappedField = parentChildMapping.get(fieldName);
      if (mappedField != null) {
        columns[i++] = functions.when(isRoot, input.col(fieldName))
          .otherwise(input.col(mappedField)).as(fieldName);
      } else {
        columns[i++] = input.col(fieldName);
      }
    }

    return levelZero.union(input.select(columns)).distinct();
  }

  /*
    Get roots by looking for rows where they parent never rows up as a child in any other row.
   */
  private Dataset<Row> getNonSelfReferencingRoots(Dataset<Row> input, List<String> dataFieldNames) {
    /*
       These types of roots are rows where the parent never shows up as a child in any other row.
       They are found by running the following query:

       select
         A.parent as parent,
         A.parent as child,
         0 as level,
         true as root,
         false as leaf,
         A.datafield1 as datafield1,
         ...,
         null as datafieldN
       from input as A left outer join (select child from input) as B on A.parent = B.child
       where B.child is null

       Whether a datafield is null or not depends on whether there is a mapping for that field.
       For example, with mapping parentCategory -> category and input rows:

         [parent:1, child:2, parentCategory:plants, category:vegetable, sold:100]
         [parent:2, child:3, parentCategory:vegetable, category:onion, sold:50]

       This results in:

         [parent:1, child:1, level:0, root:true, leaf:false, parentCategory:plants, category:plants, sold:null]

       parentCategory and category both get their value from the parentCategory key in the mapping.
       Every other data field is just null.
    */
    Column[] columns = new Column[dataFieldNames.size() + 5];
    columns[0] = new Column("A." + parentCol).as(parentCol);
    columns[1] = new Column("A." + parentCol).as(childCol);
    columns[2] = functions.lit(0).as(levelCol);
    columns[3] = functions.lit(trueStr).as(topCol);
    columns[4] = functions.lit(0).as(botCol);
    int i = 5;
    Map<String, String> childParentMapping = new HashMap<>();
    for (Map.Entry<String, String> entry : parentChildMapping.entrySet()) {
      childParentMapping.put(entry.getValue(), entry.getKey());
    }
    for (String fieldName : dataFieldNames) {
      /*
         If this is a mapped field, the child field should be used as both the parent and child.
         For example, suppose a root row looks like:

           [parent:1, child:2, parentCategory:plants, category:vegetable, sold:100]

         and there is a mapping of parentCategory -> category
         that means the result should be:

           [parent:1, child:1, level:0, root:true, leaf:false, parentCategory:plants, category:plants, sold:null]
       */
      String mappedField = childParentMapping.get(fieldName);
      if (mappedField != null) {
        // in example above, this is for setting parentCategory(plants) as category
        columns[i++] = new Column("A." + mappedField).as(fieldName);
      } else if (parentChildMapping.containsKey(fieldName)) {
        // in example above, this is for setting parentCategory as itself
        columns[i++] = new Column("A." + fieldName).as(fieldName);
      } else {
        // in example above, this is for setting null as sold
        columns[i++] = functions.lit(null).as(fieldName);
      }
    }

    /*
       we only need childCol from the input and not any of the other fields
       drop all the other fields before the join so that they don't need to be shuffled
       across the cluster, only to be dropped after the join completes.
     */
    Dataset<Row> children = input.select(new Column(childCol));
    Dataset<Row> joined = input.alias("A").join(
      children.alias("B"), new Column("A." + parentCol).equalTo(new Column("B." + childCol)), "leftouter")
      .where(new Column("B." + childCol).isNull())
      .select(columns);
    return joined;
  }

  private boolean isEmpty(Dataset<Row> dataset) {
    // unfortunately there is no way to check if a dataset is empty without running a spark job
    // this, however, is much more performant than performing a count(), which would require a pass over all the data.
    return dataset.takeAsList(1).isEmpty();
  }

}
