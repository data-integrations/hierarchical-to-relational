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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.broadcast;

/**
 * Takes an RDD that represents a hierarchical structure and flattens it.
 * <p>
 * The input RDD is expected to edges in a tree, with each record containing an id for that node and the id of its
 * parent. The flattened RDD will contain rows denoting each node -> accessible child relationship, along with
 * additional information about that relationship. For example, if the input is:
 * <p>
 * parent:null, id:A  (A as root)
 * parent:A, id:B     (A -> B)
 * parent:B, id:C     (B -> C)
 * parent:B, id:D     (B -> D)
 * <p>
 * The output will be:
 * <p>
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
  private static final boolean SHOW_DEBUG_CONTENT = true;
  private static final boolean SHOW_DEBUG_COLUMNS = true;

  private final String parentCol;
  private final String childCol;
  private Object parentRootValue = null;
  private Object childRootValue = null;
  private final Map<String, String> parentChildMapping;
  private final List<Map<String, String>> pathFields;
  private final List<Map<String, String>> connectByRootFields;

  private final String levelCol;
  private final String topCol;
  private final String botCol;

  private final String trueStr;
  private final String falseStr;

  private final int maxLevel;
  private final Boolean broadcastJoin;

  public HierarchyFlattener(HierarchyConfig config) {
    this.parentCol = config.getParentField();
    this.childCol = config.getChildField();
    this.levelCol = config.getLevelField();
    this.topCol = config.getTopField();
    this.botCol = config.getBottomField();
    this.trueStr = config.getTrueValue();
    this.falseStr = config.getFalseValue();
    this.maxLevel = config.getMaxDepth();
    this.broadcastJoin = config.isBroadcastJoin();
    this.parentChildMapping = config.getParentChildMapping();
    this.pathFields = config.getPathFields();
    this.connectByRootFields = config.getConnectByRootFields();

    /****************************
     ** DEBUG - BEGIN - REMOVE **
     ****************************/
    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("====================================");
      LOG.info("== pathFields before parsing: " + config.getRawPathFields() + " ==");
      LOG.info("====================================");
    }
    /****************************
     ** DEBUG - END - REMOVE **
     ****************************/

    /****************************
     ** DEBUG - BEGIN - REMOVE **
     ****************************/
    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("======================================");
      LOG.info("== pathFields after parsing - Begin ==");
      LOG.info("======================================");
      for (Map<String, String> map : this.pathFields) {
        for (String k : map.keySet()) {
          LOG.info("== " + k + ":" + map.get(k));
        }
      }
      LOG.info("====================================");
      LOG.info("== pathFields after parsing - End ==");
      LOG.info("====================================");

      LOG.info("================================================");
      LOG.info("=== connectByRootFields after parsing - Begin ==");
      LOG.info("================================================");
      for (Map<String, String> map : this.connectByRootFields) {
        for (String k : map.keySet()) {
          LOG.info("== " + k + ":" + map.get(k));
        }
      }
      LOG.info("=============================================");
      LOG.info("== connectByRootFields after parsing - End ==");
      LOG.info("=============================================");

      LOG.info("===============================================");
      LOG.info("=== parentChildMapping after parsing - Begin ==");
      LOG.info("===============================================");
      for (String k : parentChildMapping.keySet()) {
        LOG.info("== " + k + ":" + parentChildMapping.get(k));
      }

      LOG.info("============================================");
      LOG.info("== parentChildMapping after parsing - End ==");
      LOG.info("============================================");

    }
    /**************************
     ** DEBUG - END - REMOVE **
     **************************/
  }

  /**
   * Takes an input RDD and flattens it so that every possible ancestor to child relationship is present in the output.
   * Each output record also is annotated with whether the ancestor is a root node, whether the child is a leaf node,
   * and the distance from the ancestor to the child.
   * <p>
   * Suppose the input data represents the hierarchy
   * <p>
   * |--> 5
   * |--> 2 --|
   * 1 --|        |--|
   * |--> 3      |--> 6
   * 4 --|
   * <p>
   * This would be represented with the following rows:
   * <p>
   * [1 -> 1]
   * [1 -> 2]
   * [1 -> 3]
   * [2 -> 5]
   * [2 -> 6]
   * [4 -> 4]
   * [4 -> 6]
   * <p>
   * and would generate the following output:
   * <p>
   * [1 -> 1, level:0, root:yes, leaf:no]
   * [1 -> 2, level:1, root:yes, leaf:no]
   * [1 -> 3, level:1, root:yes, leaf:no]
   * [1 -> 5, level:2, root:yes, leaf:yes]
   * [1 -> 6, level:2, root:yes, leaf:yes]
   * [2 -> 2, level:0, root:no, leaf:no]
   * [2 -> 5, level:1, root:no, leaf:yes]
   * [2 -> 6, level:1, root:no, leaf:yes]
   * [3 -> 3, level:0, root:no, leaf:yes]
   * [4 -> 4, level:0, root:yes, leaf:no]
   * [4 -> 6, level:1, root:yes, leaf:yes]
   * [5 -> 5, level:0, root:no, leaf:yes]
   * [6 -> 6, level:0, root:no, leaf:yes]
   *
   * @param context spark plugin context
   * @param rdd     input rdd representing a hierarchy
   * @return flattened hierarchy with level, root, and leaf information
   */
  public JavaRDD<StructuredRecord> flatten(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> rdd,
                                           Schema outputSchema) {
    Schema inputSchema = context.getInputSchema();
    SQLContext sqlContext = new SQLContext(context.getSparkContext());
    StructType sparkSchema = DataFrames.toDataType(inputSchema);

    Dataset<Row> input = sqlContext.createDataFrame(
        rdd.map((StructuredRecord record) -> DataFrames.toRow(record, sparkSchema)).rdd(),
        sparkSchema);

    // TODO: Add debug info here
    LOG.info("==============================");
    LOG.info("== Content of input - BEGIN ==");
    LOG.info("==============================");
    JavaRDD<Row> test = input.javaRDD();
    Iterator<Row> iterat = test.toLocalIterator();
    for (Iterator<Row> it = input.javaRDD().toLocalIterator(); it.hasNext(); ) {
      Row line = it.next();
      LOG.info("== " + line);
    }
    LOG.info("============================");
    LOG.info("== Content of input - END ==");
    LOG.info("=============================");

    // cache the input so that the previous stages don't get re-processed
    input = input.persist(StorageLevel.DISK_ONLY());

    // field names without the parent and child fields.
    List<String> dataFieldNames = inputSchema.getFields().stream()
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
    LOG.info("======================================");
    LOG.info("== Starting computation for level 0 ==");
    LOG.info("======================================");
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

      LOG.info("=======================================");
      LOG.info("== Starting computation for level {} ==", level + 1);
      LOG.info("=======================================");

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

      // 2 * pathFields.size() to account for 2 extra columns for the path & path length
      // 1 * connectByRootFields.size() to account for 1 extra column for the coonect_by_root
      Column[] columns = new Column[inputSchema.getFields().size() + 3 + 2 * pathFields.size()
          + 1 * connectByRootFields.size()];
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

      // Path & Path_length columns
      for (Map<String, String> pathField : pathFields) {
        columns[i++] = functions.concat(
            new Column(pathField.get(HierarchyConfig.PATH_FIELD_ALIAS)),
            functions.lit(pathField.get(HierarchyConfig.PATH_SEPARATOR)),
            new Column("input." + pathField.get(HierarchyConfig.VERTEX_FIELD_NAME))
        ).as(pathField.get(HierarchyConfig.PATH_FIELD_ALIAS));
        columns[i++] = new Column(pathField.get(HierarchyConfig.PATH_FIELD_LENGTH_ALIAS)).plus(1)
            .as(pathField.get(HierarchyConfig.PATH_FIELD_LENGTH_ALIAS));
      }

      // CONNECT_BY_ROOT column
      for (Map<String, String> cbrField : connectByRootFields) {
        columns[i++] = new Column(cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_ALIAS))
            .as(cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_ALIAS));
      }

      /****************************
       ** DEBUG - BEGIN - REMOVE **
       ****************************/
      if (SHOW_DEBUG_COLUMNS) {
        LOG.info("==========================");
        LOG.info("== currentLevel - Begin ==");
        LOG.info("==========================");
        for (Column col : columns) {
          if (col != null) {
            LOG.info("== " + col);
          }
        }
        LOG.info("========================");
        LOG.info("== currentLevel - End ==");
        LOG.info("========================");
      }
      /****************************
       ** DEBUG - END - REMOVE **
       ****************************/

      Dataset<Row> nextLevel;
      if (broadcastJoin) {
        nextLevel = currentLevel.alias("current")
            .join(broadcast(input.alias("input")),
                new Column("current." + childCol).equalTo(new Column("input." + parentCol)),
                "leftouter")
            .select(columns);

//        nextLevel = currentLevel.alias("current")
//            .join(broadcast(input.alias("input")),
//                new Column("current." + childCol1).equalTo(new Column("input." + parentCol1))
//                    .and(new Column("current." + childCol2).equalTo(new Column("input." + parentCol2))),
//                "leftouter")
//            .select(columns);
      } else {
        nextLevel = currentLevel.alias("current")
            .join(input.alias("input"),
                new Column("current." + childCol).equalTo(new Column("input." + parentCol)),
                "leftouter")
            .select(columns);
      }

      if (SHOW_DEBUG_CONTENT) {
        LOG.info("==================================");
        LOG.info("== Content of nextLevel - BEGIN ==");
        LOG.info("==================================");
        for (Iterator<Row> it = nextLevel.javaRDD().toLocalIterator(); it.hasNext(); ) {
          Row line = it.next();
          LOG.info("== " + line);
        }
        LOG.info("==================================");
        LOG.info("== Content of nextLevel - END   ==");
        LOG.info("==================================");
      }

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

      if (SHOW_DEBUG_CONTENT) {
        LOG.info("==============================================");
        LOG.info("== Content of flattened - BEGIN - Level " + level + "==");
        LOG.info("==============================================");
        for (Iterator<Row> it = flattened.javaRDD().toLocalIterator(); it.hasNext(); ) {
          Row line = it.next();
          LOG.info("== " + line);
        }
        LOG.info("==============================================");
        LOG.info("== Content of flattened - END - Level " + level + "==");
        LOG.info("==============================================");
      }

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
       If there are multiple paths from one node to another, they will also be de-duplicated down to just a single
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
    Column[] columns = new Column[inputSchema.getFields().size() + 2 * pathFields.size()
        + 1 * connectByRootFields.size()];
    columns[0] = functions.first(new Column(topCol)).as(topCol);
    columns[1] = functions.when(functions.max(new Column(botCol)).equalTo(0), falseStr)
        .otherwise(trueStr).as(botCol);
    int i = 2;
    for (String fieldName : dataFieldNames) {
      columns[i++] = functions.first(new Column(fieldName)).as(fieldName);
    }

    // Path & Path_length columns
    for (Map<String, String> pathField : pathFields) {
      columns[i++] = functions.first(new Column(pathField.get(HierarchyConfig.PATH_FIELD_ALIAS)))
          .as(pathField.get(HierarchyConfig.PATH_FIELD_ALIAS));
      columns[i++] = functions.first(new Column(pathField.get(HierarchyConfig.PATH_FIELD_LENGTH_ALIAS)))
          .as(pathField.get(HierarchyConfig.PATH_FIELD_LENGTH_ALIAS));
    }

    // CONNECT_BY_ROOT column
    for (Map<String, String> cbrField : connectByRootFields) {
      columns[i++] = functions.first(new Column(cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_ALIAS)))
          .as(cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_ALIAS));
    }

    /****************************
     ** DEBUG - BEGIN - REMOVE **
     ****************************/
    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("=======================");
      LOG.info("== flattened - Begin ==");
      LOG.info("=======================");
      for (Column col : columns) {
        LOG.info("== " + col.toString());
      }
      LOG.info("=====================");
      LOG.info("== flattened - End ==");
      LOG.info("=====================");
    }
    /****************************
     ** DEBUG - END - REMOVE **
     ****************************/

    flattened = flattened.groupBy(new Column(parentCol), new Column(childCol))
        .agg(functions.min(new Column(levelCol)).as(levelCol),
            columns);

    if (SHOW_DEBUG_CONTENT) {
      LOG.info("==============================================");
      LOG.info("== Content of flattened.groupBy - BEGIN ==");
      LOG.info("==============================================");
      for (Iterator<Row> it = flattened.javaRDD().toLocalIterator(); it.hasNext(); ) {
        Row line = it.next();
        LOG.info("== " + line);
      }
      LOG.info("========================================");
      LOG.info("== Content of flattened.groupBy - END ==");
      LOG.info("========================================");
    }

    // Perform a final select to make sure fields are in the same order as expected.
    Column[] finalOutputColumns = outputSchema.getFields().stream()
        .map(Schema.Field::getName)
        .map(Column::new)
        .collect(Collectors.toList()).toArray(new Column[outputSchema.getFields().size()]);

    // TODO: Review the where clauses
    Column parentNotNull = new Column(parentCol).isNotNull();

    Column childWhere, parentWhere, notParentWhere;
    if (childRootValue == null) {
      childWhere = new Column(childCol).isNull();
    } else {
      childWhere = new Column(childCol).equalTo(childRootValue);
    }

    if (parentRootValue == null) {
      parentWhere = new Column(parentCol).isNull();
      notParentWhere = new Column(parentCol).isNotNull();
    } else {
      parentWhere = new Column(parentCol).equalTo(parentRootValue);
      notParentWhere = new Column(parentCol).notEqual(parentRootValue);
    }

    Column childNotParentWhere = new Column(parentCol).notEqual(new Column(childCol));

    /****************************
     ** DEBUG - BEGIN - REMOVE **
     ****************************/
    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("============================");
      LOG.info("== notParentWhere - Begin ==");
      LOG.info("============================");
      LOG.info("== " + notParentWhere.toString());
      LOG.info("==========================");
      LOG.info("== notParentWhere - End ==");
      LOG.info("==========================");
    }
    /****************************
     ** DEBUG - END - REMOVE **
     ****************************/


    flattened = flattened
//        .where((notParentWhere.and(childNotParentWhere))
//            .or(parentWhere.and(childWhere)))
        .where(notParentWhere)
        .select(finalOutputColumns);

//    where (parent is not parentRootValue)
//    or (parent is parentRootValue and child = childRootValue)

    /****************************
     ** DEBUG - BEGIN - REMOVE **
     ****************************/
    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("================================");
      LOG.info("== finalOutputColumns - Begin ==");
      LOG.info("================================");
      for (Column col : columns) {
        LOG.info("== " + col.toString());
      }
      LOG.info("==============================");
      LOG.info("== finalOutputColumns - End ==");
      LOG.info("==============================");
    }
    /****************************
     ** DEBUG - END - REMOVE **
     ****************************/

    if (SHOW_DEBUG_CONTENT) {
      LOG.info("===========================================================");
      LOG.info("== Content of flattened after finalOutputColumns - BEGIN ==");
      LOG.info("===========================================================");
      for (Iterator<Row> it = flattened.javaRDD().toLocalIterator(); it.hasNext(); ) {
        Row line = it.next();
        LOG.info("== " + line);
      }
      LOG.info("=========================================================");
      LOG.info("== Content of flattened after finalOutputColumns - END ==");
      LOG.info("=========================================================");
    }

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

    if (SHOW_DEBUG_CONTENT) {
      LOG.info("==================================");
      LOG.info("== Content of levelZero - BEGIN ==");
      LOG.info("==================================");
      for (Iterator<Row> it = levelZero.javaRDD().toLocalIterator(); it.hasNext(); ) {
        Row line = it.next();
        LOG.info("== " + line);
      }
      LOG.info("==================================");
      LOG.info("== Content of levelZero - END   ==");
      LOG.info("==================================");
    }

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
    // 2 * pathFields.size() to account for 2 extra columns for the path & path length
    // 1 * connectByRootFields.size() to account for 1 extra column for the connect by root field
    Column[] columns = new Column[dataFieldNames.size() + 5 + 2 * pathFields.size()
        + 1 * connectByRootFields.size()];
    columns[0] = input.col(childCol).as(parentCol);
    columns[1] = input.col(childCol).as(childCol);
    columns[2] = functions.lit(0).as(levelCol);
//    Column isRoot = functions.concat(input.col(parentCol), functions.lit("ABCD")).equalTo(input.col(childCol));
    Column isRoot = functions.isnull(input.col(parentCol));
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

    // Path & Path_length columns
    for (Map<String, String> pathField : pathFields) {
      columns[i++] = new Column(pathField.get(HierarchyConfig.VERTEX_FIELD_NAME))
          .as(pathField.get(HierarchyConfig.PATH_FIELD_ALIAS));
      columns[i++] = functions.lit(0).as(pathField.get(HierarchyConfig.PATH_FIELD_LENGTH_ALIAS));
    }

    // CONNECT_BY_ROOT column
    for (Map<String, String> cbrField : connectByRootFields) {
      columns[i++] = new Column(cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_FIELD_NAME))
          .as(cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_ALIAS));
    }

    /****************************
     ** DEBUG - BEGIN - REMOVE **
     ****************************/
    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("===============================");
      LOG.info("== getStartingPoints - Begin ==");
      LOG.info("===============================");
      for (Column col : columns) {
        if (col != null) {
          LOG.info("== " + col);
        }
      }
      LOG.info("=============================");
      LOG.info("== getStartingPoints - End ==");
      LOG.info("=============================");
    }
    /****************************
     ** DEBUG - END - REMOVE **
     ****************************/

    Dataset<Row> distinct = levelZero.union(input.select(columns)).distinct();
    if (SHOW_DEBUG_CONTENT) {
      LOG.info("=================================");
      LOG.info("== Content of distinct - BEGIN ==");
      LOG.info("=================================");
      for (Iterator<Row> it = distinct.javaRDD().toLocalIterator(); it.hasNext(); ) {
        Row line = it.next();
        LOG.info("== " + line);
      }
      LOG.info("=================================");
      LOG.info("== Content of distinct - END   ==");
      LOG.info("=================================");
    }
    return distinct;
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
    // 2 * pathFields.size() to account for 2 extra columns for the path & path length
    // 1 * connectByRootFields.size() to account for 1 extra column for the coonect_by_root
    Column[] columns = new Column[dataFieldNames.size() + 5 + 2 * pathFields.size()
        + 1 * connectByRootFields.size()];
    columns[0] = new Column("A." + parentCol).as(parentCol);
    columns[1] = new Column("A." + childCol).as(childCol);
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

    // Path & Path_length columns
    for (Map<String, String> pathField : pathFields) {
      columns[i++] = new Column("A." + pathField.get(HierarchyConfig.VERTEX_FIELD_NAME))
          .as(pathField.get(HierarchyConfig.PATH_FIELD_ALIAS));
      columns[i++] = functions.lit(0).as(pathField.get(HierarchyConfig.PATH_FIELD_LENGTH_ALIAS));
    }

    // CONNECT_BY_ROOT column
    for (Map<String, String> cbrField : connectByRootFields) {
      columns[i++] = new Column("A." + cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_FIELD_NAME))
          .as(cbrField.get(HierarchyConfig.CONNECT_BY_ROOT_ALIAS));
    }

    /****************************
     ** DEBUG - BEGIN - REMOVE **
     ****************************/
    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("========================================");
      LOG.info("== getNonSelfReferencingRoots - Begin ==");
      LOG.info("========================================");
      for (Column col : columns) {
        if (col != null) {
          LOG.info("==  " + col);
        }
      }
      LOG.info("======================================");
      LOG.info("== getNonSelfReferencingRoots - End ==");
      LOG.info("======================================");
    }
    /****************************
     ** DEBUG -  END  - REMOVE **
     ****************************/

    /*
       we only need childCol from the input and not any of the other fields
       drop all the other fields before the join so that they don't need to be shuffled
       across the cluster, only to be dropped after the join completes.
     */
    Dataset<Row> children = input.select(new Column(childCol));

    /****************************
     ** DEBUG - BEGIN - REMOVE **
     ****************************/
    if (SHOW_DEBUG_CONTENT) {
      LOG.info("==============================");
      LOG.info("== Content of input - BEGIN ==");
      LOG.info("==============================");
      for (Iterator<Row> it = input.javaRDD().toLocalIterator(); it.hasNext(); ) {
        Row line = it.next();
        LOG.info("== " + line);
      }
      LOG.info("============================");
      LOG.info("== Content of input - END ==");
      LOG.info("============================");
    }
    /****************************
     ** DEBUG -  END  - REMOVE **
     ****************************/

    // Build the combined join conditions based on the parent -> child mapping
    Column joiningColumns = null;
    for (String parentName : parentChildMapping.keySet()) {
      String childName = parentChildMapping.get(parentName);
      Column currentCol = new Column("A." + parentName).equalTo(new Column("B." + childName));
      if (joiningColumns == null) {
        joiningColumns = currentCol;
      } else {
        joiningColumns = joiningColumns.and(currentCol);
      }
    }

    /****************************
     ** DEBUG - BEGIN - REMOVE **
     ****************************/
    if (SHOW_DEBUG_COLUMNS) {
      LOG.info("============================");
      LOG.info("== joiningColumns - Begin ==");
      LOG.info("============================");
      if (joiningColumns != null) {
        LOG.info("==  " + joiningColumns);
      }
      LOG.info("==========================");
      LOG.info("== joiningColumns - End ==");
      LOG.info("==========================");
    }
    /****************************
     ** DEBUG -  END  - REMOVE **
     ****************************/

    Dataset<Row> joined;
    if (broadcastJoin) {
      joined = input.alias("A").join(
          broadcast(children.alias("B")),
          new Column("A." + parentCol).equalTo(new Column("B." + childCol)), "leftouter")
          .where(new Column("B." + childCol).isNull())
          .select(columns);
    } else {
      joined = input.alias("A").join(
          children.alias("B"), new Column("A." + parentCol).equalTo(new Column("B." + childCol)), "leftouter")
//      input.alias("B"), new Column("A." + parentCol).equalTo(new Column("B." + childCol)), "leftouter")
          .where(new Column("B." + childCol).isNull())
          .select(columns);
    }

    // Save the value for the root node, we'll use them at the very end to remove duplicates but keep the root node
    Iterator<Row> rows = joined.javaRDD().toLocalIterator();

    for (Iterator<Row> it = rows; it.hasNext(); ) {
      Row row = it.next();
      parentRootValue = row.getAs(parentCol);
      childRootValue = row.getAs(childCol);
    }

    /****************************
     ** DEBUG - BEGIN - REMOVE **
     ****************************/
    if (SHOW_DEBUG_CONTENT) {
      LOG.info("============================");
      LOG.info("== joined content - BEGIN ==");
      LOG.info("============================");
      LOG.info("== parentVal: " + parentRootValue);
      LOG.info("== childRootValue: " + childRootValue);
      LOG.info("==========================");
      LOG.info("== joined content - END ==");
      LOG.info("==========================");
    }
    /****************************
     ** DEBUG -  END  - REMOVE **
     ****************************/

    return joined;
  }

  private boolean isEmpty(Dataset<Row> dataset) {
    // unfortunately there is no way to check if a dataset is empty without running a spark job
    // this, however, is much more performant than performing a count(), which would require a pass over all the data.
    return dataset.takeAsList(1).isEmpty();
  }

}
