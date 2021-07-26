# Hierarchical to Relational Plugin

Description
----------- 
Provides the ability to flatten a hierarchical data model into a relational model.

Use Case
--------
This plugin can be used when user needs to Flatten a hierarchical data model to a relational model.
It expects incoming records to represent a direct parent-child relationship from one element to another.
The input data can contain multiple root elements, but must not contain any cycles. Self referencing nodes
(parent = child) will be ignored.
The plugin will flatten the hierarchy such that there is an output record from each element to itself and
to all of its descendents. Each output record will include the distance from the element to its descendent.

Properties
----------
**Parent field:** Specifies the field from the input schema that should be used as the parent in the hierarchical model.
Should always contain a single, non-null root element in the hierarchy

**Child field:** Specifies the field from the input schema that should be used as the child in the hierarchical model.

**Level Field Name:** The name of the field that should contain the level in the hierarchy starting at a particular
element in the tree. The level is the distance from the parent to the child. If there are multiple paths from
a parent to a child, only a single record is output, where the level is set to the minimum level of all paths. 

**Bottom Field Name:** The name of the field that determines whether a node is a leaf element or the bottom-most element
in the hierarchy. The input data can contain multiple leaf nodes. This will be true whenever the child is a leaf element,
even if the parent is not a leaf.

**Max depth:** The maximum depth upto which the data should be flattened. If a node is reached at a deeper level, 
an error will be thrown.

**Start With:** Defines a SQL condition to identify the root elements in the hierarchy.
ex: `ParentId == 2 OR ParentId == 4`

**Path Field Name:** Specifies the field from the input schema that should be used to calculate the path between nodes
in the flattened hierarchy.  The path is calculated by concatenating the path field values, separated by the
Path Separator.

**Path Alias Field Name:** The name of the output field that contains the path between nodes in the flattened
hierarchy.

**Path Separator:** Specifies the string that separates nodes in the path.

**Connect By Root:** Specifies a root field name in the input schema and the corresponding field in the output schema
that contains the root node in the path.

Example
-------
Consider the dataset below depicting sales (numbers) of specific products in every quarter for a grocery store.

|ParentId|ChildId|ParentProduct|ChildProduct|Supplier|Sales|
|--------|-------|-------------|------------|--------|-----|
|1|2|Groceries|Produce|A|50|
|1|3|Groceries|Dairy|B|40|
|2|4|Produce|Vegetables|C|50|
|4|6|Vegetables|Onion|E|30|

With the following configuration:

**Parent field**: `ParentId`

**Child field**: `ChildId`

**Level Field Name**: `Level`

**Bottom Field Name**: `Leaf`

**Connect By Root**: `ParentProduct=Root`

**Path Field Name**: `ParentProduct`

**Path Alias Field Name**: `Parent`

the following output is generated:

|ParentId|ChildId|ParentProduct|ChildProduct|Supplier|Sales|Level|Leaf|Root|Path|
|--------|-------|-------------|------------|--------|-----|-----|----|----|----|
|1|2|Groceries|Produce|A|50|1|No|Groceries|/Groceries|
|1|3|Groceries|Dairy|B|40|1|Yes|Groceries|/Groceries|
|2|4|Produce|Vegetables|C|50|1|No|Produce|/Produce|
|4|6|Vegetables|Onion|E|30|1|Yes|Vegetables|/Vegetables|
|2|4|Produce|Vegetables|C|50|2|No|Groceries|/Groceries/Produce|
|4|6|Produce|Onion|E|30|2|Yes|Produce|/Produce/Vegetables|
|4|6|Produce|Onion|E|30|3|Yes|Groceries|/Groceries/Produce/Vegetables|
