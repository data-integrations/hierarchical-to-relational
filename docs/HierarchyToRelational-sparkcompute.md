# Hierarchical to Relational Plugin

Description
----------- 
Provides the ability to flatten a hierarchical data model into a relational model.

Use Case
--------
This plugin can be used when user needs to Flatten a hierarchical data model to a relational model.

Properties
----------
**Parent field:** Specifies the field from the input schema that should be used as the parent in the hierarchical model.
Should always contain a single, non-null root element in the hierarchy

**Child field:** Specifies the field from the input schema that should be used as the child in the hierarchical model.

**Parent -> Child fields mapping:** Specifies parent child field mapping for fields that require swapping parent 
fields with tree/branch root fields.

**Level Field Name:** The name of the field that should contain the level in the hierarchy starting at a particular node
in the tree. The level is calculated as a distance of a node to a particular parent node in the tree. Default to `Level`.

**Top Field Name:** The name of the field that determines whether a node is the root element or the top-most element in
the hierarchy. The input data should always contain a single non-null root node. For that node, this field is true,
while it is marked false for all other nodes in the hierarchy. Defaults to `Top`.

**Bottom Field Name:** The name of the field that determines whether a node is a leaf element or the bottom-most element
in the hierarchy. The input data can contain multiple leaf nodes. Defaults to `Bottom`.

**True value:** The value that denotes truth in the Top and Bottom fields. Defaults to `Y`.

**False value:** The value that denotes false in the Top and Bottom fields. Defaults to `N`.

**Max depth:** The maximum depth upto which the data should be flattened. If a node is reached at a deeper level, 
an error will be thrown. Defaults to `50`.

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

**Parent -> Child fields mapping**: `ParentProduct=ChildProduct`

**Level Field Name**: `Level`

**Top Field Name**: `Root`

**Bottom Field Name**: `Leaf`

**True value**: `Yes`

**False value**: `No`

Plugin will generate the following output:

|ParentId|ChildId|ParentProduct|ChildProduct|Supplier|Sales|Level|Root|Leaf|
|--------|-------|-------------|------------|--------|-----|-----|--------|-----------|
|1|1|Groceries|Groceries|null|null|0|Yes|No|
|1|2|Groceries|Produce|A|50|1|No|No|
|1|3|Groceries|Dairy|B|40|1|No|Yes|
|1|4|Groceries|Vegetables|C|50|2|No|No|
|1|6|Groceries|Onion|E|30|3|No|Yes|
|2|2|Produce|Produce|A|50|0|No|No|
|2|4|Produce|Vegetables|C|50|1|No|No|
|2|6|Produce|Onion|E|30|2|No|Yes|
|3|3|Dairy|Dairy|B|40|0|No|Yes|
|4|4|Vegetables|Vegetables|C|50|0|No|No|
|4|4|Vegetables|Onion|E|30|1|No|Yes|
|6|6|Onion|Onion|E|30|0|No|Yes|

The dataset below is similar with the previous example with the only change of having root record
where root record has a self link dataset (record with identical ParentId and ChildId).

|ParentId|ChildId|ParentProduct|ChildProduct|Supplier|Sales|
|--------|-------|-------------|------------|--------|-----|
|1|1|Groceries|Groceries|A|0|
|1|2|Groceries|Produce|A|50|
|1|3|Groceries|Dairy|B|40|
|2|4|Produce|Vegetables|C|50|
|4|6|Vegetables|Onion|E|30|

With the same configuration as in previous example the plugin will generate the same output with
exception on root record where values are carried from input data instead of being generated:

|ParentId|ChildId|ParentProduct|ChildProduct|Supplier|Sales|Level|Root|Leaf|
|--------|-------|-------------|------------|--------|-----|-----|--------|-----------|
|1|1|Groceries|Groceries|A|0|0|Yes|No|
|1|2|Groceries|Produce|A|50|1|No|No|
|1|3|Groceries|Dairy|B|40|1|No|Yes|
|1|4|Groceries|Vegetables|C|50|2|No|No|
|1|6|Groceries|Onion|E|30|3|No|Yes|
|2|2|Produce|Produce|A|50|0|No|No|
|2|4|Produce|Vegetables|C|50|1|No|No|
|2|6|Produce|Onion|E|30|2|No|Yes|
|3|3|Dairy|Dairy|B|40|0|No|Yes|
|4|4|Vegetables|Vegetables|C|50|0|No|No|
|4|4|Vegetables|Onion|E|30|1|No|Yes|
|6|6|Onion|Onion|E|30|0|No|Yes|