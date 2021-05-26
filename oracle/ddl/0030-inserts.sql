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

CONNECT CDAP@XE;

INSERT INTO GROCERIES (ParentId, ChildId, ParentProduct, ChildProduct, Supplier, Sales)
VALUES (1,2,'Groceries','Produce','A',50);

INSERT INTO GROCERIES (ParentId, ChildId, ParentProduct, ChildProduct, Supplier, Sales)
VALUES (1,3,'Groceries','Dairy','B',40);

INSERT INTO GROCERIES (ParentId, ChildId, ParentProduct, ChildProduct, Supplier, Sales)
VALUES (2,4,'Produce','Vegetables','C',50);

INSERT INTO GROCERIES (ParentId, ChildId, ParentProduct, ChildProduct, Supplier, Sales)
VALUES (2,5,'Produce','Fruits','D',20);

INSERT INTO GROCERIES (ParentId, ChildId, ParentProduct, ChildProduct, Supplier, Sales)
VALUES (4,6,'Vegetables','Onion','E',30);

INSERT INTO GROCERIES (ParentId, ChildId, ParentProduct, ChildProduct, Supplier, Sales)
VALUES (4,7,'Vegetables','Zucchini','F',40);

INSERT INTO GROCERIES (ParentId, ChildId, ParentProduct, ChildProduct, Supplier, Sales)
VALUES (5,8,'Fruits','Oranges','G',30);

INSERT INTO GROCERIES (ParentId, ChildId, ParentProduct, ChildProduct, Supplier, Sales)
VALUES (5,9,'Fruits','Bananas','H',40);

INSERT INTO GROCERIES (ParentId, ChildId, ParentProduct, ChildProduct, Supplier, Sales)
VALUES (8,10,'Oranges','Navel Oranges','I',20);

INSERT INTO GROCERIES (ParentId, ChildId, ParentProduct, ChildProduct, Supplier, Sales)
VALUES (3,11,'Dairy','Milk','J',40);

INSERT INTO GROCERIES (ParentId, ChildId, ParentProduct, ChildProduct, Supplier, Sales)
VALUES (3,12,'Dairy','Yogurt','K',50);

INSERT INTO GROCERIES (ParentId, ChildId, ParentProduct, ChildProduct, Supplier, Sales)
VALUES (11,13,'Milk','Chocolate Milk','L',60);

INSERT INTO GROCERIES (ParentId, ChildId, ParentProduct, ChildProduct, Supplier, Sales)
VALUES (11,14,'Milk','Almond Milk','M',10);

COMMIT;
