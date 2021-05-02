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

SELECT  empno,
        MGR,
        ename,
        PRIOR ename AS Manager,
        sys_connect_by_path(ename,'/') as full_path,
        LEVEL - 1
from    CDAP.EMPLOYEES
start   with mgr IS NULL
connect by prior empno = mgr;

------------------------------------------------------
SELECT  EMPNO,
        MGR AS MGR_ID,
        ENAME,
        PRIOR ENAME AS direct_manager,
        CONNECT_BY_ROOT ENAME AS MANAGER,
        SYS_CONNECT_BY_PATH(ENAME,'/') AS PATH,
        LEVEL AS PATH_LEN
FROM    CDAP.EMPLOYEES
START WITH EMPNO IN (SELECT EMPNO FROM CDAP.EMPLOYEES)
CONNECT BY PRIOR EMPNO = mgr
ORDER BY 1;

-- Results with NULL values:
"EMPNO","MGR_ID","ENAME","DIRECT_MANAGER","MANAGER","PATH","PATH_LEN"
7369,7902,SMITH,FORD,FORD,/FORD/SMITH,2
7369,7902,SMITH,FORD,KING,/KING/JONES/FORD/SMITH,4
7369,7902,SMITH,,SMITH,/SMITH,1
7369,7902,SMITH,FORD,JONES,/JONES/FORD/SMITH,3
7499,7698,ALLEN,,ALLEN,/ALLEN,1
7499,7698,ALLEN,BLAKE,KING,/KING/BLAKE/ALLEN,3
7499,7698,ALLEN,BLAKE,BLAKE,/BLAKE/ALLEN,2
7521,7698,WARD,,WARD,/WARD,1
7521,7698,WARD,BLAKE,KING,/KING/BLAKE/WARD,3
7521,7698,WARD,BLAKE,BLAKE,/BLAKE/WARD,2
7566,7839,JONES,,JONES,/JONES,1
7566,7839,JONES,KING,KING,/KING/JONES,2
7654,7698,MARTIN,,MARTIN,/MARTIN,1
7654,7698,MARTIN,BLAKE,KING,/KING/BLAKE/MARTIN,3
7654,7698,MARTIN,BLAKE,BLAKE,/BLAKE/MARTIN,2
7698,7839,BLAKE,,BLAKE,/BLAKE,1
7698,7839,BLAKE,KING,KING,/KING/BLAKE,2
7782,7839,CLARK,,CLARK,/CLARK,1
7782,7839,CLARK,KING,KING,/KING/CLARK,2
7788,7566,SCOTT,,SCOTT,/SCOTT,1
7788,7566,SCOTT,JONES,KING,/KING/JONES/SCOTT,3
7788,7566,SCOTT,JONES,JONES,/JONES/SCOTT,2
7839,,KING,,KING,/KING,1
7844,7698,TURNER,,TURNER,/TURNER,1
7844,7698,TURNER,BLAKE,KING,/KING/BLAKE/TURNER,3
7844,7698,TURNER,BLAKE,BLAKE,/BLAKE/TURNER,2
7876,7788,ADAMS,SCOTT,SCOTT,/SCOTT/ADAMS,2
7876,7788,ADAMS,SCOTT,KING,/KING/JONES/SCOTT/ADAMS,4
7876,7788,ADAMS,SCOTT,JONES,/JONES/SCOTT/ADAMS,3
7876,7788,ADAMS,,ADAMS,/ADAMS,1
7900,7698,JAMES,,JAMES,/JAMES,1
7900,7698,JAMES,BLAKE,KING,/KING/BLAKE/JAMES,3
7900,7698,JAMES,BLAKE,BLAKE,/BLAKE/JAMES,2
7902,7566,FORD,,FORD,/FORD,1
7902,7566,FORD,JONES,KING,/KING/JONES/FORD,3
7902,7566,FORD,JONES,JONES,/JONES/FORD,2
7934,7782,MILLER,,MILLER,/MILLER,1
7934,7782,MILLER,CLARK,CLARK,/CLARK/MILLER,2
7934,7782,MILLER,CLARK,KING,/KING/CLARK/MILLER,3

------------------------------------------------------

SELECT  EMPNO, MGR_ID, ENAME, MANAGER, PATH, PATH_LEN
FROM (
SELECT  EMPNO,
        MGR AS MGR_ID,
        ENAME,
        PRIOR ENAME AS direct_manager,
        CONNECT_BY_ROOT ENAME AS MANAGER,
        SYS_CONNECT_BY_PATH(ENAME,'/') AS PATH,
        LEVEL AS PATH_LEN
FROM    CDAP.EMPLOYEES
START WITH EMPNO IN (SELECT EMPNO FROM CDAP.EMPLOYEES)
CONNECT BY PRIOR EMPNO = mgr)
WHERE direct_manager IS NOT NULL
ORDER BY 1
;

-- Results:
"EMPNO","MGR_ID","ENAME","MANAGER","PATH","PATH_LEN"
7369,7902,SMITH,FORD,/FORD/SMITH,2
7369,7902,SMITH,KING,/KING/JONES/FORD/SMITH,4
7369,7902,SMITH,JONES,/JONES/FORD/SMITH,3
7499,7698,ALLEN,BLAKE,/BLAKE/ALLEN,2
7499,7698,ALLEN,KING,/KING/BLAKE/ALLEN,3
7521,7698,WARD,BLAKE,/BLAKE/WARD,2
7521,7698,WARD,KING,/KING/BLAKE/WARD,3
7566,7839,JONES,KING,/KING/JONES,2
7654,7698,MARTIN,BLAKE,/BLAKE/MARTIN,2
7654,7698,MARTIN,KING,/KING/BLAKE/MARTIN,3
7698,7839,BLAKE,KING,/KING/BLAKE,2
7782,7839,CLARK,KING,/KING/CLARK,2
7788,7566,SCOTT,JONES,/JONES/SCOTT,2
7788,7566,SCOTT,KING,/KING/JONES/SCOTT,3
7844,7698,TURNER,BLAKE,/BLAKE/TURNER,2
7844,7698,TURNER,KING,/KING/BLAKE/TURNER,3
7876,7788,ADAMS,SCOTT,/SCOTT/ADAMS,2
7876,7788,ADAMS,KING,/KING/JONES/SCOTT/ADAMS,4
7876,7788,ADAMS,JONES,/JONES/SCOTT/ADAMS,3
7900,7698,JAMES,BLAKE,/BLAKE/JAMES,2
7900,7698,JAMES,KING,/KING/BLAKE/JAMES,3
7902,7566,FORD,JONES,/JONES/FORD,2
7902,7566,FORD,KING,/KING/JONES/FORD,3
7934,7782,MILLER,CLARK,/CLARK/MILLER,2
7934,7782,MILLER,KING,/KING/CLARK/MILLER,3

------------------------------------------------------
SELECT  CHILDID,
        PARENTID,
        CHILDPRODUCT,
        PRIOR CHILDPRODUCT AS parent_product,
        CONNECT_BY_ROOT CHILDPRODUCT AS top_product,
        SYS_CONNECT_BY_PATH(CHILDPRODUCT,'/') AS PATH,
        LEVEL AS PATH_LEN
FROM    CDAP.GROCERIES g
START WITH CHILDID IN (SELECT CHILDID FROM CDAP.GROCERIES)
CONNECT BY PRIOR CHILDID = PARENTID
ORDER BY 1;

-- Results - With NULL values:
"CHILDID","PARENTID","CHILDPRODUCT","PARENT_PRODUCT","TOP_PRODUCT","PATH","PATH_LEN"
2,1,Produce,,Produce,/Produce,1
3,1,Dairy,,Dairy,/Dairy,1
4,2,Vegetables,Produce,Produce,/Produce/Vegetables,2
4,2,Vegetables,,Vegetables,/Vegetables,1
5,2,Fruits,Produce,Produce,/Produce/Fruits,2
5,2,Fruits,,Fruits,/Fruits,1
6,4,Onion,Vegetables,Produce,/Produce/Vegetables/Onion,3
6,4,Onion,,Onion,/Onion,1
6,4,Onion,Vegetables,Vegetables,/Vegetables/Onion,2
7,4,Zucchini,Vegetables,Produce,/Produce/Vegetables/Zucchini,3
7,4,Zucchini,,Zucchini,/Zucchini,1
7,4,Zucchini,Vegetables,Vegetables,/Vegetables/Zucchini,2
8,5,Oranges,Fruits,Produce,/Produce/Fruits/Oranges,3
8,5,Oranges,,Oranges,/Oranges,1
8,5,Oranges,Fruits,Fruits,/Fruits/Oranges,2
9,5,Bananas,Fruits,Produce,/Produce/Fruits/Bananas,3
9,5,Bananas,,Bananas,/Bananas,1
9,5,Bananas,Fruits,Fruits,/Fruits/Bananas,2
10,8,Navel Oranges,Oranges,Produce,/Produce/Fruits/Oranges/Navel Oranges,4
10,8,Navel Oranges,,Navel Oranges,/Navel Oranges,1
10,8,Navel Oranges,Oranges,Oranges,/Oranges/Navel Oranges,2
10,8,Navel Oranges,Oranges,Fruits,/Fruits/Oranges/Navel Oranges,3
11,3,Milk,Dairy,Dairy,/Dairy/Milk,2
11,3,Milk,,Milk,/Milk,1
12,3,Yogurt,Dairy,Dairy,/Dairy/Yogurt,2
12,3,Yogurt,,Yogurt,/Yogurt,1
13,11,Chocolate Milk,Milk,Dairy,/Dairy/Milk/Chocolate Milk,3
13,11,Chocolate Milk,,Chocolate Milk,/Chocolate Milk,1
13,11,Chocolate Milk,Milk,Milk,/Milk/Chocolate Milk,2
14,11,Almond Milk,Milk,Dairy,/Dairy/Milk/Almond Milk,3
14,11,Almond Milk,Milk,Milk,/Milk/Almond Milk,2
14,11,Almond Milk,,Almond Milk,/Almond Milk,1

------------------------------------------------------

SELECT *
FROM (
SELECT  CHILDID,
        PARENTID,
        CHILDPRODUCT,
        PRIOR CHILDPRODUCT AS parent_product,
        CONNECT_BY_ROOT CHILDPRODUCT AS top_product,
        SYS_CONNECT_BY_PATH(CHILDPRODUCT,'/') AS PATH,
        LEVEL AS PATH_LEN
FROM    CDAP.GROCERIES g
START WITH CHILDID IN (SELECT CHILDID FROM CDAP.GROCERIES)
CONNECT BY PRIOR CHILDID = PARENTID)
WHERE PARENT_PRODUCT IS NOT NULL
ORDER BY 1;

-- Results:
"CHILDID","PARENTID","CHILDPRODUCT","PARENT_PRODUCT","TOP_PRODUCT","PATH","PATH_LEN"
4,2,Vegetables,Produce,Produce,/Produce/Vegetables,2
5,2,Fruits,Produce,Produce,/Produce/Fruits,2
6,4,Onion,Vegetables,Produce,/Produce/Vegetables/Onion,3
6,4,Onion,Vegetables,Vegetables,/Vegetables/Onion,2
7,4,Zucchini,Vegetables,Produce,/Produce/Vegetables/Zucchini,3
7,4,Zucchini,Vegetables,Vegetables,/Vegetables/Zucchini,2
8,5,Oranges,Fruits,Produce,/Produce/Fruits/Oranges,3
8,5,Oranges,Fruits,Fruits,/Fruits/Oranges,2
9,5,Bananas,Fruits,Produce,/Produce/Fruits/Bananas,3
9,5,Bananas,Fruits,Fruits,/Fruits/Bananas,2
10,8,Navel Oranges,Oranges,Produce,/Produce/Fruits/Oranges/Navel Oranges,4
10,8,Navel Oranges,Oranges,Oranges,/Oranges/Navel Oranges,2
10,8,Navel Oranges,Oranges,Fruits,/Fruits/Oranges/Navel Oranges,3
11,3,Milk,Dairy,Dairy,/Dairy/Milk,2
12,3,Yogurt,Dairy,Dairy,/Dairy/Yogurt,2
13,11,Chocolate Milk,Milk,Dairy,/Dairy/Milk/Chocolate Milk,3
13,11,Chocolate Milk,Milk,Milk,/Milk/Chocolate Milk,2
14,11,Almond Milk,Milk,Dairy,/Dairy/Milk/Almond Milk,3
14,11,Almond Milk,Milk,Milk,/Milk/Almond Milk,2

------------------------------------------------------

-- Run the following script to create the plan_table
-- @$ORACLE_HOME/rdbms/admin/utlxplan.sql

explain plan
SET STATEMENT_ID = 'all'
FOR
SELECT EMPNO, MGR_ID, ENAME, MANAGER, PATH, PATH_LEN
FROM (
SELECT  EMPNO,
        MGR AS MGR_ID,
        ENAME,
        PRIOR ENAME AS direct_manager,
        CONNECT_BY_ROOT ENAME AS MANAGER,
        SYS_CONNECT_BY_PATH(ENAME,'/') AS PATH,
        LEVEL - 1 AS PATH_LEN
FROM    CDAP.EMPLOYEES
START WITH EMPNO IN (SELECT EMPNO FROM CDAP.EMPLOYEES)
CONNECT BY PRIOR EMPNO = mgr)
WHERE direct_manager IS NOT NULL
--ORDER BY 4
;

TRUNCATE TABLE PLAN_TABLE ;

SELECT cardinality "Rows",
       lpad(' ',level-1)||operation||' '||
       options||' '||object_name "Plan"
  FROM PLAN_TABLE
CONNECT BY prior id = parent_id
        AND prior statement_id = statement_id
  START WITH id = 0
        AND statement_id = 'all'
  ORDER BY id;

