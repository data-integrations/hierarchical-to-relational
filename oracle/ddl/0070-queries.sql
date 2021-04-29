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
