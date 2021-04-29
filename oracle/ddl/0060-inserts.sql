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

CONNECT CDAP@ORCL;

INSERT INTO CDAP.EMPLOYEES (EMPNO, MGR, ENAME, LVL)
VALUES (7839, NULL, 'KING', 1);

INSERT INTO CDAP.EMPLOYEES (EMPNO, MGR, ENAME, LVL)
VALUES (7566, 7839, 'JONES', 2);

INSERT INTO CDAP.EMPLOYEES (EMPNO, MGR, ENAME, LVL)
VALUES (7788, 7566, 'SCOTT', 3);

INSERT INTO CDAP.EMPLOYEES (EMPNO, MGR, ENAME, LVL)
VALUES (7876, 7788, 'ADAMS', 4);

INSERT INTO CDAP.EMPLOYEES (EMPNO, MGR, ENAME, LVL)
VALUES (7902, 7566, 'FORD', 3);

INSERT INTO CDAP.EMPLOYEES (EMPNO, MGR, ENAME, LVL)
VALUES (7369, 7902, 'SMITH', 4);

INSERT INTO CDAP.EMPLOYEES (EMPNO, MGR, ENAME, LVL)
VALUES (7698, 7839, 'BLAKE', 2);

INSERT INTO CDAP.EMPLOYEES (EMPNO, MGR, ENAME, LVL)
VALUES (7499, 7698, 'ALLEN', 3);

INSERT INTO CDAP.EMPLOYEES (EMPNO, MGR, ENAME, LVL)
VALUES (7521, 7698, 'WARD', 3);

INSERT INTO CDAP.EMPLOYEES (EMPNO, MGR, ENAME, LVL)
VALUES (7654, 7698, 'MARTIN', 3);

INSERT INTO CDAP.EMPLOYEES (EMPNO, MGR, ENAME, LVL)
VALUES (7844, 7698, 'TURNER', 3);

INSERT INTO CDAP.EMPLOYEES (EMPNO, MGR, ENAME, LVL)
VALUES (7900, 7698, 'JAMES', 3);

INSERT INTO CDAP.EMPLOYEES (EMPNO, MGR, ENAME, LVL)
VALUES (7782, 7839, 'CLARK', 2);

INSERT INTO CDAP.EMPLOYEES (EMPNO, MGR, ENAME, LVL)
VALUES (7934, 7782, 'MILLER', 3);

COMMIT;
