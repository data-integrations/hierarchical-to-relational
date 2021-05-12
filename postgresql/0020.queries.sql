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

select *
from employees e ;

/*
=================================
== Content of input - BEGIN ==
=================================
* [7839,NONE,KING,1]
* [7566,7839,JONES,2]
* [7788,7566,SCOTT,3]
* [7876,7788,ADAMS,4]
* [7902,7566,FORD,3]
* [7369,7902,SMITH,4]
* [7698,7839,BLAKE,2]
* [7499,7698,ALLEN,3]
* [7521,7698,WARD,3]
* [7654,7698,MARTIN,3]
* [7844,7698,TURNER,3]
* [7900,7698,JAMES,3]
* [7782,7839,CLARK,2]
* [7934,7782,MILLER,3]
*/

/*
 * ==================================
 * == Content of levelZero - BEGIN ==
 * ==================================
 * [NONE,7839,0,true,0,null,null,KING,0]
 */ 

/*
=================================
== Content of distinct - BEGIN ==
=================================
* [7844,7844,0,false,0,TURNER,3,TURNER,0]
* [7839,7839,0,false,0,KING,1,KING,0]
* [7902,7902,0,false,0,FORD,3,FORD,0]
* [7566,7566,0,false,0,JONES,2,JONES,0]
* [7782,7782,0,false,0,CLARK,2,CLARK,0]
* [7654,7654,0,false,0,MARTIN,3,MARTIN,0]
* [7788,7788,0,false,0,SCOTT,3,SCOTT,0]
* [7521,7521,0,false,0,WARD,3,WARD,0]
* [7876,7876,0,false,0,ADAMS,4,ADAMS,0]
* [7900,7900,0,false,0,JAMES,3,JAMES,0]
* [NONE,7839,0,true,0,null,null,KING,0]
* [7369,7369,0,false,0,SMITH,4,SMITH,0]
* [7698,7698,0,false,0,BLAKE,2,BLAKE,0]
* [7499,7499,0,false,0,ALLEN,3,ALLEN,0]
* [7934,7934,0,false,0,MILLER,3,MILLER,0]
*/
 
/*
==================================
== Content of nextLevel - BEGIN ==
==================================
* [7369,7369,1,false,1,SMITH,4,null,1]
* [7499,7499,1,false,1,ALLEN,3,null,1]
* [7521,7521,1,false,1,WARD,3,null,1]
* [7566,7788,1,false,0,SCOTT,3,JONES/SCOTT,1]
* [7566,7902,1,false,0,FORD,3,JONES/FORD,1]
* [7654,7654,1,false,1,MARTIN,3,null,1]
* [7698,7499,1,false,0,ALLEN,3,BLAKE/ALLEN,1]
* [7698,7521,1,false,0,WARD,3,BLAKE/WARD,1]
* [7698,7654,1,false,0,MARTIN,3,BLAKE/MARTIN,1]
* [7698,7844,1,false,0,TURNER,3,BLAKE/TURNER,1]
* [7698,7900,1,false,0,JAMES,3,BLAKE/JAMES,1]
* [7782,7934,1,false,0,MILLER,3,CLARK/MILLER,1]
* [7788,7876,1,false,0,ADAMS,4,SCOTT/ADAMS,1]
* [7839,7566,1,false,0,JONES,2,KING/JONES,1]
* [7839,7698,1,false,0,BLAKE,2,KING/BLAKE,1]
* [7839,7782,1,false,0,CLARK,2,KING/CLARK,1]
* [7844,7844,1,false,1,TURNER,3,null,1]
* [7876,7876,1,false,1,ADAMS,4,null,1]
* [7900,7900,1,false,1,JAMES,3,null,1]
* [7902,7369,1,false,0,SMITH,4,FORD/SMITH,1]
* [7934,7934,1,false,1,MILLER,3,null,1]
* [NONE,7566,1,false,0,JONES,2,KING/JONES,1]
* [NONE,7698,1,false,0,BLAKE,2,KING/BLAKE,1]
* [NONE,7782,1,false,0,CLARK,2,KING/CLARK,1]
*/

/*
==============================================
== Content of flattened - BEGIN - Level 0==
==============================================
* [7844,7844,0,false,0,TURNER,3,TURNER,0]
* [7839,7839,0,false,0,KING,1,KING,0]
* [7902,7902,0,false,0,FORD,3,FORD,0]
* [7566,7566,0,false,0,JONES,2,JONES,0]
* [7782,7782,0,false,0,CLARK,2,CLARK,0]
* [7654,7654,0,false,0,MARTIN,3,MARTIN,0]
* [7788,7788,0,false,0,SCOTT,3,SCOTT,0]
* [7521,7521,0,false,0,WARD,3,WARD,0]
* [7876,7876,0,false,0,ADAMS,4,ADAMS,0]
* [7900,7900,0,false,0,JAMES,3,JAMES,0]
* [NONE,7839,0,true,0,null,null,KING,0]
* [7369,7369,0,false,0,SMITH,4,SMITH,0]
* [7698,7698,0,false,0,BLAKE,2,BLAKE,0]
* [7499,7499,0,false,0,ALLEN,3,ALLEN,0]
* [7934,7934,0,false,0,MILLER,3,MILLER,0]
* [7844,7844,1,false,1,TURNER,3,null,1]
* [7839,7782,1,false,0,CLARK,2,KING/CLARK,1]
* [7839,7698,1,false,0,BLAKE,2,KING/BLAKE,1]
* [7839,7566,1,false,0,JONES,2,KING/JONES,1]
* [7902,7369,1,false,0,SMITH,4,FORD/SMITH,1]
* [7566,7902,1,false,0,FORD,3,JONES/FORD,1]
* [7566,7788,1,false,0,SCOTT,3,JONES/SCOTT,1]
* [7782,7934,1,false,0,MILLER,3,CLARK/MILLER,1]
* [7654,7654,1,false,1,MARTIN,3,null,1]
* [7788,7876,1,false,0,ADAMS,4,SCOTT/ADAMS,1]
* [7521,7521,1,false,1,WARD,3,null,1]
* [7876,7876,1,false,1,ADAMS,4,null,1]
* [7900,7900,1,false,1,JAMES,3,null,1]
* [NONE,7782,1,false,0,CLARK,2,KING/CLARK,1]
* [NONE,7698,1,false,0,BLAKE,2,KING/BLAKE,1]
* [NONE,7566,1,false,0,JONES,2,KING/JONES,1]
* [7369,7369,1,false,1,SMITH,4,null,1]
* [7698,7900,1,false,0,JAMES,3,BLAKE/JAMES,1]
* [7698,7844,1,false,0,TURNER,3,BLAKE/TURNER,1]
* [7698,7654,1,false,0,MARTIN,3,BLAKE/MARTIN,1]
* [7698,7521,1,false,0,WARD,3,BLAKE/WARD,1]
* [7698,7499,1,false,0,ALLEN,3,BLAKE/ALLEN,1]
* [7499,7499,1,false,1,ALLEN,3,null,1]
* [7934,7934,1,false,1,MILLER,3,null,1]
*/

/*
==================================
== Content of nextLevel - BEGIN ==
==================================
* [7839,7934,2,false,0,MILLER,3,KING/CLARK/MILLER,2]
* [7839,7900,2,false,0,JAMES,3,KING/BLAKE/JAMES,2]
* [7839,7844,2,false,0,TURNER,3,KING/BLAKE/TURNER,2]
* [7839,7654,2,false,0,MARTIN,3,KING/BLAKE/MARTIN,2]
* [7839,7521,2,false,0,WARD,3,KING/BLAKE/WARD,2]
* [7839,7499,2,false,0,ALLEN,3,KING/BLAKE/ALLEN,2]
* [7839,7902,2,false,0,FORD,3,KING/JONES/FORD,2]
* [7839,7788,2,false,0,SCOTT,3,KING/JONES/SCOTT,2]
* [7902,7369,2,false,1,SMITH,4,null,2]
* [7566,7369,2,false,0,SMITH,4,JONES/FORD/SMITH,2]
* [7566,7876,2,false,0,ADAMS,4,JONES/SCOTT/ADAMS,2]
* [7782,7934,2,false,1,MILLER,3,null,2]
* [7788,7876,2,false,1,ADAMS,4,null,2]
* [NONE,7934,2,false,0,MILLER,3,KING/CLARK/MILLER,2]
* [NONE,7900,2,false,0,JAMES,3,KING/BLAKE/JAMES,2]
* [NONE,7844,2,false,0,TURNER,3,KING/BLAKE/TURNER,2]
* [NONE,7654,2,false,0,MARTIN,3,KING/BLAKE/MARTIN,2]
* [NONE,7521,2,false,0,WARD,3,KING/BLAKE/WARD,2]
* [NONE,7499,2,false,0,ALLEN,3,KING/BLAKE/ALLEN,2]
* [NONE,7902,2,false,0,FORD,3,KING/JONES/FORD,2]
* [NONE,7788,2,false,0,SCOTT,3,KING/JONES/SCOTT,2]
* [7698,7900,2,false,1,JAMES,3,null,2]
* [7698,7844,2,false,1,TURNER,3,null,2]
* [7698,7654,2,false,1,MARTIN,3,null,2]
* [7698,7521,2,false,1,WARD,3,null,2]
* [7698,7499,2,false,1,ALLEN,3,null,2]
*/

/*
==============================================
== Content of flattened - BEGIN - Level 1==
==============================================
* [7844,7844,0,false,0,TURNER,3,TURNER,0]
* [7839,7839,0,false,0,KING,1,KING,0]
* [7902,7902,0,false,0,FORD,3,FORD,0]
* [7566,7566,0,false,0,JONES,2,JONES,0]
* [7782,7782,0,false,0,CLARK,2,CLARK,0]
* [7654,7654,0,false,0,MARTIN,3,MARTIN,0]
* [7788,7788,0,false,0,SCOTT,3,SCOTT,0]
* [7521,7521,0,false,0,WARD,3,WARD,0]
* [7876,7876,0,false,0,ADAMS,4,ADAMS,0]
* [7900,7900,0,false,0,JAMES,3,JAMES,0]
* [NONE,7839,0,true,0,null,null,KING,0]
* [7369,7369,0,false,0,SMITH,4,SMITH,0]
* [7698,7698,0,false,0,BLAKE,2,BLAKE,0]
* [7499,7499,0,false,0,ALLEN,3,ALLEN,0]
* [7934,7934,0,false,0,MILLER,3,MILLER,0]
* [7844,7844,1,false,1,TURNER,3,null,1]
* [7839,7782,1,false,0,CLARK,2,KING/CLARK,1]
* [7839,7698,1,false,0,BLAKE,2,KING/BLAKE,1]
* [7839,7566,1,false,0,JONES,2,KING/JONES,1]
* [7902,7369,1,false,0,SMITH,4,FORD/SMITH,1]
* [7566,7902,1,false,0,FORD,3,JONES/FORD,1]
* [7566,7788,1,false,0,SCOTT,3,JONES/SCOTT,1]
* [7782,7934,1,false,0,MILLER,3,CLARK/MILLER,1]
* [7654,7654,1,false,1,MARTIN,3,null,1]
* [7788,7876,1,false,0,ADAMS,4,SCOTT/ADAMS,1]
* [7521,7521,1,false,1,WARD,3,null,1]
* [7876,7876,1,false,1,ADAMS,4,null,1]
* [7900,7900,1,false,1,JAMES,3,null,1]
* [NONE,7782,1,false,0,CLARK,2,KING/CLARK,1]
* [NONE,7698,1,false,0,BLAKE,2,KING/BLAKE,1]
* [NONE,7566,1,false,0,JONES,2,KING/JONES,1]
* [7369,7369,1,false,1,SMITH,4,null,1]
* [7698,7900,1,false,0,JAMES,3,BLAKE/JAMES,1]
* [7698,7844,1,false,0,TURNER,3,BLAKE/TURNER,1]
* [7698,7654,1,false,0,MARTIN,3,BLAKE/MARTIN,1]
* [7698,7521,1,false,0,WARD,3,BLAKE/WARD,1]
* [7698,7499,1,false,0,ALLEN,3,BLAKE/ALLEN,1]
* [7499,7499,1,false,1,ALLEN,3,null,1]
* [7934,7934,1,false,1,MILLER,3,null,1]
* [7839,7934,2,false,0,MILLER,3,KING/CLARK/MILLER,2]
* [7839,7900,2,false,0,JAMES,3,KING/BLAKE/JAMES,2]
* [7839,7844,2,false,0,TURNER,3,KING/BLAKE/TURNER,2]
* [7839,7654,2,false,0,MARTIN,3,KING/BLAKE/MARTIN,2]
* [7839,7521,2,false,0,WARD,3,KING/BLAKE/WARD,2]
* [7839,7499,2,false,0,ALLEN,3,KING/BLAKE/ALLEN,2]
* [7839,7902,2,false,0,FORD,3,KING/JONES/FORD,2]
* [7839,7788,2,false,0,SCOTT,3,KING/JONES/SCOTT,2]
* [7902,7369,2,false,1,SMITH,4,null,2]
* [7566,7369,2,false,0,SMITH,4,JONES/FORD/SMITH,2]
* [7566,7876,2,false,0,ADAMS,4,JONES/SCOTT/ADAMS,2]
* [7782,7934,2,false,1,MILLER,3,null,2]
* [7788,7876,2,false,1,ADAMS,4,null,2]
* [NONE,7934,2,false,0,MILLER,3,KING/CLARK/MILLER,2]
* [NONE,7900,2,false,0,JAMES,3,KING/BLAKE/JAMES,2]
* [NONE,7844,2,false,0,TURNER,3,KING/BLAKE/TURNER,2]
* [NONE,7654,2,false,0,MARTIN,3,KING/BLAKE/MARTIN,2]
* [NONE,7521,2,false,0,WARD,3,KING/BLAKE/WARD,2]
* [NONE,7499,2,false,0,ALLEN,3,KING/BLAKE/ALLEN,2]
* [NONE,7902,2,false,0,FORD,3,KING/JONES/FORD,2]
* [NONE,7788,2,false,0,SCOTT,3,KING/JONES/SCOTT,2]
* [7698,7900,2,false,1,JAMES,3,null,2]
* [7698,7844,2,false,1,TURNER,3,null,2]
* [7698,7654,2,false,1,MARTIN,3,null,2]
* [7698,7521,2,false,1,WARD,3,null,2]
* [7698,7499,2,false,1,ALLEN,3,null,2]
*/

/*
==================================
== Content of nextLevel - BEGIN ==
==================================
* [7839,7934,3,false,1,MILLER,3,null,3]
* [7839,7900,3,false,1,JAMES,3,null,3]
* [7839,7844,3,false,1,TURNER,3,null,3]
* [7839,7654,3,false,1,MARTIN,3,null,3]
* [7839,7521,3,false,1,WARD,3,null,3]
* [7839,7499,3,false,1,ALLEN,3,null,3]
* [7839,7369,3,false,0,SMITH,4,KING/JONES/FORD/SMITH,3]
* [7839,7876,3,false,0,ADAMS,4,KING/JONES/SCOTT/ADAMS,3]
* [7566,7369,3,false,1,SMITH,4,null,3]
* [7566,7876,3,false,1,ADAMS,4,null,3]
* [NONE,7934,3,false,1,MILLER,3,null,3]
* [NONE,7900,3,false,1,JAMES,3,null,3]
* [NONE,7844,3,false,1,TURNER,3,null,3]
* [NONE,7654,3,false,1,MARTIN,3,null,3]
* [NONE,7521,3,false,1,WARD,3,null,3]
* [NONE,7499,3,false,1,ALLEN,3,null,3]
* [NONE,7369,3,false,0,SMITH,4,KING/JONES/FORD/SMITH,3]
* [NONE,7876,3,false,0,ADAMS,4,KING/JONES/SCOTT/ADAMS,3]
*/

/*
==============================================
== Content of flattened - BEGIN - Level 2==
==============================================
* [7844,7844,0,false,0,TURNER,3,TURNER,0]
* [7839,7839,0,false,0,KING,1,KING,0]
* [7902,7902,0,false,0,FORD,3,FORD,0]
* [7566,7566,0,false,0,JONES,2,JONES,0]
* [7782,7782,0,false,0,CLARK,2,CLARK,0]
* [7654,7654,0,false,0,MARTIN,3,MARTIN,0]
* [7788,7788,0,false,0,SCOTT,3,SCOTT,0]
* [7521,7521,0,false,0,WARD,3,WARD,0]
* [7876,7876,0,false,0,ADAMS,4,ADAMS,0]
* [7900,7900,0,false,0,JAMES,3,JAMES,0]
* [NONE,7839,0,true,0,null,null,KING,0]
* [7369,7369,0,false,0,SMITH,4,SMITH,0]
* [7698,7698,0,false,0,BLAKE,2,BLAKE,0]
* [7499,7499,0,false,0,ALLEN,3,ALLEN,0]
* [7934,7934,0,false,0,MILLER,3,MILLER,0]
* [7844,7844,1,false,1,TURNER,3,null,1]
* [7839,7782,1,false,0,CLARK,2,KING/CLARK,1]
* [7839,7698,1,false,0,BLAKE,2,KING/BLAKE,1]
* [7839,7566,1,false,0,JONES,2,KING/JONES,1]
* [7902,7369,1,false,0,SMITH,4,FORD/SMITH,1]
* [7566,7902,1,false,0,FORD,3,JONES/FORD,1]
* [7566,7788,1,false,0,SCOTT,3,JONES/SCOTT,1]
* [7782,7934,1,false,0,MILLER,3,CLARK/MILLER,1]
* [7654,7654,1,false,1,MARTIN,3,null,1]
* [7788,7876,1,false,0,ADAMS,4,SCOTT/ADAMS,1]
* [7521,7521,1,false,1,WARD,3,null,1]
* [7876,7876,1,false,1,ADAMS,4,null,1]
* [7900,7900,1,false,1,JAMES,3,null,1]
* [NONE,7782,1,false,0,CLARK,2,KING/CLARK,1]
* [NONE,7698,1,false,0,BLAKE,2,KING/BLAKE,1]
* [NONE,7566,1,false,0,JONES,2,KING/JONES,1]
* [7369,7369,1,false,1,SMITH,4,null,1]
* [7698,7900,1,false,0,JAMES,3,BLAKE/JAMES,1]
* [7698,7844,1,false,0,TURNER,3,BLAKE/TURNER,1]
* [7698,7654,1,false,0,MARTIN,3,BLAKE/MARTIN,1]
* [7698,7521,1,false,0,WARD,3,BLAKE/WARD,1]
* [7698,7499,1,false,0,ALLEN,3,BLAKE/ALLEN,1]
* [7499,7499,1,false,1,ALLEN,3,null,1]
* [7934,7934,1,false,1,MILLER,3,null,1]
* [7839,7934,2,false,0,MILLER,3,KING/CLARK/MILLER,2]
* [7839,7900,2,false,0,JAMES,3,KING/BLAKE/JAMES,2]
* [7839,7844,2,false,0,TURNER,3,KING/BLAKE/TURNER,2]
* [7839,7654,2,false,0,MARTIN,3,KING/BLAKE/MARTIN,2]
* [7839,7521,2,false,0,WARD,3,KING/BLAKE/WARD,2]
* [7839,7499,2,false,0,ALLEN,3,KING/BLAKE/ALLEN,2]
* [7839,7902,2,false,0,FORD,3,KING/JONES/FORD,2]
* [7839,7788,2,false,0,SCOTT,3,KING/JONES/SCOTT,2]
* [7902,7369,2,false,1,SMITH,4,null,2]
* [7566,7369,2,false,0,SMITH,4,JONES/FORD/SMITH,2]
* [7566,7876,2,false,0,ADAMS,4,JONES/SCOTT/ADAMS,2]
* [7782,7934,2,false,1,MILLER,3,null,2]
* [7788,7876,2,false,1,ADAMS,4,null,2]
* [NONE,7934,2,false,0,MILLER,3,KING/CLARK/MILLER,2]
* [NONE,7900,2,false,0,JAMES,3,KING/BLAKE/JAMES,2]
* [NONE,7844,2,false,0,TURNER,3,KING/BLAKE/TURNER,2]
* [NONE,7654,2,false,0,MARTIN,3,KING/BLAKE/MARTIN,2]
* [NONE,7521,2,false,0,WARD,3,KING/BLAKE/WARD,2]
* [NONE,7499,2,false,0,ALLEN,3,KING/BLAKE/ALLEN,2]
* [NONE,7902,2,false,0,FORD,3,KING/JONES/FORD,2]
* [NONE,7788,2,false,0,SCOTT,3,KING/JONES/SCOTT,2]
* [7698,7900,2,false,1,JAMES,3,null,2]
* [7698,7844,2,false,1,TURNER,3,null,2]
* [7698,7654,2,false,1,MARTIN,3,null,2]
* [7698,7521,2,false,1,WARD,3,null,2]
* [7698,7499,2,false,1,ALLEN,3,null,2]
* [7839,7934,3,false,1,MILLER,3,null,3]
* [7839,7900,3,false,1,JAMES,3,null,3]
* [7839,7844,3,false,1,TURNER,3,null,3]
* [7839,7654,3,false,1,MARTIN,3,null,3]
* [7839,7521,3,false,1,WARD,3,null,3]
* [7839,7499,3,false,1,ALLEN,3,null,3]
* [7839,7369,3,false,0,SMITH,4,KING/JONES/FORD/SMITH,3]
* [7839,7876,3,false,0,ADAMS,4,KING/JONES/SCOTT/ADAMS,3]
* [7566,7369,3,false,1,SMITH,4,null,3]
* [7566,7876,3,false,1,ADAMS,4,null,3]
* [NONE,7934,3,false,1,MILLER,3,null,3]
* [NONE,7900,3,false,1,JAMES,3,null,3]
* [NONE,7844,3,false,1,TURNER,3,null,3]
* [NONE,7654,3,false,1,MARTIN,3,null,3]
* [NONE,7521,3,false,1,WARD,3,null,3]
* [NONE,7499,3,false,1,ALLEN,3,null,3]
* [NONE,7369,3,false,0,SMITH,4,KING/JONES/FORD/SMITH,3]
* [NONE,7876,3,false,0,ADAMS,4,KING/JONES/SCOTT/ADAMS,3]
*/
select *
from employees e1
join employees e2 on e1.mgr = e2.empno ;

/*
* [7844,7844,0,false,0,TURNER,3,TURNER,0]
* [7839,7839,0,false,0,KING,1,KING,0]
* [7902,7902,0,false,0,FORD,3,FORD,0]
* [7566,7566,0,false,0,JONES,2,JONES,0]
* [7782,7782,0,false,0,CLARK,2,CLARK,0]
* [7654,7654,0,false,0,MARTIN,3,MARTIN,0]
* [7788,7788,0,false,0,SCOTT,3,SCOTT,0]
* [7521,7521,0,false,0,WARD,3,WARD,0]
* [NONE,7839,0,true,0,null,null,KING,0]
* [7369,7369,0,false,0,SMITH,4,SMITH,0]
* [7698,7698,0,false,0,BLAKE,2,BLAKE,0]
* [7499,7499,0,false,0,ALLEN,3,ALLEN,0]
* [7934,7934,0,false,0,MILLER,3,MILLER,0]
*/

