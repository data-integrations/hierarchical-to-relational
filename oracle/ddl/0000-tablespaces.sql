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

CONNECT SYS@XE AS SYSDBA;

ALTER SESSION SET "_ORACLE_SCRIPT"=true;

CREATE TABLESPACE CDAP_DATA
   DATAFILE 'CDAP_DATA_00.dbf'
   SIZE 10m;
   
CREATE TABLESPACE CDAP_IDX
   DATAFILE 'CDAP_IDX_00.dbf'
   SIZE 10m;
