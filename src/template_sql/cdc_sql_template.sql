--  Copyright 2021 Google Inc.

--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at

--      http://www.apache.org/licenses/LICENSE-2.0

--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.

MERGE `${target_table}` AS T
USING (
  WITH
    S0 AS (
      SELECT * FROM `${base_table}`
      WHERE __timestamp >= (
        SELECT IFNULL(MAX(__timestamp), TIMESTAMP('1940-12-25 05:30:00+00'))
        FROM `${target_table}`)
    ),
    -- To handle occasional dups from SLT connector
    S1 AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ${keys}, __timestamp ORDER BY __timestamp) AS row_num
        FROM S0
      )
      WHERE row_num = 1
    ),
    T1 AS (
      SELECT ${keys}, MAX(__timestamp) AS __timestamp
      FROM `${base_table}`
      WHERE __timestamp >= (
        SELECT IFNULL(MAX(__timestamp), TIMESTAMP('1940-12-25 05:30:00+00'))
        FROM `${target_table}`)
      GROUP BY ${keys}
    )
  SELECT S1.*
  FROM S1
  INNER JOIN T1
    ON ${p_key_sub_query}
      AND S1.__timestamp = T1.__timestamp
  ) AS S
ON ${p_key}
-- ## CORTEX-CUSTOMER You can use "`is_deleted` = true" condition along with "__operation_type = 'D'",
-- if that is applicable to your CDC set up.
WHEN NOT MATCHED AND IFNULL(S.__operation_type, 'I') NOT IN ('D', 'X') THEN
  INSERT (${fields})
  VALUES (${fields})
WHEN MATCHED AND S.__operation_type IN ('D', 'X') THEN
  DELETE
WHEN MATCHED AND S.__operation_type = 'U' THEN
  UPDATE SET ${update_fields};

