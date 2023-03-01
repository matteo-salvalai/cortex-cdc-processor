--  Copyright 2022 Google LLC
--
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--      https://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.

WITH
  -- We need to dedupe the source table to handle occasional dups from SLT connector.
  SourceTable AS (
    SELECT * EXCEPT(row_num)
    FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY ${keys}, __timestamp ORDER BY __timestamp) AS row_num
      FROM `${base_table}`
    )
    WHERE row_num = 1
  ),
  T1 AS (
    SELECT ${keys}, MAX(__timestamp) AS __timestamp
    FROM SourceTable
    -- Let's make sure we bring records with NULL __operation_type values as well.
    WHERE IFNULL(__operation_type, 'I') IN ('U', 'I', 'L')
    GROUP BY ${keys}
  ),
  D1 AS (
    SELECT ${keys_with_dt1_prefix}, DT1.__timestamp
    FROM SourceTable AS DT1
    CROSS JOIN T1
    WHERE DT1.__operation_type IN ('D', 'X') 
      AND ${keys_comparator_with_dt1_t1}
      AND DT1.__timestamp > T1.__timestamp
  ),
  T1S1 AS (
    SELECT S1.* EXCEPT (__operation_type, is_deleted)
    FROM SourceTable AS S1
    INNER JOIN T1
    ON ${keys_comparator_with_t1_s1}
      AND S1.__timestamp = T1.__timestamp
  )
SELECT T1S1.* EXCEPT (__timestamp)
FROM T1S1
LEFT OUTER JOIN D1
  ON ${keys_comparator_with_t1s1_d1}
    AND D1.__timestamp > T1S1.__timestamp
WHERE D1.__timestamp IS NULL

