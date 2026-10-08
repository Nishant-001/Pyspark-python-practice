# Amazon Redshift SQL Commands — Interview Master List

---

## 1. DDL — Database / Object Definition

### 1.1 Database

```sql
CREATE DATABASE
ALTER DATABASE
DROP DATABASE
```

### 1.2 Schema

```sql
CREATE SCHEMA
ALTER SCHEMA
DROP SCHEMA
```

### 1.3 Tables

```sql
CREATE TABLE
CREATE TABLE AS
ALTER TABLE
ALTER TABLE APPEND
DROP TABLE
TRUNCATE
```

### 1.4 Views

```sql
CREATE VIEW
ALTER VIEW
DROP VIEW
```

### 1.5 Materialized Views

```sql
CREATE MATERIALIZED VIEW
ALTER MATERIALIZED VIEW
DROP MATERIALIZED VIEW
REFRESH MATERIALIZED VIEW
```

### 1.6 External Objects

```sql
CREATE EXTERNAL SCHEMA
ALTER EXTERNAL SCHEMA

CREATE EXTERNAL TABLE

CREATE EXTERNAL VIEW
ALTER EXTERNAL VIEW
DROP EXTERNAL VIEW
```

---

# 2. DML — Data Manipulation

These are the commands you'll use heavily as a Data Engineer.

```sql
SELECT
INSERT
UPDATE
DELETE
MERGE
```

### 2.1 INSERT from Another Table / Query

```sql
INSERT INTO table1
SELECT ...
FROM table2;
```

### 2.2 SELECT INTO

```sql
SELECT *
INTO new_table
FROM old_table;
```

### 2.3 MERGE

Extremely important for ETL / upsert workloads.

```sql
MERGE INTO target
USING source
    ON target.id = source.id

WHEN MATCHED THEN
    UPDATE SET
        column1 = source.column1,
        column2 = source.column2

WHEN NOT MATCHED THEN
    INSERT (id, column1, column2)
    VALUES (source.id, source.column1, source.column2);
```

---

# 3. Data Loading / Unloading

These are extremely important for Redshift interviews.

## 3.1 COPY

### S3 → Redshift

```sql
COPY sales
FROM 's3://my-bucket/sales/'
IAM_ROLE 'arn:aws:iam::123456789012:role/MyRedshiftRole'
FORMAT AS CSV
IGNOREHEADER 1;
```

`COPY` supports various sources, formats, compression types, manifests, IAM authentication methods, and load options.

### Remember

```text
S3 → Redshift
      ↓
     COPY
```

---

## 3.2 UNLOAD

### Redshift → S3

```sql
UNLOAD ('SELECT * FROM sales')
TO 's3://my-bucket/output/'
IAM_ROLE 'arn:aws:iam::123456789012:role/MyRedshiftRole'
FORMAT AS PARQUET;
```

### Remember

```text
Redshift → S3
      ↓
    UNLOAD
```

---

# 4. Query Planning / Optimization

## 4.1 EXPLAIN

```sql
EXPLAIN
SELECT *
FROM sales
WHERE customer_id = 100;
```

Used to inspect the query execution plan.

Useful for identifying:

```text
- Sequential scans
- Join strategies
- Data redistribution
- Broadcast operations
- Sort operations
- Estimated rows
- Estimated cost
```

---

## 4.2 ANALYZE

```sql
ANALYZE sales;
```

Updates table statistics used by the query optimizer.

You can also analyze specific columns:

```sql
ANALYZE sales (customer_id, sale_date);
```

---

## 4.3 ANALYZE COMPRESSION

```sql
ANALYZE COMPRESSION sales;
```

Helps determine suitable compression encodings for table columns.

---

# 5. VACUUM

VACUUM is used for table maintenance, particularly around sort order and deleted rows.

### Commands

```sql
VACUUM
VACUUM FULL
VACUUM SORT ONLY
VACUUM DELETE ONLY
VACUUM REINDEX
VACUUM RECLUSTER
```

### Examples

```sql
VACUUM sales;
```

```sql
VACUUM FULL sales;
```

```sql
VACUUM SORT ONLY sales;
```

```sql
VACUUM DELETE ONLY sales;
```

```sql
VACUUM REINDEX sales;
```

```sql
VACUUM RECLUSTER sales;
```

### Additional options

```sql
VACUUM sales TO 75 PERCENT;
```

```sql
VACUUM RECLUSTER sales BOOST;
```

### Important

Modern Redshift provides automatic table sorting and background maintenance, so manual VACUUM isn't necessarily required for every table.

---

# 6. Transactions

```sql
BEGIN
START TRANSACTION
COMMIT
END
ROLLBACK
ABORT
```

### Example

```sql
BEGIN;

UPDATE sales
SET amount = amount * 1.1
WHERE sale_date = '2026-01-01';

COMMIT;
```

If something goes wrong:

```sql
ROLLBACK;
```

---

# 7. Permissions / Security

## 7.1 GRANT

```sql
GRANT
```

Example:

```sql
GRANT SELECT
ON TABLE sales
TO analyst;
```

---

## 7.2 REVOKE

```sql
REVOKE
```

Example:

```sql
REVOKE SELECT
ON TABLE sales
FROM analyst;
```

---

## 7.3 Users

```sql
CREATE USER
ALTER USER
DROP USER
```

---

## 7.4 Groups

```sql
CREATE GROUP
ALTER GROUP
DROP GROUP
```

---

## 7.5 Roles

```sql
CREATE ROLE
ALTER ROLE
DROP ROLE
```

---

# 8. Stored Procedures / Functions

## 8.1 Stored Procedures

```sql
CREATE PROCEDURE
ALTER PROCEDURE
DROP PROCEDURE
CALL
```

Example:

```sql
CALL process_sales();
```

---

## 8.2 Functions

```sql
CREATE FUNCTION
ALTER FUNCTION
DROP FUNCTION
```

---

# 9. Machine Learning

Redshift supports SQL commands for machine-learning models.

```sql
CREATE MODEL
ALTER MODEL
DROP MODEL
SHOW MODEL
```

### External Model

```sql
CREATE EXTERNAL MODEL
```

---

# 10. Data Sharing

```sql
CREATE DATASHARE
ALTER DATASHARE
DROP DATASHARE
DESC DATASHARE
SHOW DATASHARES
```

---

# 11. Security Policies

## 11.1 Row-Level Security — RLS

```sql
CREATE RLS POLICY
ALTER RLS POLICY
DROP RLS POLICY

ATTACH RLS POLICY
DETACH RLS POLICY
```

---

## 11.2 Dynamic Data Masking

```sql
CREATE MASKING POLICY
ALTER MASKING POLICY
DROP MASKING POLICY

ATTACH MASKING POLICY
DETACH MASKING POLICY
```

---

# 12. Identity Providers

```sql
CREATE IDENTITY PROVIDER
ALTER IDENTITY PROVIDER
DROP IDENTITY PROVIDER
DESC IDENTITY PROVIDER
```

---

# 13. Libraries / External Functions

## 13.1 Libraries

```sql
CREATE LIBRARY
DROP LIBRARY
```

## 13.2 External Functions

```sql
CREATE EXTERNAL FUNCTION
```

---

# 14. Metadata / Information Commands

These are useful when working as a Data Engineer.

```sql
SHOW DATABASES
SHOW SCHEMAS
SHOW TABLES
SHOW TABLE
SHOW COLUMNS
SHOW CONSTRAINTS
SHOW VIEWS
SHOW VIEW
SHOW MATERIALIZED VIEWS
SHOW PROCEDURES
SHOW PROCEDURE
SHOW FUNCTIONS
SHOW GRANTS
SHOW COLUMN GRANTS
SHOW PARAMETERS
SHOW POLICIES
SHOW MODEL
SHOW TEMPLATES
SHOW TEMPLATE
SHOW DATASHARES
SHOW EXTERNAL TABLE
```

### Examples

```sql
SHOW TABLE analytics.sales;
```

```sql
SHOW COLUMNS FROM analytics.sales;
```

---

# 15. Session / Configuration

```sql
SET
RESET
SHOW
```

### Set a configuration

```sql
SET enable_result_cache_for_session TO off;
```

### Check configuration

```sql
SHOW enable_result_cache_for_session;
```

### Reset configuration

```sql
RESET enable_result_cache_for_session;
```

---

# 16. Cursor / Prepared Statements

## 16.1 Cursor

```sql
DECLARE
FETCH
CLOSE
```

## 16.2 Prepared Statements

```sql
PREPARE
EXECUTE
DEALLOCATE
```

### Example

```sql
PREPARE get_sales(INT)
AS
SELECT *
FROM sales
WHERE customer_id = $1;
```

Execute:

```sql
EXECUTE get_sales(100);
```

Then release:

```sql
DEALLOCATE get_sales;
```

---

# 17. Locking

```sql
LOCK
```

Example:

```sql
LOCK TABLE sales;
```

---

# 18. Query Cancellation

```sql
CANCEL
```

Used to cancel a running query/session operation where permitted.

---

# 19. Session Authorization

```sql
SET SESSION AUTHORIZATION
SET SESSION CHARACTERISTICS
```

---

# 20. Redshift SQL Command Cheat Sheet

| Category             | Commands                                                 |
| -------------------- | -------------------------------------------------------- |
| Query                | `SELECT`                                                 |
| Insert               | `INSERT`                                                 |
| Update               | `UPDATE`                                                 |
| Delete               | `DELETE`                                                 |
| Upsert               | `MERGE`                                                  |
| Create table         | `CREATE TABLE`                                           |
| CTAS                 | `CREATE TABLE AS`                                        |
| Alter table          | `ALTER TABLE`                                            |
| Append               | `ALTER TABLE APPEND`                                     |
| Delete table         | `DROP TABLE`                                             |
| Empty table          | `TRUNCATE`                                               |
| Views                | `CREATE / ALTER / DROP VIEW`                             |
| Materialized views   | `CREATE / ALTER / DROP / REFRESH MATERIALIZED VIEW`      |
| Load                 | `COPY`                                                   |
| Export               | `UNLOAD`                                                 |
| Query plan           | `EXPLAIN`                                                |
| Statistics           | `ANALYZE`                                                |
| Compression analysis | `ANALYZE COMPRESSION`                                    |
| Maintenance          | `VACUUM`                                                 |
| Transactions         | `BEGIN / COMMIT / ROLLBACK / ABORT`                      |
| Permissions          | `GRANT / REVOKE`                                         |
| Users                | `CREATE / ALTER / DROP USER`                             |
| Roles                | `CREATE / ALTER / DROP ROLE`                             |
| Procedures           | `CREATE / ALTER / DROP PROCEDURE`, `CALL`                |
| Functions            | `CREATE / ALTER / DROP FUNCTION`                         |
| Database             | `CREATE / ALTER / DROP DATABASE`                         |
| Schema               | `CREATE / ALTER / DROP SCHEMA`                           |
| Data sharing         | `CREATE / ALTER / DROP / DESC DATASHARE`                 |
| RLS                  | `CREATE / ALTER / DROP / ATTACH / DETACH RLS POLICY`     |
| Masking              | `CREATE / ALTER / DROP / ATTACH / DETACH MASKING POLICY` |
| Configuration        | `SET / RESET / SHOW`                                     |
| Metadata             | `SHOW ...`                                               |
| Cursor               | `DECLARE / FETCH / CLOSE`                                |
| Prepared SQL         | `PREPARE / EXECUTE / DEALLOCATE`                         |
| Locking              | `LOCK`                                                   |
| Cancellation         | `CANCEL`                                                 |

---

# 21. SQL Commands vs RSQL Commands

This distinction is important.

## 21.1 Redshift SQL

These are SQL statements:

```sql
SELECT *
FROM sales;
```

```sql
CREATE TABLE sales (...);
```

```sql
COPY sales
FROM 's3://bucket/sales/';
```

---

## 21.2 RSQL Meta-Commands

RSQL commands start with `\`.

```text
\d
\dt
\du
\dn
\dv
\df
\l
\q
\i
\o
\timing
\?
```

For example:

```text
\dt
```

lists tables in RSQL.

These are **RSQL client commands**, not Redshift SQL statements.

---

# 22. What to Prioritize for Data Engineer Interviews

You don't need to memorize every command equally.

### Priority 1 — SQL Fundamentals

```text
1.  SELECT
2.  JOIN
3.  GROUP BY
4.  HAVING
5.  CASE
6.  CTE
7.  Subqueries
8.  Window Functions
```

### Priority 2 — Redshift Table Design

```text
9.  CREATE TABLE
10. DISTSTYLE
11. DISTKEY
12. SORTKEY
13. COMPOUND SORTKEY
14. INTERLEAVED SORTKEY
15. ENCODE
16. ENCODE AUTO
```

### Priority 3 — Data Modification

```text
17. INSERT
18. UPDATE
19. DELETE
20. MERGE
21. TRUNCATE
```

### Priority 4 — Data Engineering

```text
22. COPY
23. UNLOAD
24. CREATE TABLE AS
25. ALTER TABLE
26. ALTER TABLE APPEND
```

### Priority 5 — Performance

```text
27. EXPLAIN
28. ANALYZE
29. ANALYZE COMPRESSION
30. VACUUM
```

### Priority 6 — Database Objects

```text
31. CREATE VIEW
32. CREATE MATERIALIZED VIEW
33. REFRESH MATERIALIZED VIEW
34. External Tables / Spectrum
```

### Priority 7 — Security

```text
35. GRANT
36. REVOKE
37. CREATE ROLE
38. RLS
39. Dynamic Data Masking
```

### Priority 8 — Advanced Redshift

```text
40. Stored Procedures
41. Data Sharing
42. External Functions
43. Redshift ML
44. Transactions
45. Session Configuration
```

---

# 23. The Redshift Topics You Should Be Able to Explain

For your interview, don't just memorize the commands. Be able to explain the relationship:

```text
                         REDSHIFT
                            │
          ┌─────────────────┼──────────────────┐
          │                 │                  │
      Data Loading      Table Design       Query Performance
          │                 │                  │
       COPY              DISTKEY             EXPLAIN
       UNLOAD            DISTSTYLE           ANALYZE
                         SORTKEY             VACUUM
                         ENCODE
          │                 │                  │
          └─────────────────┼──────────────────┘
                            │
                       ETL / ELT
                            │
              ┌─────────────┼─────────────┐
              │             │             │
           INSERT         MERGE         DELETE
              │             │             │
              └─────────────┼─────────────┘
                            │
                       Optimization
                            │
                    EXPLAIN + ANALYZE
                            │
                 DIST/SORT/ENCODE/VACUUM
```

**The highest-value Redshift interview combination to master is:**

```text
COPY
  ↓
STAGING TABLE
  ↓
MERGE / INSERT
  ↓
DISTKEY + SORTKEY + ENCODE
  ↓
EXPLAIN
  ↓
ANALYZE
  ↓
VACUUM (when required)
  ↓
OPTIMIZED QUERY
```

This gives you the complete **data ingestion → storage design → transformation → optimization** story that interviewers commonly look for in a Redshift-focused Data Engineer role.
