CREATE TABLE IF NOT EXISTS analytics.sales (
    sale_id         BIGINT  NOT NULL                    PRIMARY KEY ENCODE AZ64,
    customer_id     BIGINT  NOT NULL                    ENCODE AZ64,
    product_id      INTEGER                             ENCODE AZ64,
    sale_date       DATE    NOT NULL                    ENCODE AZ64,
    sale_timestamp  TIMESTAMP       DEFAULT GETDATE()   ENCODE AZ64,
    quantity        INTEGER NOT NULL    DEFAULT 1 CHECK (quantity > 0) ENCODE AZ64,
    amount          DECIMAL(18, 2) CHECK (amount >= 0) ENCODE AZ64,
    status          VARCHAR(30) DEFAULT 'NEW' CHECK (status IN ('NEW', 'COMPLETED', 'CANCELLED')) ENCODE ZSTD,
    description     VARCHAR(1000) ENCODE ZSTD,
    is_active       BOOLEAN DEFAULT TRUE ENCODE RAW,

    CONSTRAINT fk_sales_customer FOREIGN KEY (customer_id) REFERENCES analytics.customers(customer_id),
    CONSTRAINT uq_customer_product_date UNIQUE (customer_id, product_id, sale_date),
    CONSTRAINT chk_sale_dates CHECK (sale_date <= CURRENT_DATE)
) DISTSTYLE KEY DISTKEY (customer_id) COMPOUND SORTKEY (sale_date, customer_id) BACKUP YES;


CREATE TABLE IF NOT EXISTS analytics.sales 
(
    sale_id          BIGINT          NOT NULL ENCODE AZ64,
    customer_id      BIGINT          NOT NULL ENCODE AZ64,
    product_id       INTEGER                  ENCODE AZ64,
    sale_date        DATE            NOT NULL ENCODE AZ64,
    sale_timestamp   TIMESTAMP       DEFAULT GETDATE() ENCODE AZ64,
    quantity         INTEGER         DEFAULT 1 ENCODE AZ64,
    amount           DECIMAL(18,2)            ENCODE AZ64,
    status           VARCHAR(30)              ENCODE ZSTD,
    description      VARCHAR(1000)            ENCODE ZSTD,
    is_active        BOOLEAN         DEFAULT TRUE ENCODE RAW,

    CONSTRAINT pk_sales 
        PRIMARY KEY (sale_id),

    CONSTRAINT fk_customer 
        FOREIGN KEY (customer_id) 
        REFERENCES analytics.customers(customer_id),

    CONSTRAINT chk_quantity 
        CHECK (quantity > 0)
)
DISTSTYLE KEY
DISTKEY (customer_id)
COMPOUND SORTKEY (sale_date, customer_id)
BACKUP YES;


/* 
    In this demo, we will do hands-on related with designing table structure in amazon redshift
*/

select node,slice from stv_slices order by 1,2;

CREATE SCHEMA sch_sales_part2;

drop table if exists sch_sales_part2.orders_default;
CREATE TABLE sch_sales_part2.orders_default (
    order_id       INT ,
    customer_id    INT ,
    order_date     DATE ,
    order_amount   DECIMAL(10,2) ,
    region         VARCHAR(50)
);

show table sch_sales_part2.orders_default;

CREATE TABLE sch_sales_part2.orders_default (
    order_id integer ENCODE az64,
    customer_id integer ENCODE az64,
    order_date date ENCODE az64,
    order_amount numeric(10,2) ENCODE az64,
    region character varying(50) ENCODE lzo
)
DISTSTYLE AUTO;

INSERT INTO sch_sales_part2.orders_default (order_id, customer_id, order_date, order_amount, region) VALUES
(1, 3, '2023-01-15', 250.75, 'North'),
(2, 5, '2023-02-20', 312.10, 'East'),
(3, 2, '2023-03-05', 123.45, 'South'),
(4, 8, '2023-03-22', 456.78, 'West'),
(5, 1, '2023-04-10', 89.99, 'North'),
(6, 6, '2023-04-25', 500.00, 'East'),
(7, 7, '2023-05-15', 275.30, 'South'),
(8, 4, '2023-05-30', 190.20, 'West'),
(9, 10, '2023-06-01', 330.00, 'North'),
(10, 9, '2023-06-10', 210.90, 'East'),
(11, 3, '2023-06-25', 411.55, 'South'),
(12, 12, '2023-07-05', 132.99, 'West'),
(13, 15, '2023-07-18', 98.99, 'North'),
(14, 16, '2023-08-01', 310.75, 'East'),
(15, 5, '2023-08-15', 165.65, 'South'),
(16, 13, '2023-08-20', 472.10, 'West'),
(17, 14, '2023-09-10', 289.00, 'North'),
(18, 11, '2023-09-25', 390.20, 'East'),
(19, 18, '2023-10-05', 459.90, 'South'),
(20, 17, '2023-10-20', 321.75, 'West'),
(21, 4, '2023-11-01', 410.10, 'North'),
(22, 6, '2023-11-10', 111.11, 'East'),
(23, 7, '2023-11-18', 222.22, 'South'),
(24, 3, '2023-11-25', 333.33, 'West'),
(25, 9, '2023-12-01', 444.44, 'North'),
(26, 20, '2023-12-10', 555.55, 'East'),
(27, 2, '2023-12-15', 160.40, 'South'),
(28, 5, '2024-01-05', 201.90, 'West'),
(29, 8, '2024-01-10', 187.50, 'North'),
(30, 1, '2024-01-20', 298.75, 'East'),
(31, 13, '2024-02-01', 368.20, 'South'),
(32, 14, '2024-02-15', 212.10, 'West'),
(33, 10, '2024-03-01', 478.90, 'North'),
(34, 6, '2024-03-18', 140.60, 'East'),
(35, 11, '2024-04-01', 332.10, 'South'),
(36, 19, '2024-04-10', 125.75, 'West'),
(37, 12, '2024-04-15', 456.30, 'North'),
(38, 17, '2024-05-01', 199.99, 'East'),
(39, 18, '2024-05-10', 310.00, 'South'),
(40, 16, '2024-05-25', 278.80, 'West'),
(41, 20, '2024-06-01', 180.10, 'North'),
(42, 15, '2024-06-10', 289.70, 'East'),
(43, 2, '2024-06-20', 315.25, 'South'),
(44, 3, '2024-07-01', 260.55, 'West'),
(45, 9, '2024-07-10', 350.80, 'North'),
(46, 4, '2024-07-20', 410.30, 'East'),
(47, 7, '2024-08-01', 120.00, 'South'),
(48, 8, '2024-08-10', 250.90, 'West'),
(49, 1, '2024-08-20', 330.40, 'North'),
(50, 5, '2024-09-01', 380.60, 'East');


ANALYZE COMPRESSION sch_sales_part2.orders_default;

drop table if exists sch_sales_part2.orders_raw;
CREATE TABLE sch_sales_part2.orders_raw (
    order_id       INT ENCODE raw,
    customer_id    INT ENCODE raw,
    order_date     DATE ENCODE raw,
    order_amount   DECIMAL(10,2) ENCODE raw,
    region         VARCHAR(50) ENCODE raw
);

INSERT INTO sch_sales_part2.orders_raw (order_id, customer_id, order_date, order_amount, region) VALUES
(1, 3, '2023-01-15', 250.75, 'North'),
(2, 5, '2023-02-20', 312.10, 'East'),
(3, 2, '2023-03-05', 123.45, 'South'),
(4, 8, '2023-03-22', 456.78, 'West'),
(5, 1, '2023-04-10', 89.99, 'North'),
(6, 6, '2023-04-25', 500.00, 'East'),
(7, 7, '2023-05-15', 275.30, 'South'),
(8, 4, '2023-05-30', 190.20, 'West'),
(9, 10, '2023-06-01', 330.00, 'North'),
(10, 9, '2023-06-10', 210.90, 'East'),
(11, 3, '2023-06-25', 411.55, 'South'),
(12, 12, '2023-07-05', 132.99, 'West'),
(13, 15, '2023-07-18', 98.99, 'North'),
(14, 16, '2023-08-01', 310.75, 'East'),
(15, 5, '2023-08-15', 165.65, 'South'),
(16, 13, '2023-08-20', 472.10, 'West'),
(17, 14, '2023-09-10', 289.00, 'North'),
(18, 11, '2023-09-25', 390.20, 'East'),
(19, 18, '2023-10-05', 459.90, 'South'),
(20, 17, '2023-10-20', 321.75, 'West'),
(21, 4, '2023-11-01', 410.10, 'North'),
(22, 6, '2023-11-10', 111.11, 'East'),
(23, 7, '2023-11-18', 222.22, 'South'),
(24, 3, '2023-11-25', 333.33, 'West'),
(25, 9, '2023-12-01', 444.44, 'North'),
(26, 20, '2023-12-10', 555.55, 'East'),
(27, 2, '2023-12-15', 160.40, 'South'),
(28, 5, '2024-01-05', 201.90, 'West'),
(29, 8, '2024-01-10', 187.50, 'North'),
(30, 1, '2024-01-20', 298.75, 'East'),
(31, 13, '2024-02-01', 368.20, 'South'),
(32, 14, '2024-02-15', 212.10, 'West'),
(33, 10, '2024-03-01', 478.90, 'North'),
(34, 6, '2024-03-18', 140.60, 'East'),
(35, 11, '2024-04-01', 332.10, 'South'),
(36, 19, '2024-04-10', 125.75, 'West'),
(37, 12, '2024-04-15', 456.30, 'North'),
(38, 17, '2024-05-01', 199.99, 'East'),
(39, 18, '2024-05-10', 310.00, 'South'),
(40, 16, '2024-05-25', 278.80, 'West'),
(41, 20, '2024-06-01', 180.10, 'North'),
(42, 15, '2024-06-10', 289.70, 'East'),
(43, 2, '2024-06-20', 315.25, 'South'),
(44, 3, '2024-07-01', 260.55, 'West'),
(45, 9, '2024-07-10', 350.80, 'North'),
(46, 4, '2024-07-20', 410.30, 'East'),
(47, 7, '2024-08-01', 120.00, 'South'),
(48, 8, '2024-08-10', 250.90, 'West'),
(49, 1, '2024-08-20', 330.40, 'North'),
(50, 5, '2024-09-01', 380.60, 'East');

ANALYZE COMPRESSION sch_sales_part2.orders_raw;

VACUUM FULL sch_sales_part2.orders_raw;
ANALYZE sch_sales_part2.orders_raw;

ANALYZE COMPRESSION sch_sales_part2.orders_raw;

drop table if exists sch_sales_part2.orders_raw_10k;
CREATE TABLE sch_sales_part2.orders_raw_10k (
    order_id       INT ENCODE raw,
    customer_id    INT ENCODE raw,
    order_date     DATE ENCODE raw,
    order_amount   DECIMAL(30,2) ENCODE raw,
    region         VARCHAR(50) ENCODE raw
);

COPY sch_sales_part2.orders_raw_10k
FROM 's3://<s3_bucket>/orders_10000.csv'
IAM_ROLE '<ARN>'
FORMAT AS CSV
IGNOREHEADER 1;

select * from STL_LOAD_ERRORS;

 -- change to 30,2
ANALYZE COMPRESSION sch_sales_part2.orders_raw_10k;

-- add keys :: distkey, sortkey , primary key , encoding
CREATE TABLE sch_sales_part2.orders_optimized (
    order_id INT encode AZ64 PRIMARY KEY,
    customer_id INT encode AZ64,
    order_date DATE encode AZ64,
    order_amount DECIMAL(10,2) encode AZ64,
    region VARCHAR(50) encode lzo
)
DISTSTYLE KEY
DISTKEY(customer_id)
SORTKEY(order_date)
;

INSERT INTO sch_sales_part2.orders_optimized (order_id, customer_id, order_date, order_amount, region) VALUES
(1, 3, '2023-01-15', 250.75, 'North'),
(2, 5, '2023-02-20', 312.10, 'East'),
(3, 2, '2023-03-05', 123.45, 'South'),
(4, 8, '2023-03-22', 456.78, 'West'),
(5, 1, '2023-04-10', 89.99, 'North'),
(6, 6, '2023-04-25', 500.00, 'East'),
(7, 7, '2023-05-15', 275.30, 'South'),
(8, 4, '2023-05-30', 190.20, 'West'),
(9, 10, '2023-06-01', 330.00, 'North'),
(10, 9, '2023-06-10', 210.90, 'East'),
(11, 3, '2023-06-25', 411.55, 'South'),
(12, 12, '2023-07-05', 132.99, 'West'),
(13, 15, '2023-07-18', 98.99, 'North'),
(14, 16, '2023-08-01', 310.75, 'East'),
(15, 5, '2023-08-15', 165.65, 'South'),
(16, 13, '2023-08-20', 472.10, 'West'),
(17, 14, '2023-09-10', 289.00, 'North'),
(18, 11, '2023-09-25', 390.20, 'East'),
(19, 18, '2023-10-05', 459.90, 'South'),
(20, 17, '2023-10-20', 321.75, 'West'),
(21, 4, '2023-11-01', 410.10, 'North'),
(22, 6, '2023-11-10', 111.11, 'East'),
(23, 7, '2023-11-18', 222.22, 'South'),
(24, 3, '2023-11-25', 333.33, 'West'),
(25, 9, '2023-12-01', 444.44, 'North'),
(26, 20, '2023-12-10', 555.55, 'East'),
(27, 2, '2023-12-15', 160.40, 'South'),
(28, 5, '2024-01-05', 201.90, 'West'),
(29, 8, '2024-01-10', 187.50, 'North'),
(30, 1, '2024-01-20', 298.75, 'East'),
(31, 13, '2024-02-01', 368.20, 'South'),
(32, 14, '2024-02-15', 212.10, 'West'),
(33, 10, '2024-03-01', 478.90, 'North'),
(34, 6, '2024-03-18', 140.60, 'East'),
(35, 11, '2024-04-01', 332.10, 'South'),
(36, 19, '2024-04-10', 125.75, 'West'),
(37, 12, '2024-04-15', 456.30, 'North'),
(38, 17, '2024-05-01', 199.99, 'East'),
(39, 18, '2024-05-10', 310.00, 'South'),
(40, 16, '2024-05-25', 278.80, 'West'),
(41, 20, '2024-06-01', 180.10, 'North'),
(42, 15, '2024-06-10', 289.70, 'East'),
(43, 2, '2024-06-20', 315.25, 'South'),
(44, 3, '2024-07-01', 260.55, 'West'),
(45, 9, '2024-07-10', 350.80, 'North'),
(46, 4, '2024-07-20', 410.30, 'East'),
(47, 7, '2024-08-01', 120.00, 'South'),
(48, 8, '2024-08-10', 250.90, 'West'),
(49, 1, '2024-08-20', 330.40, 'North'),
(50, 5, '2024-09-01', 380.60, 'East');


ANALYZE COMPRESSION sch_sales_part2.orders_optimized;

drop  TABLE if exists sch_sales_part2.customers;
CREATE TABLE sch_sales_part2.customers (
    customer_id INT encode AZ64,
    name VARCHAR(50) encode LZO
)
DISTSTYLE KEY
DISTKEY(customer_id);

INSERT INTO sch_sales_part2.customers (customer_id, name) VALUES
(1, 'John Smith'),
(2, 'Jane Johnson'),
(3, 'Alex Williams'),
(4, 'Emily Brown'),
(5, 'Chris Jones'),
(6, 'Katie Garcia'),
(7, 'Mike Miller'),
(8, 'Laura Davis'),
(9, 'Robert Lopez'),
(10, 'Linda Wilson'),
(11, 'James Smith'),
(12, 'Sarah Johnson'),
(13, 'David Williams'),
(14, 'Karen Brown'),
(15, 'Brian Jones'),
(16, 'Amy Garcia'),
(17, 'Kevin Miller'),
(18, 'Rachel Davis'),
(19, 'Steve Lopez'),
(20, 'Olivia Wilson'),
(21, 'Jason Smith'),
(22, 'Emma Johnson'),
(23, 'Luke Williams'),
(24, 'Grace Brown'),
(25, 'Mark Jones'),
(26, 'Nina Garcia'),
(27, 'Tom Miller'),
(28, 'Hannah Davis'),
(29, 'Josh Lopez'),
(30, 'Zoe Wilson'),
(31, 'Ethan Smith'),
(32, 'Ava Johnson'),
(33, 'Logan Williams'),
(34, 'Chloe Brown'),
(35, 'Aaron Jones'),
(36, 'Mia Garcia'),
(37, 'Ben Miller'),
(38, 'Lily Davis'),
(39, 'Tyler Lopez'),
(40, 'Sophie Wilson'),
(41, 'Nathan Smith'),
(42, 'Isla Johnson'),
(43, 'Owen Williams'),
(44, 'Ella Brown'),
(45, 'Jack Jones'),
(46, 'Megan Garcia'),
(47, 'Ryan Miller'),
(48, 'Leah Davis'),
(49, 'Dylan Lopez'),
(50, 'Abby Wilson');

-- sql query performance

explain SELECT o.*, c.name
FROM sch_sales_part2.orders_default o
JOIN sch_sales_part2.customers c ON o.customer_id = c.customer_id;

XN Hash Join DS_DIST_ALL_NONE  (cost=0.62..3.77 rows=52 width=51)	
  Hash Cond: ("outer".customer_id = "inner".customer_id)	
  ->  XN Seq Scan on customers c  (cost=0.00..0.50 rows=50 width=19)	
  ->  XN Hash  (cost=0.50..0.50 rows=50 width=36)	
        ->  XN Seq Scan on orders_default o  (cost=0.00..0.50 rows=50 width=36)	


explain SELECT o.*, c.name
FROM sch_sales_part2.orders_raw o
JOIN sch_sales_part2.customers c ON o.customer_id = c.customer_id;

XN Hash Join DS_DIST_ALL_NONE  (cost=0.62..3.77 rows=52 width=51)	
  Hash Cond: ("outer".customer_id = "inner".customer_id)	
  ->  XN Seq Scan on customers c  (cost=0.00..0.50 rows=50 width=19)	
  ->  XN Hash  (cost=0.50..0.50 rows=50 width=36)	
        ->  XN Seq Scan on orders_raw o  (cost=0.00..0.50 rows=50 width=36)	

explain SELECT o.*, c.name
FROM sch_sales_part2.orders_optimized o
JOIN sch_sales_part2.customers c ON o.customer_id = c.customer_id;

XN Hash Join DS_DIST_NONE  (cost=0.62..2.77 rows=52 width=51)	
  Hash Cond: ("outer".customer_id = "inner".customer_id)	
  ->  XN Seq Scan on orders_optimized o  (cost=0.00..0.50 rows=50 width=36)	
  ->  XN Hash  (cost=0.50..0.50 rows=50 width=19)	
        ->  XN Seq Scan on customers c  (cost=0.00..0.50 rows=50 width=19)	

drop TABLE if exists sch_sales_part2.orders_raw_even;
CREATE TABLE sch_sales_part2.orders_raw_even (
    order_id       INT ENCODE raw,
    customer_id    INT ENCODE raw,
    order_date     DATE ENCODE raw,
    order_amount   DECIMAL(30,2) ENCODE raw,
    region         VARCHAR(50) ENCODE raw
)
DISTSTYLE EVEN;

insert into sch_sales_part2.orders_raw_even select * from sch_sales_part2.orders_raw;

explain SELECT o.*, c.name
FROM sch_sales_part2.orders_raw_even o
JOIN sch_sales_part2.customers c ON o.customer_id = c.customer_id;


XN Hash Join DS_BCAST_INNER  (cost=0.62..14000002.75 rows=50 width=143)	
  Hash Cond: ("outer".customer_id = "inner".customer_id)	
  ->  XN Seq Scan on orders_raw_even o  (cost=0.00..0.50 rows=50 width=128)	
  ->  XN Hash  (cost=0.50..0.50 rows=50 width=19)	
        ->  XN Seq Scan on customers c  (cost=0.00..0.50 rows=50 width=19)	
----- Tables missing statistics: orders_raw_even -----	
----- Update statistics by running the ANALYZE command on these tables -----	


drop  TABLE if exists sch_sales_part2.orders_optimized_diff_key;
CREATE TABLE sch_sales_part2.orders_optimized_diff_key (
    order_id INT encode AZ64 PRIMARY KEY,
    customer_id INT encode AZ64,
    order_date DATE encode AZ64,
    order_amount DECIMAL(10,2) encode AZ64,
    region VARCHAR(50) encode lzo
)
DISTSTYLE KEY
DISTKEY(order_id)
SORTKEY(order_date)
;

insert into sch_sales_part2.orders_optimized_diff_key select * from sch_sales_part2.orders_optimized;

explain SELECT o.*, c.name
FROM sch_sales_part2.orders_optimized_diff_key o
JOIN sch_sales_part2.customers c ON o.customer_id = c.customer_id;

XN Hash Join DS_BCAST_INNER  (cost=0.62..14000002.75 rows=50 width=133)	
  Hash Cond: ("outer".customer_id = "inner".customer_id)	
  ->  XN Seq Scan on orders_optimized_diff_key o  (cost=0.00..0.50 rows=50 width=118)	
  ->  XN Hash  (cost=0.50..0.50 rows=50 width=19)	
        ->  XN Seq Scan on customers c  (cost=0.00..0.50 rows=50 width=19)	
----- Tables missing statistics: orders_optimized_diff_key -----	
----- Update statistics by running the ANALYZE command on these tables -----	

-- Optimisation (other than SQL query re-writing)

VACUUM sch_sales_part2.orders_optimized;
ANALYZE sch_sales_part2.orders_optimized;

CREATE MATERIALIZED VIEW sch_sales_part2.mv_monthly_sales AS
SELECT DATE_TRUNC('month', order_date) AS order_month,
       SUM(order_amount) AS total_sales
FROM sch_sales_part2.orders_optimized
GROUP BY 1;