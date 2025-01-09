-- as extarnal tables will not be managed by either databricks or synapse we need to create credentials first (Master key )

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'admin@123';

-- tell synapse that we are using managed identity


CREATE DATABASE SCOPED CREDENTIAL cred_ayan
WITH
    IDENTITY = 'Managed Identity'



-- create data source its just like a dataset in adf we will use container level data sourcing so that we can access all files inside container
-- by creating this we do not need to write the url again and again (acts like variable)
-- whenever database tries to connect with datalake, it requires credentials thats why we are specifying credential while creating dataset


CREATE EXTERNAL DATA SOURCE src_silver
WITH
    (
        LOCATION = 'https://datalakeforproj1.dfs.core.windows.net/silver',
        CREDENTIAL = cred_ayan
    )


-- create data source for gold


CREATE EXTERNAL DATA SOURCE src_gold
WITH
    (
        LOCATION = 'https://datalakeforproj1.dfs.core.windows.net/gold',
        CREDENTIAL = cred_ayan
    )


-- like the data source we can tell database our file format( in this case parquet) these codes can be found in the documentry
-- each format has different data compression method the link to find them is : https://learn.microsoft.com/en-us/sql/t-sql/statements/create-external-file-format-transact-sql?view=sql-server-ver16&tabs=parquet



CREATE EXTERNAL FILE FORMAT parq
WITH
    (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    )


---------------------------------------
-- CREATE EXTERNAL TABLE FOR Sales
---------------------------------------


CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'Ext_sales',
    DATA_SOURCE = src_gold,
    FILE_FORMAT = parq
)
AS SELECT * FROM gold.sales

SELECT * FROM gold.extsales


---------------------------------------
-- CREATE EXTERNAL TABLE FOR calendar
---------------------------------------


CREATE EXTERNAL TABLE gold.extcalendar
WITH
(
    LOCATION = 'Ext_calendar',
    DATA_SOURCE = src_gold,
    FILE_FORMAT = parq
)
AS SELECT * FROM gold.calendar



---------------------------------------
-- CREATE EXTERNAL TABLE FOR customers
---------------------------------------


CREATE EXTERNAL TABLE gold.extcustomers
WITH
(
    LOCATION = 'Ext_customers',
    DATA_SOURCE = src_gold,
    FILE_FORMAT = parq
)
AS SELECT * FROM gold.customers



---------------------------------------
-- CREATE EXTERNAL TABLE FOR Product_Category
---------------------------------------


CREATE EXTERNAL TABLE gold.extprocat
WITH
(
    LOCATION = 'Ext_Product_Category',
    DATA_SOURCE = src_gold,
    FILE_FORMAT = parq
)
AS SELECT * FROM gold.procat



---------------------------------------
-- CREATE EXTERNAL TABLE FOR products
---------------------------------------


CREATE EXTERNAL TABLE gold.extproducts
WITH
(
    LOCATION = 'Ext_products',
    DATA_SOURCE = src_gold,
    FILE_FORMAT = parq
)
AS SELECT * FROM gold.products



---------------------------------------
-- CREATE EXTERNAL TABLE FOR Products_sub_category
---------------------------------------


CREATE EXTERNAL TABLE gold.extsubcat
WITH
(
    LOCATION = 'Ext_Products_sub_category',
    DATA_SOURCE = src_gold,
    FILE_FORMAT = parq
)
AS SELECT * FROM gold.subcat



---------------------------------------
-- CREATE EXTERNAL TABLE FOR Returns
---------------------------------------


CREATE EXTERNAL TABLE gold.extret
WITH
(
    LOCATION = 'Ext_Returns',
    DATA_SOURCE = src_gold,
    FILE_FORMAT = parq
)
AS SELECT * FROM gold.ret



---------------------------------------
-- CREATE EXTERNAL TABLE FOR territories
---------------------------------------


CREATE EXTERNAL TABLE gold.extterritories
WITH
(
    LOCATION = 'Ext_territories',
    DATA_SOURCE = src_gold,
    FILE_FORMAT = parq
)
AS SELECT * FROM gold.territories


