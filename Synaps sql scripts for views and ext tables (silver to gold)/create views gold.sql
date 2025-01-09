--------------------------------------
-- CREATE Calendar VIEW
---------------------------------------
CREATE VIEW gold.calendar
AS
SELECT
    *
FROM
    OPENROWSET
    (
        BULK 'https://datalakeforproj1.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
        format = 'PARQUET'
    ) as Q1;


--------------------------------------
-- CREATE Customers VIEW
---------------------------------------
CREATE VIEW gold.customers
AS
SELECT
    *
FROM
    OPENROWSET
    (
        BULK 'https://datalakeforproj1.dfs.core.windows.net/silver/AdventureWorks_Customers/',
        format = 'PARQUET'
    ) as Q2;


--------------------------------------
-- CREATE Product_Categories VIEW
---------------------------------------
CREATE VIEW gold.procat
AS
SELECT
    *
FROM
    OPENROWSET
    (
        BULK 'https://datalakeforproj1.dfs.core.windows.net/silver/AdventureWorks_Product_Categories/',
        format = 'PARQUET'
    ) as Q3;


--------------------------------------
-- CREATE Products VIEW
---------------------------------------
CREATE VIEW gold.products
AS
SELECT
    *
FROM
    OPENROWSET
    (
        BULK 'https://datalakeforproj1.dfs.core.windows.net/silver/AdventureWorks_Products/',
        format = 'PARQUET'
    ) as Q4;


--------------------------------------
-- CREATE ProductSubcategories VIEW
---------------------------------------
CREATE VIEW gold.subcat
AS
SELECT
    *
FROM
    OPENROWSET
    (
        BULK 'https://datalakeforproj1.dfs.core.windows.net/silver/AdventureWorks_ProductSubcategories/',
        format = 'PARQUET'
    ) as Q5;


--------------------------------------
-- CREATE Returns VIEW
---------------------------------------
CREATE VIEW gold.ret
AS
SELECT
    *
FROM
    OPENROWSET
    (
        BULK 'https://datalakeforproj1.dfs.core.windows.net/silver/AdventureWorks_Returns/',
        format = 'PARQUET'
    ) as Q6;


--------------------------------------
-- CREATE Sales VIEW
---------------------------------------
CREATE VIEW gold.sales
AS
SELECT
    *
FROM
    OPENROWSET
    (
        BULK 'https://datalakeforproj1.dfs.core.windows.net/silver/AdventureWorks_Sales/',
        format = 'PARQUET'
    ) as Q7;



--------------------------------------
-- CREATE Territories VIEW
---------------------------------------
CREATE VIEW gold.territories
AS
SELECT
    *
FROM
    OPENROWSET
    (
        BULK 'https://datalakeforproj1.dfs.core.windows.net/silver/AdventureWorks_Territories/',
        format = 'PARQUET'
    ) as Q8;