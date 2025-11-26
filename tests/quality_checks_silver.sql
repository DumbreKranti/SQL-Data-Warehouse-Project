/*
================================================================================================================
DATA QUALITY CHECK SCRIPT – SILVER LAYER
================================================================================================================
Purpose:
    This script performs data quality checks to validate consistency, accuracy, and standardization 
    across the Bronze and Silver layers of the data warehouse.

What this checks:
     Duplicate or NULL Primary Keys
     Unwanted spaces in text fields
     Date formats & invalid date ranges
     Sales–Quantity–Price consistency rules
     Product & customer standardization
     Negative or invalid numeric values
     Data consistency between Bronze → Silver transformations

Usage:
    ➤ Run AFTER loading the Silver layer.
    ➤ Investigate and fix any discrepancies returned.
----------------------------------------------------------------------------------------------------------------
*/


/****************************************************************************************************************
SECTION 1 — CRM CUSTOMER INFO (bronze.crm_cust_info / silver.crm_cust_info)
****************************************************************************************************************/

-- 1.1 Check duplicate cst_id (Primary Key)
SELECT
    cst_id,
    COUNT(*) AS dup_count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;


-- 1.2 Check unwanted spaces in customer firstname
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);


-- 1.3 Check unwanted spaces in customer lastname
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);


-- 1.4 Check unwanted spaces in gender field
SELECT *
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);


-- 1.5 Find DISTINCT gender values (for standardization validation)
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;


-- 1.6 View Silver layer customer table
SELECT *
FROM silver.crm_cust_info;


-- 1.7 ROW_NUMBER logic used in transformation (latest record only)
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29466;


-- 1.8 Identify duplicate records removed in Silver
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag_last
    FROM bronze.crm_cust_info
) T
WHERE Flag_last != 1;


-- 1.9 List the unique (latest) records that Silver keeps
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag_last
    FROM bronze.crm_cust_info
) T
WHERE Flag_last = 1;


-- 1.10 Apply trimming logic (reference for Silver transformation)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS firstname_clean,
    TRIM(cst_lastname) AS lastname_clean,
    cst_marital_status,
    cst_gndr,
    cst_create_date
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag_last
    FROM bronze.crm_cust_info
) T
WHERE Flag_last = 1;


-- 1.11 Standardize Gender
SELECT 
    cst_gndr,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS standardized_gender
FROM bronze.crm_cust_info;




/****************************************************************************************************************
SECTION 2 — PRODUCT INFO (bronze.crm_prd_info / silver.crm_prd_info)
****************************************************************************************************************/

-- 2.1 View full Bronze product table
SELECT 
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info;


-- 2.2 Check duplicate or NULL product IDs
SELECT 
    prd_id,
    COUNT(*) AS dup_count
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


-- 2.3 Check unwanted spaces in product name
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-- 2.4 Check NULL or negative product cost
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


-- 2.5 Verify standardization for product line
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;


-- 2.6 Validate correct start/end date order
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- 2.7 Product end date transformation logic (LEAD)
SELECT 
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');




/****************************************************************************************************************
SECTION 3 — SALES DETAILS (bronze.crm_sales_details / silver.crm_sales_details)
****************************************************************************************************************/

-- 3.1 Identify invalid order dates
SELECT 
    NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
    OR LEN(sls_order_dt) != 8
    OR sls_order_dt > 20500101;


-- 3.2 Validate sales = quantity * price (Silver transformation rules)
SELECT 
    sls_sales AS old_sales,
    sls_quantity AS old_qty,
    sls_price AS old_price,

    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 
             OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS calc_sales,

    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
    END AS calc_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
    OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;




/****************************************************************************************************************
SECTION 4 — ERP CUSTOMER & LOCATION TABLES
****************************************************************************************************************/

-- 4.1 Invalid or out-of-range birthdates
SELECT DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();


-- 4.2 Distinct country codes
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;




/****************************************************************************************************************
SECTION 5 — PRODUCT CATEGORY LOOKUP (bronze.erp_px_cat_g1v2)
****************************************************************************************************************/

-- 5.1 Full table
SELECT id, cat, subcat, maintenance
FROM bronze.erp_px_cat_g1v2;


-- 5.2 Check unwanted spaces
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat);


-- 5.3 Standardize category values
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;
