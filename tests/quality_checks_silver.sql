/*
==========================================================================
Quality Checks
==========================================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy,
  and standardization across the 'silver' schemas. It includes checks for:
  - Null or duplicate primary keys.
  - Unwanted spaces in string fields.
  - Data standardization and consistency.
  - Invalid date ranges and orders.
  - Data consistency between related fields.

Usage Notes:
  - Run these checks after data loading Silver layer.
  - Investigate and resolve any disrepancies found during the checks.
==========================================================================
*/

-- Data Analysing --
-- Task 1: Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT 
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No Result
-- Check for each column that have varchar

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data Standardization & Consistency
-- Check the consistency of values in low cardinality columns

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

SELECT *
FROM silver.crm_cust_info;

-- Check for Nulls or duplicates in primary key
-- Expectation: No Result

SELECT 
	prd_id,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for Nulls or negative numbers
-- Expected: No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check for invalid date orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-- Select product key compatibility
SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE 	SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN
	(SELECT sls_prd_key FROM bronze.crm_sales_details)

-- Check for invalid dates
SELECT 
	NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101

-- check for invalid date orders
SELECT
	*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_dt

-- check data consistency: between sales, quantity, and price
-- >> sales = quantity * price
-- >> values must not be NULL, zero, or negative

SELECT
	sls_sales AS old_sls_sales,
	sls_quantity,
	sls_price AS old_sls_price,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales 
	END AS sls_sales,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price 
	END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- Data Check
-- Check for existence in other column
SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-- identify out-of-range date
SELECT 
	bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01'
OR bdate > GETDATE()

-- data standardization & consistency
SELECT DISTINCT gen
FROM bronze.erp_cust_az12

-- Check unmatching data
SELECT
	REPLACE(cid, '-', '') AS cid,
	country
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN
	(SELECT cst_key FROM silver.crm_cust_info)


-- Data Standardization
SELECT DISTINCT country
FROM bronze.erp_loc_a101
ORDER BY country

SELECT DISTINCT 
	country AS old_country,
	CASE  
		WHEN TRIM(country) = 'DE' THEN 'Germany'
		WHEN TRIM(country) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(country) = '' OR country IS NULL THEN 'n/a'
		ELSE TRIM(country)
	END AS country
FROM bronze.erp_loc_a101
ORDER BY old_country

-- Check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat)
OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT
	maintenance
FROM bronze.erp_px_cat_g1v2;

