/*
==========================================================================
Quality Checks
==========================================================================
Script Purpose:
  This script performs quality checks to validate the integrity, consistency,
  and accuracy of the Gold layer. These checks ensure:
  - Uniqueness of surrogate keys in dimension tables.
  - Referential integrity between fact and dimension tables.
  - Validation of relationships in the data model for analytical purposes.

Usage Notes:
  - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
==========================================================================
*/

-- Check duplicate
SELECT cst_id, COUNT(*) FROM (
SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.country
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
ON ci.cst_key = la.cid
)t 
GROUP BY cst_id
HAVING COUNT(*) > 1

-- Check duplicate
SELECT prd_key, COUNT(*) FROM (
SELECT
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all historical data
)t
GROUP BY prd_key
HAVING COUNT(*) > 1

-- Foregin Key Integrity (Dimensions)
SELECT * 
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products AS p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL
