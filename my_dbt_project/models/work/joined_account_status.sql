{{ 
  config(
    materialized='table'
  )
}}

SELECT
    ba.*,
    baxas.*,
    bas.*
FROM 
    {{ ref('BIRV_ACCOUNT_DIM') }} AS ba
JOIN 
    {{ ref('BIRT_ACC_X_ACC_STATUS_DIM') }} AS baxas
ON 
    ba.ACCOUNT_ID = baxas.ACCOUNT_ID
JOIN 
    {{ ref('BIRT_ACCOUNT_STATUS_DIM') }} AS bas
ON 
    baxas.ACCOUNT_STATUS_ID = bas.ACCOUNT_STATUS_ID
WHERE 
    ba.SOURCE_SYSTEM_IDENTIFIER = 'RCB' 
    AND ba.END_DATE = '3500-12-31' 
    AND baxas.END_DATE = '3500-12-31' 
    AND baxas.SOURCE_SYSTEM_IDENTIFIER = 'RCB' 
    AND bas.END_DATE = '3500-12-31' 
    AND bas.ACCOUNT_STATUS_CODE <> 'CL';