{{ 
  config(
    materialized='table'
  )
}}

WITH birv_account_x_party_dim AS (
    SELECT 
        ba.account_id,
        pd.party_id
    FROM 
        {{ ref('BIRV_ACCOUNT_DIM') }} AS ba
    CROSS JOIN 
        {{ ref('PARTY_DIM') }} AS pd
)

SELECT 
    ROW_NUMBER() OVER () AS birv_account_x_party_dim,
    account_id,
    party_id
FROM 
    birv_account_x_party_dim