1. I used postgresql with pgAdmin as my DBMS and provided the database connection details in the profiles.yml file in the .dbt folder

2. I created two tables in my database (MI_WAREHOUSE) named BIRV_ACCOUNT_DIM and PARTY_DIM. The BIRV_ACCOUNT_DIM table had columns ACCOUNT_ID (PK) etc. The PARTY_DIM table had columns CONF_BANK_ENTITY_CODE , PARTY_ID (PK) etc
-- BIRV_ACCOUNT_DIM table
CREATE TABLE BIRV_ACCOUNT_DIM (
    account_id SERIAL PRIMARY KEY,
    account_name VARCHAR(100),
    account_type VARCHAR(50),
    created_date DATE,
    balance NUMERIC(12,2)
);

-- PARTY_DIM table
CREATE TABLE PARTY_DIM (
    party_id SERIAL PRIMARY KEY,
    party_name VARCHAR(100),
    party_type VARCHAR(50),
    registration_date DATE,
    address VARCHAR(255),
    contact_number VARCHAR(20),
    conf_bank_entity_code VARCHAR(10)
);

-- Inserting data into BIRV_ACCOUNT_DIM table
INSERT INTO BIRV_ACCOUNT_DIM (account_name, account_type, created_date, balance) VALUES
('Account 1', 'Savings', '2023-01-15', 1500.00),
('Account 2', 'Checking', '2023-02-28', 2500.00),
('Account 3', 'Savings', '2023-03-10', 1800.00),
('Account 4', 'Investment', '2023-04-05', 5000.00),
('Account 5', 'Savings', '2023-05-20', 3000.00),
('Account 6', 'Checking', '2023-06-12', 3500.00),
('Account 7', 'Investment', '2023-07-03', 6000.00),
('Account 8', 'Savings', '2023-08-18', 4200.00),
('Account 9', 'Checking', '2023-09-29', 2800.00),
('Account 10', 'Investment', '2023-10-15', 7000.00);

-- Inserting data into PARTY_DIM table
INSERT INTO PARTY_DIM (party_name, party_type, registration_date, address, contact_number, conf_bank_entity_code) VALUES
('Party 1', 'Individual', '2022-12-01', '123 Main St', '123-456-7890', 'ABC123'),
('Party 2', 'Business', '2022-11-15', '456 Elm St', '987-654-3210', 'DEF456'),
('Party 3', 'Individual', '2023-01-20', '789 Oak St', '111-222-3333', 'GHI789'),
('Party 4', 'Business', '2023-02-10', '321 Maple St', '444-555-6666', 'JKL012'),
('Party 5', 'Individual', '2023-03-05', '654 Pine St', '777-888-9999', 'MNO345'),
('Party 6', 'Business', '2023-04-18', '987 Cedar St', '222-333-4444', 'PQR678'),
('Party 7', 'Individual', '2023-05-30', '123 Walnut St', '555-666-7777', 'STU901'),
('Party 8', 'Business', '2023-06-25', '456 Birch St', '888-999-0000', 'VWX234'),
('Party 9', 'Individual', '2023-07-12', '789 Oak St', '333-444-5555', 'YZA567'),
('Party 10', 'Business', '2023-08-03', '321 Cedar St', '999-000-1111', 'BCD890');


3. I then used dbt to model (birv_account_x_party_dim.sql) a target table BIRV_ACCOUNT_X_PARTY_DIM which had both ACCOUNT_ID (FK) and PARTY_ID (FK). I used a cross-join because we assume the two tables already exist with data in them, if not we could have just created a simple 3rd table with a unique id and fk columns ACCOUNT_ID (FK) and PARTY_ID (FK).

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

4. I created 2 additional tables BIRT_ACC_X_ACC_STATUS_DIM and BIRTH_ACCOUNT_STATUS_DIM

-- BIRT_ACC_X_ACC_STATUS_DIM table
CREATE TABLE BIRT_ACC_X_ACC_STATUS_DIM (
    ACCOUNT_ID INT,
    ACCOUNT_STATUS_ID INT,
    START_DATE DATE,
    END_DATE DATE,
    SOURCE_SYSTEM_IDENTIFIER VARCHAR(50),
    -- Additional columns as needed
    PRIMARY KEY (ACCOUNT_ID, ACCOUNT_STATUS_ID),
    FOREIGN KEY (ACCOUNT_ID) REFERENCES BIRV_ACCOUNT_DIM(ACCOUNT_ID),
    FOREIGN KEY (ACCOUNT_STATUS_ID) REFERENCES BIRTH_ACCOUNT_STATUS_DIM(ACCOUNT_STATUS_ID)
);

-- BIRTH_ACCOUNT_STATUS_DIM table
CREATE TABLE BIRTH_ACCOUNT_STATUS_DIM (
    ACCOUNT_STATUS_ID SERIAL PRIMARY KEY,
    ACCOUNT_STATUS_CODE VARCHAR(10),
    DESCRIPTION TEXT,
    START_DATE DATE,
    END_DATE DATE
);

-- Inserting data into BIRTH_ACCOUNT_STATUS_DIM table
INSERT INTO BIRTH_ACCOUNT_STATUS_DIM (ACCOUNT_STATUS_CODE, DESCRIPTION, START_DATE, END_DATE) VALUES
('A1', 'Active', '2023-01-01', '3500-12-31'),
('A2', 'Inactive', '2023-01-01', '3500-12-31'),
('A3', 'Suspended', '2023-01-01', '3500-12-31'),
('A4', 'Closed', '2023-01-01', '3500-12-31'),
('A5', 'Pending', '2023-01-01', '3500-12-31'),
('A6', 'Blocked', '2023-01-01', '3500-12-31'),
('A7', 'Cancelled', '2023-01-01', '3500-12-31'),
('A8', 'Expired', '2023-01-01', '3500-12-31'),
('A9', 'Approved', '2023-01-01', '3500-12-31'),
('A10', 'Rejected', '2023-01-01', '3500-12-31');

-- Inserting data into BIRT_ACC_X_ACC_STATUS_DIM table
INSERT INTO BIRT_ACC_X_ACC_STATUS_DIM (ACCOUNT_ID, ACCOUNT_STATUS_ID, START_DATE, END_DATE, SOURCE_SYSTEM_IDENTIFIER) VALUES
(1, 1, '2023-01-01', '3500-12-31', 'System1'),
(2, 2, '2023-01-01', '3500-12-31', 'System2'),
(3, 3, '2023-01-01', '3500-12-31', 'System1'),
(4, 4, '2023-01-01', '3500-12-31', 'System2'),
(5, 5, '2023-01-01', '3500-12-31', 'System1'),
(6, 6, '2023-01-01', '3500-12-31', 'System2'),
(7, 7, '2023-01-01', '3500-12-31', 'System1'),
(8, 8, '2023-01-01', '3500-12-31', 'System2'),
(9, 9, '2023-01-01', '3500-12-31', 'System1'),
(10, 10, '2023-01-01', '3500-12-31', 'System2');


5. I also created a model (joined_account_status.sql) that joins 3 tables BIRV_ACCOUNT_DIM, BIRT_ACC_X_ACC_STATUS_DIM and BIRTH_ACCOUNT_STATUS_DIM while applying filters specified in the Table Joins sheet.

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



































Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
