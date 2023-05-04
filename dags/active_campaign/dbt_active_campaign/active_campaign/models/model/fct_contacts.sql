

{{ config(materialized='table') }}

-- get all contact details and their company names.
SELECT
  contacts.id,
  contacts.first_name,
  contacts.last_name,
  accounts.name as company_name,
  accounts.id as account_id,
  email,
  phone,
  DATE(date_trunc(contacts.created_timestamp, MONTH)) as created_month
FROM
  `pure-ace-372509.active_campaign_dbt_raw.account_contacts` AS account_contacts
LEFT JOIN
  `pure-ace-372509.active_campaign_dbt_raw.contacts` AS contacts
ON
  contacts.id = account_contacts.contact
LEFT JOIN
  `pure-ace-372509.active_campaign_dbt_raw.accounts` AS accounts
ON
  account_contacts.account = accounts.id
  --ORDER BY `pure-ace-372509.active_campaign_dbt_raw.contacts`.id ASC
LIMIT
  1000