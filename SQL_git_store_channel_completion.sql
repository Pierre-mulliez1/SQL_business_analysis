-- store by store analysis of performance: data filled, profit, frequence, average basket over the last twelve month and more.
-- Later filetered by store and channel with data_studio
-- nnn for not to be disclose.

WITH first_sale AS
(
SELECT 
  s1.customer_id,
  c1.MOBILE_NUMBER_nnn,
  min(sale_date) AS dob_first_sale
FROM 
  `nnn` AS c1
  LEFT JOIN `nnn` AS s1
  ON c1.CUSTOMER_ID = s1.customer_id 
WHERE 1=1
  AND c1.company_code like 'nnn'
GROUP BY 
  s1.customer_id,
  BIRTHDAY_DATE,
  c1.MOBILE_NUMBER_'nnn'
),

-- not further used in the queries I left the 2 CTE that compute the number of duplicate account
mob_dupl AS 
(
SELECT 
 MOBILE_NUMBER_nnn,
 count(customer_id) AS MOB_count,
 max(customer_id) as last_cust
FROM 
  `nnn`
WHERE company_code like 'nnn'
GROUP BY 
  MOBILE_NUMBER_nnn
-- avoid duplicates with 2 email AND 2 mobile to be counted twice
HAVING 
  count(distinct EMAIL_nnn) >= 1
),

mail_dupl AS 
(
SELECT 
 EMAIL_nnn,
 count(customer_id) AS MAIL_COUNT, 
 max(customer_id) as last_cust
FROM 
  `nnn`
WHERE company_code like 'nnn'
GROUP BY 
  EMAIL_nnn
),

 DOB AS (
SELECT  
  store_label,   
  s1.customer_id, 
  sale_date,
  s1.amount_including_taxes,
  ticket_number,
  margin_excluding_taxes,
  c1.MOBILE_NUMBER_FILLED,
  c1.EMAIL_FILLED,
  dob_first_sale,
  c1.ADDRESS_ZIPCODE,
  ADDRESS_CITY,
  ADDRESS_STREET_NAME_nnn,
  ADDRESS_SUPPLEMENT_nnn,
  ADDRESS_LOCALITY_nnn,
  DATE_diff(current_date(), BIRTHDAY_DATE, year) AS dob_today,
  DATE_diff(extract(date from CONTACT_CREATION_DATETIME), BIRTHDAY_DATE, year) AS dob_creation,
  max(sale_date) AS dob_last_sale,
  CASE 
    WHEN SUBSTR(s1.customer_id, 5, 1) like '9' THEN 'Butler'
    WHEN c1.CREATION_STORE_ID BETWEEN 2000 AND 2100 THEN 'store'
    WHEN c1.CREATION_STORE_ID = 2600 THEN 'e-commerce'
    WHEN c1.CREATION_STORE_ID = 2998 THEN 'appli'
    ELSE 'Unwanted'
    END AS channel
FROM 
  `nnn` AS c1
  LEFT JOIN `nnn` AS s1
  ON c1.CUSTOMER_ID = s1.customer_id 
  LEFT JOIN `nnn`as st1
-- Analysis based on the creation store 
  ON c1.CREATION_STORE_ID  = st1.store_id
  LEFT JOIN first_sale AS s2
  ON c1.CUSTOMER_ID = s2.customer_id 
WHERE 1=1
  AND c1.company_code like 'nnn'
  AND s1.amount_including_taxes >= 0
  AND s1.sale_date BETWEEN DATE_SUB(current_date(), INTERVAL 1 year) and current_date()
  --OR  s1.store_id = 2998)

GROUP BY
  store_label,
  CREATION_STORE_ID,
  s1.customer_id, 
  c1.BIRTHDAY_DATE, 
  ticket_number, 
  sale_date, 
  s1.amount_including_taxes,
  margin_excluding_taxes,
  c1.MOBILE_NUMBER_FILLED,
  c1.EMAIL_FILLED,
  dob_first_sale,
  CONTACT_CREATION_DATETIME,
  ADDRESS_ZIPCODE,
  ADDRESS_CITY,
  ADDRESS_STREET_NAME_nnn,
  ADDRESS_SUPPLEMENT_nnn,
  ADDRESS_LOCALITY_nnn
HAVING 
  count(distinct ticket_number) >= 1
),

computing AS 
(
SELECT 
  store_label,
  channel,
  ADDRESS_ZIPCODE,
  ADDRESS_CITY,
  ADDRESS_STREET_NAME_nnn,
  ADDRESS_SUPPLEMENT_nnn,
  ADDRESS_LOCALITY_nnn,
  dob_today,
  dob_creation,
  date_diff(dob_last_sale, dob_first_sale, month) as cust_lifetime_from_creation,
  sum(amount_including_taxes) AS turnover,
  sum(margin_excluding_taxes) as profit,  
  CASE WHEN dob_today <= 18 THEN '7_18'
       WHEN dob_today between 19 and 25 THEN '19_25'
       WHEN dob_today between 26 and 35 THEN '26_35'
       WHEN dob_today between 36 and 50 THEN '36_50'
       ELSE '51 and over'
  END AS age_clustering,
  CASE WHEN dob_creation <= 18 THEN '7_18'
       WHEN dob_creation between 19 and 25 THEN '19_25'
       WHEN dob_creation between 26 and 35 THEN '26_35'
       WHEN dob_creation between 36 and 50 THEN '36_50'
       ELSE '51 and over'
  END AS age_clustering_creation,
  customer_id, 
  ticket_number,
  MOBILE_NUMBER_FILLED,
  EMAIL_FILLED,
  dob_first_sale
FROM 
  DOB
WHERE 
  dob_today BETWEEN 7 AND 100
  AND channel NOT LIKE 'Unwanted'
GROUP BY 
  store_label,
  channel,
  customer_id, 
  ticket_number,
  MOBILE_NUMBER_FILLED,
  EMAIL_FILLED,
  dob_first_sale,
  dob_last_sale,
  dob_today,
  dob_creation,
  ADDRESS_ZIPCODE,
  ADDRESS_CITY,
  ADDRESS_STREET_NAME_nnn,
  ADDRESS_SUPPLEMENT_nnn,
  ADDRESS_LOCALITY_nnn
),


frequencyCTE AS 
(
SELECT 
  count(distinct ticket_number) AS frequency, 
  customer_id
FROM 
  DOB 
GROUP BY 
  customer_id
),

finalcalc as
(
SELECT 
  distinct f1.customer_id,
  max(store_label) as last_store,
  max(channel) as last_channel,
  frequency, 
  dob_today,
  dob_creation,
  age_clustering,
  age_clustering_creation,
  sum(turnover) AS total_turnover,  
  sum(profit) as total_profit,
  CASE WHEN MOBILE_NUMBER_FILLED like 'True' then 'filled' ELSE null 
  END AS Mobile,
  CASE WHEN EMAIL_FILLED like 'True' then 'filled' ELSE null 
  END AS Email,
  ADDRESS_ZIPCODE,
  CASE 
       WHEN countif(EMAIL_FILLED LIKE 'True' AND MOBILE_NUMBER_FILLED LIKE 'True' AND 
                         (ADDRESS_ZIPCODE is not null AND ADDRESS_CITY is not null AND 
                            (ADDRESS_STREET_NAME_nnn is not null OR                                                                                        ADDRESS_SUPPLEMENT_nnn is not null OR ADDRESS_LOCALITY_nnn is not null 
                            )
                          )
                       ) >= 1 THEN 1 
       ELSE 0 
  END AS complete_address,
   CASE 
       WHEN max(g1.cust_lifetime_from_creation) <= 12 AND count(distinct ticket_number) = 1 THEN 0.01
       WHEN max(g1.cust_lifetime_from_creation) <= 12 THEN 1
       WHEN max(g1.cust_lifetime_from_creation) BETWEEN 13 AND 24 THEN 2
       WHEN max(g1.cust_lifetime_from_creation) BETWEEN 25 AND 36 THEN 3
       WHEN max(g1.cust_lifetime_from_creation) BETWEEN 37 AND 48 THEN 4
       WHEN max(g1.cust_lifetime_from_creation) BETWEEN 49 AND 60 THEN 5
       WHEN max(g1.cust_lifetime_from_creation) BETWEEN 61 AND 73 THEN 6
       ELSE 7
  END AS customer_lifetime
FROM 
  computing AS g1 LEFT join 
  frequencyCTE AS f1
  ON g1.customer_id = f1.customer_id 
WHERE
  turnover >= 0
  AND frequency < 30
  AND cust_lifetime_from_creation >= 0
GROUP BY 
  age_clustering,
  age_clustering_creation,
  dob_today,
  dob_creation,
  f1.customer_id, 
  frequency, 
  MOBILE_NUMBER_FILLED,
  EMAIL_FILLED,
  ADDRESS_ZIPCODE,
  ADDRESS_CITY,
  ADDRESS_STREET_NAME_nnn,
  ADDRESS_SUPPLEMENT_nnn,
  ADDRESS_LOCALITY_nnn
),

end_calc as 
(
SELECT
  last_store,
  last_channel,
  age_clustering,
  age_clustering_creation,
  Mobile,
  email,
 complete_address,
  total_turnover,
  dob_today,
  dob_creation,
  total_profit,
  frequency,
  customer_id,
  customer_lifetime,
  ADDRESS_ZIPCODE,
  Round((total_turnover / frequency),2) as PM
FROM 
  finalcalc AS e1
),

sumCTE as 
(SELECT 
  CASE WHEN last_store like 'nnn' THEN 'APPLICATION'
       ELSE last_store 
  END AS store_descr,
  last_channel,
  count(distinct customer_id) as customer_count,
  round(avg(total_turnover),2) as avg_Revenue,
  round(avg(frequency),2) as avg_frequence,
  round(avg(PM),2) as avg_PM,
  round(avg(total_profit),2) as avg_Net_profit,
  round(avg(customer_lifetime),1) as avg_cust_life,
  round(avg(dob_today),0) as Avg_age,
  round(avg(dob_creation),0) as Avg_age_creation,
  countif(age_clustering_creation like '7_18') AS Youth_creation,
  countif(age_clustering_creation like '19_25') AS Mid_age_young_creation,
  countif(age_clustering_creation like '26_35') AS Mid_age_old_creation,
  countif(age_clustering_creation like '36_50') AS Parents_creation,
  countif(age_clustering_creation like '51 and over') AS Senior_creation,
  countif(age_clustering like '7_18') AS Youth,
  countif(age_clustering like '19_25') AS Mid_age_young,
  countif(age_clustering like '26_35') AS Mid_age_old,
  countif(age_clustering like '36_50') AS Parents,
  countif(age_clustering like '51 and over') AS Senior,
  round(count(Mobile) / count( customer_id),2) as mobile_filled,
  round(count(email) / count( customer_id),2) as email_filled,
  round(count(ADDRESS_ZIPCODE) / count( customer_id),2) as zip_filled,
   round(sum(complete_address) / count( customer_id),2) AS complete_addresses,
   sum(complete_address) as ad 
FROM 
  end_calc AS e1
GROUP BY 
  last_store,
  last_channel
)

SELECT 
  store_descr,
  last_channel,
  customer_count,
  avg_Revenue,
  avg_frequence,
  avg_PM,
  avg_Net_profit,
  avg_cust_life,
  Avg_age,
  Avg_age_creation,
  Youth,
  Mid_age_young,
  Mid_age_old,
  Parents,
  Senior,
  Youth_creation,
  Mid_age_young_creation,
  Mid_age_old_creation,
  Parents_creation,
  Senior_creation,
  email_filled,
  mobile_filled,
  zip_filled,
  complete_addresses,
  ad
FROM 
  sumCTE
WHERE 1=1
  AND customer_count > 35
  AND store_descr not like '(nnn)'
  AND store_descr not like 'nnn'
  AND store_descr not like 'nnn'
  AND store_descr not like 'nnn'
  AND store_descr not like 'nnn'
  AND store_descr not like 'nnn'
  AND store_descr not like 'nnn'
  AND store_descr not like 'nnn'
  AND store_descr not like '(nnn)'
  AND store_descr not like '(nnn)'
  AND store_descr not like '(nnn)'
  AND store_descr not like '(nnn)'
  AND store_descr not like '(nnn)'
  AND store_descr not like '(nnn)'
  AND store_descr not like '(nnn)'
  AND store_descr not like '(nnn)'
  AND store_descr not like '(nnn)'
  AND store_descr not like '(nnn)'
  AND store_descr not like '(nnn)'
  AND store_descr not like '(nnn)'
  AND store_descr not like '(nnn)'
  AND store_descr not like '(nnn)'
ORDER BY 
  last_channel asc, 
  store_descr asc