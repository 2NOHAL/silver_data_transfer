# Databricks notebook source
# MAGIC %md
# MAGIC ### Musubi_proxy_logsのシルバーデータ作成

# COMMAND ----------

from datetime import datetime, timedelta, timezone

target_date = datetime.now(timezone(timedelta(hours=+9), "JST")) - timedelta(days=1)
print(f"target_date:{target_date}")

year = target_date.strftime("%Y")
month = target_date.strftime("%m")
day = target_date.strftime("%d")

spark.sql(
    f"""
CREATE OR REPLACE TABLE workspace.h_tsuno.silver_proxy_logs_agg AS
WITH Pharmacy AS (
SELECT
	id
	,organization_id
	,name
	,address
    ,medical_institution_code
    ,created_at
    ,start_date
    ,contract_type
    FROM musubi.ddb.pharmarcy
    )

, log AS (
SELECT
      http_user_agent
    , request_url
    , url_decode(regexp_extract(request_url, '(.*?)($|\\?)', 1)) path
    , url_decode(regexp_extract(request_url, 'transactionId=(.*?)(&|$)', 1)) transaction_id
    , url_decode(regexp_extract(request_url, 'action=(.*?)(&|$)', 1)) action
    , url_decode(regexp_extract(request_url, 'mainPatientId=(.*?)(&|$)', 1)) main_patient_id
    , if(
        request_url like '%pharmacyId=%'
    	, url_decode(regexp_extract(request_url, 'pharmacyId=(.*?)(&|$)', 1))
    	, url_decode(regexp_extract(request_url, 'mainPatientId=(.*?)_', 1))
    ) pharmacy_id
    , url_decode(regexp_extract(request_url, 'userId=(.*?)(&|$)', 1)) user_id
    , regexp_extract(http_user_agent, 'musubi-webapp/[0-9\\.]* \\((.*)\\)', 1) machine_id
    , regexp_extract(http_user_agent, 'musubi-webapp/([0-9\\.]*)', 1) version
    , request_url
    , concat(year, '-', month, '-', day) ymd
    , logevent_timestamp
    , time_iso8601
    , elb_status_code
    FROM musubi.logs.proxy_logs
    WHERE true
    AND year = {year}
    AND month = {month}
    AND day = {day}
    AND regexp_extract(request_url, 'action=(.*?)(&|$)', 1) = 'open_patient_summary'
    )
    
SELECT
	p.id
	,p.name
	,version
	,ymd
	,count(*)
FROM log l
JOIN pharmacy p ON p.id = l.pharmacy_id
GROUP BY 1, 2, 3, 4
"""
)

# COMMAND ----------

# MAGIC %sql GRANT USAGE ON SCHEMA musubi.logs TO musubi_confidential;
# MAGIC GRANT
# MAGIC SELECT
# MAGIC   ON TABLE musubi.logs.silver_versions TO musubi_confidential;
