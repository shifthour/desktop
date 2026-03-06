# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC Description:This Job Loads the Aged Load file to EAM DISCREPANCY_ANALYSIS table. This Job only updates existing rows
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ======================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project               
# MAGIC ======================================================================================================================================
# MAGIC ReddySanam               2021-01-14                  US329820                                         Original Programming                                                    IntegrateDev2      Kalyan Neelam    2021-01-14
# MAGIC ReddySanam               2021-01-27                  US329820                                         Added Billing and MedPrdt Files                                    IntegrateDev2  Kalyan Neelam   2021-01-27
# MAGIC ReddySanam               2021-02-01                  US329820                                         Added MidMonth File                                                    IntegrateDev2   Jeyaprasanna    2021-02-01
# MAGIC ReddySanam               2021-02-03                  US329820                                         Added BLMM File                                                        IntegrateDev2    Jeyaprasanna    2021-02-04
# MAGIC ReddySanam               2021-02-06                  US329820                                         Added DntlPln File                                                       IntegrateDev2    Jeyaprasanna    2021-02-08
# MAGIC ReddySanam               2021-02-16                  US329820                                           Added EAM RxID TC 72 File                                     IntegrateDev2     Jeyaprasanna    2021-02-17
# MAGIC ReddySanam               2021-02-22                  US329820                                           Added AgentIF72 File                                                IntegrateDev2     Jeyaprasanna    2021-02-23
# MAGIC                                                                                                                                     Added LICS File     
# MAGIC JohnAbraham               2021-08-11                  US391328                  Added EAMRPT Env variables and updated appropriate tables         IntegrateDev1
# MAGIC                                                                                                                with these parameters
# MAGIC 
# MAGIC Arpitha V                     2024-03-20                        US 611997           Added ESRDSTRTDT_File,LISCOPAYLEVELCAT_File,                      IntegrateDev2      Jeyaprasanna   2024-06-24
# MAGIC                                                                                                                             LISENDDATE_File
# MAGIC 
# MAGIC Arpitha V                     2024-07-03             US 611748,611749,       Added ESRDENDDT_File,KDTSPSTRTDT_File,                                  IntegrateDev2      Jeyaprasanna    2024-08-02
# MAGIC                                                                             612321                               KDTSPSTRTDT_File, EFFDT_File

# MAGIC This Job Loads the Aged Load file to EAM DISCREPANCY_ANALYSIS table. This Job only updates existing rows
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, DateType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


EAMRPTOwner = get_widget_value('EAMRPTOwner','')
eamrpt_secret_name = get_widget_value('eamrpt_secret_name','')
RUNID = get_widget_value('RUNID','')

schema = StructType([
    StructField("ID_SK", IntegerType(), False),
    StructField("AGE", IntegerType(), False),
    StructField("LAST_UPDATED_DATE", DateType(), False)
])

cdf_curr = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_AGED.{RUNID}.dat")
)

cdf_lbill = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LBILL_AGED.{RUNID}.dat")
)

cdf_medpln = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_MEDLPLN_AGED.{RUNID}.dat")
)

cdf_midmonth = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_MIDM_AGED.{RUNID}.dat")
)

cdf_blmm = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_BLMM_AGED.{RUNID}.dat")
)

cdf_dntlpln = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_DNTLPLN_AGED.{RUNID}.dat")
)

cdf_rxidtc72 = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_RXIDTC72_AGED.{RUNID}.dat")
)

cdf_agnt = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_AGNT_AGED.{RUNID}.dat")
)

cdf_lics = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LICS_AGED.{RUNID}.dat")
)

cdf_esrdenddt = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_ESRDENDDT_AGED.{RUNID}.dat")
)

cdf_effdt = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_EFFDT_AGED.{RUNID}.dat")
)

cdf_kdtspstrtdt = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_KDTSPSTRTDT_AGED.{RUNID}.dat")
)

cdf_kdtspenddt = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_KDTSPENDDT_AGED.{RUNID}.dat")
)

cdf_lisenddate = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LISENDDATE_AGED.{RUNID}.dat")
)

cdf_esrdstrtdt = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_ESRDSTRTDT_AGED.{RUNID}.dat")
)

cdf_liscopaylevelcat = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LISCOPAYLEVELCAT_AGED.{RUNID}.dat")
)

cdf_medpln_f = cdf_medpln.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")
cdf_lbill_f = cdf_lbill.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")
cdf_curr_f = cdf_curr.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")
cdf_midmonth_f = cdf_midmonth.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")
cdf_blmm_f = cdf_blmm.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")
cdf_dntlpln_f = cdf_dntlpln.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")
cdf_rxidtc72_f = cdf_rxidtc72.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")
cdf_agnt_f = cdf_agnt.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")
cdf_lics_f = cdf_lics.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")
cdf_esrdstrtdt_f = cdf_esrdstrtdt.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")
cdf_esrdenddt_f = cdf_esrdenddt.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")
cdf_effdt_f = cdf_effdt.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")
cdf_liscopaylevelcat_f = cdf_liscopaylevelcat.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")
cdf_lisenddate_f = cdf_lisenddate.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")
cdf_kdtspstrtdt_f = cdf_kdtspstrtdt.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")
cdf_kdtspenddt_f = cdf_kdtspenddt.selectExpr("ID_SK as ID", "AGE", "LAST_UPDATED_DATE")

df_fnl = (
    cdf_medpln_f
    .unionByName(cdf_lbill_f)
    .unionByName(cdf_curr_f)
    .unionByName(cdf_midmonth_f)
    .unionByName(cdf_blmm_f)
    .unionByName(cdf_dntlpln_f)
    .unionByName(cdf_rxidtc72_f)
    .unionByName(cdf_agnt_f)
    .unionByName(cdf_lics_f)
    .unionByName(cdf_esrdstrtdt_f)
    .unionByName(cdf_esrdenddt_f)
    .unionByName(cdf_effdt_f)
    .unionByName(cdf_liscopaylevelcat_f)
    .unionByName(cdf_lisenddate_f)
    .unionByName(cdf_kdtspstrtdt_f)
    .unionByName(cdf_kdtspenddt_f)
)

df_fnl = df_fnl.select("ID", "AGE", "LAST_UPDATED_DATE")

jdbc_url, jdbc_props = get_db_config(eamrpt_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.FacetsEAMDiscAnalysisAgedlUpdt_DISC_ANALYSIS_temp",
    jdbc_url,
    jdbc_props
)

df_fnl.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.FacetsEAMDiscAnalysisAgedlUpdt_DISC_ANALYSIS_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {EAMRPTOwner}.DISCREPANCY_ANALYSIS AS T
USING STAGING.FacetsEAMDiscAnalysisAgedlUpdt_DISC_ANALYSIS_temp AS S
ON T.ID = S.ID
WHEN MATCHED THEN UPDATE SET
  T.AGE = S.AGE,
  T.LAST_UPDATED_DATE = S.LAST_UPDATED_DATE
WHEN NOT MATCHED THEN
  INSERT (ID, AGE, LAST_UPDATED_DATE)
  VALUES (S.ID, S.AGE, S.LAST_UPDATED_DATE);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)