# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:    Update the P_RUN_CYC table CDM load indicator fields to show those records have been copied to the Claim Data Mart.
# MAGIC 
# MAGIC       
# MAGIC PROCESSING:  Each IDS Claim source has a seperate SQL to extract the maximum run cycle value.  These run cycle values are used to update the IDS P_RUN_CYCLE, load indicator field for the particular source and subject.  All run cycles less than the maximum where the indicator is "N" are updated to "Y".
# MAGIC 
# MAGIC Called By: IdsClmMartCntl
# MAGIC   
# MAGIC MODIFICATIONS:
# MAGIC                                                  Project/Altiris #      Change Description                                                                    Development                 Code                          Date 
# MAGIC Developer          Date                                                                                                                                                Project                           Reviewer                   Reviewed       
# MAGIC ------------------       ----------------       ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC 2006-04-07      Brent Leland            Original Programming.
# MAGIC 2006-05-08      Brent Leland            Added Argus SQL
# MAGIC 2006-07-03      Brent Leland            Changed SQL update statement.  Was incorrectly updating EDW_LOAD_IN.  Changed to update CDM_LOAD_IN.
# MAGIC 
# MAGIC Sandrew           2008-10-03     3784(PBM)            Added ESI as a Source to update CDM_LOAD_IN                      devlIDSnew                  Steph Goddard           10/28/2008
# MAGIC 
# MAGIC Archana Palivela   2013-10-21  5114                    Original Programming.                                                                   IntegrateWrhsDevl        Jag Yelavarthi             2013-11-30       
# MAGIC                                                 
# MAGIC Deepa Bajaj       2019-10-11            6131- PBM   Added Added "OPTUMRX_Clm" SQL connector                         IntegrateDev5               Sandeep Kotturi          2019-10-11
# MAGIC                                                         Replacement
# MAGIC 
# MAGIC Reddy sanam  2020-09-14   US281070                              Added new data source for Lumeris and Medimpact                      IntegrateDev2     Kalyan Neelam         2020-09-15
# MAGIC 
# MAGIC Reddy sanam  2020-12-09   US329127                              Added new data source for EyeMed and DentaQuest                      IntegrateDev2   
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi   2021-05-05    US-377804                    Added new data source for Lvnghlth (LIVONGO)                            IntegrateDev2    Jeyaprasanna           2021-05-06
# MAGIC 
# MAGIC Reddy Sanam        2022-01-06     US471929                    Added new data sources for NationsBenifits                        IntegrateDev2               Jeyaprasanna           2022-01-10
# MAGIC 
# MAGIC Saranya                 2023-12-22     US 599810                   Added DB2 Stages for MARC and DOMINION                               IntegrateDev1   Jeyaprasanna            2023-12-26
# MAGIC 
# MAGIC Arpitha V               2024-03-21     US 614485                   Added DB2 Stage for NATIONBFTSFLEX                                IntegrateDev2          Jeyaprasanna            2024-03-28

# MAGIC Web DataMart Load Run Cycle Extract Update
# MAGIC Get list of source systems and the maximum run cycles for the source system from the driver table.
# MAGIC Argus is only used when called by IdsWebDMDrugLoadSeq
# MAGIC ESI is only used when called by IdsWebDMDrugLoadSeq
# MAGIC Update the P_RUN_CYC table to indicate source system data has been loaded to the Web Data Mart for each of the run cycles processed.
# MAGIC These Stages are related to Nations Benifits
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_Nasco_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'NPS' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_Argus_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'ARGUS' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_ESI_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'ESI' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_Facets_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'FACETS' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_OPTUMRX_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'OPTUMRX' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_Lumeris_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'LUMERIS' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_MedImpact_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'MEDIMPACT' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_EyeMed_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'EYEMED' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_DentaQuest_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'DENTAQUEST' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_Lvnghlth_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'LVNGHLTH' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_NATIONBFTSRWD_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'NATIONBFTSRWD' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_NATIONBFTSOTC_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'NATIONBFTSOTC' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_NATIONBFTSHRNG_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'NATIONBFTSHRNG' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_NATIONBFTSBBB_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'NATIONBFTSBBB' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_MARC_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'MARC' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_DOMINION_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'DOMINION' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_NATIONBFTSFLEX_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'NATIONBFTSFLEX' GROUP BY SRC_SYS_CD"
    )
    .load()
)

df_Funnel_13 = df_Facets_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK") \
.union(df_Nasco_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.union(df_Argus_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.union(df_ESI_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.union(df_OPTUMRX_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.union(df_MedImpact_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.union(df_Lumeris_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.union(df_EyeMed_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.union(df_DentaQuest_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.union(df_Lvnghlth_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.union(df_NATIONBFTSRWD_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.union(df_NATIONBFTSHRNG_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.union(df_NATIONBFTSOTC_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.union(df_NATIONBFTSBBB_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.union(df_MARC_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.union(df_DOMINION_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.union(df_NATIONBFTSFLEX_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK"))

df_Xmr = df_Funnel_13.withColumn("TRGT_SYS_CD", lit("IDS")) \
.withColumn("SUBJ_CD", lit("CLAIM")) \
.withColumn("RUN_CYC_NO", col("LAST_UPDT_RUN_CYC_EXCTN_SK")) \
.withColumn("CDM_LOAD_IN", lit("Y"))

df_P_RUN_CYC = df_Xmr.select(
    "TRGT_SYS_CD",
    "SRC_SYS_CD",
    "SUBJ_CD",
    "RUN_CYC_NO",
    rpad(col("CDM_LOAD_IN"), 1, " ").alias("CDM_LOAD_IN")
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.WebDmLoadRunCycUpd_EE_P_RUN_CYC_temp", jdbc_url, jdbc_props)

df_P_RUN_CYC.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.WebDmLoadRunCycUpd_EE_P_RUN_CYC_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.P_RUN_CYC AS T
USING STAGING.WebDmLoadRunCycUpd_EE_P_RUN_CYC_temp AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.SUBJ_CD = S.SUBJ_CD
    AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
    AND T.RUN_CYC_NO <= S.RUN_CYC_NO
    AND T.CDM_LOAD_IN = 'N'
WHEN MATCHED THEN
    UPDATE SET T.CDM_LOAD_IN = S.CDM_LOAD_IN
WHEN NOT MATCHED THEN
    INSERT (SRC_SYS_CD, SUBJ_CD, TRGT_SYS_CD, RUN_CYC_NO, CDM_LOAD_IN)
    VALUES (S.SRC_SYS_CD, S.SUBJ_CD, S.TRGT_SYS_CD, S.RUN_CYC_NO, S.CDM_LOAD_IN);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)