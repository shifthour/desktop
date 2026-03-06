# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:    Update the P_RUN_CYC table CDM load indicator fields to show those records have been copied to the Claim Data Mart.
# MAGIC       
# MAGIC PROCESSING:  Each IDS Claim source has a seperate SQL to extract the maximum run cycle value.  These run cycle values are used to update the IDS P_RUN_CYCLE, load indicator field for the particular source and subject.  All run cycles less than the maximum where the indicator is "N" are updated to "Y".
# MAGIC 
# MAGIC Called by :IdsWebDMDrugLoadSeq
# MAGIC   
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2006-04-07      Brent Leland            Original Programming.
# MAGIC 2006-05-08      Brent Leland            Added Argus SQL
# MAGIC 2006-07-03      Brent Leland            Changed SQL update statement.  Was incorrectly updating EDW_LOAD_IN.  Changed to update CDM_LOAD_IN.
# MAGIC 
# MAGIC                                                  Project/Altiris #                      Change Description                                                                    Development                 Code                          Date 
# MAGIC Developer          Date                                                                                                                                                                Project                           Reviewer                   Reviewed       
# MAGIC ------------------       ----------------       ------------------------                      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Sandrew           2008-10-03     3784(PBM)                            Added ESI as a Source to update CDM_LOAD_IN                      devlIDSnew                  Steph Goddard           10/28/2008
# MAGIC 
# MAGIC Srinidhi              2019-10-11   6131- PBM Replacement       Added new data source for OPTUMRX                                        IntegrateDev1                Kalyan Neelam            2019-11-25
# MAGIC Kambham
# MAGIC 
# MAGIC Reddy sanam  2020-09-14   US281070                              Added new data source for Lumeris and Medimpact                      IntegrateDev2                Kalyan Neelam            2020-09-15
# MAGIC 
# MAGIC Reddy sanam  2020-12-09   US329127                              Added new data source for EyeMed and DentaQuest                    IntegrateDev2                
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi 2021-05-05   US-377804                       Added new data source for LIVONGO(Lvnghlth)                            IntegrateDev2                 Jeyaprasanna             2021-05-06

# MAGIC Argus is only used when called by IdsWebDMDrugLoadSeq
# MAGIC ESI is only used when called by IdsWebDMDrugLoadSeq
# MAGIC Web DataMart Load Run Cycle Extract Update
# MAGIC Update the P_RUN_CYC table to indicate source system data has been loaded to the Web Data Mart for each of the run cycles processed.
# MAGIC Get list of source systems and the maximum run cycles for the source system from the driver table.
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


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_Facets_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD, max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'FACETS' GROUP BY SRC_SYS_CD")
    .load()
)

df_Nasco_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD, max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'NPS' GROUP BY SRC_SYS_CD")
    .load()
)

df_Argus_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD, max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'ARGUS' GROUP BY SRC_SYS_CD")
    .load()
)

df_ESI_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD, max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'ESI' GROUP BY SRC_SYS_CD")
    .load()
)

df_OPTUMRX_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD, max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'OPTUMRX' GROUP BY SRC_SYS_CD")
    .load()
)

df_MedImpact_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD, max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'MEDIMPACT' GROUP BY SRC_SYS_CD")
    .load()
)

df_Lumeris_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD, max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'LUMERIS' GROUP BY SRC_SYS_CD")
    .load()
)

df_EyeMed_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD, max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'EYEMED' GROUP BY SRC_SYS_CD")
    .load()
)

df_DentaQuest_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD, max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'DENTAQUEST' GROUP BY SRC_SYS_CD")
    .load()
)

df_Lvnghlth_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD, max(LAST_UPDT_RUN_CYC_EXCTN_SK) AS LAST_UPDT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.W_WEBDM_ETL_DRVR WHERE SRC_SYS_CD = 'LVNGHLTH' GROUP BY SRC_SYS_CD")
    .load()
)

df_Collector = (
    df_Facets_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK")
    .unionByName(df_Nasco_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .unionByName(df_Argus_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .unionByName(df_ESI_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .unionByName(df_OPTUMRX_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .unionByName(df_MedImpact_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .unionByName(df_Lumeris_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .unionByName(df_EyeMed_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .unionByName(df_DentaQuest_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .unionByName(df_Lvnghlth_Clm.select("SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK"))
)

df_Update = df_Collector.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit("CLAIM").alias("SUBJ_CD"),
    lit("IDS").alias("TRGT_SYS_CD"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("RUN_CYC_NO"),
    rpad(lit("Y"), 1, " ").alias("CDM_LOAD_IN")
)

execute_dml("DROP TABLE IF EXISTS STAGING.WebDmLoadRunCycUpd_P_RUN_CYC_temp", jdbc_url, jdbc_props)

df_Update.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.WebDmLoadRunCycUpd_P_RUN_CYC_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {IDSOwner}.P_RUN_CYC AS T
USING STAGING.WebDmLoadRunCycUpd_P_RUN_CYC_temp AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.SUBJ_CD = S.SUBJ_CD
    AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
    AND T.RUN_CYC_NO <= S.RUN_CYC_NO
    AND T.CDM_LOAD_IN = 'N')
WHEN MATCHED THEN
  UPDATE SET T.CDM_LOAD_IN = S.CDM_LOAD_IN
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, SUBJ_CD, TRGT_SYS_CD, RUN_CYC_NO, CDM_LOAD_IN)
  VALUES (S.SRC_SYS_CD, S.SUBJ_CD, S.TRGT_SYS_CD, S.RUN_CYC_NO, S.CDM_LOAD_IN);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)