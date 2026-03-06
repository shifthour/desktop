# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/12/07 10:00:11 Batch  14561_36015 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 08/27/07 08:55:46 Batch  14484_32151 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 06/07/07 15:06:33 Batch  14403_54395 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 05/23/07 13:53:34 Batch  14388_50029 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 05/23/07 13:45:45 Batch  14388_49559 INIT bckcett testIDS30 dsadm bls for sa
# MAGIC ^1_1 05/22/07 12:32:34 Batch  14387_45157 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_1 05/22/07 12:30:51 Batch  14387_45055 INIT bckcett devlIDS30 u10157 sa
# MAGIC 
# MAGIC **************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC                
# MAGIC JOB NAME:    FctsIncomePreReqHashFiles
# MAGIC CALLED BY:  FctsIncomePrereqSeq
# MAGIC             
# MAGIC PROCESSING:  Extract invoices from Facets and load into TMP_IDS_INCOME.
# MAGIC 
# MAGIC 
# MAGIC hf_fcts_incm_drvr_invc_chngd
# MAGIC hf_fcts_incm_drvr_dscrtny_chngd
# MAGIC hf_fcts_incm_drvr_subprem_chngd
# MAGIC hf_fcts_incm_drvr_invc_due
# MAGIC hf_fcts_incm_drvr_subprem_due
# MAGIC hf_fcts_incm_drvr_invoices
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC           05/01/2006-      Sharon Andrew             Originally Programmed
# MAGIC 
# MAGIC           03/21/2007-      Sharon Andrew             Re-Org'd to not pull in future due dated' invoices, to pull in invoices if subscriber premiums have been added, to pull in invoices that have been rebilled, 
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi  2021-03-24 358186    Changed Datatype length for field BLIV_ID                                                                                                                           IntegrateDev2
# MAGIC                                                                   char(12) to Varchar(15)
# MAGIC Prabhu ES             2022-03-02  S2S         MSSQL ODBC conn added and other param changes                                                                                                          IntegrateDev5	Ken Bradmon	2022-06-13

# MAGIC ** This job should run after loading the tmp_income table with invoice Id's 
# MAGIC ** The job groups  the Ids in tmp_income tables in to rebills and non rebills and puts them in thier respective hash files.
# MAGIC End Load of rebilled and non rebilled invoices into hash files
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value('DriverTable','')
FacetsOwnerParam = get_widget_value('$FacetsOwner','$PROJDEF')
facets_secret_name = get_widget_value('facets_secret_name','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

df_TmpTableIds = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT \n\nTmpInvc.BILL_INVC_ID\n\n\n\nFROM  \ntempdb..#DriverTable#  TmpInvc"
    )
    .load()
)

df_SummLastIds = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT \n\nTmpInvc.BILL_INVC_ID\n\nFROM  \ntempdb..#DriverTable#  TmpInvc,\n"
        + f"{FacetsOwnerParam}"
        + ".CMC_BLBL_BILL_SUMM SUMM \nWHERE \n                 TmpInvc.BILL_INVC_ID=SUMM.BLBL_BLIV_ID_LAST"
    )
    .load()
)

df_LastLkup = df_SummLastIds.dropDuplicates(["BILL_INVC_ID"])

df_RebillLogic = df_TmpTableIds.alias("TmpTableIds").join(
    df_LastLkup.alias("LastLkup"),
    F.col("TmpTableIds.BILL_INVC_ID") == F.col("LastLkup.BILL_INVC_ID"),
    "left"
)

df_NonRebills = df_RebillLogic.filter(F.col("LastLkup.BILL_INVC_ID").isNotNull())
df_NonRebills = df_NonRebills.withColumn("BILL_INVC_ID", trim(F.col("TmpTableIds.BILL_INVC_ID")))
df_NonRebills = df_NonRebills.withColumn(
    "BILL_INVC_ID",
    F.rpad(F.col("BILL_INVC_ID"), 50, " ")
)
df_NonRebills = df_NonRebills.select("BILL_INVC_ID")

df_Rebills = df_RebillLogic.filter(F.col("LastLkup.BILL_INVC_ID").isNull())
df_Rebills = df_Rebills.withColumn("BILL_INVC_ID", trim(F.col("TmpTableIds.BILL_INVC_ID")))
df_Rebills = df_Rebills.withColumn(
    "BILL_INVC_ID",
    F.rpad(F.col("BILL_INVC_ID"), 50, " ")
)
df_Rebills = df_Rebills.select("BILL_INVC_ID")

write_files(
    df_NonRebills,
    "hf_invc_non_rebills.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

write_files(
    df_Rebills,
    "hf_invc_rebills.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)