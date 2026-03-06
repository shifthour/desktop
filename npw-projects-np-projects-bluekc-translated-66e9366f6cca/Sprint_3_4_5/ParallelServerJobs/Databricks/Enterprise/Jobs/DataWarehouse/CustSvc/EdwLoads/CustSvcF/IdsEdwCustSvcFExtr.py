# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                                                                                                 Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------     ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty                2/14/2007           Cust Svc/3028           Originally Programmed                                                                                                                                             devlEDW10              
# MAGIC Bhoomi Dasari                 01/27/2009         IAD Prod Supp/15       Added  MPPNG.TRGT_CD <> 'IDCARD'
# MAGIC 	     	     	     	     	     	         AND CS.RCVD_DT_SK <> '1753-01-01'
# MAGIC 	     	     	     	  	     	         in all lkup's to add new logic to TASK_AGE.                                                                                                           devlEDW                  Steph Goddard               02/02/2009
# MAGIC                                                                                                          to be used in deriving TASK_AGE
# MAGIC SAndrew                         2009-05-26          TTR                          In all Min and Max Date extractions, excluded cust srv type of MBR.  Just like Bhoomi's changes marked above,     devlEDW                  Steph Goddard               05/28/2009
# MAGIC                                                                                                     this is for more accurately deriving the customer services' task age.                                                                             
# MAGIC                                                                                                      Now using stage variables to do so
# MAGIC 
# MAGIC Bhupinder Kaur         12/17/2013        5114                               Create Load File for EDW Table CUST_SVC_F                                                                                                   EnterpriseWhseDevl           Jag Yelavarthi                2014-01-29

# MAGIC Job: IdsEdwCustSvcFExtr
# MAGIC Business logic and null handling
# MAGIC Code SK lookups for Denormalization
# MAGIC Write CUST_SVC_F Data into a Sequential file for Load Job IdsEdwCustSvcFLoad.
# MAGIC InputDtm Field is converted from timestamp to date
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

# Retrieve DB config
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Stage: db2_CD_MPPNG_Extr
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT  CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from {IDSOwner}.CD_MPPNG"
    )
    .load()
)

# Stage: db2_MinRcvdDt_in
df_db2_MinRcvdDt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT
  CS.CUST_SVC_SK,
  min(CS.RCVD_DT_SK) as RCVD_DT_SK
FROM {IDSOwner}.CUST_SVC_TASK CS,
     {IDSOwner}.W_CUST_SVC_DRVR DRVR,
     {IDSOwner}.CD_MPPNG MPPNG
WHERE CS.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
  AND CS.CUST_SVC_ID = DRVR.CUST_SVC_ID
  AND CS.CUST_SVC_TASK_PG_TYP_CD_SK = MPPNG.CD_MPPNG_SK
  AND MPPNG.TRGT_CD <> 'IDCARD'
  AND MPPNG.TRGT_CD <> 'MBR'
  AND CS.RCVD_DT_SK <> '1753-01-01'
GROUP BY CS.CUST_SVC_SK
"""
    )
    .load()
)

# Stage: db2_MinClsdDt_in
df_db2_MinClsdDt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT
  TASK.CUST_SVC_SK,
  min(TASK.CLSD_DT_SK) CLSD_DT_SK
FROM {IDSOwner}.CUST_SVC SVC,
     {IDSOwner}.CUST_SVC_TASK TASK,
     {IDSOwner}.W_CUST_SVC_DRVR DRVR,
     {IDSOwner}.CD_MPPNG MPPNG
WHERE SVC.CUST_SVC_SK = TASK.CUST_SVC_SK
  AND SVC.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
  AND SVC.CUST_SVC_ID = DRVR.CUST_SVC_ID
  AND TASK.CUST_SVC_TASK_PG_TYP_CD_SK = MPPNG.CD_MPPNG_SK
  AND MPPNG.TRGT_CD <> 'IDCARD'
  AND MPPNG.TRGT_CD <> 'MBR'
  AND TASK.RCVD_DT_SK <> '1753-01-01'
GROUP BY TASK.CUST_SVC_SK
"""
    )
    .load()
)

# Stage: db2_MaxClsdDt_in
df_db2_MaxClsdDt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT
  CS.CUST_SVC_SK,
  MAX(CS.CLSD_DT_SK) as CLSD_DT_SK
FROM {IDSOwner}.CUST_SVC_TASK CS,
     {IDSOwner}.W_CUST_SVC_DRVR DRVR,
     {IDSOwner}.CD_MPPNG MPPNG
WHERE CS.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
  AND CS.CUST_SVC_ID = DRVR.CUST_SVC_ID
  AND CS.CUST_SVC_TASK_PG_TYP_CD_SK = MPPNG.CD_MPPNG_SK
  AND MPPNG.TRGT_CD <> 'IDCARD'
  AND MPPNG.TRGT_CD <> 'MBR'
  AND CS.RCVD_DT_SK <> '1753-01-01'
GROUP BY CS.CUST_SVC_SK
"""
    )
    .load()
)

# Stage: db2_CUST_SVC_F_in
df_db2_CUST_SVC_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT
  CS.CUST_SVC_SK,
  CS.CUST_SVC_ID,
  CS.CRT_RUN_CYC_EXCTN_SK,
  CS.SRC_SYS_CD_SK
FROM {IDSOwner}.CUST_SVC CS,
     {IDSOwner}.W_CUST_SVC_DRVR DRVR
WHERE CS.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
  AND CS.CUST_SVC_ID = DRVR.CUST_SVC_ID
"""
    )
    .load()
)

# Stage: db2_MinInptDtm_in
df_db2_MinInptDtm_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT
  TASK.CUST_SVC_SK,
  min(TASK.INPT_DTM) INPT_DTM
FROM {IDSOwner}.CUST_SVC SVC,
     {IDSOwner}.CUST_SVC_TASK TASK,
     {IDSOwner}.W_CUST_SVC_DRVR DRVR,
     {IDSOwner}.CD_MPPNG MPPNG
WHERE SVC.CUST_SVC_SK = TASK.CUST_SVC_SK
  AND SVC.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
  AND SVC.CUST_SVC_ID = DRVR.CUST_SVC_ID
  AND TASK.CUST_SVC_TASK_PG_TYP_CD_SK = MPPNG.CD_MPPNG_SK
  AND MPPNG.TRGT_CD <> 'IDCARD'
  AND MPPNG.TRGT_CD <> 'MBR'
  AND TASK.RCVD_DT_SK <> '1753-01-01'
GROUP BY TASK.CUST_SVC_SK
"""
    )
    .load()
)

# Stage: xfrm_IptDtm
df_ref_MinInptDtm = df_db2_MinInptDtm_in.select(
    F.col("CUST_SVC_SK"),
    TimestampToDate(F.col("INPT_DTM")).alias("INPT_DTM")
)

# Stage: Add_lkp_Codes (PxLookup with left joins)
df_Add_lkp_Codes = (
    df_db2_CUST_SVC_F_in.alias("lnk_IdsEdwCustSvcF_Extr_In")
    .join(
        df_db2_MinRcvdDt_in.alias("ref_MinRcvdDt"),
        F.col("lnk_IdsEdwCustSvcF_Extr_In.CUST_SVC_SK") == F.col("ref_MinRcvdDt.CUST_SVC_SK"),
        "left"
    )
    .join(
        df_db2_MinClsdDt_in.alias("ref_MinClsdDt"),
        F.col("lnk_IdsEdwCustSvcF_Extr_In.CUST_SVC_SK") == F.col("ref_MinClsdDt.CUST_SVC_SK"),
        "left"
    )
    .join(
        df_db2_MaxClsdDt_in.alias("ref_MaxClsdDt"),
        F.col("lnk_IdsEdwCustSvcF_Extr_In.CUST_SVC_SK") == F.col("ref_MaxClsdDt.CUST_SVC_SK"),
        "left"
    )
    .join(
        df_ref_MinInptDtm.alias("ref_MinInptDtm"),
        F.col("lnk_IdsEdwCustSvcF_Extr_In.CUST_SVC_SK") == F.col("ref_MinInptDtm.CUST_SVC_SK"),
        "left"
    )
    .join(
        df_db2_CD_MPPNG_Extr.alias("ref_SrcSysCd"),
        F.col("lnk_IdsEdwCustSvcF_Extr_In.SRC_SYS_CD_SK") == F.col("ref_SrcSysCd.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwCustSvcF_Extr_In.CUST_SVC_SK").alias("CUST_SVC_SK"),
        F.col("lnk_IdsEdwCustSvcF_Extr_In.CUST_SVC_ID").alias("CUST_SVC_ID"),
        F.col("lnk_IdsEdwCustSvcF_Extr_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("ref_SrcSysCd.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("ref_MinInptDtm.INPT_DTM").alias("INPT_DTM"),
        F.col("ref_MinRcvdDt.RCVD_DT_SK").alias("RCVD_DT_SK"),
        F.col("ref_MinClsdDt.CLSD_DT_SK").alias("CLSD_DT_SK_MIN"),
        F.col("ref_MaxClsdDt.CLSD_DT_SK").alias("CLSD_DT_SK_MAX")
    )
)

# Stage: xfrm_Business_rule
# Add stage variables
df_xfrm_Business_rule = df_Add_lkp_Codes
df_xfrm_Business_rule = df_xfrm_Business_rule.withColumn(
    "svRcvdDt",
    F.when(trim(F.col("RCVD_DT_SK")) == "", F.lit("1753-01-01"))
     .otherwise(F.col("RCVD_DT_SK"))
)
df_xfrm_Business_rule = df_xfrm_Business_rule.withColumn(
    "svClsdDt",
    F.when(trim(F.col("CLSD_DT_SK_MIN")) == "", F.lit("1753-01-01"))
     .otherwise(F.col("CLSD_DT_SK_MIN"))
)
df_xfrm_Business_rule = df_xfrm_Business_rule.withColumn(
    "svClsdDtMax",
    F.when(trim(F.col("CLSD_DT_SK_MAX")) == "", F.lit("1753-01-01"))
     .otherwise(F.col("CLSD_DT_SK_MAX"))
)
df_xfrm_Business_rule = df_xfrm_Business_rule.withColumn(
    "svMinDate",
    F.when(
        F.col("svRcvdDt") == "1753-01-01", 
        F.col("INPT_DTM")
    )
    .when(
        F.col("INPT_DTM") > F.col("svRcvdDt"), 
        F.col("svRcvdDt")
    )
    .otherwise(F.col("INPT_DTM"))
)

# svAge logic
# Expression from DataStage:
# if svClsdDt = '1753-01-01' and ((( if svRcvdDt = '1753-01-01' then 0 else DaysSinceFromDate(INPT_DTM, svRcvdDt)) + 1) < 0)
# then (DaysSinceFromDate(EDWRunCycleDate, INPT_DTM)) + 1
# else if svClsdDt = '1753-01-01' and ((( if svRcvdDt = '1753-01-01' then 0 else DaysSinceFromDate(INPT_DTM, svRcvdDt)) + 1) > (-1))
# then if svRcvdDt = '1753-01-01' then 0 else (DaysSinceFromDate(EDWRunCycleDate, svRcvdDt)) + 1
# else if svClsdDt <> '1753-01-01'
# then ( if svClsdDtMax = '1753-01-01' then 0 else DaysSinceFromDate(svClsdDtMax, svMinDate)) + 1
# else 0

df_xfrm_Business_rule = df_xfrm_Business_rule.withColumn(
    "tempDaysSinceRcvd",
    F.when(
        F.col("svRcvdDt") == "1753-01-01",
        F.lit(0)
    ).otherwise(DaysSinceFromDate(F.col("INPT_DTM"), F.col("svRcvdDt")))
)
df_xfrm_Business_rule = df_xfrm_Business_rule.withColumn(
    "condPartA",
    df_xfrm_Business_rule["tempDaysSinceRcvd"] + F.lit(1)
)
df_xfrm_Business_rule = df_xfrm_Business_rule.withColumn(
    "tempDaysSinceClsdMax",
    F.when(
        F.col("svClsdDtMax") == "1753-01-01",
        F.lit(0)
    ).otherwise(DaysSinceFromDate(F.col("svClsdDtMax"), F.col("svMinDate")))
)

df_xfrm_Business_rule = df_xfrm_Business_rule.withColumn(
    "svAge",
    F.when(
        (F.col("svClsdDt") == "1753-01-01") & (F.col("condPartA") < 0),
        DaysSinceFromDate(F.lit(EDWRunCycleDate), F.col("INPT_DTM")) + F.lit(1)
    ).when(
        (F.col("svClsdDt") == "1753-01-01") & (F.col("condPartA") > -1),
        F.when(
            F.col("svRcvdDt") == "1753-01-01",
            F.lit(0)
        ).otherwise(
            DaysSinceFromDate(F.lit(EDWRunCycleDate), F.col("svRcvdDt")) + F.lit(1)
        )
    ).when(
        F.col("svClsdDt") != "1753-01-01",
        F.col("tempDaysSinceClsdMax") + F.lit(1)
    ).otherwise(F.lit(0))
)

# Prepare for the multiple output links (constraints)
# We can create a row_number for the entire dataset to replicate the @INROWNUM=1 logic
window_spec = Window.orderBy(F.lit(1))
df_xfrm_Business_rule = df_xfrm_Business_rule.withColumn(
    "row_num",
    F.row_number().over(window_spec)
)

# lnk_Full_Data_Out => constraint: CUST_SVC_SK <> 0 AND CUST_SVC_SK <> 1
df_lnk_Full_Data_Out = df_xfrm_Business_rule.filter(
    (F.col("CUST_SVC_SK") != 0) & (F.col("CUST_SVC_SK") != 1)
).select(
    F.col("CUST_SVC_SK").alias("CUST_SVC_SK"),
    F.when(
        F.col("SRC_SYS_CD").isNull() | (F.length(trim(F.col("SRC_SYS_CD"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("CUST_SVC_ID").alias("CUST_SVC_ID"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("svAge").alias("CUST_SVC_AGE"),
    F.lit(1).alias("CUST_SVC_CT"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# lnk_UNK_Out => constraint: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1
# We interpret that as row_num == 1
df_lnk_UNK_Out = df_xfrm_Business_rule.filter(F.col("row_num") == 1).select(
    F.lit(0).alias("CUST_SVC_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("CUST_SVC_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("CUST_SVC_AGE"),
    F.lit(0).alias("CUST_SVC_CT"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# lnk_NA_Out => also row_num == 1
df_lnk_NA_Out = df_xfrm_Business_rule.filter(F.col("row_num") == 1).select(
    F.lit(1).alias("CUST_SVC_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("CUST_SVC_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("CUST_SVC_AGE"),
    F.lit(0).alias("CUST_SVC_CT"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Stage: Fnl_UNK_NA_data (PxFunnel)
# Combine the three data frames in the order: lnk_NA_Out, lnk_UNK_Out, lnk_Full_Data_Out
# All have the same schema
df_funnel = df_lnk_NA_Out.union(df_lnk_UNK_Out).union(df_lnk_Full_Data_Out)

# We must rpad columns of type char(10)
df_funnel = df_funnel.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

# Stage: seq_CUST_SVC_F_csv_load
# Write to: #$FilePath#/load/CUST_SVC_F.dat => => f"{adls_path}/load/CUST_SVC_F.dat"
output_file_path = f"{adls_path}/load/CUST_SVC_F.dat"

# Final select to maintain column order exactly as the funnel's output
df_final = df_funnel.select(
    "CUST_SVC_SK",
    "SRC_SYS_CD",
    "CUST_SVC_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CUST_SVC_AGE",
    "CUST_SVC_CT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

write_files(
    df_final,
    output_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# End of Job