# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extracting data from MBR_ACCUM
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #                                  Change Description                                   Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------                          ---------------------------------------------------------        ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                11/27/2007       Membership/                                     Originally Programmed                                devlEDW10                  Steph Goddard           11/28/2007  
# MAGIC SAndrew                         2009-06-26       TTR539                                             Added IDS run cycle to extract                   devl EDW                    Steph Goddard            06/16/2009
# MAGIC Hugh Sisson                   2012-08-13        TTR-435                                           Added MBR_CAROVR_AMT and 
# MAGIC                                                                                                                           MBR_COB_OOP_AMT to end of row        EnterpriseNewDevl       Sanderw                        2012-08-20
# MAGIC 
# MAGIC Nagesh Bandi                 2013-07-10        5114-Enterprise Efficiencies              Move ids to edw for MBR ACCUM D       EnterpriseWrhseDevl     Sanderw                        2012-08-20
# MAGIC 
# MAGIC 
# MAGIC Karthik Chintalapani        2016-11-04     5634             Added new columns PLN_YR_EFF_DT and  PLN_YR_END_DT   EnterpriseDev2             NReynolsd                    2016-11-22

# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwMbrAccumDExtr
# MAGIC 
# MAGIC Table:
# MAGIC MBR_ACCUM_D
# MAGIC Read from source table MBR_ACCUM from IDS. Apply Run Cycle filters to get just the needed rows forward
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write MBR_ACCUM_D Data into a Sequential file for Load Job.
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys: 
# MAGIC CD_MPPNG_SK
# MAGIC Add Defaults and Null Handling
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
EDWRunCycle = get_widget_value('EDWRunCycle', '')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate', '')
IDSRunCycle = get_widget_value('IDSRunCycle', '')

# Configure JDBC for IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# db2_MBR_ACCUM_Extr
extract_query_db2_MBR_ACCUM_Extr = f"""
SELECT
  MBR_ACCUM.MBR_ACCUM_SK,
  COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
  MBR_ACCUM.MBR_UNIQ_KEY,
  MBR_ACCUM.PROD_ACCUM_ID,
  MBR_ACCUM.MBR_ACCUM_TYP_CD_SK,
  MBR_ACCUM.ACCUM_NO,
  MBR_ACCUM.YR_NO,
  MBR_ACCUM.GRP_SK,
  MBR_ACCUM.MBR_SK,
  MBR_ACCUM.ACCUM_AMT,
  MBR_ACCUM.CAROVR_AMT,
  MBR_ACCUM.COB_OOP_AMT,
  MBR_ACCUM.PLN_YR_EFF_DT,
  MBR_ACCUM.PLN_YR_END_DT
FROM {IDSOwner}.MBR_ACCUM MBR_ACCUM
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CD
  ON MBR_ACCUM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE MBR_ACCUM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
"""

df_db2_MBR_ACCUM_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_MBR_ACCUM_Extr)
    .load()
)

# db2_CD_MPPNG_Extr
extract_query_db2_CD_MPPNG_Extr = f"""
SELECT
  CD_MPPNG_SK,
  COALESCE(TRGT_CD,'UNK') TRGT_CD,
  COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

# db2_ACCUM
extract_query_db2_ACCUM = f"""
SELECT *
FROM {IDSOwner}.ACCUM
"""

df_db2_ACCUM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_ACCUM)
    .load()
)

# lkp_Codes (PxLookup)
df_lkp_Codes_temp = (
    df_db2_MBR_ACCUM_Extr.alias("lnk_MbrAccum_inABC")
    .join(
        df_db2_CD_MPPNG_Extr.alias("MbrAccum_CdLkup"),
        col("lnk_MbrAccum_inABC.MBR_ACCUM_TYP_CD_SK") == col("MbrAccum_CdLkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_ACCUM.alias("Lkp_Accum_Desc"),
        (
            (col("lnk_MbrAccum_inABC.ACCUM_NO") == col("Lkp_Accum_Desc.ACCUM_NO")) &
            (col("lnk_MbrAccum_inABC.MBR_ACCUM_TYP_CD_SK") == col("Lkp_Accum_Desc.ACCUM_TYP_CD_SK"))
        ),
        "left"
    )
)

df_lkp_Codes = df_lkp_Codes_temp.select(
    col("lnk_MbrAccum_inABC.MBR_ACCUM_SK").alias("MBR_ACCUM_SK"),
    col("lnk_MbrAccum_inABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_MbrAccum_inABC.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("lnk_MbrAccum_inABC.PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    col("lnk_MbrAccum_inABC.MBR_ACCUM_TYP_CD_SK").alias("MBR_ACCUM_TYP_CD_SK"),
    col("lnk_MbrAccum_inABC.ACCUM_NO").alias("ACCUM_NO"),
    col("lnk_MbrAccum_inABC.YR_NO").alias("YR_NO"),
    col("lnk_MbrAccum_inABC.GRP_SK").alias("GRP_SK"),
    col("lnk_MbrAccum_inABC.MBR_SK").alias("MBR_SK"),
    col("lnk_MbrAccum_inABC.ACCUM_AMT").alias("ACCUM_AMT"),
    col("lnk_MbrAccum_inABC.CAROVR_AMT").alias("CAROVR_AMT"),
    col("lnk_MbrAccum_inABC.COB_OOP_AMT").alias("COB_OOP_AMT"),
    col("MbrAccum_CdLkup.TRGT_CD").alias("MBR_ACCUM_TYP_CD"),
    col("MbrAccum_CdLkup.TRGT_CD_NM").alias("MBR_ACCUM_TYP_DESC"),
    col("lnk_MbrAccum_inABC.PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    col("lnk_MbrAccum_inABC.PLN_YR_END_DT").alias("PLN_YR_END_DT"),
    col("Lkp_Accum_Desc.ACCUM_DESC").alias("ACCUM_DESC")
)

# xfm_BusinessLogic (CTransformerStage)
df_xfm_BusinessLogic = (
    df_lkp_Codes
    .withColumn(
        "SRC_SYS_CD",
        when(trim(col("SRC_SYS_CD")) == "", lit("UNK")).otherwise(col("SRC_SYS_CD"))
    )
    .withColumn(
        "MBR_ACCUM_TYP_CD",
        when(trim(col("MBR_ACCUM_TYP_CD")) == "", lit("UNK")).otherwise(col("MBR_ACCUM_TYP_CD"))
    )
    .withColumn(
        "MBR_ACCUM_TYP_DESC",
        when(trim(col("MBR_ACCUM_TYP_DESC")) == "", lit("UNK")).otherwise(col("MBR_ACCUM_TYP_DESC"))
    )
    .withColumn(
        "MBR_CAROVR_AMT",
        when(
            trim(when(col("CAROVR_AMT").isNotNull(), col("CAROVR_AMT")).otherwise(lit(" "))) == "",
            lit(0.00)
        ).otherwise(col("CAROVR_AMT"))
    )
    .withColumn(
        "MBR_COB_OOP_AMT",
        when(
            trim(when(col("COB_OOP_AMT").isNotNull(), col("COB_OOP_AMT")).otherwise(lit(" "))) == "",
            lit(0.00)
        ).otherwise(col("COB_OOP_AMT"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    .withColumn("MBR_ACCUM_NO", col("ACCUM_NO"))
    .withColumn("MBR_ACCUM_YR_NO", col("YR_NO"))
    .withColumn("MBR_ACCUM_AMT", col("ACCUM_AMT"))
    .withColumn("PROD_ACCUM_DESC", col("ACCUM_DESC"))
)

df_xfm_BusinessLogic = df_xfm_BusinessLogic.select(
    "MBR_ACCUM_SK",
    "SRC_SYS_CD",
    "MBR_UNIQ_KEY",
    "PROD_ACCUM_ID",
    "MBR_ACCUM_TYP_CD",
    "MBR_ACCUM_NO",
    "MBR_ACCUM_YR_NO",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "GRP_SK",
    "MBR_SK",
    "MBR_ACCUM_AMT",
    "MBR_ACCUM_TYP_DESC",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_ACCUM_TYP_CD_SK",
    "MBR_CAROVR_AMT",
    "MBR_COB_OOP_AMT",
    "PLN_YR_EFF_DT",
    "PLN_YR_END_DT",
    "PROD_ACCUM_DESC"
)

# Adjust char/varchar columns with rpad for fixed length
df_xfm_BusinessLogic = (
    df_xfm_BusinessLogic
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
)

# seq_MBR_ACCUM_D_Load (PxSequentialFile)
write_files(
    df_xfm_BusinessLogic,
    f"{adls_path}/load/MBR_ACCUM_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)