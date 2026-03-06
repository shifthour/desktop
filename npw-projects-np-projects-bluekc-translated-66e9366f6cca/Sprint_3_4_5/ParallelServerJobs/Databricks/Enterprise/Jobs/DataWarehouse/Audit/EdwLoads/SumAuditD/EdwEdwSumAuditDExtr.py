# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:  
# MAGIC 
# MAGIC   Pulls data from EDW Working Audit table W_SUM_AUDIT into EDW table SUM_AUDIT_D
# MAGIC       
# MAGIC INPUTS:
# MAGIC                   W_SUM_AUDIT
# MAGIC                   GRP_D
# MAGIC                   MBR_D
# MAGIC                   SUB_D
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC               Parikshith   12/07/2006  ---    Originally Programmed
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               12/07/2007          3044                              Originally Programmed                           devlEDW10                  
# MAGIC 
# MAGIC Raj Mangalampally            05-08-2013            5114                             Original Programming                             EnterpriseWrhsDevl     Jag Yelavarthi               2013-10-18
# MAGIC                                                                                                              (Server to Parallel Conversion)   
# MAGIC 
# MAGIC Balkarn Gill                        11/20/2013          5114                             NA and UNK changes                            EnterpriseWrhsDevl
# MAGIC                                                                                                             are made as per the DG standards

# MAGIC Job:
# MAGIC EdwEdwSumAuditFExtr
# MAGIC Table:
# MAGIC  SUM_AUDIT_D         
# MAGIC This Job Will read the source Data from W_SUM_AUDIT table and summarizes Member counts
# MAGIC Read from source table; W_SUM_AUDIT;
# MAGIC Add Defaults with UNK and NA rows and Null Handling
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write this to Dataset into a PKEY job
# MAGIC Write SUM_AUDIT_F Dataset for the EdwEdwSumAuditFPkey Job.
# MAGIC 
# MAGIC Partitioning:Hash
# MAGIC Summarize the DP_RATE_AUDIT_D count, MBR_ELIG_AUDIT_D count, SUB_ADDR_AUDIT_D count, SUB_ELIG_AUDIT_D count, SUB_AUDIT_D count, MBR_AUDIT_D count, MBR_PCP_AUDIT_D count, MBR_COB_AUDIT_D count, SUB_CLS_AUDIT_D count
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
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query_db2_W_SUM_AUDIT_in = f"""SELECT 
W_SUM_AUDIT.SUB_SK,
W_SUM_AUDIT.MBR_SFX_NO,
W_SUM_AUDIT.SRC_SYS_CRT_DT_SK,
W_SUM_AUDIT.SRC_SYS_CRT_USER_ID,
GRP_D.GRP_SK,
GRP_D.GRP_ID,
MBR_D.MBR_SK 
FROM 
{EDWOwner}.W_SUM_AUDIT W_SUM_AUDIT,
{EDWOwner}.GRP_D GRP_D,
{EDWOwner}.MBR_D MBR_D,
{EDWOwner}.SUB_D SUB_D 
WHERE
W_SUM_AUDIT.SUB_SK=SUB_D.SUB_SK AND 
SUB_D.GRP_SK=GRP_D.GRP_SK AND 
SUB_D.SUB_SK=MBR_D.SUB_SK  AND 
W_SUM_AUDIT.MBR_SFX_NO =MBR_D.MBR_SFX_NO
"""

df_db2_W_SUM_AUDIT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_W_SUM_AUDIT_in)
    .load()
)

extract_query_db2_CNT_W_SUM_AUDIT_in = f"""SELECT 
W_SUM_AUDIT.SUB_SK,
W_SUM_AUDIT.MBR_SFX_NO,
W_SUM_AUDIT.SRC_SYS_CRT_DT_SK,
W_SUM_AUDIT.SRC_SYS_CRT_USER_ID,
W_SUM_AUDIT.MBR_PCP_AUDIT_CT,
W_SUM_AUDIT.SUB_ADDR_AUDIT_CT,
W_SUM_AUDIT.MBR_ELIG_AUDIT_CT,
W_SUM_AUDIT.SUB_CLS_AUDIT_CT,
W_SUM_AUDIT.SUB_AUDIT_CT,
W_SUM_AUDIT.MBR_AUDIT_CT,
W_SUM_AUDIT.DP_RATE_AUDIT_CT,
W_SUM_AUDIT.SUB_ELIG_AUDIT_CT,
W_SUM_AUDIT.MBR_COB_AUDIT_CT
FROM {EDWOwner}.W_SUM_AUDIT W_SUM_AUDIT
"""

df_db2_CNT_W_SUM_AUDIT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CNT_W_SUM_AUDIT_in)
    .load()
)

df_Agg_MbrPcpCnt = (
    df_db2_CNT_W_SUM_AUDIT_in
    .groupBy("SUB_SK","MBR_SFX_NO","SRC_SYS_CRT_DT_SK","SRC_SYS_CRT_USER_ID")
    .agg(
        F.sum("MBR_PCP_AUDIT_CT").alias("MBR_PCP_AUDIT_CT"),
        F.sum("MBR_AUDIT_CT").alias("MBR_AUDIT_CT"),
        F.sum("SUB_ADDR_AUDIT_CT").alias("SUB_ADDR_AUDIT_CT"),
        F.sum("MBR_ELIG_AUDIT_CT").alias("MBR_ELIG_AUDIT_CT"),
        F.sum("SUB_CLS_AUDIT_CT").alias("SUB_CLS_AUDIT_CT"),
        F.sum("SUB_AUDIT_CT").alias("SUB_AUDIT_CT"),
        F.sum("DP_RATE_AUDIT_CT").alias("DP_RATE_AUDIT_CT"),
        F.sum("MBR_COB_AUDIT_CT").alias("MBR_COB_AUDIT_CT"),
        F.sum("SUB_ELIG_AUDIT_CT").alias("SUB_ELIG_AUDIT_CT")
    )
)

df_lkp_CntSumAudit = (
    df_db2_W_SUM_AUDIT_in.alias("lnk_EdwEdwWSumAuditDExtr_InABC")
    .join(
        df_Agg_MbrPcpCnt.alias("lnk_WSumAuditCnt_out"),
        (
            (F.col("lnk_EdwEdwWSumAuditDExtr_InABC.SUB_SK") == F.col("lnk_WSumAuditCnt_out.SUB_SK"))
            & (F.col("lnk_EdwEdwWSumAuditDExtr_InABC.MBR_SFX_NO") == F.col("lnk_WSumAuditCnt_out.MBR_SFX_NO"))
            & (F.col("lnk_EdwEdwWSumAuditDExtr_InABC.SRC_SYS_CRT_DT_SK") == F.col("lnk_WSumAuditCnt_out.SRC_SYS_CRT_DT_SK"))
            & (F.col("lnk_EdwEdwWSumAuditDExtr_InABC.SRC_SYS_CRT_USER_ID") == F.col("lnk_WSumAuditCnt_out.SRC_SYS_CRT_USER_ID"))
        ),
        "left"
    )
    .select(
        F.col("lnk_EdwEdwWSumAuditDExtr_InABC.SUB_SK").alias("SUB_SK"),
        F.col("lnk_EdwEdwWSumAuditDExtr_InABC.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("lnk_EdwEdwWSumAuditDExtr_InABC.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        F.col("lnk_EdwEdwWSumAuditDExtr_InABC.SRC_SYS_CRT_USER_ID").alias("SRC_SYS_CRT_USER_ID"),
        F.col("lnk_EdwEdwWSumAuditDExtr_InABC.GRP_SK").alias("GRP_SK"),
        F.col("lnk_EdwEdwWSumAuditDExtr_InABC.GRP_ID").alias("GRP_ID"),
        F.col("lnk_EdwEdwWSumAuditDExtr_InABC.MBR_SK").alias("MBR_SK"),
        F.col("lnk_WSumAuditCnt_out.MBR_PCP_AUDIT_CT").alias("MBR_PCP_AUDIT_CT"),
        F.col("lnk_WSumAuditCnt_out.MBR_AUDIT_CT").alias("MBR_AUDIT_CT"),
        F.col("lnk_WSumAuditCnt_out.SUB_ADDR_AUDIT_CT").alias("SUB_ADDR_AUDIT_CT"),
        F.col("lnk_WSumAuditCnt_out.MBR_ELIG_AUDIT_CT").alias("MBR_ELIG_AUDIT_CT"),
        F.col("lnk_WSumAuditCnt_out.SUB_CLS_AUDIT_CT").alias("SUB_CLS_AUDIT_CT"),
        F.col("lnk_WSumAuditCnt_out.SUB_AUDIT_CT").alias("SUB_AUDIT_CT"),
        F.col("lnk_WSumAuditCnt_out.DP_RATE_AUDIT_CT").alias("DP_RATE_AUDIT_CT"),
        F.col("lnk_WSumAuditCnt_out.MBR_COB_AUDIT_CT").alias("MBR_COB_AUDIT_CT"),
        F.col("lnk_WSumAuditCnt_out.SUB_ELIG_AUDIT_CT").alias("SUB_ELIG_AUDIT_CT")
    )
)

df_xfm_BusinessLogic_main = (
    df_lkp_CntSumAudit
    .withColumn("svMbrPcpCnt", F.when(F.col("MBR_PCP_AUDIT_CT").isNull(), 0).otherwise(F.col("MBR_PCP_AUDIT_CT")).cast("int"))
    .withColumn("svMbrAuditCnt", F.when(F.col("MBR_AUDIT_CT").isNull(), 0).otherwise(F.col("MBR_AUDIT_CT")).cast("int"))
    .withColumn("svSubAddrCnt", F.when(F.col("SUB_ADDR_AUDIT_CT").isNull(), 0).otherwise(F.col("SUB_ADDR_AUDIT_CT")).cast("int"))
    .withColumn("svMbrEligCnt", F.when(F.col("MBR_ELIG_AUDIT_CT").isNull(), 0).otherwise(F.col("MBR_ELIG_AUDIT_CT")).cast("int"))
    .withColumn("svSubClsCnt", F.when(F.col("SUB_CLS_AUDIT_CT").isNull(), 0).otherwise(F.col("SUB_CLS_AUDIT_CT")).cast("int"))
    .withColumn("svSubCnt", F.when(F.col("SUB_AUDIT_CT").isNull(), 0).otherwise(F.col("SUB_AUDIT_CT")).cast("int"))
    .withColumn("svDpRateCnt", F.when(F.col("DP_RATE_AUDIT_CT").isNull(), 0).otherwise(F.col("DP_RATE_AUDIT_CT")).cast("int"))
    .withColumn("svMbrCobCnt", F.when(F.col("MBR_COB_AUDIT_CT").isNull(), 0).otherwise(F.col("MBR_COB_AUDIT_CT")).cast("int"))
    .withColumn("svSubEligCnt", F.when(F.col("SUB_ELIG_AUDIT_CT").isNull(), 0).otherwise(F.col("SUB_ELIG_AUDIT_CT")).cast("int"))
    .filter(
        "((SUB_SK <> 0 and MBR_SFX_NO <> 'UNK' and SRC_SYS_CRT_DT_SK <> '1753-01-01' and SRC_SYS_CRT_USER_ID <> 'UNK') or "
        "(SUB_SK <> 1 and MBR_SFX_NO <> 'NA' and SRC_SYS_CRT_DT_SK <> '1753-01-01' and SRC_SYS_CRT_USER_ID <> 'NA'))"
    )
    .withColumn("SUB_SK", F.col("SUB_SK").cast("string"))
    .withColumn("GRP_SK", F.col("GRP_SK").cast("string"))
    .withColumn("MBR_SK", F.col("MBR_SK").cast("string"))
    .withColumn("GRP_ID", F.col("GRP_ID").cast("string"))
    .select(
        F.col("SUB_SK").alias("SUB_SK"),
        F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
        F.col("SRC_SYS_CRT_USER_ID").alias("SRC_SYS_CRT_USER_ID"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.when(F.col("svDpRateCnt") > 0, F.lit("Y")).otherwise(F.lit("N")).alias("DP_RATE_AUDIT_IN"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.when(F.col("svMbrAuditCnt") > 0, F.lit("Y")).otherwise(F.lit("N")).alias("MBR_AUDIT_IN"),
        F.when(F.col("svMbrCobCnt") > 0, F.lit("Y")).otherwise(F.lit("N")).alias("MBR_COB_AUDIT_IN"),
        F.when(F.col("svMbrEligCnt") > 0, F.lit("Y")).otherwise(F.lit("N")).alias("MBR_ELIG_AUDIT_IN"),
        F.when(F.col("svMbrPcpCnt") > 0, F.lit("Y")).otherwise(F.lit("N")).alias("MBR_PCP_AUDIT_IN"),
        F.when(F.col("svSubAddrCnt") > 0, F.lit("Y")).otherwise(F.lit("N")).alias("SUB_ADDR_AUDIT_IN"),
        F.when(F.col("svSubCnt") > 0, F.lit("Y")).otherwise(F.lit("N")).alias("SUB_AUDIT_IN"),
        F.when(F.col("svSubClsCnt") > 0, F.lit("Y")).otherwise(F.lit("N")).alias("SUB_CLS_AUDIT_IN"),
        F.when(F.col("svSubEligCnt") > 0, F.lit("Y")).otherwise(F.lit("N")).alias("SUB_ELIG_AUDIT_IN"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_xfm_BusinessLogic_UNKRow = spark.createDataFrame(
    [
        (
            "0","",
            "1753-01-01","UNK",
            "1753-01-01","1753-01-01",
            "0","0",
            "N","UNK",
            "N","N","N","N","N","N","N","N",
            "100","100"
        )
    ],
    [
        "SUB_SK","MBR_SFX_NO","SRC_SYS_CRT_DT_SK","SRC_SYS_CRT_USER_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "GRP_SK","MBR_SK","DP_RATE_AUDIT_IN","GRP_ID","MBR_AUDIT_IN",
        "MBR_COB_AUDIT_IN","MBR_ELIG_AUDIT_IN","MBR_PCP_AUDIT_IN",
        "SUB_ADDR_AUDIT_IN","SUB_AUDIT_IN","SUB_CLS_AUDIT_IN","SUB_ELIG_AUDIT_IN",
        "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK"
    ]
)

df_xfm_BusinessLogic_NARow = spark.createDataFrame(
    [
        (
            "1","",
            "1753-01-01","NA",
            "1753-01-01","1753-01-01",
            "1","1",
            "N","NA",
            "N","N","N","N","N","N","N","N",
            "100","100"
        )
    ],
    [
        "SUB_SK","MBR_SFX_NO","SRC_SYS_CRT_DT_SK","SRC_SYS_CRT_USER_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "GRP_SK","MBR_SK","DP_RATE_AUDIT_IN","GRP_ID","MBR_AUDIT_IN",
        "MBR_COB_AUDIT_IN","MBR_ELIG_AUDIT_IN","MBR_PCP_AUDIT_IN",
        "SUB_ADDR_AUDIT_IN","SUB_AUDIT_IN","SUB_CLS_AUDIT_IN","SUB_ELIG_AUDIT_IN",
        "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK"
    ]
)

df_fnl_UNK_NA = (
    df_xfm_BusinessLogic_main
    .union(df_xfm_BusinessLogic_UNKRow)
    .union(df_xfm_BusinessLogic_NARow)
)

df_final = df_fnl_UNK_NA.select(
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.rpad(F.col("SRC_SYS_CRT_DT_SK"),10," ").alias("SRC_SYS_CRT_DT_SK"),
    F.col("SRC_SYS_CRT_USER_ID").alias("SRC_SYS_CRT_USER_ID"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"),10," ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),10," ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.rpad(F.col("DP_RATE_AUDIT_IN"),1," ").alias("DP_RATE_AUDIT_IN"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.rpad(F.col("MBR_AUDIT_IN"),1," ").alias("MBR_AUDIT_IN"),
    F.rpad(F.col("MBR_COB_AUDIT_IN"),1," ").alias("MBR_COB_AUDIT_IN"),
    F.rpad(F.col("MBR_ELIG_AUDIT_IN"),1," ").alias("MBR_ELIG_AUDIT_IN"),
    F.rpad(F.col("MBR_PCP_AUDIT_IN"),1," ").alias("MBR_PCP_AUDIT_IN"),
    F.rpad(F.col("SUB_ADDR_AUDIT_IN"),1," ").alias("SUB_ADDR_AUDIT_IN"),
    F.rpad(F.col("SUB_AUDIT_IN"),1," ").alias("SUB_AUDIT_IN"),
    F.rpad(F.col("SUB_CLS_AUDIT_IN"),1," ").alias("SUB_CLS_AUDIT_IN"),
    F.rpad(F.col("SUB_ELIG_AUDIT_IN"),1," ").alias("SUB_ELIG_AUDIT_IN"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_final,
    f"{adls_path}/ds/SUM_AUDIT_D.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)