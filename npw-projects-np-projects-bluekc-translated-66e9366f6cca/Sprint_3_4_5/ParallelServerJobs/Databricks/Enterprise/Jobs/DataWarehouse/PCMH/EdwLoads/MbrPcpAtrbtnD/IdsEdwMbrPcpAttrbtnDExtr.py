# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC 	   Pulls data from IDS and codes hash file to load into the EDW
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                     Date                   Project       Description                    Environment                      Code Reviewer                      Review Date
# MAGIC ----------------------------------         --------------------------------    -------------     ------------------------------------  -----------------------------        ------------------------------------       -----------------------------------
# MAGIC Santosh Bokka                      06/11/2013                            - Originally Program          - EdwNewDevl                  Kalyan Neelam                       2013-06-27
# MAGIC                                                                                                                                                                                Sharon Andrew                      2013-10-25   final review so the whole process can be pushed to test    
# MAGIC 
# MAGIC Pooja Sunkara                       03/27/2014                5114      Rewrite in Parallel            EnterpriseWrhsDevl        Bhoomi Dasari                         4/23/2014     
# MAGIC 
# MAGIC Santosh Bokka                     2014-06-11         4917             Added BCBSKC Runcyle 
# MAGIC                                                                                              to load TREO Historical file. EnterpriseNewDevl       Bhoomi Dasari                        6/23/2014 
# MAGIC 
# MAGIC Kalyan Neelam                  2015-05-27             5212 - PI        Added 3 new columns -         EnterpriseCurDevl     Bhoomi Dasari                        5/28/2015
# MAGIC                                                                                         ATTRBTN_BCBS_PLN_CD,
# MAGIC                                                                                        ATTRBTN_BCBS_PLN_CD_SK, ATTRBTN_BCBS_PLN_NM
# MAGIC                                                                                        and also added get extract IDS run cycle for BCBSA

# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK and LAST_UPDT_RUN_CYC_EXCTN_DT_SK
# MAGIC Job name:
# MAGIC IdsEdwMbrPcpAttrbtnDExtr
# MAGIC Extracts source data from the MBR_PCP_ATTRBTN and PROV tables in the IDS
# MAGIC Write MBR_PCP_ATTRBTN_D Data into a Sequential file for Load Ready Job.
# MAGIC Pull data from MBR_PCP_ATTRBTN
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql.functions import col, when, lit, length, rpad, row_number
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve job parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunID = get_widget_value('RunID','')
IDSBCBSKCRunCycle = get_widget_value('IDSBCBSKCRunCycle','')
IdsBCBSARunCycle = get_widget_value('IdsBCBSARunCycle','')

# Stage: db2_MBR_PCP_ATTRBTN_in (DB2ConnectorPX)
jdbc_url_db2_MBR_PCP_ATTRBTN_in, jdbc_props_db2_MBR_PCP_ATTRBTN_in = get_db_config(ids_secret_name)
extract_query_db2_MBR_PCP_ATTRBTN_in = f"""
SELECT 
MBR_PCP_ATTRBTN.MBR_PCP_ATTRBTN_SK,
MBR_PCP_ATTRBTN.MBR_UNIQ_KEY,
MBR_PCP_ATTRBTN.ROW_EFF_DT_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
MBR_PCP_ATTRBTN.CRT_RUN_CYC_EXCTN_SK,
MBR_PCP_ATTRBTN.LAST_UPDT_RUN_CYC_EXCTN_SK,
MBR_PCP_ATTRBTN.MBR_SK,
MBR_PCP_ATTRBTN.PROV_SK,
MBR_PCP_ATTRBTN.REL_GRP_PROV_SK,
MBR_PCP_ATTRBTN.COB_IN,
MBR_PCP_ATTRBTN.LAST_EVAL_AND_MNG_SVC_DT_SK,
MBR_PCP_ATTRBTN.INDV_BE_KEY,
MBR_PCP_ATTRBTN.MBR_PCP_MO_NO,
MBR_PCP_ATTRBTN.MED_HOME_ID,
MBR_PCP_ATTRBTN.MED_HOME_DESC,
MBR_PCP_ATTRBTN.MED_HOME_GRP_ID,
MBR_PCP_ATTRBTN.MED_HOME_GRP_DESC,
MBR_PCP_ATTRBTN.MED_HOME_LOC_ID,
MBR_PCP_ATTRBTN.MED_HOME_LOC_DESC,
MBR_PCP_ATTRBTN.ATTRBTN_BCBS_PLN_CD,
MBR_PCP_ATTRBTN.ATTRBTN_BCBS_PLN_CD_SK 
FROM 
{IDSOwner}.MBR_PCP_ATTRBTN MBR_PCP_ATTRBTN
LEFT JOIN 
{IDSOwner}.CD_MPPNG CD
ON MBR_PCP_ATTRBTN.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE 
((CD.TRGT_CD = 'TREO' and MBR_PCP_ATTRBTN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}) or 
 (CD.TRGT_CD = 'BCBSKC' and MBR_PCP_ATTRBTN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSBCBSKCRunCycle}) OR
 (CD.TRGT_CD = 'BCBSA' and MBR_PCP_ATTRBTN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IdsBCBSARunCycle}))
"""
df_db2_MBR_PCP_ATTRBTN_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_MBR_PCP_ATTRBTN_in)
    .options(**jdbc_props_db2_MBR_PCP_ATTRBTN_in)
    .option("query", extract_query_db2_MBR_PCP_ATTRBTN_in)
    .load()
)

# Stage: db2_CD_MPPNG_In (DB2ConnectorPX)
jdbc_url_db2_CD_MPPNG_In, jdbc_props_db2_CD_MPPNG_In = get_db_config(ids_secret_name)
extract_query_db2_CD_MPPNG_In = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM
{IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG_In)
    .options(**jdbc_props_db2_CD_MPPNG_In)
    .option("query", extract_query_db2_CD_MPPNG_In)
    .load()
)

# Stage: db2_PROV_In (DB2ConnectorPX)
jdbc_url_db2_PROV_In, jdbc_props_db2_PROV_In = get_db_config(ids_secret_name)
extract_query_db2_PROV_In = f"""
SELECT PROV.PROV_SK, PROV.PROV_ID, PROV.PROV_NM
FROM {IDSOwner}.PROV PROV
"""
df_db2_PROV_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_PROV_In)
    .options(**jdbc_props_db2_PROV_In)
    .option("query", extract_query_db2_PROV_In)
    .load()
)

# Stage: Cpy_PROV (PxCopy)
df_Cpy_PROV_in = df_db2_PROV_In

df_ref_prov_sk = df_Cpy_PROV_in.select(
    col("PROV_SK").alias("PROV_SK"),
    col("PROV_ID").alias("PROV_ID"),
    col("PROV_NM").alias("PROV_NM")
)

df_ref_rel_grp_prov_sk = df_Cpy_PROV_in.select(
    col("PROV_SK").alias("PROV_SK"),
    col("PROV_ID").alias("PROV_ID"),
    col("PROV_NM").alias("PROV_NM")
)

# Stage: Lkup_Codes (PxLookup)
df_Lkup_Codes = (
    df_db2_MBR_PCP_ATTRBTN_in.alias("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC")
    .join(
        df_ref_prov_sk.alias("ref_prov_sk"),
        col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.PROV_SK") == col("ref_prov_sk.PROV_SK"),
        "left"
    )
    .join(
        df_ref_rel_grp_prov_sk.alias("ref_rel_grp_prov_sk"),
        col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.REL_GRP_PROV_SK") == col("ref_rel_grp_prov_sk.PROV_SK"),
        "left"
    )
    .join(
        df_db2_CD_MPPNG_In.alias("ref_AttrbtnPlnCd"),
        col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.ATTRBTN_BCBS_PLN_CD_SK") == col("ref_AttrbtnPlnCd.CD_MPPNG_SK"),
        "left"
    )
)

df_Lkup_Codes_out = df_Lkup_Codes.select(
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.MBR_PCP_ATTRBTN_SK").alias("MBR_PCP_ATTRBTN_SK"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.MBR_SK").alias("MBR_SK"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.PROV_SK").alias("PROV_SK"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.REL_GRP_PROV_SK").alias("REL_GRP_PROV_SK"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.COB_IN").alias("COB_IN"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.LAST_EVAL_AND_MNG_SVC_DT_SK").alias("LAST_EVAL_AND_MNG_SVC_DT_SK"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.INDV_BE_KEY").alias("INDV_BE_KEY"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.MBR_PCP_MO_NO").alias("MBR_PCP_MO_NO"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.MED_HOME_ID").alias("MED_HOME_ID"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.MED_HOME_DESC").alias("MED_HOME_DESC"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.MED_HOME_GRP_ID").alias("MED_HOME_GRP_ID"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.MED_HOME_GRP_DESC").alias("MED_HOME_GRP_DESC"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.MED_HOME_LOC_ID").alias("MED_HOME_LOC_ID"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.MED_HOME_LOC_DESC").alias("MED_HOME_LOC_DESC"),
    col("ref_prov_sk.PROV_ID").alias("PROV_ID"),
    col("ref_prov_sk.PROV_NM").alias("PROV_NM"),
    col("ref_rel_grp_prov_sk.PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
    col("ref_rel_grp_prov_sk.PROV_NM").alias("PROV_REL_GRP_PROV_NM"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.ATTRBTN_BCBS_PLN_CD").alias("ATTRBTN_BCBS_PLN_CD"),
    col("ref_AttrbtnPlnCd.TRGT_CD").alias("ATTRBTN_BCBS_PLN_NM"),
    col("Ink_IdsEdwMbrPcpAttrbtnDExtr_inABC.ATTRBTN_BCBS_PLN_CD_SK").alias("ATTRBTN_BCBS_PLN_CD_SK")
)

# Stage: xmf_businessLogic (CTransformerStage)
# Add a seq column to replicate row-based constraints
w = Window.orderBy(lit(1))
df_xmf_businessLogic_in = df_Lkup_Codes_out.withColumn("seq", row_number().over(w))

# Rename columns that will be overwritten or needed as original
df_xmf_businessLogic_in_renamed = (
    df_xmf_businessLogic_in
    .withColumnRenamed("CRT_RUN_CYC_EXCTN_SK", "ORIG_CRT_RUN_CYC_EXCTN_SK")
    .withColumnRenamed("LAST_UPDT_RUN_CYC_EXCTN_SK", "ORIG_LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Main output link: Ink_Main_OutABC
df_Ink_Main_OutABC_temp = df_xmf_businessLogic_in_renamed.filter(
    (col("MBR_PCP_ATTRBTN_SK") != 0) & (col("MBR_PCP_ATTRBTN_SK") != 1)
)

df_Ink_Main_OutABC = df_Ink_Main_OutABC_temp.select(
    col("MBR_PCP_ATTRBTN_SK"),
    col("MBR_UNIQ_KEY"),
    col("ATTRBTN_BCBS_PLN_CD"),
    rpad(col("ROW_EFF_DT_SK"), 10, " ").alias("ROW_EFF_DT_SK"),
    when(col("SRC_SYS_CD").isNull(), lit("UNK")).otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    rpad(lit(EDWRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(lit(EDWRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("MBR_SK"),
    col("PROV_SK"),
    col("REL_GRP_PROV_SK"),
    when(col("ATTRBTN_BCBS_PLN_NM").isNull(), lit("UNK")).otherwise(col("ATTRBTN_BCBS_PLN_NM")).alias("ATTRBTN_BCBS_PLN_NM"),
    rpad(col("COB_IN"), 1, " ").alias("COB_IN"),
    rpad(col("LAST_EVAL_AND_MNG_SVC_DT_SK"), 10, " ").alias("LAST_EVAL_AND_MNG_SVC_DT_SK"),
    col("INDV_BE_KEY"),
    col("MBR_PCP_MO_NO"),
    col("MED_HOME_ID"),
    col("MED_HOME_DESC"),
    col("MED_HOME_GRP_ID"),
    col("MED_HOME_GRP_DESC"),
    col("MED_HOME_LOC_ID"),
    col("MED_HOME_LOC_DESC"),
    when((col("PROV_ID").isNull()) | (length(trim(col("PROV_ID"))) == 0), lit("UNK")).otherwise(col("PROV_ID")).alias("PROV_ID"),
    when((col("PROV_NM").isNull()) | (length(trim(col("PROV_NM"))) == 0), lit("UNK")).otherwise(col("PROV_NM")).alias("PROV_NM"),
    when((col("PROV_REL_GRP_PROV_ID").isNull()) | (length(trim(col("PROV_REL_GRP_PROV_ID"))) == 0), lit("UNK")).otherwise(col("PROV_REL_GRP_PROV_ID")).alias("PROV_REL_GRP_PROV_ID"),
    when((col("PROV_REL_GRP_PROV_NM").isNull()) | (length(trim(col("PROV_REL_GRP_PROV_NM"))) == 0), lit("UNK")).otherwise(col("PROV_REL_GRP_PROV_NM")).alias("PROV_REL_GRP_PROV_NM"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ORIG_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ATTRBTN_BCBS_PLN_CD_SK")
)

# NALink (constraint => single row, with constants)
df_NALink_temp = df_xmf_businessLogic_in_renamed.filter(col("seq") == 1)
df_NALink = df_NALink_temp.select(
    lit(1).alias("MBR_PCP_ATTRBTN_SK"),
    lit(1).alias("MBR_UNIQ_KEY"),
    lit("NA").alias("ATTRBTN_BCBS_PLN_CD"),
    rpad(lit("1753-01-01"), 10, " ").alias("ROW_EFF_DT_SK"),
    lit("NA").alias("SRC_SYS_CD"),
    rpad(lit("1753-01-01"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(lit("1753-01-01"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit(1).alias("MBR_SK"),
    lit(1).alias("PROV_SK"),
    lit(1).alias("REL_GRP_PROV_SK"),
    lit("NA").alias("ATTRBTN_BCBS_PLN_NM"),
    rpad(lit("N"), 1, " ").alias("COB_IN"),
    rpad(lit("1753-01-01"), 10, " ").alias("LAST_EVAL_AND_MNG_SVC_DT_SK"),
    lit(1).alias("INDV_BE_KEY"),
    lit(0).alias("MBR_PCP_MO_NO"),
    lit("NA").alias("MED_HOME_ID"),
    lit("").alias("MED_HOME_DESC"),
    lit("NA").alias("MED_HOME_GRP_ID"),
    lit("").alias("MED_HOME_GRP_DESC"),
    lit("NA").alias("MED_HOME_LOC_ID"),
    lit("").alias("MED_HOME_LOC_DESC"),
    lit("NA").alias("PROV_ID"),
    lit("NA").alias("PROV_NM"),
    lit("NA").alias("PROV_REL_GRP_PROV_ID"),
    lit("NA").alias("PROV_REL_GRP_PROV_NM"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("ATTRBTN_BCBS_PLN_CD_SK")
)

# UNKLink (constraint => single row, with constants)
df_UNKLink_temp = df_xmf_businessLogic_in_renamed.filter(col("seq") == 1)
df_UNKLink = df_UNKLink_temp.select(
    lit(0).alias("MBR_PCP_ATTRBTN_SK"),
    lit(0).alias("MBR_UNIQ_KEY"),
    lit("UNK").alias("ATTRBTN_BCBS_PLN_CD"),
    rpad(lit("1753-01-01"), 10, " ").alias("ROW_EFF_DT_SK"),
    lit("UNK").alias("SRC_SYS_CD"),
    rpad(lit("1753-01-01"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(lit("1753-01-01"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit(0).alias("MBR_SK"),
    lit(0).alias("PROV_SK"),
    lit(0).alias("REL_GRP_PROV_SK"),
    lit("UNK").alias("ATTRBTN_BCBS_PLN_NM"),
    rpad(lit("N"), 1, " ").alias("COB_IN"),
    rpad(lit("1753-01-01"), 10, " ").alias("LAST_EVAL_AND_MNG_SVC_DT_SK"),
    lit(0).alias("INDV_BE_KEY"),
    lit(0).alias("MBR_PCP_MO_NO"),
    lit("UNK").alias("MED_HOME_ID"),
    lit("").alias("MED_HOME_DESC"),
    lit("UNK").alias("MED_HOME_GRP_ID"),
    lit("").alias("MED_HOME_GRP_DESC"),
    lit("UNK").alias("MED_HOME_LOC_ID"),
    lit("").alias("MED_HOME_LOC_DESC"),
    lit("UNK").alias("PROV_ID"),
    lit("UNK").alias("PROV_NM"),
    lit("UNK").alias("PROV_REL_GRP_PROV_ID"),
    lit("UNK").alias("PROV_REL_GRP_PROV_NM"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("ATTRBTN_BCBS_PLN_CD_SK")
)

# Stage: Fnl_All (PxFunnel)
df_Fnl_All = (
    df_Ink_Main_OutABC.unionByName(df_NALink)
    .unionByName(df_UNKLink)
)

# Stage: MBR_PCP_ATTRBTN_D (PxSequentialFile)
# Write to .dat file with the specified properties
# The column order must match exactly, so do a final select in the same order:
df_final = df_Fnl_All.select(
    "MBR_PCP_ATTRBTN_SK",
    "MBR_UNIQ_KEY",
    "ATTRBTN_BCBS_PLN_CD",
    "ROW_EFF_DT_SK",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "MBR_SK",
    "PROV_SK",
    "REL_GRP_PROV_SK",
    "ATTRBTN_BCBS_PLN_NM",
    "COB_IN",
    "LAST_EVAL_AND_MNG_SVC_DT_SK",
    "INDV_BE_KEY",
    "MBR_PCP_MO_NO",
    "MED_HOME_ID",
    "MED_HOME_DESC",
    "MED_HOME_GRP_ID",
    "MED_HOME_GRP_DESC",
    "MED_HOME_LOC_ID",
    "MED_HOME_LOC_DESC",
    "PROV_ID",
    "PROV_NM",
    "PROV_REL_GRP_PROV_ID",
    "PROV_REL_GRP_PROV_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ATTRBTN_BCBS_PLN_CD_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/MBR_PCP_ATTRBTN_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)