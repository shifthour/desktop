# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          ClmRIClmHmoClmProvBalSeq (Multiple Instance)
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                      Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               10/22/2007          3264                              Originally Programmed                                                      devlIDS30                                
# MAGIC  
# MAGIC Manasa Andru                   12/21/2011       TTR- 1036               Added null check conditions in the                                      IntegrateCurDevl            SAndrew                    2012-01-02
# MAGIC                                                                                                    transformers in the Notify output links
# MAGIC 
# MAGIC Manasa Andru                   01/13/2012       TTR- 1036         Changed the null check conditions in the  transformers             IntegrateCurDevl            SAndrew                   2012-01-19
# MAGIC                                                                                           in the Notify output links to give notification when needed.

# MAGIC Rows with Failed Comparison on Specified columns
# MAGIC Detail files for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC Detail files for on-call to research errors
# MAGIC File checked later for rows and email to on-call
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")
RunID = get_widget_value("RunID","")
SrcSysCd = get_widget_value("SrcSysCd","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# =============================================================================
# Stage: SrcTrgtRowComp (DB2Connector) - Pin Pkey
# =============================================================================
query_SrcTrgtRowComp_Pkey = f"""
SELECT
CLM.CLM_SK AS CLM_CLM_SK,
CLM_PROV.CLM_SK AS CLM_PROV_CLM_SK
FROM 
{IDSOwner}.CLM CLM FULL OUTER JOIN {IDSOwner}.CLM_PROV CLM_PROV
ON CLM.CLM_SK = CLM_PROV.CLM_SK,
{IDSOwner}.NTWK NTWK,
{IDSOwner}.CD_MPPNG MPPNG1,
{IDSOwner}.CD_MPPNG MPPNG2,
{IDSOwner}.CD_MPPNG MPPNG3,
{IDSOwner}.CD_MPPNG MPPNG4
WHERE 
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CLM.SRC_SYS_CD_SK = MPPNG1.CD_MPPNG_SK AND MPPNG1.TRGT_CD = '{SrcSysCd}'
AND CLM.CLM_STTUS_CD_SK = MPPNG2.CD_MPPNG_SK AND MPPNG2.TRGT_CD IN ('A02','A08','A09')
AND CLM.NTWK_SK = NTWK.NTWK_SK AND NTWK.NTWK_TYP_CD_SK = MPPNG3.CD_MPPNG_SK AND MPPNG3.TRGT_CD = 'HMO'
AND CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = MPPNG4.CD_MPPNG_SK AND MPPNG4.TRGT_CD = 'PCP'
"""
df_SrcTrgtRowComp_Pkey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtRowComp_Pkey)
    .load()
)

# =============================================================================
# Stage: SrcTrgtRowComp (DB2Connector) - Pin Match
# =============================================================================
query_SrcTrgtRowComp_Match = f"""
SELECT
CLM.SRC_SYS_CD_SK AS CLM_SRC_SYS_CD_SK,
CLM.CLM_ID AS CLM_CLM_ID,
CLM_PROV.SRC_SYS_CD_SK AS CLM_PROV_SRC_SYS_CD_SK,
CLM_PROV.CLM_ID AS CLM_PROV_CLM_ID
FROM 
{IDSOwner}.CLM CLM INNER JOIN {IDSOwner}.CLM_PROV CLM_PROV
ON CLM.SRC_SYS_CD_SK = CLM_PROV.SRC_SYS_CD_SK
AND CLM.CLM_ID = CLM_PROV.CLM_ID,
{IDSOwner}.NTWK NTWK,
{IDSOwner}.CD_MPPNG MPPNG1,
{IDSOwner}.CD_MPPNG MPPNG2,
{IDSOwner}.CD_MPPNG MPPNG3,
{IDSOwner}.CD_MPPNG MPPNG4
WHERE 
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CLM.SRC_SYS_CD_SK = MPPNG1.CD_MPPNG_SK AND MPPNG1.TRGT_CD = '{SrcSysCd}'
AND CLM.CLM_STTUS_CD_SK = MPPNG2.CD_MPPNG_SK AND MPPNG2.TRGT_CD IN ('A02','A08','A09')
AND CLM.NTWK_SK = NTWK.NTWK_SK AND NTWK.NTWK_TYP_CD_SK = MPPNG3.CD_MPPNG_SK AND MPPNG3.TRGT_CD = 'HMO'
AND CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = MPPNG4.CD_MPPNG_SK AND MPPNG4.TRGT_CD = 'PCP'
"""
df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtRowComp_Match)
    .load()
)

# =============================================================================
# Stage: SrcTrgtRowComp (DB2Connector) - Pin NatKey
# =============================================================================
query_SrcTrgtRowComp_NatKey = f"""
SELECT
CLM.SRC_SYS_CD_SK AS CLM_SRC_SYS_CD_SK,
CLM.CLM_ID AS CLM_CLM_ID,
CLM_PROV.SRC_SYS_CD_SK AS CLM_PROV_SRC_SYS_CD_SK,
CLM_PROV.CLM_ID AS CLM_PROV_CLM_ID
FROM 
{IDSOwner}.CLM CLM FULL OUTER JOIN {IDSOwner}.CLM_PROV CLM_PROV
ON CLM.SRC_SYS_CD_SK = CLM_PROV.SRC_SYS_CD_SK
AND CLM.CLM_ID = CLM_PROV.CLM_ID,
{IDSOwner}.NTWK NTWK,
{IDSOwner}.CD_MPPNG MPPNG1,
{IDSOwner}.CD_MPPNG MPPNG2,
{IDSOwner}.CD_MPPNG MPPNG3,
{IDSOwner}.CD_MPPNG MPPNG4
WHERE 
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CLM.SRC_SYS_CD_SK = MPPNG1.CD_MPPNG_SK AND MPPNG1.TRGT_CD = '{SrcSysCd}'
AND CLM.CLM_STTUS_CD_SK = MPPNG2.CD_MPPNG_SK AND MPPNG2.TRGT_CD IN ('A02','A08','A09')
AND CLM.NTWK_SK = NTWK.NTWK_SK AND NTWK.NTWK_TYP_CD_SK = MPPNG3.CD_MPPNG_SK AND MPPNG3.TRGT_CD = 'HMO'
AND CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = MPPNG4.CD_MPPNG_SK AND MPPNG4.TRGT_CD = 'PCP'
"""
df_SrcTrgtRowComp_NatKey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtRowComp_NatKey)
    .load()
)

# =============================================================================
# Stage: ParChldMatch (CSeqFileStage) - from df_SrcTrgtRowComp_Match
# =============================================================================
df_ParChldMatch = df_SrcTrgtRowComp_Match.select(
    "CLM_SRC_SYS_CD_SK",
    "CLM_CLM_ID",
    "CLM_PROV_SRC_SYS_CD_SK",
    "CLM_PROV_CLM_ID"
)
write_files(
    df_ParChldMatch,
    f"{adls_path}/balancing/sync/ClmHmoClmProvBalancingTotalMatch.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# =============================================================================
# Stage: Transform1 (CTransformerStage) - input from df_SrcTrgtRowComp_Pkey
# =============================================================================
df_Transform1_in = df_SrcTrgtRowComp_Pkey

df_Transform1_out_Research1 = df_Transform1_in.filter("CLM_CLM_SK IS NULL").select(
    "CLM_CLM_SK",
    "CLM_PROV_CLM_SK"
)

df_Transform1_out_Research2 = df_Transform1_in.filter("CLM_PROV_CLM_SK IS NULL").select(
    "CLM_CLM_SK",
    "CLM_PROV_CLM_SK"
)

df_T1_notify_candidates = df_Transform1_in.filter("(CLM_CLM_SK IS NULL OR CLM_PROV_CLM_SK IS NULL)")
df_Transform1_out_Notify = df_T1_notify_candidates.limit(1).select(
    F.rpad(
        F.lit(f"REFERENTIAL INTEGRITY BALANCING PRIMARY KEY {SrcSysCd} - IDS CLM AND CLM PROV (HMO) CHECK FOR OUT OF TOLERANCE"),
        70,
        " "
    ).alias("NOTIFICATION")
)

# =============================================================================
# Stage: NotificationFile1 (CSeqFileStage) - from df_Transform1_out_Notify
# =============================================================================
write_files(
    df_Transform1_out_Notify,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# =============================================================================
# Stage: ResearchFile1 (CSeqFileStage) - from df_Transform1_out_Research1
# =============================================================================
write_files(
    df_Transform1_out_Research1,
    f"{adls_path}/balancing/research/PkeyParChldClmHmoClmProvRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# =============================================================================
# Stage: ResearchFile2 (CSeqFileStage) - from df_Transform1_out_Research2
# =============================================================================
write_files(
    df_Transform1_out_Research2,
    f"{adls_path}/balancing/research/PkeyChldParClmHmoClmProvRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# =============================================================================
# Stage: Transform2 (CTransformerStage) - input from df_SrcTrgtRowComp_NatKey
# =============================================================================
df_Transform2_in = df_SrcTrgtRowComp_NatKey

df_Transform2_out_Research3 = df_Transform2_in.filter(
    "(CLM_CLM_ID IS NULL OR CLM_SRC_SYS_CD_SK IS NULL)"
).select(
    "CLM_SRC_SYS_CD_SK",
    "CLM_CLM_ID",
    "CLM_PROV_SRC_SYS_CD_SK",
    "CLM_PROV_CLM_ID"
)

df_Transform2_out_Research4 = df_Transform2_in.filter(
    "(CLM_PROV_CLM_ID IS NULL OR CLM_PROV_SRC_SYS_CD_SK IS NULL)"
).select(
    "CLM_SRC_SYS_CD_SK",
    "CLM_CLM_ID",
    "CLM_PROV_SRC_SYS_CD_SK",
    "CLM_PROV_CLM_ID"
)

df_T2_notify_candidates = df_Transform2_in.filter(
    "((CLM_CLM_ID IS NULL OR CLM_SRC_SYS_CD_SK IS NULL) OR (CLM_PROV_CLM_ID IS NULL OR CLM_PROV_SRC_SYS_CD_SK IS NULL))"
)
df_Transform2_out_Notify = df_T2_notify_candidates.limit(1).select(
    F.rpad(
        F.lit(f"REFERENTIAL INTEGRITY BALANCING NATURAL KEYS {SrcSysCd} - IDS CLM AND CLM PROV (HMO) CHECK FOR OUT OF TOLERANCE"),
        70,
        " "
    ).alias("NOTIFICATION")
)

# =============================================================================
# Stage: ResearchFile3 (CSeqFileStage) - from df_Transform2_out_Research3
# =============================================================================
write_files(
    df_Transform2_out_Research3,
    f"{adls_path}/balancing/research/NatkeyParChldClmHmoClmProvRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# =============================================================================
# Stage: ResearchFile4 (CSeqFileStage) - from df_Transform2_out_Research4
# =============================================================================
write_files(
    df_Transform2_out_Research4,
    f"{adls_path}/balancing/research/NatkeyChldParClmHmoClmProvRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# =============================================================================
# Stage: NotificationFile2 (CSeqFileStage) - from df_Transform2_out_Notify
# =============================================================================
write_files(
    df_Transform2_out_Notify,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)