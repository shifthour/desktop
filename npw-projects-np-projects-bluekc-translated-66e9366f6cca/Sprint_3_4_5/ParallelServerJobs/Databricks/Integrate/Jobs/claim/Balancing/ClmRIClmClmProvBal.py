# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          ClmRIClmClmProvBalSeq (Multiple Instance)
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                                 Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                           ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               10/22/2007          3264                              Originally Programmed                                                               devlIDS30                                
# MAGIC 
# MAGIC Manasa Andru                   12/21/2011       TTR- 1036               Added null check conditions in the                                                  IntegrateCurDevl           SAndrew                    2012-01-02
# MAGIC                                                                                                    transformers in the Notify output links
# MAGIC 
# MAGIC Manasa Andru                   01/13/2012       TTR- 1036         Changed the null check conditions in the  transformers                         IntegrateCurDevl          SAndrew                   2012-01-19
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
idsOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")
RunID = get_widget_value("RunID","")
SrcSysCd = get_widget_value("SrcSysCd","")

# Database config
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from DB2Connector Stage: SrcTrgtRowComp (with 3 output pins)

# Pkey
extract_query_pkey = f"""
SELECT
CLM.CLM_SK AS CLM_CLM_SK,
CLM_PROV.CLM_SK AS CLM_PROV_CLM_SK
FROM
{idsOwner}.CLM CLM FULL OUTER JOIN {idsOwner}.CLM_PROV CLM_PROV
ON CLM.CLM_SK = CLM_PROV.CLM_SK,
{idsOwner}.CD_MPPNG MPPNG1,
{idsOwner}.CD_MPPNG MPPNG2,
{idsOwner}.CD_MPPNG MPPNG3
WHERE
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CLM.SRC_SYS_CD_SK = MPPNG1.CD_MPPNG_SK AND MPPNG1.TRGT_CD = '{SrcSysCd}'
AND CLM.CLM_STTUS_CD_SK = MPPNG2.CD_MPPNG_SK AND MPPNG2.TRGT_CD IN ('A02','A08','A09')
AND CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = MPPNG3.CD_MPPNG_SK AND MPPNG3.TRGT_CD IN ('SVC','PD')
""".strip()

df_SrcTrgtRowComp_Pkey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_pkey)
    .load()
)

# Match
extract_query_match = f"""
SELECT
CLM.SRC_SYS_CD_SK AS CLM_SRC_SYS_CD_SK,
CLM.CLM_ID AS CLM_CLM_ID,
CLM_PROV.SRC_SYS_CD_SK AS CLM_PROV_SRC_SYS_CD_SK,
CLM_PROV.CLM_ID AS CLM_PROV_CLM_ID
FROM
{idsOwner}.CLM CLM INNER JOIN {idsOwner}.CLM_PROV CLM_PROV
ON CLM.SRC_SYS_CD_SK = CLM_PROV.SRC_SYS_CD_SK
AND CLM.CLM_ID = CLM_PROV.CLM_ID,
{idsOwner}.CD_MPPNG MPPNG1,
{idsOwner}.CD_MPPNG MPPNG2,
{idsOwner}.CD_MPPNG MPPNG3
WHERE
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CLM.SRC_SYS_CD_SK = MPPNG1.CD_MPPNG_SK AND MPPNG1.TRGT_CD = '{SrcSysCd}'
AND CLM.CLM_STTUS_CD_SK = MPPNG2.CD_MPPNG_SK AND MPPNG2.TRGT_CD IN ('A02','A08','A09')
AND CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = MPPNG3.CD_MPPNG_SK AND MPPNG3.TRGT_CD IN ('SVC','PD')
""".strip()

df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_match)
    .load()
)

# NatKey
extract_query_natkey = f"""
SELECT
CLM.SRC_SYS_CD_SK AS CLM_SRC_SYS_CD_SK,
CLM.CLM_ID AS CLM_CLM_ID,
CLM_PROV.SRC_SYS_CD_SK AS CLM_PROV_SRC_SYS_CD_SK,
CLM_PROV.CLM_ID AS CLM_PROV_CLM_ID
FROM
{idsOwner}.CLM CLM FULL OUTER JOIN {idsOwner}.CLM_PROV CLM_PROV
ON CLM.SRC_SYS_CD_SK = CLM_PROV.SRC_SYS_CD_SK
AND CLM.CLM_ID = CLM_PROV.CLM_ID,
{idsOwner}.CD_MPPNG MPPNG1,
{idsOwner}.CD_MPPNG MPPNG2,
{idsOwner}.CD_MPPNG MPPNG3
WHERE
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CLM.SRC_SYS_CD_SK = MPPNG1.CD_MPPNG_SK AND MPPNG1.TRGT_CD = '{SrcSysCd}'
AND CLM.CLM_STTUS_CD_SK = MPPNG2.CD_MPPNG_SK AND MPPNG2.TRGT_CD IN ('A02','A08','A09')
AND CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = MPPNG3.CD_MPPNG_SK AND MPPNG3.TRGT_CD IN ('SVC','PD')
""".strip()

df_SrcTrgtRowComp_NatKey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_natkey)
    .load()
)

# Write Stage: ParChldMatch (CSeqFileStage) for df_SrcTrgtRowComp_Match
write_files(
    df_SrcTrgtRowComp_Match.select(
        "CLM_SRC_SYS_CD_SK",
        "CLM_CLM_ID",
        "CLM_PROV_SRC_SYS_CD_SK",
        "CLM_PROV_CLM_ID"
    ),
    f"{adls_path}/balancing/sync/ClmClmProvBalancingTotalMatch.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\""
)

# Transform1 (CTransformerStage) from df_SrcTrgtRowComp_Pkey
df_Transform1 = df_SrcTrgtRowComp_Pkey

df_Transform1_Research1 = df_Transform1.filter(
    F.col("CLM_CLM_SK").isNull()
).select(
    "CLM_CLM_SK",
    "CLM_PROV_CLM_SK"
)

df_Transform1_Research2 = df_Transform1.filter(
    F.col("CLM_PROV_CLM_SK").isNull()
).select(
    "CLM_CLM_SK",
    "CLM_PROV_CLM_SK"
)

df_Transform1_Notify_temp = df_Transform1.filter(
    F.col("CLM_CLM_SK").isNull() | F.col("CLM_PROV_CLM_SK").isNull()
)
df_Transform1_Notify = df_Transform1_Notify_temp.limit(1).selectExpr("'' as NOTIFICATION")
df_Transform1_Notify = df_Transform1_Notify.withColumn(
    "NOTIFICATION",
    F.rpad(
        F.lit(f"REFERENTIAL INTEGRITY BALANCING PRIMARY KEY {SrcSysCd} - IDS CLM AND CLM PROV CHECK FOR OUT OF TOLERANCE"),
        70,
        " "
    )
)

# Write to NotificationFile1 (CSeqFileStage)
write_files(
    df_Transform1_Notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\""
)

# Write to ResearchFile1 (CSeqFileStage)
write_files(
    df_Transform1_Research1.select("CLM_CLM_SK", "CLM_PROV_CLM_SK"),
    f"{adls_path}/balancing/research/PkeyParChldClmClmProvRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\""
)

# Write to ResearchFile2 (CSeqFileStage)
write_files(
    df_Transform1_Research2.select("CLM_CLM_SK", "CLM_PROV_CLM_SK"),
    f"{adls_path}/balancing/research/PkeyChldParClmClmProvRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\""
)

# Transform2 (CTransformerStage) from df_SrcTrgtRowComp_NatKey
df_Transform2 = df_SrcTrgtRowComp_NatKey

df_Transform2_Research3 = df_Transform2.filter(
    F.col("CLM_CLM_ID").isNull() | F.col("CLM_SRC_SYS_CD_SK").isNull()
).select(
    "CLM_SRC_SYS_CD_SK",
    "CLM_CLM_ID",
    "CLM_PROV_SRC_SYS_CD_SK",
    "CLM_PROV_CLM_ID"
)

df_Transform2_Research4 = df_Transform2.filter(
    F.col("CLM_PROV_CLM_ID").isNull() | F.col("CLM_PROV_SRC_SYS_CD_SK").isNull()
).select(
    "CLM_SRC_SYS_CD_SK",
    "CLM_CLM_ID",
    "CLM_PROV_SRC_SYS_CD_SK",
    "CLM_PROV_CLM_ID"
)

df_Transform2_Notify_temp = df_Transform2.filter(
    (F.col("CLM_CLM_ID").isNull() | F.col("CLM_SRC_SYS_CD_SK").isNull())
    | (F.col("CLM_PROV_CLM_ID").isNull() | F.col("CLM_PROV_SRC_SYS_CD_SK").isNull())
)
df_Transform2_Notify = df_Transform2_Notify_temp.limit(1).selectExpr("'' as NOTIFICATION")
df_Transform2_Notify = df_Transform2_Notify.withColumn(
    "NOTIFICATION",
    F.rpad(
        F.lit(f"REFERENTIAL INTEGRITY BALANCING NATURAL KEYS {SrcSysCd} - IDS CLM AND CLM PROV CHECK FOR OUT OF TOLERANCE"),
        70,
        " "
    )
)

# Write to ResearchFile3 (CSeqFileStage)
write_files(
    df_Transform2_Research3.select(
        "CLM_SRC_SYS_CD_SK",
        "CLM_CLM_ID",
        "CLM_PROV_SRC_SYS_CD_SK",
        "CLM_PROV_CLM_ID"
    ),
    f"{adls_path}/balancing/research/NatkeyParChldClmClmProvRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\""
)

# Write to ResearchFile4 (CSeqFileStage)
write_files(
    df_Transform2_Research4.select(
        "CLM_SRC_SYS_CD_SK",
        "CLM_CLM_ID",
        "CLM_PROV_SRC_SYS_CD_SK",
        "CLM_PROV_CLM_ID"
    ),
    f"{adls_path}/balancing/research/NatkeyChldParClmClmProvRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\""
)

# Write to NotificationFile2 (CSeqFileStage)
write_files(
    df_Transform2_Notify.select("NOTIFICATION"),
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\""
)