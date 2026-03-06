# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_2 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/02/07 15:17:19 Batch  14551_55057 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 11/02/07 14:59:36 Batch  14551_53980 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 11/01/07 14:32:31 Batch  14550_52366 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 11/01/07 14:26:29 Batch  14550_51994 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          ClmRIClmFcltyClmBalSeq (Multiple Instance)
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target tables are compared on the specified column to see if there are any discrepancies
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                       ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               10/19/2007          3264                              Originally Programmed                                                             devlIDS30                                
# MAGIC 
# MAGIC Manasa Andru                   12/21/2011       TTR- 1036               Added null check conditions in the                                              IntegrateCurDevl               SAndrew                    2012-01-02
# MAGIC                                                                                                    transformers in the Notify output links
# MAGIC 
# MAGIC Manasa Andru                   01/13/2012       TTR- 1036         Changed the null check conditions in the  transformers                     IntegrateCurDevl              SAndrew                     2012-01-19
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
from pyspark.sql.functions import col, lit, rpad, row_number
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# -------------------------------------------------------
# DB2Connector: SrcTrgtRowComp => Pkey
# -------------------------------------------------------
query_SrcTrgtRowComp_Pkey = f"""
SELECT
CLM.CLM_SK,
FCLTY_CLM.CLM_SK
FROM 
{IDSOwner}.CLM CLM FULL OUTER JOIN {IDSOwner}.FCLTY_CLM FCLTY_CLM
ON CLM.CLM_SK = FCLTY_CLM.CLM_SK,
{IDSOwner}.CD_MPPNG MPPNG1,
{IDSOwner}.CD_MPPNG MPPNG2,
{IDSOwner}.CD_MPPNG MPPNG3

WHERE 
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle} 
AND CLM.SRC_SYS_CD_SK = MPPNG1.CD_MPPNG_SK AND MPPNG1.TRGT_CD = '{SrcSysCd}' 
AND CLM.CLM_SUBTYP_CD_SK = MPPNG2.CD_MPPNG_SK AND MPPNG2.TRGT_CD IN ('IP','OP')
AND CLM.CLM_STTUS_CD_SK = MPPNG3.CD_MPPNG_SK AND MPPNG3.TRGT_CD IN ('A02','A08','A09')
"""
df_SrcTrgtRowComp_Pkey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtRowComp_Pkey)
    .load()
)

# -------------------------------------------------------
# DB2Connector: SrcTrgtRowComp => Match
# -------------------------------------------------------
query_SrcTrgtRowComp_Match = f"""
SELECT
CLM.SRC_SYS_CD_SK,
CLM.CLM_ID,
FCLTY_CLM.SRC_SYS_CD_SK,
FCLTY_CLM.CLM_ID
FROM 
{IDSOwner}.CLM CLM INNER JOIN {IDSOwner}.FCLTY_CLM FCLTY_CLM
ON CLM.SRC_SYS_CD_SK = FCLTY_CLM.SRC_SYS_CD_SK 
AND CLM.CLM_ID = FCLTY_CLM.CLM_ID,
{IDSOwner}.CD_MPPNG MPPNG1,
{IDSOwner}.CD_MPPNG MPPNG2,
{IDSOwner}.CD_MPPNG MPPNG3

WHERE 
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CLM.SRC_SYS_CD_SK = MPPNG1.CD_MPPNG_SK AND MPPNG1.TRGT_CD = '{SrcSysCd}'
AND CLM.CLM_SUBTYP_CD_SK = MPPNG2.CD_MPPNG_SK AND MPPNG2.TRGT_CD IN ('IP','OP')
AND CLM.CLM_STTUS_CD_SK = MPPNG3.CD_MPPNG_SK AND MPPNG3.TRGT_CD IN ('A02','A08','A09')
"""
df_SrcTrgtRowComp_Match = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtRowComp_Match)
    .load()
)

# -------------------------------------------------------
# DB2Connector: SrcTrgtRowComp => NatKey
# -------------------------------------------------------
query_SrcTrgtRowComp_NatKey = f"""
SELECT
CLM.SRC_SYS_CD_SK,
CLM.CLM_ID,
FCLTY_CLM.SRC_SYS_CD_SK,
FCLTY_CLM.CLM_ID
FROM 
{IDSOwner}.CLM CLM FULL OUTER JOIN {IDSOwner}.FCLTY_CLM FCLTY_CLM
ON CLM.SRC_SYS_CD_SK = FCLTY_CLM.SRC_SYS_CD_SK 
AND CLM.CLM_ID = FCLTY_CLM.CLM_ID,
{IDSOwner}.CD_MPPNG MPPNG1,
{IDSOwner}.CD_MPPNG MPPNG2,
{IDSOwner}.CD_MPPNG MPPNG3

WHERE 
CLM.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtrRunCycle}
AND CLM.SRC_SYS_CD_SK = MPPNG1.CD_MPPNG_SK AND MPPNG1.TRGT_CD = '{SrcSysCd}'
AND CLM.CLM_SUBTYP_CD_SK = MPPNG2.CD_MPPNG_SK AND MPPNG2.TRGT_CD IN ('IP','OP')
AND CLM.CLM_STTUS_CD_SK = MPPNG3.CD_MPPNG_SK AND MPPNG3.TRGT_CD IN ('A02','A08','A09')
"""
df_SrcTrgtRowComp_NatKey = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_SrcTrgtRowComp_NatKey)
    .load()
)

# -------------------------------------------------------
# ParChldMatch => CSeqFileStage (from Match)
# -------------------------------------------------------
df_ParChldMatch = df_SrcTrgtRowComp_Match.select(
    col(df_SrcTrgtRowComp_Match.columns[0]),
    col(df_SrcTrgtRowComp_Match.columns[1]),
    col(df_SrcTrgtRowComp_Match.columns[2]),
    col(df_SrcTrgtRowComp_Match.columns[3])
)

write_files(
    df_ParChldMatch,
    f"{adls_path}/balancing/sync/ClmFcltyClmBalancingTotalMatch.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# -------------------------------------------------------
# Transform1 => CTransformerStage (input: Pkey)
# -------------------------------------------------------
df_Transform1_in = df_SrcTrgtRowComp_Pkey
# Rename columns for clarity (the query had CLM_SK and CLM_SK_1 in Spark)
colList_Transform1 = df_Transform1_in.columns
if len(colList_Transform1) == 2:
    df_Transform1_in = df_Transform1_in.withColumnRenamed(colList_Transform1[0], "CLM_CLM_SK") \
                                       .withColumnRenamed(colList_Transform1[1], "FCLTY_CLM_CLM_SK")

# Research1: ISNULL(Pkey.CLM_CLM_SK)
df_ResearchFile1 = df_Transform1_in.filter(col("CLM_CLM_SK").isNull()).select(
    "CLM_CLM_SK",
    "FCLTY_CLM_CLM_SK"
)

# Research2: ISNULL(Pkey.FCLTY_CLM_CLM_SK)
df_ResearchFile2 = df_Transform1_in.filter(col("FCLTY_CLM_CLM_SK").isNull()).select(
    "CLM_CLM_SK",
    "FCLTY_CLM_CLM_SK"
)

# Notify: @OUTROWNUM = 1 AND (ISNULL(CLM_CLM_SK) OR ISNULL(FCLTY_CLM_CLM_SK))
df_temp_Notify_T1 = df_Transform1_in.filter(
    (col("CLM_CLM_SK").isNull()) | (col("FCLTY_CLM_CLM_SK").isNull())
)
windowSpecT1 = Window.orderBy(lit(1))
df_NotificationFile1 = df_temp_Notify_T1.withColumn("rn", row_number().over(windowSpecT1)) \
    .filter(col("rn") == 1) \
    .drop("rn") \
    .select(
        lit(f"REFERENTIAL INTEGRITY BALANCING PRIMARY KEY {SrcSysCd} - IDS CLM AND FCLTY CLM CHECK FOR OUT OF TOLERANCE").alias("NOTIFICATION")
    )

# -------------------------------------------------------
# ResearchFile1 => CSeqFileStage
# -------------------------------------------------------
write_files(
    df_ResearchFile1,
    f"{adls_path}/balancing/research/PkeyParChldClmFcltyClmRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# -------------------------------------------------------
# ResearchFile2 => CSeqFileStage
# -------------------------------------------------------
write_files(
    df_ResearchFile2,
    f"{adls_path}/balancing/research/PkeyChldParClmFcltyClmRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# -------------------------------------------------------
# NotificationFile1 => CSeqFileStage
# -------------------------------------------------------
df_NotificationFile1_write = df_NotificationFile1.select(
    rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)
write_files(
    df_NotificationFile1_write,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# -------------------------------------------------------
# Transform2 => CTransformerStage (input: NatKey)
# -------------------------------------------------------
df_Transform2_in = df_SrcTrgtRowComp_NatKey
colList_Transform2 = df_Transform2_in.columns
if len(colList_Transform2) == 4:
    df_Transform2_in = df_Transform2_in.withColumnRenamed(colList_Transform2[0], "CLM_SRC_SYS_CD_SK") \
                                       .withColumnRenamed(colList_Transform2[1], "CLM_CLM_ID") \
                                       .withColumnRenamed(colList_Transform2[2], "FCLTY_CLM_SRC_SYS_CD_SK") \
                                       .withColumnRenamed(colList_Transform2[3], "FCLTY_CLM_CLM_ID")

# Research3: ISNULL(NatKey.CLM_CLM_ID) OR ISNULL(NatKey.CLM_SRC_SYS_CD_SK)
df_ResearchFile3 = df_Transform2_in.filter(
    (col("CLM_CLM_ID").isNull()) | (col("CLM_SRC_SYS_CD_SK").isNull())
).select(
    "CLM_SRC_SYS_CD_SK",
    "CLM_CLM_ID",
    "FCLTY_CLM_SRC_SYS_CD_SK",
    "FCLTY_CLM_CLM_ID"
)

# Research4: ISNULL(NatKey.FCLTY_CLM_CLM_ID) OR ISNULL(NatKey.FCLTY_CLM_SRC_SYS_CD_SK)
df_ResearchFile4 = df_Transform2_in.filter(
    (col("FCLTY_CLM_CLM_ID").isNull()) | (col("FCLTY_CLM_SRC_SYS_CD_SK").isNull())
).select(
    "CLM_SRC_SYS_CD_SK",
    "CLM_CLM_ID",
    "FCLTY_CLM_SRC_SYS_CD_SK",
    "FCLTY_CLM_CLM_ID"
)

# Notify: @OUTROWNUM = 1 AND ((ISNULL(CLM_CLM_ID) OR ISNULL(CLM_SRC_SYS_CD_SK)) OR (ISNULL(FCLTY_CLM_CLM_ID) OR ISNULL(FCLTY_CLM_SRC_SYS_CD_SK)))
df_temp_Notify_T2 = df_Transform2_in.filter(
    ((col("CLM_CLM_ID").isNull()) | (col("CLM_SRC_SYS_CD_SK").isNull())) |
    ((col("FCLTY_CLM_CLM_ID").isNull()) | (col("FCLTY_CLM_SRC_SYS_CD_SK").isNull()))
)
windowSpecT2 = Window.orderBy(lit(1))
df_NotificationFile2 = df_temp_Notify_T2.withColumn("rn", row_number().over(windowSpecT2)) \
    .filter(col("rn") == 1) \
    .drop("rn") \
    .select(
        lit(f"REFERENTIAL INTEGRITY BALANCING NATURAL KEYS {SrcSysCd} - IDS CLM AND FCLTY CLM CHECK FOR OUT OF TOLERANCE").alias("NOTIFICATION")
    )

# -------------------------------------------------------
# ResearchFile3 => CSeqFileStage
# -------------------------------------------------------
write_files(
    df_ResearchFile3,
    f"{adls_path}/balancing/research/NatkeyParChldClmFcltyClmRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# -------------------------------------------------------
# ResearchFile4 => CSeqFileStage
# -------------------------------------------------------
write_files(
    df_ResearchFile4,
    f"{adls_path}/balancing/research/NatkeyChldParClmFcltyClmRIResearch.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# -------------------------------------------------------
# NotificationFile2 => CSeqFileStage
# -------------------------------------------------------
df_NotificationFile2_write = df_NotificationFile2.select(
    rpad(col("NOTIFICATION"), 70, " ").alias("NOTIFICATION")
)
write_files(
    df_NotificationFile2_write,
    f"{adls_path}/balancing/notify/ClaimsBalancingNotification.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)