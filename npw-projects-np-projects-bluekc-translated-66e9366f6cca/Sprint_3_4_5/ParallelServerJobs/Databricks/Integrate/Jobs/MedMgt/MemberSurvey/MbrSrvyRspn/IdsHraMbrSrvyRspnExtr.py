# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2012 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     HlthFtnsHRAMbrSrvyRspnExtrSeq, 
# MAGIC                     AlineoHRAMbrSrvyRspnExtrSeq
# MAGIC 
# MAGIC Processing: Extract job for Health Fitness HRA Member Survey Response
# MAGIC                    
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                Project/                                                                                                                                                       Code                   Date
# MAGIC Developer         Date              Altiris #           Change Description                                                                                                               Reviewer            Reviewed
# MAGIC ----------------------   -------------------   -------------------   -------------------------------------------------------------------------------------------------------------------------------------------   ----------------------     -------------------   
# MAGIC Kalyan Neelam  2012-09-19    4830 & 4735      Initial Programming                                                                                                            Bhoomi Dasari    09/24/2012
# MAGIC Raja Gummadi   2013-11-20    5063                Added 5 new columns to W_MBR_RSPN Table                          IntegrateNewDevl         Kalyan Neelam   2013-11-24         
# MAGIC                                                                                   MBR_SRVY_RSPN_UPDT_SRC_CD                
# MAGIC                                                                                   CMPLTN_DT_SK
# MAGIC                                                                                   STRT_DT_SK
# MAGIC                                                                                   UPDT_DT_SK
# MAGIC                                                                                   HRA_ID
# MAGIC Raja Gummadi   2014-03-04    5063                Added Join for HLTHFTNS records from W table                         IntegrateNewDevl         Kalyan Neelam   2014-03-17
# MAGIC 
# MAGIC Raja Gummadi   2016-05-05    TFS-12342      Added Alineo query for input and added sort step before Sequencing   IntegrateDev2     Jag Yelavarthi      2016-05-20
# MAGIC 
# MAGIC Raja Gummadi   2016-06-09    TFS-12342      preserve W table Sequencing for Alineo SRC_SYS_CD                       IntegrateDev1      Jag Yelavarthi      2016-06-09
# MAGIC 
# MAGIC Abhiram Dasarathy  2017-07-26  TFS-19282      Chnaged the column name in SQL using "as" to match with                 IntegrateDev1      Jag Yelavarthi      2017-07-26
# MAGIC                                                                            the column name defined in the metadata for SEQ_NO and  
# MAGIC                                                                             W_SEQ_NO  
# MAGIC Saranya A                    2022-01-25                      US482344                       Update QSTN_CD length from 35 to 100                   IntegrateDev2                  Jaideep Mankala             01/26/2022

# MAGIC The file should always be appended, DO NOT Overwrite the existing file.
# MAGIC The table is loaded by the EDW job IdsMbrSrvyCntl
# MAGIC The W_MBR_RSPN table is initially loaded by combining the input file and the reprocess file. The Group and Member fields are defaulted to 0s in the initial load
# MAGIC The lower part of the Union query looks for any Questions Codes that are needed to be voided for ALINEO.
# MAGIC This container is used in:
# MAGIC IdsMbrSrvyRspnExtr
# MAGIC IdsWMbrRspnHraExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Output to build driver table for use by EdwMbrSrvyRspnRiskFctrFHraExtr  
# MAGIC GOES TO EDWFilePath  and is loaded only in EDW
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/MbrSrvyRspnPK
# COMMAND ----------

# Retrieve job parameters
EDWFilePath = get_widget_value('EDWFilePath','')
IDSOwner = get_widget_value('IDSOwner','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','2011-04-13')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','20110413')
SrcSysCd = get_widget_value('SrcSysCd','HLTHFTNS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','-1957488081')
ids_secret_name = get_widget_value('ids_secret_name','')

# Prepare JDBC config for IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------
# 1) Dummy table for hf_mbr_srvy_rspn_temp (Scenario B read-modify-write hashed file)
#    "hf_mbr_srvy_rspn_temp_lkup" => read from dummy table
#    "hf_mbr_srvy_rspn_temp_updt" => write to same dummy table via merge
# --------------------------------------------------------------------------------
# Read from dummy_hf_mbr_srvy_rspn_temp
df_hf_mbr_srvy_rspn_temp_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT SRC_SYS_CD, MBR_SRVY_TYP_CD, MBR_UNIQ_KEY, QSTN_CD_TX, RSPN_DT_SK, SEQ_NO "
        "FROM IDS.dummy_hf_mbr_srvy_rspn_temp"
    )
    .load()
)

# --------------------------------------------------------------------------------
# 2) Read from DB2Connector W_MBR_RSPN
# --------------------------------------------------------------------------------
extract_query_W_MBR_RSPN = f"""
SELECT 
W_MBR_RSPN.MBR_SRVY_RSPN_DT_SK,
W_MBR_RSPN.MBR_SRVY_QSTN_CD_TX,
W_MBR_RSPN.MBR_SRVY_RSPN_ANSWER_TX,
COALESCE(M.SEQ_NO,0) AS SEQ_NO,
W_MBR_RSPN.GRP_SK,
W_MBR_RSPN.GRP_ID,
W_MBR_RSPN.MBR_SK,
W_MBR_RSPN.MBR_UNIQ_KEY,
W_MBR_RSPN.REPRCS_CT,
W_MBR_RSPN.SRC_SYS_CD,
W_MBR_RSPN.MBR_SRVY_TYP_CD,
W_MBR_RSPN.SYS_CRT_DT_SK,
W_MBR_RSPN.MBR_RSPN_VOID_DT_SK ,
W_MBR_RSPN.MBR_SRVY_RSPN_UPDT_SRC_CD,
W_MBR_RSPN.CMPLTN_DT_SK,
W_MBR_RSPN.STRT_DT_SK,
W_MBR_RSPN.UPDT_DT_SK,
W_MBR_RSPN.HRA_ID,
W_MBR_RSPN.SEQ_NO AS W_SEQ_NO
FROM {IDSOwner}.W_MBR_RSPN W_MBR_RSPN
LEFT OUTER JOIN (
    SELECT M.MBR_UNIQ_KEY, M.RSPN_DT_SK, M.QSTN_CD_TX,  M.HRA_ID, MAX(M.SEQ_NO) as SEQ_NO  
    FROM {IDSOwner}.MBR_SRVY_RSPN M 
    GROUP BY M.MBR_UNIQ_KEY, M.RSPN_DT_SK, M.QSTN_CD_TX, M.HRA_ID
) M
ON
    W_MBR_RSPN.MBR_UNIQ_KEY = M.MBR_UNIQ_KEY AND
    W_MBR_RSPN.MBR_SRVY_RSPN_DT_SK = M.RSPN_DT_SK AND
    W_MBR_RSPN.MBR_SRVY_QSTN_CD_TX = M.QSTN_CD_TX AND
    W_MBR_RSPN.HRA_ID = M.HRA_ID
WHERE 
    W_MBR_RSPN.MBR_RSPN_VOID_DT_SK = '1753-01-01' AND
    W_MBR_RSPN.SRC_SYS_CD = 'HLTHFTNS'

UNION

SELECT 
W_MBR_RSPN.MBR_SRVY_RSPN_DT_SK,
W_MBR_RSPN.MBR_SRVY_QSTN_CD_TX,
W_MBR_RSPN.MBR_SRVY_RSPN_ANSWER_TX,
W_MBR_RSPN.SEQ_NO,
W_MBR_RSPN.GRP_SK,
W_MBR_RSPN.GRP_ID,
W_MBR_RSPN.MBR_SK,
W_MBR_RSPN.MBR_UNIQ_KEY,
W_MBR_RSPN.REPRCS_CT,
W_MBR_RSPN.SRC_SYS_CD,
W_MBR_RSPN.MBR_SRVY_TYP_CD,
W_MBR_RSPN.SYS_CRT_DT_SK,
W_MBR_RSPN.MBR_RSPN_VOID_DT_SK ,
W_MBR_RSPN.MBR_SRVY_RSPN_UPDT_SRC_CD,
W_MBR_RSPN.CMPLTN_DT_SK,
W_MBR_RSPN.STRT_DT_SK,
W_MBR_RSPN.UPDT_DT_SK,
W_MBR_RSPN.HRA_ID,
W_MBR_RSPN.SEQ_NO AS W_SEQ_NO
FROM {IDSOwner}.W_MBR_RSPN W_MBR_RSPN
WHERE 
    W_MBR_RSPN.MBR_RSPN_VOID_DT_SK = '1753-01-01' AND
    W_MBR_RSPN.SRC_SYS_CD <> 'HLTHFTNS' AND
    W_MBR_RSPN.SRC_SYS_CD <> 'ALINEO'

UNION

SELECT 
W_MBR_RSPN.MBR_SRVY_RSPN_DT_SK,
W_MBR_RSPN.MBR_SRVY_QSTN_CD_TX,
W_MBR_RSPN.MBR_SRVY_RSPN_ANSWER_TX,
(COALESCE(M.SEQ_NO,0)+W_MBR_RSPN.SEQ_NO) AS SEQ_NO,
W_MBR_RSPN.GRP_SK,
W_MBR_RSPN.GRP_ID,
W_MBR_RSPN.MBR_SK,
W_MBR_RSPN.MBR_UNIQ_KEY,
W_MBR_RSPN.REPRCS_CT,
W_MBR_RSPN.SRC_SYS_CD,
W_MBR_RSPN.MBR_SRVY_TYP_CD,
W_MBR_RSPN.SYS_CRT_DT_SK,
W_MBR_RSPN.MBR_RSPN_VOID_DT_SK ,
W_MBR_RSPN.MBR_SRVY_RSPN_UPDT_SRC_CD,
W_MBR_RSPN.CMPLTN_DT_SK,
W_MBR_RSPN.STRT_DT_SK,
W_MBR_RSPN.UPDT_DT_SK,
W_MBR_RSPN.HRA_ID,
W_MBR_RSPN.SEQ_NO AS W_SEQ_NO
FROM {IDSOwner}.W_MBR_RSPN W_MBR_RSPN
LEFT OUTER JOIN (
    SELECT M.MBR_UNIQ_KEY, M.RSPN_DT_SK, M.QSTN_CD_TX,  MAX(M.SEQ_NO) as SEQ_NO  
    FROM {IDSOwner}.MBR_SRVY_RSPN M 
    GROUP BY M.MBR_UNIQ_KEY, M.RSPN_DT_SK, M.QSTN_CD_TX
) M
ON
    W_MBR_RSPN.MBR_UNIQ_KEY = M.MBR_UNIQ_KEY AND
    W_MBR_RSPN.MBR_SRVY_RSPN_DT_SK = M.RSPN_DT_SK AND
    W_MBR_RSPN.MBR_SRVY_QSTN_CD_TX = M.QSTN_CD_TX
WHERE 
    W_MBR_RSPN.MBR_RSPN_VOID_DT_SK = '1753-01-01' AND
    W_MBR_RSPN.SRC_SYS_CD = 'ALINEO'

ORDER BY
HRA_ID
"""

df_W_MBR_RSPN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_W_MBR_RSPN)
    .load()
)

# --------------------------------------------------------------------------------
# 3) Transformer Stage (Responses -> sort, Dedup)
#    This Transformer outputs:
#      - Link "sort": many columns with logic
#      - Link "Dedup": only MBR_UNIQ_KEY
# --------------------------------------------------------------------------------

# Prepare DataFrame for the "Dedup" link (MBR_UNIQ_KEY)
df_Transformer_for_dedup = df_W_MBR_RSPN.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

# Prepare columns for the "sort" link
# Stage variables used:
#   RowPassThru = 'Y'
#   svMbrSrvyTypCd = Responses.MBR_SRVY_TYP_CD
#   svSeqNo = IF(SRC_SYS_CD='HLTHFTNS') THEN SEQ_NO+1 ELSE SEQ_NO
df_Transformer_for_sort = df_W_MBR_RSPN
df_Transformer_for_sort = df_Transformer_for_sort.withColumn(
    "svMbrSrvyTypCd", F.col("MBR_SRVY_TYP_CD")
)
df_Transformer_for_sort = df_Transformer_for_sort.withColumn(
    "svSeqNo",
    F.when(F.col("SRC_SYS_CD") == "HLTHFTNS", F.col("SEQ_NO") + F.lit(1)).otherwise(F.col("SEQ_NO"))
)

# Create the final columns for the "sort" link
# Use the stage variable or expression logic for each
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("INSRT_UPDT_CD", F.lit("I"))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("DISCARD_IN", F.lit("N"))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("PASS_THRU_IN", F.lit("Y"))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("FIRST_RECYC_DT", F.lit(CurrRunCycleDate))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("ERR_CT", F.lit(0))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("RECYCLE_CT", F.lit(0))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("SRC_SYS_CD", F.col("SRC_SYS_CD"))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn(
    "PRI_KEY_STRING",
    F.concat_ws(
        ";",
        F.col("SRC_SYS_CD"),
        F.col("svMbrSrvyTypCd"),
        F.col("MBR_UNIQ_KEY").cast(StringType()),
        F.col("MBR_SRVY_QSTN_CD_TX"),
        F.col("MBR_SRVY_RSPN_DT_SK"),
        F.col("svSeqNo").cast(StringType())
    )
)
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("MBR_SRVY_RSPN_SK", F.lit(0))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("MBR_SRVY_TYP_CD", F.col("svMbrSrvyTypCd"))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("MBR_UNIQ_KEY", F.col("MBR_UNIQ_KEY"))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn(
    "QSTN_CD_TX",
    F.when(
        F.col("MBR_SRVY_QSTN_CD_TX").isNull() | (F.length(trim(F.col("MBR_SRVY_QSTN_CD_TX"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("MBR_SRVY_QSTN_CD_TX"))
)
df_Transformer_for_sort = df_Transformer_for_sort.withColumn(
    "RSPN_DT_SK",
    F.when(
        F.col("MBR_SRVY_RSPN_DT_SK").isNull() | (F.length(trim(F.col("MBR_SRVY_RSPN_DT_SK"))) == 0),
        F.lit("1753-01-01")
    ).otherwise(F.col("MBR_SRVY_RSPN_DT_SK"))
)
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("SEQ_NO", F.col("svSeqNo"))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("GRP_ID", F.col("GRP_ID"))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("SYS_CRT_DT_SK", F.col("SYS_CRT_DT_SK"))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("RCRD_CT", F.lit(1))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn(
    "RSPN_ANSWER_TX",
    F.when(
        F.col("MBR_SRVY_RSPN_ANSWER_TX").isNull() | (F.length(trim(F.col("MBR_SRVY_RSPN_ANSWER_TX"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("MBR_SRVY_RSPN_ANSWER_TX"))
)
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("MBR_SRVY_RSPN_UPDT_SRC_CD", F.col("MBR_SRVY_RSPN_UPDT_SRC_CD"))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("CMPLTN_DT_SK", F.col("CMPLTN_DT_SK"))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("STRT_DT_SK", F.col("STRT_DT_SK"))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("UPDT_DT_SK", F.col("UPDT_DT_SK"))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("HRA_ID", F.col("HRA_ID"))
df_Transformer_for_sort = df_Transformer_for_sort.withColumn("W_SEQ_NO", F.col("W_SEQ_NO"))

# Select final columns in exact order for the "sort" link
df_Transformer_for_sort = df_Transformer_for_sort.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "MBR_SRVY_RSPN_SK",
    "SRC_SYS_CD_SK",
    "MBR_SRVY_TYP_CD",
    "MBR_UNIQ_KEY",
    "QSTN_CD_TX",
    "RSPN_DT_SK",
    "SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "GRP_ID",
    "SYS_CRT_DT_SK",
    "RCRD_CT",
    "RSPN_ANSWER_TX",
    "MBR_SRVY_RSPN_UPDT_SRC_CD",
    "CMPLTN_DT_SK",
    "STRT_DT_SK",
    "UPDT_DT_SK",
    "HRA_ID",
    "W_SEQ_NO"
)

# --------------------------------------------------------------------------------
# 4) hf_hra_pmbrsrvydrvr_dedupe: Scenario A => deduplicate on MBR_UNIQ_KEY
#    Then feed "P_MBR_SRVY_DRVR"
# --------------------------------------------------------------------------------
df_Dedup = df_Transformer_for_dedup.dropDuplicates(["MBR_UNIQ_KEY"])

# Write to P_MBR_SRVY_DRVR (CSeqFileStage)
# File path "#$EDWFilePath#/load/P_MBR_SRVY_DRVR.dat" => not "landing" or "external" => use adls_path
p_mbr_srvy_drvr_path = f"{adls_path}/load/P_MBR_SRVY_DRVR.dat"

df_Dedup_final = df_Dedup.select("MBR_UNIQ_KEY")
write_files(
    df_Dedup_final,
    p_mbr_srvy_drvr_path,
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# 5) Sort_195
#    Sort by MBR_UNIQ_KEY asc, QSTN_CD_TX asc, RSPN_DT_SK asc, W_SEQ_NO asc
# --------------------------------------------------------------------------------
df_Sort_195 = df_Transformer_for_sort.orderBy(
    F.col("MBR_UNIQ_KEY").asc(),
    F.col("QSTN_CD_TX").asc(),
    F.col("RSPN_DT_SK").asc(),
    F.col("W_SEQ_NO").asc()
)

# --------------------------------------------------------------------------------
# 6) Pkey Transformer
#    Left join with df_hf_mbr_srvy_rspn_temp_lkup
#    StageVar svSeq => If IsNull(l.SEQ_NO) then t.SEQ_NO else l.SEQ_NO+1
#    Outputs:
#      "updt" => to dummy table (SEQ_NO=svSeq)
#      "Transform" => pass along, but uses SEQ_NO => if t.SRC_SYS_CD='ALINEO' then t.SEQ_NO else svSeq
# --------------------------------------------------------------------------------
t = df_Sort_195.alias("t")
l = df_hf_mbr_srvy_rspn_temp_lkup.alias("l")

df_Pkey_joined = t.join(
    l,
    on=(
        (t["SRC_SYS_CD"] == l["SRC_SYS_CD"])
        & (t["MBR_SRVY_TYP_CD"] == l["MBR_SRVY_TYP_CD"])
        & (t["MBR_UNIQ_KEY"] == l["MBR_UNIQ_KEY"])
        & (t["QSTN_CD_TX"] == l["QSTN_CD_TX"])
        & (t["RSPN_DT_SK"] == l["RSPN_DT_SK"])
    ),
    how="left"
)

df_Pkey_joined = df_Pkey_joined.withColumn(
    "svSeq",
    F.when(F.col("l.SEQ_NO").isNull(), F.col("t.SEQ_NO")).otherwise(F.col("l.SEQ_NO") + 1)
)

# Output link "updt" => columns: [SRC_SYS_CD, MBR_SRVY_TYP_CD, MBR_UNIQ_KEY, QSTN_CD_TX, RSPN_DT_SK, SEQ_NO=svSeq]
df_Pkey_updt = df_Pkey_joined.select(
    F.col("t.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("t.MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    F.col("t.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("t.QSTN_CD_TX").alias("QSTN_CD_TX"),
    F.col("t.RSPN_DT_SK").alias("RSPN_DT_SK"),
    F.col("svSeq").alias("SEQ_NO")
)

# Output link "Transform" => columns with SEQ_NO => if t.SRC_SYS_CD='ALINEO' then t.SEQ_NO else svSeq
df_Pkey_transform = df_Pkey_joined.withColumn(
    "final_SEQ_NO",
    F.when(F.col("t.SRC_SYS_CD") == "ALINEO", F.col("t.SEQ_NO")).otherwise(F.col("svSeq"))
)

df_Pkey_transform = df_Pkey_transform.select(
    F.col("t.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("t.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("t.DISCARD_IN").alias("DISCARD_IN"),
    F.col("t.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("t.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("t.ERR_CT").alias("ERR_CT"),
    F.col("t.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("t.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("t.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("t.MBR_SRVY_RSPN_SK").alias("MBR_SRVY_RSPN_SK"),
    F.col("t.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("t.MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    F.col("t.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("t.QSTN_CD_TX").alias("QSTN_CD_TX"),
    F.col("t.RSPN_DT_SK").alias("RSPN_DT_SK"),
    F.col("final_SEQ_NO").alias("SEQ_NO"),
    F.col("t.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("t.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("t.GRP_ID").alias("GRP_ID"),
    F.col("t.SYS_CRT_DT_SK").alias("SYS_CRT_DT_SK"),
    F.col("t.RCRD_CT").alias("RCRD_CT"),
    F.col("t.RSPN_ANSWER_TX").alias("RSPN_ANSWER_TX"),
    F.col("t.MBR_SRVY_RSPN_UPDT_SRC_CD").alias("MBR_SRVY_RSPN_UPDT_SRC_CD"),
    F.col("t.CMPLTN_DT_SK").alias("CMPLTN_DT_SK"),
    F.col("t.STRT_DT_SK").alias("STRT_DT_SK"),
    F.col("t.UPDT_DT_SK").alias("UPDT_DT_SK"),
    F.col("t.HRA_ID").alias("HRA_ID")
)

# --------------------------------------------------------------------------------
# 7) Write back to dummy table: "hf_mbr_srvy_rspn_temp_updt" => scenario B
#    We merge df_Pkey_updt into IDS.dummy_hf_mbr_srvy_rspn_temp
# --------------------------------------------------------------------------------

# Create a temporary staging table
temp_table_name = "STAGING.IdsHraMbrSrvyRspnExtr_hf_mbr_srvy_rspn_temp_updt_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url_ids, jdbc_props_ids)

df_Pkey_updt.write.format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE INTO IDS.dummy_hf_mbr_srvy_rspn_temp T
USING STAGING.IdsHraMbrSrvyRspnExtr_hf_mbr_srvy_rspn_temp_updt_temp S
ON T.SRC_SYS_CD=S.SRC_SYS_CD
 AND T.MBR_SRVY_TYP_CD=S.MBR_SRVY_TYP_CD
 AND T.MBR_UNIQ_KEY=S.MBR_UNIQ_KEY
 AND T.QSTN_CD_TX=S.QSTN_CD_TX
 AND T.RSPN_DT_SK=S.RSPN_DT_SK
WHEN MATCHED THEN
  UPDATE SET
    T.SEQ_NO=S.SEQ_NO
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, MBR_SRVY_TYP_CD, MBR_UNIQ_KEY, QSTN_CD_TX, RSPN_DT_SK, SEQ_NO)
  VALUES (S.SRC_SYS_CD, S.MBR_SRVY_TYP_CD, S.MBR_UNIQ_KEY, S.QSTN_CD_TX, S.RSPN_DT_SK, S.SEQ_NO);
"""

execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)

# --------------------------------------------------------------------------------
# 8) MbrSrvyRspnPK (Shared Container)
# --------------------------------------------------------------------------------
params_container = {"CurrRunCycle": CurrRunCycle}
df_MbrSrvyRspnPK = MbrSrvyRspnPK(df_Pkey_transform, params_container)

# --------------------------------------------------------------------------------
# 9) MbrSrvyRspn (CSeqFileStage)
#    Write to: "key/HlthFtnsHRAMbrSrvyRspnExtr.MbrSrvyRspn.dat.#RunID#"
#    File extension .dat, no header, overwrite
#    Rpad char(n) columns
# --------------------------------------------------------------------------------
final_output_path = f"{adls_path}/key/HlthFtnsHRAMbrSrvyRspnExtr.MbrSrvyRspn.dat.{RunID}"

df_MbrSrvyRspn_final = df_MbrSrvyRspnPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("MBR_SRVY_RSPN_SK").alias("MBR_SRVY_RSPN_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("QSTN_CD_TX").alias("QSTN_CD_TX"),
    F.rpad(F.col("RSPN_DT_SK"), 10, " ").alias("RSPN_DT_SK"),
    F.col("SEQ_NO").alias("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.rpad(F.col("SYS_CRT_DT_SK"), 10, " ").alias("SYS_CRT_DT_SK"),
    F.col("RCRD_CT").alias("RCRD_CT"),
    F.col("RSPN_ANSWER_TX").alias("RSPN_ANSWER_TX"),
    F.col("MBR_SRVY_RSPN_UPDT_SRC_CD").alias("MBR_SRVY_RSPN_UPDT_SRC_CD"),
    F.rpad(F.col("CMPLTN_DT_SK"), 10, " ").alias("CMPLTN_DT_SK"),
    F.rpad(F.col("STRT_DT_SK"), 10, " ").alias("STRT_DT_SK"),
    F.rpad(F.col("UPDT_DT_SK"), 10, " ").alias("UPDT_DT_SK"),
    F.col("HRA_ID").alias("HRA_ID")
)

write_files(
    df_MbrSrvyRspn_final,
    final_output_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)