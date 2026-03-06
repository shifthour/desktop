# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: IhmfConstituentVbbExtrSeq
# MAGIC  
# MAGIC PROCESSING:   Extracts data from Ihmf Constituent database and creates a key file for VBB_CMPNT_RWRD
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam        2013-06-11     4963 VBB Phase III    Initial Programming                                                                       IntegrateNewDevl       Bhoomi Dasari           7/17/2013 
# MAGIC Raja Gummadi         2013-10-16     4963 VBB                    Added snapshot file to the job                                                     IntegrateNewDevl      Kalyan Neelam             2013-10-25

# MAGIC SQL Server is not allowing to Union all the queryies in one SQL getting datatype error as each query retrieves different type of data, it is also not allowing to have multiple connections within the same ODBC. That is why using a separate ODBC for each SQL.
# MAGIC IDS VBB_CMPNT_RWRD Extract
# MAGIC VBB_CMPNT primary key hashed file should NOT cleared.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, DoubleType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/VbbCmpntRwrdPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
VBBOwner = get_widget_value('VBBOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
vbb_secret_name = get_widget_value('vbb_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','IHMFCNSTTNT')
SrcSysCdSk = get_widget_value('SrcSysCdSk','-1979267011')
RunId = get_widget_value('RunId','100')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
LastRunDtm = get_widget_value('LastRunDtm','2012-04-29 00:00:00.000')
CurrDate = get_widget_value('CurrDate','2012-06-04')
Environment = get_widget_value('Environment','Test')

# Read from hashed file hf_vbb_cmpnt (Scenario C: read parquet)
df_hf_vbb_cmpnt = spark.read.parquet(f"{adls_path}/hf_vbb_cmpnt.parquet")

# Read from CODBCStage TZIM_IPRP_INCENTIVE_PGM_RWD
jdbc_url_TZIM, jdbc_props_TZIM = get_db_config(vbb_secret_name)
extract_query = f"""SELECT
TZIM_IPRP_INCENTIVE_PGM_RWD.INPR_ID,
TZIM_IPRP_INCENTIVE_PGM_RWD.IPRP_SEQ_NO,
TZIM_IPRP_INCENTIVE_PGM_RWD.IPRP_DESCRIPTION,
TZIM_IPRP_INCENTIVE_PGM_RWD.IPRP_START_DT,
TZIM_IPRP_INCENTIVE_PGM_RWD.IPRP_END_DT,
TZIM_IPRP_INCENTIVE_PGM_RWD.IPAL_ACHIEVEMENT_LEVEL,
TZIM_IPRP_INCENTIVE_PGM_RWD.VNVN_ID,
TZIM_IPRP_INCENTIVE_PGM_RWD.VNRW_SEQ_NO,
TZIM_IPRP_INCENTIVE_PGM_RWD.CTCT_ID,
TZIM_IPRP_INCENTIVE_PGM_RWD.RWDC_ID,
TZIM_IPRP_INCENTIVE_PGM_RWD.IPRP_EARNED_RWD_START_DT_TYPE,
TZIM_IPRP_INCENTIVE_PGM_RWD.IPRP_EARNED_RWD_START_DT,
TZIM_IPRP_INCENTIVE_PGM_RWD.IPRP_EARNED_RWD_END_DT_TYPE,
TZIM_IPRP_INCENTIVE_PGM_RWD.IPRP_EARNED_RWD_END_DT,
TZIM_IPRP_INCENTIVE_PGM_RWD.HIPL_ID,
TZIM_IPRP_INCENTIVE_PGM_RWD.HIEL_ID_SEQ,
TZIM_IPRP_INCENTIVE_PGM_RWD.IPRP_CREATE_DT,
TZIM_IPRP_INCENTIVE_PGM_RWD.IPRP_CREATE_APP_USER,
TZIM_IPRP_INCENTIVE_PGM_RWD.IPRP_UPDATE_DT,
TZIM_IPRP_INCENTIVE_PGM_RWD.IPRP_UPDATE_DB_USER,
TZIM_IPRP_INCENTIVE_PGM_RWD.IPRP_UPDATE_APP_USER,
TZIM_IPRP_INCENTIVE_PGM_RWD.IPRP_UPDATE_SEQ
FROM {VBBOwner}.TZIM_IPRP_INCENTIVE_PGM_RWD TZIM_IPRP_INCENTIVE_PGM_RWD
WHERE TZIM_IPRP_INCENTIVE_PGM_RWD.IPRP_UPDATE_DT > '{LastRunDtm}'"""
df_TZIM_IPRP_INCENTIVE_PGM_RWD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_TZIM)
    .options(**jdbc_props_TZIM)
    .option("query", extract_query)
    .load()
)

# Transformer "Trim" logic with left join on hf_vbb_cmpnt
df_joined_for_trim = (
    df_TZIM_IPRP_INCENTIVE_PGM_RWD.alias("Extract")
    .join(
        df_hf_vbb_cmpnt.alias("DSLink50"),
        (
            (strip_field(F.col("Extract.INPR_ID")) == F.col("DSLink50.VBB_CMPNT_UNIQ_KEY"))
            & (F.lit(SrcSysCd) == F.col("DSLink50.SRC_SYS_CD"))
        ),
        "left"
    )
)

df_trim_intermediate = df_joined_for_trim.select(
    F.col("Extract.INPR_ID").alias("Extract_INPR_ID"),
    F.col("Extract.IPRP_SEQ_NO").alias("Extract_IPRP_SEQ_NO"),
    F.col("Extract.IPRP_DESCRIPTION").alias("Extract_IPRP_DESCRIPTION"),
    F.col("Extract.IPRP_START_DT").alias("Extract_IPRP_START_DT"),
    F.col("Extract.IPRP_END_DT").alias("Extract_IPRP_END_DT"),
    F.col("Extract.IPAL_ACHIEVEMENT_LEVEL").alias("Extract_IPAL_ACHIEVEMENT_LEVEL"),
    F.col("Extract.VNVN_ID").alias("Extract_VNVN_ID"),
    F.col("Extract.VNRW_SEQ_NO").alias("Extract_VNRW_SEQ_NO"),
    F.col("Extract.CTCT_ID").alias("Extract_CTCT_ID"),
    F.col("Extract.RWDC_ID").alias("Extract_RWDC_ID"),
    F.col("Extract.IPRP_EARNED_RWD_START_DT_TYPE").alias("Extract_IPRP_EARNED_RWD_START_DT_TYPE"),
    F.col("Extract.IPRP_EARNED_RWD_START_DT").alias("Extract_IPRP_EARNED_RWD_START_DT"),
    F.col("Extract.IPRP_EARNED_RWD_END_DT_TYPE").alias("Extract_IPRP_EARNED_RWD_END_DT_TYPE"),
    F.col("Extract.IPRP_EARNED_RWD_END_DT").alias("Extract_IPRP_EARNED_RWD_END_DT"),
    F.col("Extract.HIPL_ID").alias("Extract_HIPL_ID"),
    F.col("Extract.HIEL_ID_SEQ").alias("Extract_HIEL_ID_SEQ"),
    F.col("Extract.IPRP_CREATE_DT").alias("Extract_IPRP_CREATE_DT"),
    F.col("Extract.IPRP_CREATE_APP_USER").alias("Extract_IPRP_CREATE_APP_USER"),
    F.col("Extract.IPRP_UPDATE_DT").alias("Extract_IPRP_UPDATE_DT"),
    F.col("Extract.IPRP_UPDATE_DB_USER").alias("Extract_IPRP_UPDATE_DB_USER"),
    F.col("Extract.IPRP_UPDATE_APP_USER").alias("Extract_IPRP_UPDATE_APP_USER"),
    F.col("Extract.IPRP_UPDATE_SEQ").alias("Extract_IPRP_UPDATE_SEQ"),
    F.col("DSLink50.VBB_CMPNT_UNIQ_KEY").alias("DSLink50_VBB_CMPNT_UNIQ_KEY"),
)

df_trim_with_vars = df_trim_intermediate.withColumn(
    "svInprid",
    F.when(
        (F.col("Extract_INPR_ID").isNull()) | (F.length(trim(strip_field(F.col("Extract_INPR_ID")))) == 0),
        F.lit("N")
    ).otherwise(F.lit("Y"))
).withColumn(
    "svIprpSeqNo",
    F.when(
        (F.col("Extract_IPRP_SEQ_NO").isNull()) | (F.length(trim(strip_field(F.col("Extract_IPRP_SEQ_NO")))) == 0),
        F.lit("N")
    ).otherwise(F.lit("Y"))
).withColumn(
    "svVbbCmpntCheck",
    F.when(F.col("DSLink50_VBB_CMPNT_UNIQ_KEY").isNotNull(), F.lit("Y")).otherwise(F.lit("N"))
).withColumn(
    "svGoodRecord",
    F.when(
        (F.col("svInprid") == "Y") & (F.col("svIprpSeqNo") == "Y") & (F.col("svVbbCmpntCheck") == "Y"),
        F.lit("Y")
    ).otherwise(F.lit("N"))
)

df_CmpntData = df_trim_with_vars.filter(F.col("svGoodRecord") == "Y").select(
    trim(strip_field(F.col("Extract_INPR_ID"))).alias("INPR_ID"),
    trim(strip_field(F.col("Extract_IPRP_SEQ_NO"))).alias("IPRP_SEQ_NO"),
    trim(strip_field(F.col("Extract_IPRP_DESCRIPTION"))).alias("IPRP_DESCRIPTION"),
    trim(strip_field(F.col("Extract_IPRP_START_DT"))).alias("IPRP_START_DT"),
    trim(strip_field(F.col("Extract_IPRP_END_DT"))).alias("IPRP_END_DT"),
    trim(strip_field(F.col("Extract_IPAL_ACHIEVEMENT_LEVEL"))).alias("IPAL_ACHIEVEMENT_LEVEL"),
    trim(strip_field(F.col("Extract_VNVN_ID"))).alias("VNVN_ID"),
    trim(strip_field(F.col("Extract_VNRW_SEQ_NO"))).alias("VNRW_SEQ_NO"),
    trim(strip_field(F.col("Extract_CTCT_ID"))).alias("CTCT_ID"),
    trim(strip_field(F.col("Extract_RWDC_ID"))).alias("RWDC_ID"),
    trim(strip_field(F.col("Extract_IPRP_EARNED_RWD_START_DT_TYPE"))).alias("IPRP_EARNED_RWD_START_DT_TYPE"),
    trim(strip_field(F.col("Extract_IPRP_EARNED_RWD_START_DT"))).alias("IPRP_EARNED_RWD_START_DT"),
    trim(strip_field(F.col("Extract_IPRP_EARNED_RWD_END_DT_TYPE"))).alias("IPRP_EARNED_RWD_END_DT_TYPE"),
    trim(strip_field(F.col("Extract_IPRP_EARNED_RWD_END_DT"))).alias("IPRP_EARNED_RWD_END_DT"),
    trim(strip_field(F.col("Extract_HIPL_ID"))).alias("HIPL_ID"),
    trim(strip_field(F.col("Extract_HIEL_ID_SEQ"))).alias("HIEL_ID_SEQ"),
    trim(strip_field(F.col("Extract_IPRP_CREATE_DT"))).alias("IPRP_CREATE_DT"),
    trim(strip_field(F.col("Extract_IPRP_CREATE_APP_USER"))).alias("IPRP_CREATE_APP_USER"),
    trim(strip_field(F.col("Extract_IPRP_UPDATE_DT"))).alias("IPRP_UPDATE_DT"),
    trim(strip_field(F.col("Extract_IPRP_UPDATE_DB_USER"))).alias("IPRP_UPDATE_DB_USER"),
    trim(strip_field(F.col("Extract_IPRP_UPDATE_APP_USER"))).alias("IPRP_UPDATE_APP_USER"),
    trim(strip_field(F.col("Extract_IPRP_UPDATE_SEQ"))).alias("IPRP_UPDATE_SEQ")
)

df_ErrorFile = df_trim_with_vars.filter(F.col("svGoodRecord") == "N").select(
    trim(strip_field(F.col("Extract_INPR_ID"))).alias("INPR_ID"),
    trim(strip_field(F.col("Extract_IPRP_SEQ_NO"))).alias("IPRP_SEQ_NO"),
    trim(strip_field(F.col("Extract_IPRP_DESCRIPTION"))).alias("IPRP_DESCRIPTION"),
    trim(strip_field(F.col("Extract_IPRP_START_DT"))).alias("IPRP_START_DT"),
    trim(strip_field(F.col("Extract_IPRP_END_DT"))).alias("IPRP_END_DT"),
    trim(strip_field(F.col("Extract_IPAL_ACHIEVEMENT_LEVEL"))).alias("IPAL_ACHIEVEMENT_LEVEL"),
    trim(strip_field(F.col("Extract_VNVN_ID"))).alias("VNVN_ID"),
    trim(strip_field(F.col("Extract_VNRW_SEQ_NO"))).alias("VNRW_SEQ_NO"),
    trim(strip_field(F.col("Extract_CTCT_ID"))).alias("CTCT_ID"),
    trim(strip_field(F.col("Extract_RWDC_ID"))).alias("RWDC_ID"),
    trim(strip_field(F.col("Extract_IPRP_EARNED_RWD_START_DT_TYPE"))).alias("IPRP_EARNED_RWD_START_DT_TYPE"),
    trim(strip_field(F.col("Extract_IPRP_EARNED_RWD_START_DT"))).alias("IPRP_EARNED_RWD_START_DT"),
    trim(strip_field(F.col("Extract_IPRP_EARNED_RWD_END_DT_TYPE"))).alias("IPRP_EARNED_RWD_END_DT_TYPE"),
    trim(strip_field(F.col("Extract_IPRP_EARNED_RWD_END_DT"))).alias("IPRP_EARNED_RWD_END_DT"),
    trim(strip_field(F.col("Extract_HIPL_ID"))).alias("HIPL_ID"),
    trim(strip_field(F.col("Extract_HIEL_ID_SEQ"))).alias("HIEL_ID_SEQ"),
    trim(strip_field(F.col("Extract_IPRP_CREATE_DT"))).alias("IPRP_CREATE_DT"),
    trim(strip_field(F.col("Extract_IPRP_CREATE_APP_USER"))).alias("IPRP_CREATE_APP_USER"),
    trim(strip_field(F.col("Extract_IPRP_UPDATE_DT"))).alias("IPRP_UPDATE_DT"),
    trim(strip_field(F.col("Extract_IPRP_UPDATE_DB_USER"))).alias("IPRP_UPDATE_DB_USER"),
    trim(strip_field(F.col("Extract_IPRP_UPDATE_APP_USER"))).alias("IPRP_UPDATE_APP_USER"),
    trim(strip_field(F.col("Extract_IPRP_UPDATE_SEQ"))).alias("IPRP_UPDATE_SEQ")
)

# Write ErrorFile as CSeqFileStage
write_files(
    df_ErrorFile,
    f"{adls_path_publish}/external/VBB_CMPNT_RWRD_ErrorsOut_{RunId}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# Stages RTRT_ID1,2,3,4,5,8,9,10,11 => read from DB (same vbb_secret_name)
jdbc_url_rtrt, jdbc_props_rtrt = get_db_config(vbb_secret_name)

query_RTRT_ID1 = f"""--REWARD TYPE = CASH
SELECT 
PGM.INPR_ID,
PGM.IPRP_SEQ_NO,
PGM.VNVN_ID,
PGM.VNRW_SEQ_NO,
PARM.IPPA_PARM AS DOLLAR_AMT
FROM
{VBBOwner}.TZIM_VNRW_VEND_RWD VEND,
{VBBOwner}.TZIM_IPRP_INCENTIVE_PGM_RWD PGM,
{VBBOwner}.TZIM_IPPA_INCENTIVE_PGM_RWD_PARM PARM
WHERE VEND.RTRT_ID = 1 AND
 VEND.VNVN_ID = PGM.VNVN_ID AND
 VEND.VNRW_SEQ_NO = PGM.VNRW_SEQ_NO AND
 PGM.INPR_ID = PARM.INPR_ID AND
 PGM.IPRP_SEQ_NO = PARM.IPRP_SEQ_NO AND
 PARM.IPPA_PARM_NO = 1"""
df_RTRT_ID1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_rtrt)
    .options(**jdbc_props_rtrt)
    .option("query", query_RTRT_ID1)
    .load()
)

query_RTRT_ID2 = f"""--REWARD TYPE = POINTS
SELECT 
PGM.INPR_ID,
PGM.IPRP_SEQ_NO,
PGM.VNVN_ID,
PGM.VNRW_SEQ_NO,
PARM.IPPA_PARM AS DOLLAR_AMT
FROM
{VBBOwner}.TZIM_VNRW_VEND_RWD VEND,
{VBBOwner}.TZIM_IPRP_INCENTIVE_PGM_RWD PGM,
{VBBOwner}.TZIM_IPPA_INCENTIVE_PGM_RWD_PARM PARM
WHERE VEND.RTRT_ID = 2 AND
 VEND.VNVN_ID = PGM.VNVN_ID AND
 VEND.VNRW_SEQ_NO = PGM.VNRW_SEQ_NO AND
 PGM.INPR_ID = PARM.INPR_ID AND
 PGM.IPRP_SEQ_NO = PARM.IPRP_SEQ_NO AND
 PARM.IPPA_PARM_NO = 1"""
df_RTRT_ID2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_rtrt)
    .options(**jdbc_props_rtrt)
    .option("query", query_RTRT_ID2)
    .load()
)

query_RTRT_ID3 = f"""--REWARD TYPE = PREMIUM SHARING REDUCTION
SELECT
PGM.INPR_ID,
PGM.IPRP_SEQ_NO,
PGM.VNVN_ID,
PGM.VNRW_SEQ_NO,
0 AS DOLLAR_AMT
FROM
{VBBOwner}.TZIM_VNRW_VEND_RWD VEND,
{VBBOwner}.TZIM_IPRP_INCENTIVE_PGM_RWD PGM,
{VBBOwner}.TZIM_IPPA_INCENTIVE_PGM_RWD_PARM PARM_1,
{VBBOwner}.TZIM_IPPA_INCENTIVE_PGM_RWD_PARM PARM_2
WHERE (VEND.RTRT_ID = 3 Or VEND.RTRT_ID = 16) AND
 VEND.VNVN_ID = PGM.VNVN_ID AND
 VEND.VNRW_SEQ_NO = PGM.VNRW_SEQ_NO AND
 PGM.INPR_ID = PARM_1.INPR_ID AND
 PGM.IPRP_SEQ_NO = PARM_1.IPRP_SEQ_NO AND
 PARM_1.IPPA_PARM_NO = 1 AND
 PARM_1.INPR_ID = PARM_1.INPR_ID AND
 PARM_2.IPRP_SEQ_NO = PARM_1.IPRP_SEQ_NO AND
 PARM_2.IPPA_PARM_NO = 2"""
df_RTRT_ID3 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_rtrt)
    .options(**jdbc_props_rtrt)
    .option("query", query_RTRT_ID3)
    .load()
)

query_RTRT_ID4 = f"""--REWARD TYPE = HRA CONTRIBUTION
SELECT 
PGM.INPR_ID,
PGM.IPRP_SEQ_NO,
PGM.VNVN_ID,
PGM.VNRW_SEQ_NO,
PARM_1.IPPA_PARM AS DOLLAR_AMT
FROM
{VBBOwner}.TZIM_VNRW_VEND_RWD VEND,
{VBBOwner}.TZIM_IPRP_INCENTIVE_PGM_RWD PGM,
{VBBOwner}.TZIM_IPPA_INCENTIVE_PGM_RWD_PARM PARM_1,
{VBBOwner}.TZIM_IPPA_INCENTIVE_PGM_RWD_PARM PARM_2
WHERE VEND.RTRT_ID = 4 AND
 VEND.VNVN_ID = PGM.VNVN_ID AND
 VEND.VNRW_SEQ_NO = PGM.VNRW_SEQ_NO AND
 PGM.INPR_ID = PARM_1.INPR_ID AND
 PGM.IPRP_SEQ_NO = PARM_1.IPRP_SEQ_NO AND
 PARM_1.IPPA_PARM_NO = 1 AND
 PARM_1.INPR_ID = PARM_1.INPR_ID AND
 PARM_2.IPRP_SEQ_NO = PARM_1.IPRP_SEQ_NO AND
 PARM_2.IPPA_PARM_NO = 2"""
df_RTRT_ID4 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_rtrt)
    .options(**jdbc_props_rtrt)
    .option("query", query_RTRT_ID4)
    .load()
)

query_RTRT_ID5 = f"""--REWARD TYPE = ENHANCED MEDICAL BENIFIT
SELECT
PGM.INPR_ID,
PGM.IPRP_SEQ_NO,
PGM.VNVN_ID,
PGM.VNRW_SEQ_NO,
0 AS DOLLAR_AMT
FROM
{VBBOwner}.TZIM_VNRW_VEND_RWD VEND,
{VBBOwner}.TZIM_IPRP_INCENTIVE_PGM_RWD PGM,
{VBBOwner}.TZIM_IPPA_INCENTIVE_PGM_RWD_PARM PARM
WHERE (VEND.RTRT_ID = 5 Or VEND.RTRT_ID = 14) AND
 VEND.VNVN_ID = PGM.VNVN_ID AND
 VEND.VNRW_SEQ_NO = PGM.VNRW_SEQ_NO AND
 PGM.INPR_ID = PARM.INPR_ID AND
 PGM.IPRP_SEQ_NO = PARM.IPRP_SEQ_NO AND
 PARM.IPPA_PARM_NO = 1"""
df_RTRT_ID5 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_rtrt)
    .options(**jdbc_props_rtrt)
    .option("query", query_RTRT_ID5)
    .load()
)

query_RTRT_ID8 = f"""--GIFT CARD
SELECT 
PGM.INPR_ID,
PGM.IPRP_SEQ_NO,
PGM.VNVN_ID,
PGM.VNRW_SEQ_NO,
PARM.IPPA_PARM AS DOLLAR_AMT
FROM
{VBBOwner}.TZIM_VNRW_VEND_RWD VEND,
{VBBOwner}.TZIM_IPRP_INCENTIVE_PGM_RWD PGM,
{VBBOwner}.TZIM_IPPA_INCENTIVE_PGM_RWD_PARM PARM
WHERE VEND.RTRT_ID = 8 AND
 VEND.VNVN_ID = PGM.VNVN_ID AND
 VEND.VNRW_SEQ_NO = PGM.VNRW_SEQ_NO AND
 PGM.INPR_ID = PARM.INPR_ID AND
 PGM.IPRP_SEQ_NO = PARM.IPRP_SEQ_NO AND
 PARM.IPPA_PARM_NO = 1"""
df_RTRT_ID8 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_rtrt)
    .options(**jdbc_props_rtrt)
    .option("query", query_RTRT_ID8)
    .load()
)

query_RTRT_ID9 = f"""--REWARD TYPE = HAS CONTRIBUTION
SELECT 
PGM.INPR_ID,
PGM.IPRP_SEQ_NO,
PGM.VNVN_ID,
PGM.VNRW_SEQ_NO,
PARM.IPPA_PARM AS DOLLAR_AMT
FROM
{VBBOwner}.TZIM_VNRW_VEND_RWD VEND,
{VBBOwner}.TZIM_IPRP_INCENTIVE_PGM_RWD PGM,
{VBBOwner}.TZIM_IPPA_INCENTIVE_PGM_RWD_PARM PARM
WHERE VEND.RTRT_ID = 9 AND
 VEND.VNVN_ID = PGM.VNVN_ID AND
 VEND.VNRW_SEQ_NO = PGM.VNRW_SEQ_NO AND
 PGM.INPR_ID = PARM.INPR_ID AND
 PGM.IPRP_SEQ_NO = PARM.IPRP_SEQ_NO AND
 PARM.IPPA_PARM_NO = 1"""
df_RTRT_ID9 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_rtrt)
    .options(**jdbc_props_rtrt)
    .option("query", query_RTRT_ID9)
    .load()
)

query_RTRT_ID10 = f"""--REWARD TYPE = HALLMARK PREMIERE GIFT CARD
SELECT 
PGM.INPR_ID,
PGM.IPRP_SEQ_NO,
PGM.VNVN_ID,
PGM.VNRW_SEQ_NO,
PARM.IPPA_PARM AS DOLLAR_AMT
FROM
{VBBOwner}.TZIM_VNRW_VEND_RWD VEND,
{VBBOwner}.TZIM_IPRP_INCENTIVE_PGM_RWD PGM,
{VBBOwner}.TZIM_IPPA_INCENTIVE_PGM_RWD_PARM PARM
WHERE VEND.RTRT_ID = 10 AND
 VEND.VNVN_ID = PGM.VNVN_ID AND
 VEND.VNRW_SEQ_NO = PGM.VNRW_SEQ_NO AND
 PGM.INPR_ID = PARM.INPR_ID AND
 PGM.IPRP_SEQ_NO = PARM.IPRP_SEQ_NO AND
 PARM.IPPA_PARM_NO = 1"""
df_RTRT_ID10 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_rtrt)
    .options(**jdbc_props_rtrt)
    .option("query", query_RTRT_ID10)
    .load()
)

query_RTRT_ID11 = f"""--REWARD TYPE = HALLMARK SPECIFIED GIFT CARD
SELECT
PGM.INPR_ID,
PGM.IPRP_SEQ_NO,
PGM.VNVN_ID,
PGM.VNRW_SEQ_NO,
PARM_1.IPPA_PARM AS DOLLAR_AMT
FROM
{VBBOwner}.TZIM_VNRW_VEND_RWD VEND,
{VBBOwner}.TZIM_IPRP_INCENTIVE_PGM_RWD PGM,
{VBBOwner}.TZIM_IPPA_INCENTIVE_PGM_RWD_PARM PARM_1,
{VBBOwner}.TZIM_IPPA_INCENTIVE_PGM_RWD_PARM PARM_2
WHERE VEND.RTRT_ID = 11 AND
 VEND.VNVN_ID = PGM.VNVN_ID AND
 VEND.VNRW_SEQ_NO = PGM.VNRW_SEQ_NO AND
 PGM.INPR_ID = PARM_1.INPR_ID AND
 PGM.IPRP_SEQ_NO = PARM_1.IPRP_SEQ_NO AND
 PARM_1.IPPA_PARM_NO = 1 AND
 PARM_1.INPR_ID = PARM_1.INPR_ID AND
 PARM_2.IPRP_SEQ_NO = PARM_1.IPRP_SEQ_NO AND
 PARM_2.IPPA_PARM_NO = 2"""
df_RTRT_ID11 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_rtrt)
    .options(**jdbc_props_rtrt)
    .option("query", query_RTRT_ID11)
    .load()
)

# Link Collector 105 (Union of the 9 dataframes)
df_Link_Collector_105 = (
    df_RTRT_ID1
    .unionByName(df_RTRT_ID2)
    .unionByName(df_RTRT_ID3)
    .unionByName(df_RTRT_ID4)
    .unionByName(df_RTRT_ID5)
    .unionByName(df_RTRT_ID8)
    .unionByName(df_RTRT_ID9)
    .unionByName(df_RTRT_ID10)
    .unionByName(df_RTRT_ID11)
)

# Stage hf_vbbcmpntrwrd_ernrwrdamt (Scenario C: write then read parquet)
write_files(
    df_Link_Collector_105,
    f"{adls_path}/hf_vbbcmpntrwrd_ernrwrdamt.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_vbbcmpntrwrd_ernrwrdamt = spark.read.parquet(f"{adls_path}/hf_vbbcmpntrwrd_ernrwrdamt.parquet")

# Transformer_6: Primary link = df_CmpntData, Lookup link = df_hf_vbbcmpntrwrd_ernrwrdamt
df_Transformer_6_join = (
    df_CmpntData.alias("CmpntData")
    .join(
        df_hf_vbbcmpntrwrd_ernrwrdamt.alias("Amt"),
        (
            (F.col("CmpntData.INPR_ID") == F.col("Amt.INPR_ID"))
            & (F.col("CmpntData.IPRP_SEQ_NO") == F.col("Amt.IPRP_SEQ_NO"))
            & (F.col("CmpntData.VNVN_ID") == F.col("Amt.VNVN_ID"))
            & (F.col("CmpntData.VNRW_SEQ_NO") == F.col("Amt.VNRW_SEQ_NO"))
        ),
        "left"
    )
)

df_AllCol = df_Transformer_6_join.select(
    F.col("CmpntData.INPR_ID").alias("VBB_CMPNT_UNIQ_KEY"),
    F.col("CmpntData.IPRP_SEQ_NO").alias("VBB_CMPNT_RWRD_SEQ_NO"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    F.col("CmpntData.IPRP_CREATE_DT").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat_ws(";", F.col("CmpntData.INPR_ID"), F.col("CmpntData.IPRP_SEQ_NO"), F.lit(SrcSysCd)).alias("PRI_KEY_STRING"),
    F.lit(0).alias("VBB_CMPNT_RWRD_SK"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CmpntData.VNVN_ID").alias("VNVN_ID"),
    F.col("CmpntData.VNRW_SEQ_NO").alias("VNRW_SEQ_NO"),
    F.when(
        (F.col("CmpntData.IPRP_EARNED_RWD_END_DT_TYPE").isNull()) | (F.length(F.trim(F.col("CmpntData.IPRP_EARNED_RWD_END_DT_TYPE"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("CmpntData.IPRP_EARNED_RWD_END_DT_TYPE")).alias("ERN_RWRD_END_DT_TYP_CD"),
    F.when(
        (F.col("CmpntData.IPRP_EARNED_RWD_START_DT_TYPE").isNull()) | (F.length(F.trim(F.col("CmpntData.IPRP_EARNED_RWD_START_DT_TYPE"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("CmpntData.IPRP_EARNED_RWD_START_DT_TYPE")).alias("ERN_RWRD_STRT_DT_TYP_CD"),
    F.rpad(
        F.when(
            (F.col("CmpntData.IPRP_START_DT").isNull()) | (F.length(F.trim(F.col("CmpntData.IPRP_START_DT"))) == 0),
            F.lit("1753-01-01")
        ).otherwise(F.col("CmpntData.IPRP_START_DT").substr(F.lit(1), F.lit(10))),
        10, " "
    ).alias("VBB_CMPNT_RWRD_BEG_DT_SK"),
    F.rpad(
        F.when(
            (F.col("CmpntData.IPRP_END_DT").isNull()) | (F.length(F.trim(F.col("CmpntData.IPRP_END_DT"))) == 0),
            F.lit("2199-12-31")
        ).otherwise(F.col("CmpntData.IPRP_END_DT").substr(F.lit(1), F.lit(10))),
        10, " "
    ).alias("VBB_CMPNT_RWRD_END_DT_SK"),
    F.rpad(
        F.when(
            (F.col("CmpntData.IPRP_EARNED_RWD_END_DT").isNull()) | (F.length(F.trim(F.col("CmpntData.IPRP_EARNED_RWD_END_DT"))) == 0),
            F.lit("2199-12-31")
        ).otherwise(F.col("CmpntData.IPRP_EARNED_RWD_END_DT").substr(F.lit(1), F.lit(10))),
        10, " "
    ).alias("ERN_RWRD_END_DT_SK"),
    F.rpad(
        F.when(
            (F.col("CmpntData.IPRP_EARNED_RWD_START_DT").isNull()) | (F.length(F.trim(F.col("CmpntData.IPRP_EARNED_RWD_START_DT"))) == 0),
            F.lit("1753-01-01")
        ).otherwise(F.col("CmpntData.IPRP_EARNED_RWD_START_DT").substr(F.lit(1), F.lit(10))),
        10, " "
    ).alias("ERN_RWRD_STRT_DT_SK"),
    # FORMAT.DATE(...) translates to approximate logic, but in instructions we keep direct approach. Using user-defined function for now:
    F.when(
        (F.col("CmpntData.IPRP_CREATE_DT").isNull()),
        None
    ).otherwise(F.col("CmpntData.IPRP_CREATE_DT")).alias("SRC_SYS_CRT_DTM"),
    F.when(
        (F.col("CmpntData.IPRP_UPDATE_DT").isNull()),
        None
    ).otherwise(F.col("CmpntData.IPRP_UPDATE_DT")).alias("SRC_SYS_UPDT_DTM"),
    F.when(
        F.col("Amt.IPPA_PARM").isNull(),
        F.lit("0.00")
    ).otherwise(
        F.when(F.col("Amt.IPPA_PARM") == "", F.lit("0")).otherwise(F.col("Amt.IPPA_PARM"))
    ).alias("ERN_RWRD_AMT"),
    F.when(
        (F.col("CmpntData.IPAL_ACHIEVEMENT_LEVEL").isNull()) | (F.length(F.trim(F.col("CmpntData.IPAL_ACHIEVEMENT_LEVEL"))) == 0),
        F.lit("0")
    ).otherwise(F.col("CmpntData.IPAL_ACHIEVEMENT_LEVEL")).alias("RQRD_ACHV_LVL_NO"),
    F.when(
        (F.col("CmpntData.IPRP_DESCRIPTION").isNull()) | (F.length(F.trim(F.col("CmpntData.IPRP_DESCRIPTION"))) == 0),
        F.lit("NA")
    ).otherwise(F.upper(F.col("CmpntData.IPRP_DESCRIPTION"))).alias("VBB_CMPNT_RWRD_DESC")
)

df_Transform = df_Transformer_6_join.select(
    F.col("CmpntData.INPR_ID").alias("VBB_CMPNT_UNIQ_KEY"),
    F.col("CmpntData.IPRP_SEQ_NO").alias("VBB_CMPNT_RWRD_SEQ_NO"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

df_Snapshot = df_Transformer_6_join.select(
    F.col("CmpntData.INPR_ID").alias("VBB_CMPNT_UNIQ_KEY"),
    F.col("CmpntData.IPRP_SEQ_NO").alias("VBB_CMPNT_RWRD_SEQ_NO"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

# Write Snapshot -> B_VBB_CMPNT_RWRD.dat
write_files(
    df_Snapshot,
    f"{adls_path}/load/B_VBB_CMPNT_RWRD.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Call Shared Container VbbCmpntRwrdPK with 2 inputs (Transform, AllCol)
params_VbbCmpntRwrdPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}
df_VbbCmpntRwrdKey = VbbCmpntRwrdPK(df_Transform, df_AllCol, params_VbbCmpntRwrdPK)

# Write VbbCmpntRwrdKey -> key/IhmfConstituentVbbCmpntRwrdExtr.VbbCmpntRwrd.dat.#RunId#
write_files(
    df_VbbCmpntRwrdKey,
    f"{adls_path}/key/IhmfConstituentVbbCmpntRwrdExtr.VbbCmpntRwrd.dat.{RunId}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)