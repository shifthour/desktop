# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Extracts data and assigns Primary Keys for the IDS table MBR_VBB_CMPNT_RWRD.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi         2013-05-13          4963 VBB Phase III     Initial Programming                                                                      IntegrateNewDevl           Bhoomi Dasari            7/3/2013
# MAGIC Raja Gummadi         2013-10-16          4963 VBB                    Added snapshot file to the job                                                     IntegrateNewDevl           Kalyan Neelam            2013-10-25

# MAGIC IDS MBR_VBB_CMPNT_RWRD Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


VBBOwner = get_widget_value('VBBOwner','')
vbb_secret_name = get_widget_value('vbb_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunId = get_widget_value('RunId','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
LastRunDtm = get_widget_value('LastRunDtm','')
CurrDate = get_widget_value('CurrDate','')
Environment = get_widget_value('Environment','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/MbrVbbCmpntRwrdPK
# COMMAND ----------

jdbc_url, jdbc_props = get_db_config(vbb_secret_name)

extract_query = f"""
SELECT 
TZIM_MEIR_MEMBER_INCENTIVE_PGM_RWD.MEIP_MEMBER_ID, 
TZIM_MEIR_MEMBER_INCENTIVE_PGM_RWD.INPR_ID, 
TZIM_MEIR_MEMBER_INCENTIVE_PGM_RWD.MEIA_ACHIEVEMENT_LEVEL, 
TZIM_MEIR_MEMBER_INCENTIVE_PGM_RWD.IPRP_SEQ_NO, 
TZIM_MEIR_MEMBER_INCENTIVE_PGM_RWD.MEIR_EARNED_RWD_START_DT,
TZIM_MEIR_MEMBER_INCENTIVE_PGM_RWD.MEIR_EARNED_RWD_END_DT,
TZIM_MEIR_MEMBER_INCENTIVE_PGM_RWD.MEIR_CREATE_DT, 
TZIM_MEIR_MEMBER_INCENTIVE_PGM_RWD.MEIR_UPDATE_DT, 
TZIM_MERP_MEMBER_REWARD_PAYMENTS.MERP_REWARD_PAID, 
TZIM_MERP_MEMBER_REWARD_PAYMENTS.MERP_REWARD_ACCUM, 
TZIM_MERP_MEMBER_REWARD_PAYMENTS.MERP_LIMIT_REASON
FROM {VBBOwner}.TZIM_MEIR_MEMBER_INCENTIVE_PGM_RWD TZIM_MEIR_MEMBER_INCENTIVE_PGM_RWD 
LEFT JOIN {VBBOwner}.TZIM_MERP_MEMBER_REWARD_PAYMENTS TZIM_MERP_MEMBER_REWARD_PAYMENTS
ON TZIM_MEIR_MEMBER_INCENTIVE_PGM_RWD.MEIP_MEMBER_ID = TZIM_MERP_MEMBER_REWARD_PAYMENTS.MEIP_MEMBER_ID
AND TZIM_MEIR_MEMBER_INCENTIVE_PGM_RWD.INPR_ID = TZIM_MERP_MEMBER_REWARD_PAYMENTS.INPR_ID
WHERE TZIM_MEIR_MEMBER_INCENTIVE_PGM_RWD.MEIR_UPDATE_DT > '{LastRunDtm}'
"""

df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

xref_query = f"""
SELECT 
TZIM_MKCR_MEMBER_KEY_CROSS_REFERENCE.MKCR_GUID,
TZIM_MKCR_MEMBER_KEY_CROSS_REFERENCE.MKCR_SEGMENT_VALUE
FROM {VBBOwner}.TZIM_MKCR_MEMBER_KEY_CROSS_REFERENCE TZIM_MKCR_MEMBER_KEY_CROSS_REFERENCE
WHERE
TZIM_MKCR_MEMBER_KEY_CROSS_REFERENCE.MKCR_SEGMENT_NAME = 'MEME_CK'
"""

df_Xref = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", xref_query)
    .load()
)

df_forTrim = (
    df_Extract
    .withColumn(
        "INPR_ID_cleaned",
        F.trim(F.regexp_replace(F.col("INPR_ID"), "[\n\r\t]", ""))
    )
    .withColumn(
        "svHiplId",
        F.when(
            F.isnull("INPR_ID_cleaned") | (F.length("INPR_ID_cleaned") == 0),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
    .withColumn(
        "svGoodRecord",
        F.when(F.col("svHiplId") == "Y", F.lit("Y")).otherwise(F.lit("N"))
    )
)

df_CmpntData = (
    df_forTrim
    .filter(F.col("svGoodRecord") == "Y")
    .select(
        F.trim("MEIP_MEMBER_ID").alias("MEIP_MEMBER_ID"),
        F.trim("INPR_ID").alias("INPR_ID"),
        F.trim("MEIA_ACHIEVEMENT_LEVEL").alias("MEIA_ACHIEVEMENT_LEVEL"),
        F.trim("IPRP_SEQ_NO").alias("IPRP_SEQ_NO"),
        F.trim("MEIR_EARNED_RWD_START_DT").alias("MEIR_EARNED_RWD_START_DT"),
        F.trim("MEIR_EARNED_RWD_END_DT").alias("MEIR_EARNED_RWD_END_DT"),
        F.trim("MEIR_CREATE_DT").alias("MEIR_CREATE_DT"),
        F.trim("MEIR_UPDATE_DT").alias("MEIR_UPDATE_DT"),
        F.trim("MERP_REWARD_PAID").alias("MERP_REWARD_PAID"),
        F.trim("MERP_REWARD_ACCUM").alias("MERP_REWARD_ACCUM"),
        F.upper(F.trim("MERP_LIMIT_REASON")).alias("MERP_LIMIT_REASON")
    )
)

df_ErrorFile = (
    df_forTrim
    .filter(F.col("svGoodRecord") == "N")
    .select(
        "MEIP_MEMBER_ID",
        "INPR_ID",
        "MEIA_ACHIEVEMENT_LEVEL",
        "IPRP_SEQ_NO",
        "MEIR_EARNED_RWD_START_DT",
        "MEIR_EARNED_RWD_END_DT",
        "MEIR_CREATE_DT",
        "MEIR_UPDATE_DT",
        "MERP_REWARD_PAID",
        "MERP_REWARD_ACCUM",
        "MERP_LIMIT_REASON"
    )
)

write_files(
    df_ErrorFile.select(
        "MEIP_MEMBER_ID",
        "INPR_ID",
        "MEIA_ACHIEVEMENT_LEVEL",
        "IPRP_SEQ_NO",
        "MEIR_EARNED_RWD_START_DT",
        "MEIR_EARNED_RWD_END_DT",
        "MEIR_CREATE_DT",
        "MEIR_UPDATE_DT",
        "MERP_REWARD_PAID",
        "MERP_REWARD_ACCUM",
        "MERP_LIMIT_REASON"
    ),
    f"{adls_path_publish}/external/MBR_VBB_CMPNT_RWRD_ErrorsOut_{RunId}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

df_Xref_dedup = df_Xref.dropDuplicates(["MKCR_GUID"])

df_Transformer_6 = (
    df_CmpntData.alias("CmpntData")
    .join(
        df_Xref_dedup.alias("xreflkup"),
        F.col("CmpntData.MEIP_MEMBER_ID") == F.col("xreflkup.MKCR_GUID"),
        "left"
    )
    .withColumn(
        "svSrcSysCrtDt",
        F.when(
            F.isnull("CmpntData.MEIR_CREATE_DT")
            | (F.length("CmpntData.MEIR_CREATE_DT") == 0),
            F.lit("1753-01-01 00:00:00.000")
        ).otherwise(F.col("CmpntData.MEIR_CREATE_DT"))
    )
    .withColumn(
        "svSrcSysUptDt",
        F.when(
            F.isnull("CmpntData.MEIR_UPDATE_DT")
            | (F.length("CmpntData.MEIR_UPDATE_DT") == 0),
            F.lit("1753-01-01 00:00:00.000")
        ).otherwise(F.col("CmpntData.MEIR_UPDATE_DT"))
    )
    .withColumn(
        "svErnRwrdEndDtSk",
        F.when(
            F.isnull("CmpntData.MEIR_EARNED_RWD_END_DT")
            | (F.length("CmpntData.MEIR_EARNED_RWD_END_DT") == 0),
            F.lit("2199-12-31")
        ).otherwise(F.substring("CmpntData.MEIR_EARNED_RWD_END_DT", 1, 10))
    )
    .withColumn(
        "svErnRwrdStrtDtSk",
        F.when(
            F.isnull("CmpntData.MEIR_EARNED_RWD_START_DT")
            | (F.length("CmpntData.MEIR_EARNED_RWD_START_DT") == 0),
            F.lit("1753-01-01")
        ).otherwise(F.substring("CmpntData.MEIR_EARNED_RWD_START_DT", 1, 10))
    )
    .withColumn(
        "MBR_UNIQ_KEY",
        F.when(
            F.isnull("xreflkup.MKCR_GUID") | (F.length("xreflkup.MKCR_GUID") == 0),
            F.lit("0")
        ).otherwise(F.col("xreflkup.MKCR_SEGMENT_VALUE"))
    )
    .withColumn("VBB_CMPNT_UNIQ_KEY", F.col("CmpntData.INPR_ID"))
    .withColumn("VBB_CMPNT_RWRD_SEQ_NO", F.col("CmpntData.IPRP_SEQ_NO"))
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.rpad(F.lit("I"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.lit("N"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.lit("Y"), 1, " "))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD_1", F.lit(SrcSysCd))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws(
            ";",
            F.trim(F.col("xreflkup.MKCR_SEGMENT_VALUE")),
            F.trim(F.col("CmpntData.INPR_ID")),
            F.trim(F.col("CmpntData.IPRP_SEQ_NO")),
            F.lit(SrcSysCd)
        )
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn(
        "MERP_LIMIT_REASON",
        F.when(
            F.isnull("CmpntData.MERP_LIMIT_REASON")
            | (F.length(F.trim("CmpntData.MERP_LIMIT_REASON")) == 0),
            F.lit("NA")
        ).otherwise(F.col("CmpntData.MERP_LIMIT_REASON"))
    )
    .withColumn("ERN_RWRD_END_DT_SK", F.rpad(F.col("svErnRwrdEndDtSk"), 10, " "))
    .withColumn("ERN_RWRD_STRT_DT_SK", F.rpad(F.col("svErnRwrdStrtDtSk"), 10, " "))
    .withColumn("SRC_SYS_CRT_DTM", F.col("svSrcSysCrtDt"))
    .withColumn("SRC_SYS_UPDT_DTM", F.col("svSrcSysUptDt"))
    .withColumn(
        "CMPLD_ACHV_LVL_NO",
        F.when(
            F.isnull("CmpntData.MEIA_ACHIEVEMENT_LEVEL")
            | (F.length(F.trim("CmpntData.MEIA_ACHIEVEMENT_LEVEL")) == 0),
            F.lit("0")
        ).otherwise(F.col("CmpntData.MEIA_ACHIEVEMENT_LEVEL"))
    )
    .withColumn(
        "ERN_RWRD_AMT",
        F.when(
            F.isnull("CmpntData.MERP_REWARD_ACCUM")
            | (F.length(F.trim("CmpntData.MERP_REWARD_ACCUM")) == 0),
            F.lit("0")
        ).otherwise(F.col("CmpntData.MERP_REWARD_ACCUM"))
    )
    .withColumn(
        "PD_RWRD_AMT",
        F.when(
            F.isnull("CmpntData.MERP_REWARD_PAID")
            | (F.length(F.trim("CmpntData.MERP_REWARD_PAID")) == 0),
            F.lit("0")
        ).otherwise(F.col("CmpntData.MERP_REWARD_PAID"))
    )
    .withColumn("TRZ_MBR_UNVRS_ID", F.col("CmpntData.MEIP_MEMBER_ID"))
)

df_AllCol = df_Transformer_6.select(
    "MBR_UNIQ_KEY",
    "VBB_CMPNT_UNIQ_KEY",
    "VBB_CMPNT_RWRD_SEQ_NO",
    "SRC_SYS_CD_SK",
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD_1",
    "PRI_KEY_STRING",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MERP_LIMIT_REASON",
    "ERN_RWRD_END_DT_SK",
    "ERN_RWRD_STRT_DT_SK",
    "SRC_SYS_CRT_DTM",
    "SRC_SYS_UPDT_DTM",
    "CMPLD_ACHV_LVL_NO",
    "ERN_RWRD_AMT",
    "PD_RWRD_AMT",
    "TRZ_MBR_UNVRS_ID"
)

df_Transform = df_Transformer_6.select(
    "MBR_UNIQ_KEY",
    "VBB_CMPNT_UNIQ_KEY",
    "VBB_CMPNT_RWRD_SEQ_NO",
    "SRC_SYS_CD_SK"
)

df_Snapshot = df_Transformer_6.select(
    "MBR_UNIQ_KEY",
    "VBB_CMPNT_UNIQ_KEY",
    "VBB_CMPNT_RWRD_SEQ_NO",
    "SRC_SYS_CD_SK"
)

write_files(
    df_Snapshot.select(
        "MBR_UNIQ_KEY",
        "VBB_CMPNT_UNIQ_KEY",
        "VBB_CMPNT_RWRD_SEQ_NO",
        "SRC_SYS_CD_SK"
    ),
    f"{adls_path}/load/B_MBR_VBB_CMPNT_RWRD.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

container_params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}

df_Key = MbrVbbCmpntRwrdPK(df_Transform, df_AllCol, container_params)

df_Key_final = df_Key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "MBR_VBB_CMPNT_RWRD_SK",
    "MBR_UNIQ_KEY",
    "VBB_CMPNT_UNIQ_KEY",
    "VBB_CMPNT_RWRD_SEQ_NO",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MERP_LIMIT_REASON",
    "ERN_RWRD_END_DT_SK",
    "ERN_RWRD_STRT_DT_SK",
    "SRC_SYS_CRT_DTM",
    "SRC_SYS_UPDT_DTM",
    "CMPLD_ACHV_LVL_NO",
    "ERN_RWRD_AMT",
    "PD_RWRD_AMT",
    "TRZ_MBR_UNVRS_ID"
)

write_files(
    df_Key_final,
    f"{adls_path}/key/IhmfConstituentMbrVbbCmpntRwrdExtr.MbrVbbCmpntRwrd.dat.{RunId}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)