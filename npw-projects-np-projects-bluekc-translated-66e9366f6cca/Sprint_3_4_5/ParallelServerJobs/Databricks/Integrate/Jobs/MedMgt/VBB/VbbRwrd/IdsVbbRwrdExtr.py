# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: IhmfConstituentVbbExtrSed
# MAGIC  
# MAGIC PROCESSING:   Extract job for IDS table VBB_RWRD
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Karthik Chintalapani  2013-05-13     4963 VBB Phase III    Initial Programming                                                                       IntegrateNewDevl      Bhoomi Dasari           5/16/2013

# MAGIC IDS VBB_RWRD Extract
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


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
VBBOwner = get_widget_value('VBBOwner','')
vbb_secret_name = get_widget_value('vbb_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunId = get_widget_value('RunId','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
LastRunDtm = get_widget_value('LastRunDtm','')
CurrDate = get_widget_value('CurrDate','')
Environment = get_widget_value('Environment','')

# Database connection for IHMFCONSTITUENT (Extract link)
jdbc_url, jdbc_props = get_db_config(vbb_secret_name)
df_IHMFCONSTITUENT_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT TZIM_VNRW_VEND_RWD.VNVN_ID, TZIM_VNRW_VEND_RWD.VNRW_SEQ_NO, TZIM_VNRW_VEND_RWD.VNRW_NAME, TZIM_VNRW_VEND_RWD.VNRW_DESCRIPTION, Cast(TZIM_VNRW_VEND_RWD.VNRW_START_DT As Date) As VNRW_START_DT, Cast(TZIM_VNRW_VEND_RWD.VNRW_END_DT As Date) As VNRW_END_DT, TZIM_VNRW_VEND_RWD.RTRT_ID, TZIM_VNRW_VEND_RWD.VNRW_CREATE_DT, TZIM_VNRW_VEND_RWD.VNRW_CREATE_APP_USER, TZIM_VNRW_VEND_RWD.VNRW_UPDATE_DT, TZIM_VNRW_VEND_RWD.VNRW_UPDATE_DB_USER, TZIM_VNRW_VEND_RWD.VNRW_UPDATE_APP_USER, TZIM_VNRW_VEND_RWD.VNRW_UPDATE_SEQ FROM {VBBOwner}.TZIM_VNRW_VEND_RWD TZIM_VNRW_VEND_RWD WHERE TZIM_VNRW_VEND_RWD.VNRW_UPDATE_DT > '{LastRunDtm}'"
    )
    .load()
)

# Database connection for IHMFCONSTITUENT (rtrt_lkp link), then deduplicate (Scenario A hashed file)
df_IHMFCONSTITUENT_rtrt_lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT RTRT_ID, RTRT_TYPE FROM {VBBOwner}.TZIM_RTRT_RWD_TYPE")
    .load()
)
df_hf_vndr_rwrd_rtrt_type = dedup_sort(df_IHMFCONSTITUENT_rtrt_lkp, ["RTRT_ID"], [("RTRT_ID","A")])

# Trim Transformer logic
df_Trim = (
    df_IHMFCONSTITUENT_Extract
    .withColumn("cleaned_VNVN_ID", trim(strip_field(F.col("VNVN_ID"))))
    .withColumn("cleaned_VNRW_SEQ_NO", trim(strip_field(F.col("VNRW_SEQ_NO"))))
    .withColumn("cleaned_RTRT_ID", trim(strip_field(F.col("RTRT_ID"))))
    .withColumn(
        "svVnvnId",
        F.when(
            (F.col("cleaned_VNVN_ID") == "") | F.col("cleaned_VNVN_ID").isNull(),
            "N"
        ).otherwise("Y")
    )
    .withColumn(
        "svVnrwSeqNo",
        F.when(
            (F.col("cleaned_VNRW_SEQ_NO") == "") | F.col("cleaned_VNRW_SEQ_NO").isNull(),
            "N"
        ).otherwise("Y")
    )
    .withColumn(
        "svRtrtId",
        F.when(
            (F.col("cleaned_RTRT_ID") == "") | F.col("cleaned_RTRT_ID").isNull(),
            "N"
        ).otherwise("Y")
    )
    .withColumn(
        "svGoodRecord",
        F.when(
            (F.col("svVnvnId") == "Y") & (F.col("svVnrwSeqNo") == "Y") & (F.col("svRtrtId") == "Y"),
            "Y"
        ).otherwise("N")
    )
)

df_RewardData = (
    df_Trim
    .filter(F.col("svGoodRecord") == "Y")
    .select(
        F.col("cleaned_VNVN_ID").alias("VNVN_ID"),
        F.col("cleaned_VNRW_SEQ_NO").alias("VNRW_SEQ_NO"),
        F.upper(trim(strip_field(F.col("VNRW_NAME")))).alias("VNRW_NAME"),
        F.upper(trim(strip_field(F.col("VNRW_DESCRIPTION")))).alias("VNRW_DESCRIPTION"),
        trim(strip_field(F.col("VNRW_START_DT"))).alias("VNRW_START_DT"),
        trim(strip_field(F.col("VNRW_END_DT"))).alias("VNRW_END_DT"),
        F.col("cleaned_RTRT_ID").alias("RTRT_ID"),
        trim(strip_field(F.col("VNRW_CREATE_DT"))).alias("VNRW_CREATE_DT"),
        F.upper(trim(strip_field(F.col("VNRW_CREATE_APP_USER")))).alias("VNRW_CREATE_APP_USER"),
        trim(strip_field(F.col("VNRW_UPDATE_DT"))).alias("VNRW_UPDATE_DT"),
        F.upper(trim(strip_field(F.col("VNRW_UPDATE_DB_USER")))).alias("VNRW_UPDATE_DB_USER"),
        F.upper(trim(strip_field(F.col("VNRW_UPDATE_APP_USER")))).alias("VNRW_UPDATE_APP_USER"),
        trim(strip_field(F.col("VNRW_UPDATE_SEQ"))).alias("VNRW_UPDATE_SEQ")
    )
)

df_Rejects = (
    df_Trim
    .filter(F.col("svGoodRecord") == "N")
    .select(
        F.col("VNVN_ID"),
        F.col("VNRW_SEQ_NO"),
        F.col("VNRW_NAME"),
        F.col("VNRW_DESCRIPTION"),
        F.col("VNRW_START_DT"),
        F.col("VNRW_END_DT"),
        F.col("RTRT_ID"),
        F.col("VNRW_CREATE_DT"),
        F.col("VNRW_CREATE_APP_USER"),
        F.col("VNRW_UPDATE_DT"),
        F.col("VNRW_UPDATE_DB_USER"),
        F.col("VNRW_UPDATE_APP_USER"),
        F.col("VNRW_UPDATE_SEQ")
    )
)

write_files(
    df_Rejects,
    f"{adls_path_publish}/external/VBB_RWRD_ErrorsOut_{RunId}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# BusinessRules Transformer with left join
df_BusinessRules_main = (
    df_RewardData.alias("RewardData")
    .join(
        df_hf_vndr_rwrd_rtrt_type.alias("ref_rtrt_typ"),
        F.col("RewardData.RTRT_ID") == F.col("ref_rtrt_typ.RTRT_ID"),
        "left"
    )
)

df_BusinessRules = (
    df_BusinessRules_main
    .withColumn(
        "svVnrwStartDt",
        F.when(
            F.col("RewardData.VNRW_START_DT").isNull() | (F.length(F.col("RewardData.VNRW_START_DT")) == 0),
            F.lit("1753-01-01")
        ).otherwise(F.col("RewardData.VNRW_START_DT"))
    )
    .withColumn(
        "svVnrwEndDt",
        F.when(
            F.col("RewardData.VNRW_END_DT").isNull() | (F.length(F.col("RewardData.VNRW_END_DT")) == 0),
            F.lit("2199-12-31")
        ).otherwise(F.col("RewardData.VNRW_END_DT"))
    )
    .withColumn(
        "svVnrwCreateDt",
        F.when(
            F.col("RewardData.VNRW_CREATE_DT").isNull() | (F.length(F.col("RewardData.VNRW_CREATE_DT")) == 0),
            F.lit("1753-01-01 00:00:00.000000")
        ).otherwise(format_date(F.col("RewardData.VNRW_CREATE_DT"), 'SYBASE', 'TIMESTAMP','DB2TIMESTAMP'))
    )
    .withColumn(
        "svVnrwUpdateDt",
        F.when(
            F.col("RewardData.VNRW_UPDATE_DT").isNull() | (F.length(F.col("RewardData.VNRW_UPDATE_DT")) == 0),
            F.lit("1753-01-01 00:00:00.000000")
        ).otherwise(format_date(F.col("RewardData.VNRW_UPDATE_DT"), 'SYBASE', 'TIMESTAMP','DB2TIMESTAMP'))
    )
)

df_BusinessRules_AllCol = (
    df_BusinessRules
    .select(
        F.col("RewardData.VNVN_ID").alias("VBB_VNDR_UNIQ_KEY"),
        F.col("RewardData.VNRW_SEQ_NO").alias("VBB_VNDR_RWRD_SEQ_NO"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.lit(CurrDate).alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD_1"),
        F.concat(trim(F.col("RewardData.VNVN_ID")), F.lit(";"), trim(F.col("RewardData.VNRW_SEQ_NO")), F.lit(";"), F.lit(SrcSysCd)).alias("PRI_KEY_STRING"),
        F.lit(0).alias("VBB_RWRD_SK"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svVnrwStartDt").alias("VBB_RWRD_BEG_DT_SK"),
        F.col("svVnrwEndDt").alias("VBB_RWRD_END_DT_SK"),
        F.col("svVnrwCreateDt").alias("SRC_SYS_CRT_DTM"),
        F.col("svVnrwUpdateDt").alias("SRC_SYS_UPDT_DTM"),
        F.when(
            F.col("RewardData.VNRW_DESCRIPTION").isNull() | (F.length(F.col("RewardData.VNRW_DESCRIPTION")) == 0),
            F.lit("UNK")
        ).otherwise(F.col("RewardData.VNRW_DESCRIPTION")).alias("VBB_RWRD_DESC"),
        F.when(
            F.col("ref_rtrt_typ.RTRT_TYPE").isNull() | (F.length(F.col("ref_rtrt_typ.RTRT_TYPE")) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(trim(F.col("ref_rtrt_typ.RTRT_TYPE")))).alias("VBB_RWRD_TYP_NM"),
        F.when(
            F.col("RewardData.VNRW_NAME").isNull() | (F.length(F.col("RewardData.VNRW_NAME")) == 0),
            F.lit("UNK")
        ).otherwise(F.col("RewardData.VNRW_NAME")).alias("VBB_VNDR_NM")
    )
)

df_BusinessRules_Transform = (
    df_BusinessRules
    .select(
        F.col("RewardData.VNVN_ID").alias("VBB_VNDR_UNIQ_KEY"),
        F.col("RewardData.VNRW_SEQ_NO").alias("VBB_VNDR_RWRD_SEQ_NO"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
    )
)

df_BusinessRules_Snapshot = (
    df_BusinessRules
    .select(
        F.col("RewardData.VNVN_ID").alias("VBB_VNDR_UNIQ_KEY"),
        F.col("RewardData.VNRW_SEQ_NO").alias("VBB_VNDR_RWRD_SEQ_NO"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
    )
)

write_files(
    df_BusinessRules_Snapshot.select("VBB_VNDR_UNIQ_KEY","VBB_VNDR_RWRD_SEQ_NO","SRC_SYS_CD_SK"),
    f"{adls_path}/load/B_VBB_RWRD.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/VbbRwrdPK
# COMMAND ----------

params_VbbRwrdPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}
df_VbbRwrdPK_key = VbbRwrdPK(df_BusinessRules_Transform, df_BusinessRules_AllCol, params_VbbRwrdPK)

df_VbbRwrdPK_key_final = (
    df_VbbRwrdPK_key
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("VBB_RWRD_BEG_DT_SK", F.rpad(F.col("VBB_RWRD_BEG_DT_SK"), 10, " "))
    .withColumn("VBB_RWRD_END_DT_SK", F.rpad(F.col("VBB_RWRD_END_DT_SK"), 10, " "))
)

df_VbbRwrdPK_key_final_select = df_VbbRwrdPK_key_final.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "VBB_RWRD_SK",
    "VBB_VNDR_UNIQ_KEY",
    "VBB_VNDR_RWRD_SEQ_NO",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "VBB_RWRD_BEG_DT_SK",
    "VBB_RWRD_END_DT_SK",
    "SRC_SYS_CRT_DTM",
    "SRC_SYS_UPDT_DTM",
    "VBB_RWRD_DESC",
    "VBB_RWRD_TYP_NM",
    "VBB_VNDR_NM"
)

write_files(
    df_VbbRwrdPK_key_final_select,
    f"{adls_path}/key/IhmfConstituentVbbRwrdExtr.VbbRwrd.dat.{RunId}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)