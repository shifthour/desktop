# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdgeEdgeRiskAsmtCmsMbrEnrPerdHistLoad
# MAGIC DESCRIPTION:  This job loads  Member enrollment Perd  History data to CMS_MBR_ENR_PERD_HIST table 
# MAGIC CALLED BY:  EdgeRiskAsmntEnrSbmsnRAExtrSeq
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                   Date                     Project/Ticket #\(9)      Change Description\(9)\(9)\(9)          Development Project\(9)     Code Reviewer\(9)       Date Reviewed       
# MAGIC -------------------------          -------------------          --------------------------            ----------------------------------------------------------------------              ---------------------------------          ------------------------          -------------------------  
# MAGIC Pooja Sunkara           2015-02-11            5125 Risk Adjustment     Original Programming                                                  EnterpriseNewDevl             Kalyan Neelam           2015-03-04
# MAGIC 
# MAGIC Raja Gummadi            2015-07-30            5125                             Added RISK_ADJ_YR                                                 EnterpriseDev2                   Bhoomi Dasari             07/30/2015
# MAGIC                                                                                                        and SUB_INDV_BE_KEY columns
# MAGIC Harsha Ravuri\(9)   2018-12-27\(9)5873 Risk Adjustment      Added MBR_RELSHP_CD column\(9)to file\(9)          EnterpriseDev2           Kalyan Neelam             2019-01-30
# MAGIC Harsha Ravuri\(9)2023-10-03\(9)US#597589\(9)\(9)Added below 7 new fields to source /Target\(9)EnterpriseDev2              Jeyaprasanna             2023-10-08
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)zipCode, federalAPTC, statePremiumSubsidy, stateCSR, ICHRA_QSEHRA, 
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)QSEHRA_Spousal, QSEHRA_Medical

# MAGIC File created in extract job is used to load CMS_MBR_ENR_PERD_HIST table
# MAGIC Job Name: EdgeEdgeRiskAsmtCmsMbrEnrPerdHistLoad
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDGERiskAsmntOwner = get_widget_value('EDGERiskAsmntOwner','')
edgeriskasmnt_secret_name = get_widget_value('edgeriskasmnt_secret_name','')
RunID = get_widget_value('RunID','')

schema_Seq_CMS_MBR_ENR_PERD_HIST_Extr = StructType([
    StructField("ISSUER_ID", StringType(), True),
    StructField("FILE_ID", StringType(), True),
    StructField("MBR_RCRD_ID", IntegerType(), True),
    StructField("MBR_ENR_PERD_RCRD_ID", IntegerType(), True),
    StructField("SUB_IN", StringType(), True),
    StructField("SUB_MBR_UNIQ_KEY", IntegerType(), True),
    StructField("QHP_ID", StringType(), True),
    StructField("MBR_ENR_EFF_DT_SK", TimestampType(), True),
    StructField("MBR_ENR_TERM_DT_SK", TimestampType(), True),
    StructField("ENR_MNTN_TYP_CD", StringType(), True),
    StructField("PRM_AMT", DecimalType(38,10), True),
    StructField("RATE_AREA_ID", StringType(), True),
    StructField("MBR_ENR_PERD_PRCS_STTUS_CD", StringType(), True),
    StructField("GNRTN_DTM", TimestampType(), True),
    StructField("LAST_UPDT_DTM", TimestampType(), True),
    StructField("LAST_UPDT_USER_ID", StringType(), True),
    StructField("SUB_INDV_BE_KEY", DecimalType(38,10), True),
    StructField("RISK_ADJ_YR", StringType(), True),
    StructField("MBR_RELSHP_CD", StringType(), True),
    StructField("SUB_ZIP_CD_5", StringType(), True),
    StructField("FED_APTC_CD", StringType(), True),
    StructField("ST_PRM_SBSDY_CD", StringType(), True),
    StructField("ST_CSR_CD", StringType(), True),
    StructField("HRA_TYP_CD", StringType(), True),
    StructField("QLFD_SM_EMPLR_HRA_SPOUSAL_CD", StringType(), True),
    StructField("QLFD_SM_EMPLR_HRA_MED_CD", StringType(), True)
])

df_Seq_CMS_MBR_ENR_PERD_HIST_Extr = spark.read.csv(
    path=f"{adls_path}/load/CMS_MBR_ENR_PERD_HIST.{RunID}.dat",
    schema=schema_Seq_CMS_MBR_ENR_PERD_HIST_Extr,
    sep=",",
    quote='"',
    header=False,
    nullValue=None
)

df_cp_buffer = df_Seq_CMS_MBR_ENR_PERD_HIST_Extr

df_Odbc_CMS_MBR_ENR_PERD_HIST_Out = df_cp_buffer.select(
    "ISSUER_ID",
    "FILE_ID",
    "MBR_RCRD_ID",
    "MBR_ENR_PERD_RCRD_ID",
    "SUB_IN",
    "SUB_MBR_UNIQ_KEY",
    "QHP_ID",
    "MBR_ENR_EFF_DT_SK",
    "MBR_ENR_TERM_DT_SK",
    "ENR_MNTN_TYP_CD",
    "PRM_AMT",
    "RATE_AREA_ID",
    "MBR_ENR_PERD_PRCS_STTUS_CD",
    "GNRTN_DTM",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID",
    "SUB_INDV_BE_KEY",
    "RISK_ADJ_YR",
    "MBR_RELSHP_CD",
    "SUB_ZIP_CD_5",
    "FED_APTC_CD",
    "ST_PRM_SBSDY_CD",
    "ST_CSR_CD",
    "HRA_TYP_CD",
    "QLFD_SM_EMPLR_HRA_SPOUSAL_CD",
    "QLFD_SM_EMPLR_HRA_MED_CD"
)

jdbc_url, jdbc_props = get_db_config(edgeriskasmnt_secret_name)

drop_sql = "DROP TABLE IF EXISTS STAGING.EdgeRiskAsmntCmsMbrEnrPerdHistLoad_Odbc_CMS_MBR_ENR_PERD_HIST_Out_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_Odbc_CMS_MBR_ENR_PERD_HIST_Out.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.EdgeRiskAsmntCmsMbrEnrPerdHistLoad_Odbc_CMS_MBR_ENR_PERD_HIST_Out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {EDGERiskAsmntOwner}.CMS_MBR_ENR_PERD_HIST AS target
USING STAGING.EdgeRiskAsmntCmsMbrEnrPerdHistLoad_Odbc_CMS_MBR_ENR_PERD_HIST_Out_temp AS source
ON
    target.ISSUER_ID = source.ISSUER_ID
    AND target.FILE_ID = source.FILE_ID
    AND target.MBR_RCRD_ID = source.MBR_RCRD_ID
    AND target.MBR_ENR_PERD_RCRD_ID = source.MBR_ENR_PERD_RCRD_ID
WHEN MATCHED THEN
    UPDATE SET
        target.SUB_IN = source.SUB_IN,
        target.SUB_MBR_UNIQ_KEY = source.SUB_MBR_UNIQ_KEY,
        target.QHP_ID = source.QHP_ID,
        target.MBR_ENR_EFF_DT_SK = source.MBR_ENR_EFF_DT_SK,
        target.MBR_ENR_TERM_DT_SK = source.MBR_ENR_TERM_DT_SK,
        target.ENR_MNTN_TYP_CD = source.ENR_MNTN_TYP_CD,
        target.PRM_AMT = source.PRM_AMT,
        target.RATE_AREA_ID = source.RATE_AREA_ID,
        target.MBR_ENR_PERD_PRCS_STTUS_CD = source.MBR_ENR_PERD_PRCS_STTUS_CD,
        target.GNRTN_DTM = source.GNRTN_DTM,
        target.LAST_UPDT_DTM = source.LAST_UPDT_DTM,
        target.LAST_UPDT_USER_ID = source.LAST_UPDT_USER_ID,
        target.SUB_INDV_BE_KEY = source.SUB_INDV_BE_KEY,
        target.RISK_ADJ_YR = source.RISK_ADJ_YR,
        target.MBR_RELSHP_CD = source.MBR_RELSHP_CD,
        target.SUB_ZIP_CD_5 = source.SUB_ZIP_CD_5,
        target.FED_APTC_CD = source.FED_APTC_CD,
        target.ST_PRM_SBSDY_CD = source.ST_PRM_SBSDY_CD,
        target.ST_CSR_CD = source.ST_CSR_CD,
        target.HRA_TYP_CD = source.HRA_TYP_CD,
        target.QLFD_SM_EMPLR_HRA_SPOUSAL_CD = source.QLFD_SM_EMPLR_HRA_SPOUSAL_CD,
        target.QLFD_SM_EMPLR_HRA_MED_CD = source.QLFD_SM_EMPLR_HRA_MED_CD
WHEN NOT MATCHED THEN
    INSERT
    (
        ISSUER_ID,
        FILE_ID,
        MBR_RCRD_ID,
        MBR_ENR_PERD_RCRD_ID,
        SUB_IN,
        SUB_MBR_UNIQ_KEY,
        QHP_ID,
        MBR_ENR_EFF_DT_SK,
        MBR_ENR_TERM_DT_SK,
        ENR_MNTN_TYP_CD,
        PRM_AMT,
        RATE_AREA_ID,
        MBR_ENR_PERD_PRCS_STTUS_CD,
        GNRTN_DTM,
        LAST_UPDT_DTM,
        LAST_UPDT_USER_ID,
        SUB_INDV_BE_KEY,
        RISK_ADJ_YR,
        MBR_RELSHP_CD,
        SUB_ZIP_CD_5,
        FED_APTC_CD,
        ST_PRM_SBSDY_CD,
        ST_CSR_CD,
        HRA_TYP_CD,
        QLFD_SM_EMPLR_HRA_SPOUSAL_CD,
        QLFD_SM_EMPLR_HRA_MED_CD
    )
    VALUES
    (
        source.ISSUER_ID,
        source.FILE_ID,
        source.MBR_RCRD_ID,
        source.MBR_ENR_PERD_RCRD_ID,
        source.SUB_IN,
        source.SUB_MBR_UNIQ_KEY,
        source.QHP_ID,
        source.MBR_ENR_EFF_DT_SK,
        source.MBR_ENR_TERM_DT_SK,
        source.ENR_MNTN_TYP_CD,
        source.PRM_AMT,
        source.RATE_AREA_ID,
        source.MBR_ENR_PERD_PRCS_STTUS_CD,
        source.GNRTN_DTM,
        source.LAST_UPDT_DTM,
        source.LAST_UPDT_USER_ID,
        source.SUB_INDV_BE_KEY,
        source.RISK_ADJ_YR,
        source.MBR_RELSHP_CD,
        source.SUB_ZIP_CD_5,
        source.FED_APTC_CD,
        source.ST_PRM_SBSDY_CD,
        source.ST_CSR_CD,
        source.HRA_TYP_CD,
        source.QLFD_SM_EMPLR_HRA_SPOUSAL_CD,
        source.QLFD_SM_EMPLR_HRA_MED_CD
    );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_reject = StructType([
    StructField("ISSUER_ID", StringType(), True),
    StructField("FILE_ID", StringType(), True),
    StructField("MBR_RCRD_ID", IntegerType(), True),
    StructField("MBR_ENR_PERD_RCRD_ID", IntegerType(), True),
    StructField("SUB_IN", StringType(), True),
    StructField("SUB_MBR_UNIQ_KEY", IntegerType(), True),
    StructField("QHP_ID", StringType(), True),
    StructField("MBR_ENR_EFF_DT_SK", TimestampType(), True),
    StructField("MBR_ENR_TERM_DT_SK", TimestampType(), True),
    StructField("ENR_MNTN_TYP_CD", StringType(), True),
    StructField("PRM_AMT", DecimalType(38,10), True),
    StructField("RATE_AREA_ID", StringType(), True),
    StructField("MBR_ENR_PERD_PRCS_STTUS_CD", StringType(), True),
    StructField("GNRTN_DTM", TimestampType(), True),
    StructField("LAST_UPDT_DTM", TimestampType(), True),
    StructField("LAST_UPDT_USER_ID", StringType(), True),
    StructField("SUB_INDV_BE_KEY", DecimalType(38,10), True),
    StructField("RISK_ADJ_YR", StringType(), True),
    StructField("MBR_RELSHP_CD", StringType(), True),
    StructField("SUB_ZIP_CD_5", StringType(), True),
    StructField("FED_APTC_CD", StringType(), True),
    StructField("ST_PRM_SBSDY_CD", StringType(), True),
    StructField("ST_CSR_CD", StringType(), True),
    StructField("HRA_TYP_CD", StringType(), True),
    StructField("QLFD_SM_EMPLR_HRA_SPOUSAL_CD", StringType(), True),
    StructField("QLFD_SM_EMPLR_HRA_MED_CD", StringType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_CMS_MBR_ENR_PERD_HIST_rej = spark.createDataFrame([], schema_reject)

df_seq_CMS_MBR_ENR_PERD_HIST_rej = (
    df_seq_CMS_MBR_ENR_PERD_HIST_rej
    .withColumn("ISSUER_ID", F.rpad(F.col("ISSUER_ID"), 5, " "))
    .withColumn("FILE_ID", F.rpad(F.col("FILE_ID"), 12, " "))
    .withColumn("SUB_IN", F.rpad(F.col("SUB_IN"), 1, " "))
    .withColumn("RISK_ADJ_YR", F.rpad(F.col("RISK_ADJ_YR"), 4, " "))
    .withColumn("SUB_ZIP_CD_5", F.rpad(F.col("SUB_ZIP_CD_5"), 5, " "))
)

df_seq_CMS_MBR_ENR_PERD_HIST_rej = df_seq_CMS_MBR_ENR_PERD_HIST_rej.select(
    "ISSUER_ID",
    "FILE_ID",
    "MBR_RCRD_ID",
    "MBR_ENR_PERD_RCRD_ID",
    "SUB_IN",
    "SUB_MBR_UNIQ_KEY",
    "QHP_ID",
    "MBR_ENR_EFF_DT_SK",
    "MBR_ENR_TERM_DT_SK",
    "ENR_MNTN_TYP_CD",
    "PRM_AMT",
    "RATE_AREA_ID",
    "MBR_ENR_PERD_PRCS_STTUS_CD",
    "GNRTN_DTM",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID",
    "SUB_INDV_BE_KEY",
    "RISK_ADJ_YR",
    "MBR_RELSHP_CD",
    "SUB_ZIP_CD_5",
    "FED_APTC_CD",
    "ST_PRM_SBSDY_CD",
    "ST_CSR_CD",
    "HRA_TYP_CD",
    "QLFD_SM_EMPLR_HRA_SPOUSAL_CD",
    "QLFD_SM_EMPLR_HRA_MED_CD",
    "ERRORCODE",
    "ERRORTEXT"
)

write_files(
    df_seq_CMS_MBR_ENR_PERD_HIST_rej,
    f"{adls_path}/load/CMS_MBR_ENR_PERD_HIST.{RunID}_Rej.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)