# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2020 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY :  GxOptumAccumReconSeq
# MAGIC 
# MAGIC DESCRIPTION:      OptumRX Daily Accum Recon File Processing
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                         Date                 Project/Altiris #                           Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                      --------------------     ------------------------                            -----------------------------------------------------------------------                                                   --------------------------------       -------------------------------   ----------------------------      
# MAGIC Rekha Radhakrishna      10/01/2020     OPTUMRX Accum Recon            Initial Programming                                                                                          IntegrateDev2                    Jaideep Mankala       11/19/2020           
# MAGIC Rekha Radhakrishna      12/16/2020     OPTUMRX Accum Recon            Changed MBR_ID derivation for MedD                                                           IntegrateDev2               Manasa Andru            2020-12-28
# MAGIC Rekha Radhakrishna      01/06/2020     OPTUMRX Accum Recon            Added Member Birth Date defaulting                                                               IntegrateDev2             Jaideep Mankala        01/12/2021    
# MAGIC Sudeep Reddy                03/09/2022     US-482980                                   Added additional validation on date column to makesure                                IntegrateDev2               Jeyaprasanna             2022-04-05
# MAGIC                                                                                                                      date column shouldn't be zero(0) and Null and push exception 
# MAGIC                                                                                                                      records into exception file.
# MAGIC Ashok kumar B                  2024/09/03     US628482                           Modified the logic to exlude group 20624000 and 34189000                                 IntegrateDev2                 Jeyaprasanna            2024-09-05

# MAGIC OptumRX Daily Accum Recon File Processing.
# MAGIC 
# MAGIC Takes OptumRX Daily  Recon File and loads the Accum Recon DB Table OPTUMRX_ACCUM_RECON_STG
# MAGIC Added validation on date column. It shouldn't be Zero(0) and Null.
# MAGIC Exception records related to invalid value in date column
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, FloatType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------

GxCAccumReconOwner = get_widget_value('GxCAccumReconOwner','')
gxcaccumrecon_secret_name = get_widget_value('gxcaccumrecon_secret_name','')
InFile = get_widget_value('InFile','')
col_details_seq_OptumRXReconDailyFile = {
    "CAR": {"len": 9, "type": StringType()},
    "ACCT": {"len": 15, "type": StringType()},
    "GRP": {"len": 15, "type": StringType()},
    "MBR": {"len": 18, "type": StringType()},
    "CLNT_BNF_CD": {"len": 10, "type": StringType()},
    "ACCUM_TYP": {"len": 1, "type": StringType()},
    "PERD_FROM_DT": {"len": 8, "type": StringType()},
    "PERD_THRU_DT": {"len": 8, "type": StringType()},
    "ACCUM_LVL": {"len": 1, "type": StringType()},
    "ACCUM_CD": {"len": 10, "type": StringType()},
    "ACCUM_LIST": {"len": 10, "type": StringType()},
    "ACCUM_BSS_CD": {"len": 1, "type": StringType()},
    "INDV_ACCUM_BAL": {"len": 10, "type": StringType()},
    "INDV_CLM_TOT": {"len": 10, "type": StringType()},
    "PDX_SEND_INDV_ADJ_TOT": {"len": 10, "type": StringType()},
    "MED_SEND_INDV_ADJ_TOT": {"len": 10, "type": StringType()},
    "UNK_INDV_ADJ_TOT": {"len": 10, "type": StringType()},
    "FMLY_ACCUM_BAL": {"len": 10, "type": StringType()},
    "FMLY_CLM_TOT": {"len": 10, "type": StringType()},
    "PDX_SEND_FMLY_ADJ_TOT": {"len": 10, "type": StringType()},
    "MED_SEND_FMLY_ADJ_TOT": {"len": 10, "type": StringType()},
    "UNK_FMLY_ADJ_TOT": {"len": 10, "type": StringType()},
    "MBR_FMLY_TYP": {"len": 1, "type": StringType()},
    "MBR_ELIG_EFF_DT": {"len": 8, "type": StringType()},
    "MBR_ELIG_THRU_DT": {"len": 8, "type": StringType()},
    "MBR_FIRST_NM": {"len": 15, "type": StringType()},
    "MBR_LAST_NM": {"len": 25, "type": StringType()},
    "MBR_BRTH_DT": {"len": 8, "type": StringType()},
    "MBR_RELSHP": {"len": 1, "type": StringType()},
    "ALIAS_ID": {"len": 20, "type": StringType()},
    "GNDR": {"len": 1, "type": StringType()},
    "Dummy": {"len": 57, "type": StringType()}
}
df_seq_OptumRXReconDailyFile = fixed_file_read_write(
    f"{adls_path_raw}/landing/{InFile}",
    col_details_seq_OptumRXReconDailyFile,
    "read"
)
df_xfm = (
    df_seq_OptumRXReconDailyFile
    .withColumn(
        "MbrFromDt",
        F.when(F.col("MBR_ELIG_EFF_DT") == "19000000", F.lit("19001231"))
         .otherwise(F.col("MBR_ELIG_EFF_DT"))
    )
    .withColumn(
        "MbrToDt",
        F.when(F.col("MBR_ELIG_THRU_DT") == "19000000", F.lit("19001231"))
         .otherwise(F.col("MBR_ELIG_THRU_DT"))
    )
    .withColumn("MbrBirthDtDflt", F.lit("19000101"))
    .withColumn(
        "StgDateCheck",
        F.when(
            (F.col("PERD_FROM_DT") == "00000000") | (F.col("PERD_FROM_DT") == "") |
            (F.col("PERD_THRU_DT") == "00000000") | (F.col("PERD_THRU_DT") == "") |
            (F.col("MBR_ELIG_EFF_DT") == "00000000") | (F.col("MBR_ELIG_EFF_DT") == "") |
            (F.col("MBR_ELIG_THRU_DT") == "00000000") | (F.col("MBR_ELIG_THRU_DT") == ""),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)
df_in_Trg = df_xfm.filter(
    (F.col("StgDateCheck") == "N")
    & (
        (F.trim(F.col("GRP")) != "20624000")
        | (F.trim(F.col("GRP")) != "34189000")
    )
)
df_in_Trg = df_in_Trg.select(
    F.rpad(F.trim(F.col("CAR")), 9, " ").alias("CAR_ID"),
    F.col("ACCT").cast("int").alias("ACCT_ID"),
    F.rpad(F.trim(F.col("GRP")), 15, " ").alias("GRP_CD"),
    F.when(
        F.trim(F.col("CAR")) == "BKCMEDD",
        F.concat(F.substring(F.trim(F.col("MBR")), 4, 9999), F.lit("00"))
    ).otherwise(F.trim(F.col("MBR"))).alias("MBR_ID"),
    F.rpad(F.trim(F.col("CLNT_BNF_CD")), 10, " ").alias("CLNT_BNF_CD"),
    F.rpad(F.trim(F.col("ACCUM_TYP")), 1, " ").alias("ACCUM_TYP"),
    F.concat(
        F.substring(F.col("PERD_FROM_DT"), 1, 4), F.lit("-"),
        F.substring(F.col("PERD_FROM_DT"), 5, 2), F.lit("-"),
        F.substring(F.col("PERD_FROM_DT"), 7, 2), F.lit(" 00:00:00")
    ).alias("PERD_FROM_DT"),
    F.concat(
        F.substring(F.col("PERD_THRU_DT"), 1, 4), F.lit("-"),
        F.substring(F.col("PERD_THRU_DT"), 5, 2), F.lit("-"),
        F.substring(F.col("PERD_THRU_DT"), 7, 2), F.lit(" 00:00:00")
    ).alias("PERD_TO_DT"),
    F.rpad(F.trim(F.col("ACCUM_LVL")), 1, " ").alias("ACCUM_LVL"),
    F.rpad(F.trim(F.col("ACCUM_CD")), 10, " ").alias("ACCUM_CD"),
    F.rpad(F.trim(F.col("ACCUM_LIST")), 10, " ").alias("ACCUM_LIST"),
    F.col("ACCUM_BSS_CD").cast("int").alias("ACCUM_BASSIS_CD"),
    F.col("INDV_ACCUM_BAL").cast("int").alias("INDV_ACCUM_BAL_AMT"),
    F.when(
        F.length(F.trim(F.col("INDV_CLM_TOT"))) == 0,
        F.lit(None)
    ).otherwise(
        F.col("INDV_CLM_TOT").cast(FloatType()) / 100
    ).alias("INDV_CLM_TOT_AMT"),
    F.when(
        F.length(F.trim(F.col("PDX_SEND_INDV_ADJ_TOT"))) == 0,
        F.lit(None)
    ).otherwise(
        F.col("PDX_SEND_INDV_ADJ_TOT").cast(FloatType()) / 100
    ).alias("INDV_ADJ_TOT_PDX_SEND_TYP_AMT"),
    F.when(
        F.length(F.trim(F.col("MED_SEND_INDV_ADJ_TOT"))) == 0,
        F.lit(None)
    ).otherwise(
        F.col("MED_SEND_INDV_ADJ_TOT").cast(FloatType()) / 100
    ).alias("INDV_ADJ_TOT_MED_SEND_TYP_AMT"),
    F.when(
        F.length(F.trim(F.col("UNK_INDV_ADJ_TOT"))) == 0,
        F.lit(None)
    ).otherwise(
        F.col("UNK_INDV_ADJ_TOT").cast(FloatType()) / 100
    ).alias("INDV_ADJ_TOT_UNK_TYP_AMT"),
    F.when(
        F.length(F.trim(F.col("FMLY_ACCUM_BAL"))) == 0,
        F.lit(None)
    ).otherwise(
        F.col("FMLY_ACCUM_BAL").cast(FloatType()) / 100
    ).alias("FMLY_ACCUM_BAL_AMT"),
    F.when(
        F.length(F.trim(F.col("FMLY_CLM_TOT"))) == 0,
        F.lit(None)
    ).otherwise(
        F.col("FMLY_CLM_TOT").cast(FloatType()) / 100
    ).alias("FMLY_CLM_TOT_AMT"),
    F.when(
        F.length(F.trim(F.col("PDX_SEND_FMLY_ADJ_TOT"))) == 0,
        F.lit(None)
    ).otherwise(
        F.col("PDX_SEND_FMLY_ADJ_TOT").cast(FloatType()) / 100
    ).alias("FMLY_ADJ_TOT_PDX_SEND_TYP_AMT"),
    F.when(
        F.length(F.trim(F.col("MED_SEND_FMLY_ADJ_TOT"))) == 0,
        F.lit(None)
    ).otherwise(
        F.col("MED_SEND_FMLY_ADJ_TOT").cast(FloatType()) / 100
    ).alias("FMLY_ADJ_TOT_MED_SEND_TYP_AMT"),
    F.when(
        F.length(F.trim(F.col("UNK_FMLY_ADJ_TOT"))) == 0,
        F.lit(None)
    ).otherwise(
        F.col("UNK_FMLY_ADJ_TOT").cast(FloatType()) / 100
    ).alias("FMLY_ADJ_TOT_UNK_TYP_AMT"),
    F.col("MBR_FMLY_TYP").alias("MBR_FMLY_TYP"),
    F.concat(
        F.substring(F.col("MbrFromDt"), 1, 4), F.lit("-"),
        F.substring(F.col("MbrFromDt"), 5, 2), F.lit("-"),
        F.substring(F.col("MbrFromDt"), 7, 2), F.lit(" 00:00:00")
    ).alias("MBR_ELIG_EFF_FROM_DT"),
    F.concat(
        F.substring(F.col("MbrToDt"), 1, 4), F.lit("-"),
        F.substring(F.col("MbrToDt"), 5, 2), F.lit("-"),
        F.substring(F.col("MbrToDt"), 7, 2), F.lit(" 00:00:00")
    ).alias("MBR_ELIG_EFF_TO_DT"),
    F.when(
        F.length(F.trim(F.col("MBR_FIRST_NM"))) == 0,
        F.lit(" ")
    ).otherwise(
        F.col("MBR_FIRST_NM")
    ).alias("MBR_FIRST_NM"),
    F.when(
        F.length(F.trim(F.col("MBR_LAST_NM"))) == 0,
        F.lit(" ")
    ).otherwise(
        F.col("MBR_LAST_NM")
    ).alias("MBR_LAST_NM"),
    F.when(
        F.length(F.trim(F.col("MBR_BRTH_DT"))) == 0,
        F.concat(
            F.substring(F.col("MbrBirthDtDflt"), 1, 4), F.lit("-"),
            F.substring(F.col("MbrBirthDtDflt"), 5, 2), F.lit("-"),
            F.substring(F.col("MbrBirthDtDflt"), 7, 2), F.lit(" 00:00:00")
        )
    ).otherwise(
        F.concat(
            F.substring(F.col("MBR_BRTH_DT"), 1, 4), F.lit("-"),
            F.substring(F.col("MBR_BRTH_DT"), 5, 2), F.lit("-"),
            F.substring(F.col("MBR_BRTH_DT"), 7, 2), F.lit(" 00:00:00")
        )
    ).alias("MBR_BRTH_DT"),
    F.when(
        F.length(F.trim(F.col("MBR_RELSHP"))) == 0,
        F.lit(None)
    ).otherwise(
        F.col("MBR_RELSHP").cast(IntegerType())
    ).alias("MBR_RELSHP_CD"),
    F.when(
        F.length(F.trim(F.col("ALIAS_ID"))) == 0,
        F.lit(None)
    ).otherwise(
        F.col("ALIAS_ID").cast(IntegerType())
    ).alias("ALIAS_ID"),
    F.rpad(F.trim(F.col("GNDR")), 1, " ").alias("GNDR_CD")
)
df_lnk_exception = df_xfm.filter(
    F.col("StgDateCheck") == "Y"
).select(
    F.col("CAR"),
    F.col("ACCT"),
    F.col("GRP"),
    F.col("MBR"),
    F.col("CLNT_BNF_CD"),
    F.col("ACCUM_TYP"),
    F.col("PERD_FROM_DT"),
    F.col("PERD_THRU_DT"),
    F.col("ACCUM_LVL"),
    F.col("ACCUM_CD"),
    F.col("ACCUM_LIST"),
    F.col("ACCUM_BSS_CD"),
    F.col("INDV_ACCUM_BAL"),
    F.col("INDV_CLM_TOT"),
    F.col("PDX_SEND_INDV_ADJ_TOT"),
    F.col("MED_SEND_INDV_ADJ_TOT"),
    F.col("UNK_INDV_ADJ_TOT"),
    F.col("FMLY_ACCUM_BAL"),
    F.col("FMLY_CLM_TOT"),
    F.col("PDX_SEND_FMLY_ADJ_TOT"),
    F.col("MED_SEND_FMLY_ADJ_TOT"),
    F.col("UNK_FMLY_ADJ_TOT"),
    F.col("MBR_FMLY_TYP"),
    F.col("MBR_ELIG_EFF_DT"),
    F.col("MBR_ELIG_THRU_DT"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_BRTH_DT"),
    F.col("MBR_RELSHP"),
    F.col("ALIAS_ID"),
    F.col("GNDR"),
    F.col("Dummy")
)
jdbc_url, jdbc_props = get_db_config(gxcaccumrecon_secret_name)
execute_dml(
    f"DELETE FROM {GxCAccumReconOwner}.OPTUMRX_ACCUM_RECON_STG",
    jdbc_url,
    jdbc_props
)
(
    df_in_Trg.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", f"{GxCAccumReconOwner}.OPTUMRX_ACCUM_RECON_STG")
    .mode("append")
    .save()
)
col_details_seq_OptumRXReconDailyFile_Exception_file = {
    "CAR": {"len": 9, "type": StringType()},
    "ACCT": {"len": 15, "type": StringType()},
    "GRP": {"len": 15, "type": StringType()},
    "MBR": {"len": 18, "type": StringType()},
    "CLNT_BNF_CD": {"len": 10, "type": StringType()},
    "ACCUM_TYP": {"len": 1, "type": StringType()},
    "PERD_FROM_DT": {"len": 8, "type": StringType()},
    "PERD_THRU_DT": {"len": 8, "type": StringType()},
    "ACCUM_LVL": {"len": 1, "type": StringType()},
    "ACCUM_CD": {"len": 10, "type": StringType()},
    "ACCUM_LIST": {"len": 10, "type": StringType()},
    "ACCUM_BSS_CD": {"len": 1, "type": StringType()},
    "INDV_ACCUM_BAL": {"len": 10, "type": StringType()},
    "INDV_CLM_TOT": {"len": 10, "type": StringType()},
    "PDX_SEND_INDV_ADJ_TOT": {"len": 10, "type": StringType()},
    "MED_SEND_INDV_ADJ_TOT": {"len": 10, "type": StringType()},
    "UNK_INDV_ADJ_TOT": {"len": 10, "type": StringType()},
    "FMLY_ACCUM_BAL": {"len": 10, "type": StringType()},
    "FMLY_CLM_TOT": {"len": 10, "type": StringType()},
    "PDX_SEND_FMLY_ADJ_TOT": {"len": 10, "type": StringType()},
    "MED_SEND_FMLY_ADJ_TOT": {"len": 10, "type": StringType()},
    "UNK_FMLY_ADJ_TOT": {"len": 10, "type": StringType()},
    "MBR_FMLY_TYP": {"len": 1, "type": StringType()},
    "MBR_ELIG_EFF_DT": {"len": 8, "type": StringType()},
    "MBR_ELIG_THRU_DT": {"len": 8, "type": StringType()},
    "MBR_FIRST_NM": {"len": 15, "type": StringType()},
    "MBR_LAST_NM": {"len": 25, "type": StringType()},
    "MBR_BRTH_DT": {"len": 8, "type": StringType()},
    "MBR_RELSHP": {"len": 1, "type": StringType()},
    "ALIAS_ID": {"len": 20, "type": StringType()},
    "GNDR": {"len": 1, "type": StringType()},
    "Dummy": {"len": 57, "type": StringType()}
}
fixed_file_read_write(
    df_lnk_exception,
    f"{adls_path}/verified/Exception_{InFile}",
    col_details_seq_OptumRXReconDailyFile_Exception_file,
    "write"
)