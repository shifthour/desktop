# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  BCBSADrugLandSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  Adjusted to/from claims don't need to be in hash file. 
# MAGIC          *  The primary key hash file hf_clm is the output of this job and is used by the following tables for keying
# MAGIC              CLM
# MAGIC              DRUG_CLM
# MAGIC              CLM_REMIT_HIST
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                              Date               Project/Altiris#                Description                                                                   Project                         Code Reviewer          Date Reviewed
# MAGIC ----------------------------               ------------------       -----------------------               -----------------------------------------------------------------------         ---------------------------------         -------------------------------   ----------------------------
# MAGIC 
# MAGIC 
# MAGIC Karthik Chinalapani              2016-04-29       5212                           Initial programming                                                   IntegrateDev 2                     Kalyan Neelam           2016-05-11
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Karthik Chinalapani              2016-09-20       5212         Added logic to remove leading zeroes for CLM_LN_NO            IntegrateDev 1                    Kalyan Neelam            2016-10-11
# MAGIC                                                                                       in Xfm stage.

# MAGIC Get SK for primary key on input record
# MAGIC BCBSA Rx Claim Primary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code claim ID and seq_no
# MAGIC Output required by container but not used here
# MAGIC This container is used in:
# MAGIC ESIClmLnPKExtr
# MAGIC FctsClmLnPKExtr
# MAGIC FctsClmLnRemitPKExtr
# MAGIC MCSourceClmLnPKExtr
# MAGIC MedicaidClmLnPKExtr
# MAGIC NascoClmLnPKExtr
# MAGIC PcsClmLnPKExtr
# MAGIC WellDyneClmLnPKExtr
# MAGIC MedtrakClmLnPKExtr
# MAGIC BCBSSCClmLnPKExtr
# MAGIC BCBSSCMedClmLnPKExtr
# MAGIC BCAClmLnPKExtr
# MAGIC BCBSARxClmLnPKExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, when, regexp_replace, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','MEDTRAK')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# MAGIC %run ../../../../../../../shared_containers/PrimaryKey/ClmLnLoadPK
# COMMAND ----------

schema_BCBSARxClmLand = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("NDW_HOME_PLN_ID", StringType(), True),
    StructField("CLM_LN_NO", StringType(), True),
    StructField("TRACEABILITY_FLD", StringType(), True),
    StructField("ADJ_SEQ_NO", StringType(), True),
    StructField("HOST_PLN_ID", StringType(), True),
    StructField("HOME_PLAN_PROD_ID_CD", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("MBR_ZIP_CD_ON_CLM", StringType(), True),
    StructField("MBR_CTRY_ON_CLM", StringType(), True),
    StructField("NPI_REND_PROV_ID", StringType(), True),
    StructField("PRSCRB_PROV_ID", StringType(), True),
    StructField("NPI_PRSCRB_PROV_ID", StringType(), True),
    StructField("BNF_PAYMT_STTUS_CD", StringType(), True),
    StructField("PDX_CARVE_OUT_SUBMSN_IN", StringType(), True),
    StructField("CAT_OF_SVC", StringType(), True),
    StructField("CLM_PAYMT_STTUS", StringType(), True),
    StructField("DAW_CD", StringType(), True),
    StructField("DAYS_SUPL", StringType(), True),
    StructField("FRMLRY_IN", StringType(), True),
    StructField("PLN_SPEC_DRUG_IN", StringType(), True),
    StructField("PROD_SVC_ID", StringType(), True),
    StructField("QTY_DISPNS", StringType(), True),
    StructField("CLM_PD_DT", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("AVG_WHLSL_PRICE_SUBMT_AMT", StringType(), True),
    StructField("CONSIS_MBR_ID", StringType(), True),
    StructField("MMI_ID", StringType(), True),
    StructField("SUBGRP_SK", IntegerType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("SUB_SK", IntegerType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("MBR_GNDR_CD_SK", IntegerType(), True),
    StructField("MBR_GNDR_CD", StringType(), True),
    StructField("BRTH_DT_SK", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("GRP_ID", StringType(), True)
])

df_BCBSARxClmLand = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_BCBSARxClmLand)
    .load(f"{adls_path}/verified/BCBSADrugClm_Land.dat.{RunID}")
)

df_Trans1 = df_BCBSARxClmLand.select(
    lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    when(trim(col("CLM_LN_NO")) == "000", lit("1"))
    .otherwise(regexp_replace(trim(col("CLM_LN_NO")), r"^0+", ""))
    .alias("CLM_LN_SEQ_NO")
)

params_ClmLnLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}

df_ClmLnLoadPK = ClmLnLoadPK(df_Trans1, params_ClmLnLoadPK)

df_final = df_ClmLnLoadPK.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    rpad(col("CLM_LN_SEQ_NO"), <...>, " ").alias("CLM_LN_SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("CLM_LN_SK")
)

write_files(
    df_final,
    "hf_clm_ln_pk_lkup.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)