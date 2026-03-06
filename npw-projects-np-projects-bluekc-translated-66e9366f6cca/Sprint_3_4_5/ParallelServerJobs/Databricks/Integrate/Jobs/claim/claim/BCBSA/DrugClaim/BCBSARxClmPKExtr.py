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
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)   Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)    --------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Karthik Chinalapani        2016-14-29            5212                    Initial Programming                                                     Integrate Dev2                       Kalyan Neelam        2016-05-11

# MAGIC BCBSA Drug Claim Primary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC Output required by container but not used here
# MAGIC Get SK for records with out keys
# MAGIC This container is used in:
# MAGIC ESIClmInvoicePKExtr
# MAGIC ESIClmPKExtr
# MAGIC FctsClmPKExtr
# MAGIC FctsPcaClmPKExtr
# MAGIC MCSourceClmPKExtr
# MAGIC MedicaidClmPKExtr
# MAGIC NascoClmExtr
# MAGIC NascoClmPKExtr
# MAGIC PcsClmPKExtr
# MAGIC WellDyneClmPKExtr
# MAGIC MedtrakClmPKExtr
# MAGIC BCBSSCClmPKExtr
# MAGIC BCBSSCMedClmPKExtr
# MAGIC BCAClmPKExtr
# MAGIC BCBSARxClmPKExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/ClmLoadPK
# COMMAND ----------

SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_BCBSARxClmLand = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
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
    StructField("SUBGRP_SK", IntegerType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("SUB_SK", IntegerType(), False),
    StructField("SUB_ID", StringType(), False),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("MBR_GNDR_CD_SK", IntegerType(), False),
    StructField("MBR_GNDR_CD", StringType(), True),
    StructField("BRTH_DT_SK", StringType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False)
])

df_BCBSARxClmLand = (
    spark.read
    .format("csv")
    .option("delimiter", ",")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_BCBSARxClmLand)
    .load(f"{adls_path}/verified/BCBSADrugClm_Land.dat.{RunID}")
)

df_Trans2 = df_BCBSARxClmLand.select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID")
)

params = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}
df_ClmLoadPK, = ClmLoadPK(df_Trans2, params)

df_hf_clm_pk_lkup = df_ClmLoadPK.select(
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK")
)

write_files(
    df_hf_clm_pk_lkup,
    f"{adls_path}/hf_clm_pk_lkup.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)