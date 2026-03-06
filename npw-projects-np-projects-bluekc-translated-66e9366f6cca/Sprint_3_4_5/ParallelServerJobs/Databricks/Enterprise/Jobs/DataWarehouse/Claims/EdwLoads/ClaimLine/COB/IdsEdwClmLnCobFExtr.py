# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwClmLnCobFactExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids and edw  -  prepares files to compare and update edw claim alternate payee dimension table
# MAGIC   Loads the data from EDW and IDS into two hash files for subsequent comparison.
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS - CLM_LN_COB
# MAGIC                 IDS - W_EDW_ETL_DRVR
# MAGIC                 EDW - CLM_LN_COB_F
# MAGIC                 EDW - W_EDW_ETL_DRVR
# MAGIC 
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes         - CDMA code table - resolve code lookups
# MAGIC                 hf_CLM_LN_COB_ids - hash file from ids
# MAGIC                 hf_CLM_LN_COB_F_edw - hash file from edw
# MAGIC                
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source and dump to hf. Do nothing but trim the fields.
# MAGIC               
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Each of the stream output hash files
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Steph Goddard  02/01/2004-   Originally Programmed
# MAGIC               Brent leland     03/24/2006    Changed parameters to evironment variables.
# MAGIC                                                              Took out trim() on SK values.
# MAGIC             BJ Luce            06/12/2006     change name of CLM_COB_LN_SK to CLM_LN_COB_SK

# MAGIC Extracts all data from IDS reference table CD_MPPNG,
# MAGIC DIAG_CD
# MAGIC 
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC CLM_LN_COB_CAR_PRORTN_CD_SK,
# MAGIC CLM_LN_COB_LIAB_TYP_CD_SK,
# MAGIC SRC_SYS_CD_SK,
# MAGIC CLM_LN_COB_TYP_CD_SK
# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK ,LAST_UPDT_RUN_CYC_EXCTN_DT_SK,NA and UNK rows
# MAGIC Job name:
# MAGIC IdsEdwClmLnCobFExtr
# MAGIC EDW Claim Line COB Facet exract from IDS
# MAGIC Write CLM_LN_COB_F Data into a Sequential file for Load Ready Job.
# MAGIC Read data from source tables CLM_LN_COB, 
# MAGIC W_EDW_ETL_DRVR
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Stage: db2_CLM_LN_COB_in
extract_query_db2_CLM_LN_COB_in = f"""SELECT
CLM_COB_LN_SK,
COB.SRC_SYS_CD_SK,
COB.CLM_ID,
CLM_LN_SEQ_NO,
CLM_LN_COB_TYP_CD_SK,
CRT_RUN_CYC_EXCTN_SK,
COB.LAST_UPDT_RUN_CYC_EXCTN_SK,
CLM_LN_SK,
CLM_LN_COB_CAR_PRORTN_CD_SK,
CLM_LN_COB_LIAB_TYP_CD_SK,
COB_CAR_ADJ_AMT,
ALW_AMT,
APLD_AMT,
COPAY_AMT,
DEDCT_AMT,
DSALW_AMT,
MED_COINS_AMT,
MNTL_HLTH_COINS_AMT,
OOP_AMT,
PD_AMT,
SANC_AMT,
SAV_AMT,
SUBTR_AMT,
COB_CAR_RSN_CD_TX,
COB_CAR_RSN_TX
FROM
{IDSOwner}.CLM_LN_COB COB,
{IDSOwner}.W_EDW_ETL_DRVR DRVR
WHERE
COB.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
AND COB.CLM_ID = DRVR.CLM_ID
"""
df_db2_CLM_LN_COB_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_LN_COB_in)
    .load()
)

# Stage: db2_CD_MPPNG2_in
extract_query_db2_CD_MPPNG2_in = f"""SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') AS TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM
FROM
{IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG2_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG2_in)
    .load()
)

# Stage: Copy_CdMppng (producing multiple outputs from the same input)
df_ref_ClmCobType = df_db2_CD_MPPNG2_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_CarPrortnCd = df_db2_CD_MPPNG2_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_CobLiabTyp = df_db2_CD_MPPNG2_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_SrcSys = df_db2_CD_MPPNG2_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# Stage: Lkp_CdmaCodes
df_Lkp_CdmaCodes = (
    df_db2_CLM_LN_COB_in.alias("Ink_IdsEdwClmLnCobFExtr_inABC")
    .join(
        df_ref_CarPrortnCd.alias("ref_CarPrortnCd"),
        F.col("Ink_IdsEdwClmLnCobFExtr_inABC.CLM_LN_COB_CAR_PRORTN_CD_SK") == F.col("ref_CarPrortnCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_ref_CobLiabTyp.alias("ref_CobLiabTyp"),
        F.col("Ink_IdsEdwClmLnCobFExtr_inABC.CLM_LN_COB_LIAB_TYP_CD_SK") == F.col("ref_CobLiabTyp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_ref_SrcSys.alias("ref_SrcSys"),
        F.col("Ink_IdsEdwClmLnCobFExtr_inABC.SRC_SYS_CD_SK") == F.col("ref_SrcSys.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_ref_ClmCobType.alias("ref_ClmCobType"),
        F.col("Ink_IdsEdwClmLnCobFExtr_inABC.CLM_LN_COB_TYP_CD_SK") == F.col("ref_ClmCobType.CD_MPPNG_SK"),
        "left"
    )
)

df_Lkp_CdmaCodes_out = df_Lkp_CdmaCodes.select(
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.CLM_COB_LN_SK").alias("CLM_COB_LN_SK"),
    F.col("ref_SrcSys.TRGT_CD").alias("SRC_SYS_CD"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.CLM_ID").alias("CLM_ID"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.CLM_LN_COB_TYP_CD_SK").alias("CLM_LN_COB_TYP_CD_SK"),
    F.col("ref_ClmCobType.TRGT_CD").alias("CLM_LN_COB_TYP_CD"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.CLM_LN_COB_CAR_PRORTN_CD_SK").alias("CLM_LN_COB_CAR_PRORTN_CD_SK"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.CLM_LN_COB_LIAB_TYP_CD_SK").alias("CLM_LN_COB_LIAB_TYP_CD_SK"),
    F.col("ref_CarPrortnCd.TRGT_CD").alias("CLM_LN_COB_CAR_PRORTN_CD"),
    F.col("ref_CobLiabTyp.TRGT_CD").alias("CLM_LN_COB_LIAB_TYP_CD"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.COB_CAR_ADJ_AMT").alias("COB_CAR_ADJ_AMT"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.ALW_AMT").alias("ALW_AMT"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.APLD_AMT").alias("APLD_AMT"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.COPAY_AMT").alias("COPAY_AMT"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.DSALW_AMT").alias("DSALW_AMT"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.MED_COINS_AMT").alias("MED_COINS_AMT"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.MNTL_HLTH_COINS_AMT").alias("MNTL_HLTH_COINS_AMT"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.OOP_AMT").alias("OOP_AMT"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.PD_AMT").alias("PD_AMT"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.SANC_AMT").alias("SANC_AMT"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.SAV_AMT").alias("SAV_AMT"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.SUBTR_AMT").alias("SUBTR_AMT"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
    F.col("Ink_IdsEdwClmLnCobFExtr_inABC.COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX")
)

# Stage: xmf_businessLogic
# Primary Link / Main Flow
df_xmf_businessLogic_main = (
    df_Lkp_CdmaCodes_out
    .filter((F.col("CLM_COB_LN_SK") != 0) & (F.col("CLM_COB_LN_SK") != 1))
    .select(
        F.col("CLM_COB_LN_SK").alias("CLM_LN_COB_SK"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_COB_TYP_CD").alias("CLM_LN_COB_TYP_CD"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("CLM_LN_SK").alias("CLM_LN_SK"),
        F.col("CLM_LN_COB_CAR_PRORTN_CD").alias("CLM_LN_COB_CAR_PRORTN_CD"),
        F.col("CLM_LN_COB_LIAB_TYP_CD").alias("CLM_LN_COB_LIAB_TYP_CD"),
        F.col("COB_CAR_ADJ_AMT").alias("CLM_LN_COB_ADJ_AMT"),
        F.col("ALW_AMT").alias("CLM_LN_COB_ALW_AMT"),
        F.col("APLD_AMT").alias("CLM_LN_COB_APLD_AMT"),
        F.col("COPAY_AMT").alias("CLM_LN_COB_COPAY_AMT"),
        F.col("DEDCT_AMT").alias("CLM_LN_COB_DEDCT_AMT"),
        F.col("DSALW_AMT").alias("CLM_LN_COB_DSALW_AMT"),
        F.col("MED_COINS_AMT").alias("CLM_LN_COB_MED_COINS_AMT"),
        F.col("MNTL_HLTH_COINS_AMT").alias("CLM_LN_COB_MNTL_HLTH_COINS_AMT"),
        F.col("OOP_AMT").alias("CLM_LN_COB_OOP_AMT"),
        F.col("PD_AMT").alias("CLM_LN_COB_PD_AMT"),
        F.col("SANC_AMT").alias("CLM_LN_COB_SANC_AMT"),
        F.col("SAV_AMT").alias("CLM_LN_COB_SAV_AMT"),
        F.col("SUBTR_AMT").alias("CLM_LN_COB_SUBTR_AMT"),
        F.col("COB_CAR_RSN_CD_TX").alias("CLM_LN_COB_CAR_RSN_CD_TX"),
        F.col("COB_CAR_RSN_TX").alias("CLM_LN_COB_CAR_RSN_TX"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_LN_COB_TYP_CD_SK").alias("CLM_LN_COB_TYP_CD_SK"),
        F.col("CLM_LN_COB_CAR_PRORTN_CD_SK").alias("CLM_LN_COB_CAR_PRORTN_CD_SK"),
        F.col("CLM_LN_COB_LIAB_TYP_CD_SK").alias("CLM_LN_COB_LIAB_TYP_CD_SK")
    )
)

# UNKRow
schema_unk_na = StructType([
    StructField("CLM_LN_COB_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_SEQ_NO", IntegerType(), True),
    StructField("CLM_LN_COB_TYP_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CLM_LN_SK", IntegerType(), True),
    StructField("CLM_LN_COB_CAR_PRORTN_CD", StringType(), True),
    StructField("CLM_LN_COB_LIAB_TYP_CD", StringType(), True),
    StructField("CLM_LN_COB_ADJ_AMT", DoubleType(), True),
    StructField("CLM_LN_COB_ALW_AMT", DoubleType(), True),
    StructField("CLM_LN_COB_APLD_AMT", DoubleType(), True),
    StructField("CLM_LN_COB_COPAY_AMT", DoubleType(), True),
    StructField("CLM_LN_COB_DEDCT_AMT", DoubleType(), True),
    StructField("CLM_LN_COB_DSALW_AMT", DoubleType(), True),
    StructField("CLM_LN_COB_MED_COINS_AMT", DoubleType(), True),
    StructField("CLM_LN_COB_MNTL_HLTH_COINS_AMT", DoubleType(), True),
    StructField("CLM_LN_COB_OOP_AMT", DoubleType(), True),
    StructField("CLM_LN_COB_PD_AMT", DoubleType(), True),
    StructField("CLM_LN_COB_SANC_AMT", DoubleType(), True),
    StructField("CLM_LN_COB_SAV_AMT", DoubleType(), True),
    StructField("CLM_LN_COB_SUBTR_AMT", DoubleType(), True),
    StructField("CLM_LN_COB_CAR_RSN_CD_TX", StringType(), True),
    StructField("CLM_LN_COB_CAR_RSN_TX", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CLM_LN_COB_TYP_CD_SK", IntegerType(), True),
    StructField("CLM_LN_COB_CAR_PRORTN_CD_SK", IntegerType(), True),
    StructField("CLM_LN_COB_LIAB_TYP_CD_SK", IntegerType(), True),
])

df_xmf_businessLogicUNK = spark.createDataFrame([
    (0,'UNK','UNK',0,'UNK','1753-01-01','1753-01-01',0,'UNK','UNK',0,0,0,0,0,0,0,0,0,0,0,0,0,None,None,100,100,0,0,0)
], schema_unk_na)

# NARow
df_xmf_businessLogicNA = spark.createDataFrame([
    (1,'NA','NA',0,'NA','1753-01-01','1753-01-01',1,'NA','NA',0,0,0,0,0,0,0,0,0,0,0,0,0,None,None,100,100,1,1,1)
], schema_unk_na)

# Stage: fnl_dataLinks (Funnel)
df_fnl_dataLinks_out = df_xmf_businessLogic_main.unionByName(df_xmf_businessLogicUNK).unionByName(df_xmf_businessLogicNA)

# Select final columns in the required order
df_fnl_dataLinks_out = df_fnl_dataLinks_out.select(
    "CLM_LN_COB_SK",
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_COB_TYP_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLM_LN_SK",
    "CLM_LN_COB_CAR_PRORTN_CD",
    "CLM_LN_COB_LIAB_TYP_CD",
    "CLM_LN_COB_ADJ_AMT",
    "CLM_LN_COB_ALW_AMT",
    "CLM_LN_COB_APLD_AMT",
    "CLM_LN_COB_COPAY_AMT",
    "CLM_LN_COB_DEDCT_AMT",
    "CLM_LN_COB_DSALW_AMT",
    "CLM_LN_COB_MED_COINS_AMT",
    "CLM_LN_COB_MNTL_HLTH_COINS_AMT",
    "CLM_LN_COB_OOP_AMT",
    "CLM_LN_COB_PD_AMT",
    "CLM_LN_COB_SANC_AMT",
    "CLM_LN_COB_SAV_AMT",
    "CLM_LN_COB_SUBTR_AMT",
    "CLM_LN_COB_CAR_RSN_CD_TX",
    "CLM_LN_COB_CAR_RSN_TX",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_COB_TYP_CD_SK",
    "CLM_LN_COB_CAR_PRORTN_CD_SK",
    "CLM_LN_COB_LIAB_TYP_CD_SK"
)

# Apply rpad to columns with SqlType char(10)
df_fnl_dataLinks_out = df_fnl_dataLinks_out.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

# Stage: seq_CLM_LN_COB_F_csv_load (write to file)
write_files(
    df_fnl_dataLinks_out,
    f"{adls_path}/load/CLM_LN_COB_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)