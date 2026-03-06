# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwClmExtrnlProvDimExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from IDS clm_extrnl_prov table and EDW clm_extrnl_prov_d table
# MAGIC   Loads the data from EDW and IDS into two hash files for subsequent comparison.
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS - CLM_EXTRNL_PROV
# MAGIC                 IDS - W_EDW_ETL_DRVR
# MAGIC                 EDW - CLM_EXTRNL_PROV_D
# MAGIC                 EDW - W_EDW_ETL_DRVR
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes         - Load the CDMA codes to get cd a nd cd_nm from cd_sk
# MAGIC                 hf_CLM_EXTRNL_PROV_D_ids - hash file from ids to create edw
# MAGIC                 hf_CLM_EXTRNL_PROV_D_edw - hash file from edw to compare
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
# MAGIC               Name               Date                 Description                                                   Project#        Environment                         Code Reviewer                  Date Reviewed
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC               Tom Harrocks 08/02/2004-   Originally Programmed
# MAGIC               Oliver Nielsen  12/06/2004 -   Parameterize Hash File Names
# MAGIC               Hugh Sisson    01/10/2006 -  Replaced single postal code field with 2 new fields - the 5-digit ZIP code and 
# MAGIC                                                               the 4-digit ZIP+4 extension
# MAGIC               Brent leland     03/24/2006    Took out trim() on SK values, changed parameters to environment variables
# MAGIC               Brent Leland    05/11/2006    Removed trim() on codes and IDs
# MAGIC         
# MAGIC               Pooja Sunkara   10/10/2013   Rewrite in Parallel                                              5114          EnterpriseWrhsDevl         Peter Marshall                      12/23/2013

# MAGIC Extracts all data from IDS reference table CD_MPPNG,
# MAGIC 
# MAGIC 
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC SRC_SYS_CD_SK
# MAGIC CLM_EXTRNL_PROV_ST_CD_SK
# MAGIC CLM_EXTRNL_PROV_CTRY_CD_SK
# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK ,LAST_UPDT_RUN_CYC_EXCTN_DT_SK,NA and UNK rows
# MAGIC Job name:
# MAGIC IdsEdwClmExtrnlProvDExtr
# MAGIC EDW Claim External Provider Extact from IDS
# MAGIC Write CLM_EXTRNL_PROV_D Data into a Sequential file for Load Ready Job.
# MAGIC Read data from source tables CLM_EXTRNL_PROV, 
# MAGIC W_EDW_ETL_DRVR
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

# --------------------------------------------------------------------------------
# Stage: db2_CLM_EXTRNL_PROV_in
# --------------------------------------------------------------------------------
jdbc_url_db2_CLM_EXTRNL_PROV_in, jdbc_props_db2_CLM_EXTRNL_PROV_in = get_db_config(ids_secret_name)
extract_query_db2_CLM_EXTRNL_PROV_in = f"""
SELECT 
PROV.CLM_EXTRNL_PROV_SK,
PROV.SRC_SYS_CD_SK,
PROV.CLM_ID,
PROV.CRT_RUN_CYC_EXCTN_SK,
PROV.LAST_UPDT_RUN_CYC_EXCTN_SK,
PROV.CLM_SK,
PROV.PROV_NM,
PROV.ADDR_LN_1,
PROV.ADDR_LN_2,
PROV.ADDR_LN_3,
PROV.CITY_NM,
PROV.CLM_EXTRNL_PROV_ST_CD_SK,
PROV.POSTAL_CD,
PROV.CNTY_NM,
PROV.CLM_EXTRNL_PROV_CTRY_CD_SK,
PROV.PHN_NO,
PROV.PROV_NPI,
PROV.SVC_PROV_ID,
PROV.SVC_PROV_NPI,
PROV.SVC_PROV_NM,
PROV.TAX_ID
FROM {IDSOwner}.CLM_EXTRNL_PROV PROV,
     {IDSOwner}.W_EDW_ETL_DRVR DRVR
WHERE PROV.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK
  AND PROV.CLM_ID = DRVR.CLM_ID
"""
df_db2_CLM_EXTRNL_PROV_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CLM_EXTRNL_PROV_in)
    .options(**jdbc_props_db2_CLM_EXTRNL_PROV_in)
    .option("query", extract_query_db2_CLM_EXTRNL_PROV_in)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_CD_MPPNG2_in
# --------------------------------------------------------------------------------
jdbc_url_db2_CD_MPPNG2_in, jdbc_props_db2_CD_MPPNG2_in = get_db_config(ids_secret_name)
extract_query_db2_CD_MPPNG2_in = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') AS TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG2_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG2_in)
    .options(**jdbc_props_db2_CD_MPPNG2_in)
    .option("query", extract_query_db2_CD_MPPNG2_in)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Copy_CdMppng (PxCopy)
# --------------------------------------------------------------------------------
df_ref_Ctry = df_db2_CD_MPPNG2_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_refSrcSys = df_db2_CD_MPPNG2_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_refSt = df_db2_CD_MPPNG2_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# --------------------------------------------------------------------------------
# Stage: Lkp_CdmaCodes (PxLookup)
# --------------------------------------------------------------------------------
df_Lkp_CdmaCodes = (
    df_db2_CLM_EXTRNL_PROV_in.alias("Ink_IdsEdwClmExtrnlProvDExtr_inABC")
    .join(
        df_refSrcSys.alias("refSrcSys"),
        F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.SRC_SYS_CD_SK") == F.col("refSrcSys.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_refSt.alias("refSt"),
        F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.CLM_EXTRNL_PROV_ST_CD_SK") == F.col("refSt.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_ref_Ctry.alias("ref_Ctry"),
        F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.CLM_EXTRNL_PROV_CTRY_CD_SK") == F.col("ref_Ctry.CD_MPPNG_SK"),
        "left"
    )
)

df_Lkp_CdmaCodes_out = df_Lkp_CdmaCodes.select(
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.CLM_EXTRNL_PROV_SK").alias("CLM_EXTRNL_PROV_SK"),
    F.col("refSrcSys.TRGT_CD").alias("SRC_SYS_CD"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.CLM_ID").alias("CLM_ID"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.PROV_NM").alias("PROV_NM"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.CITY_NM").alias("CITY_NM"),
    F.col("refSt.TRGT_CD").alias("CLM_EXTRNL_PROV_ST_CD"),
    F.col("refSt.TRGT_CD_NM").alias("CLM_EXTRNL_PROV_ST_NM"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.CNTY_NM").alias("CNTY_NM"),
    F.col("ref_Ctry.TRGT_CD").alias("CLM_EXTRNL_PROV_CTRY_CD"),
    F.col("ref_Ctry.TRGT_CD_NM").alias("CLM_EXTRNL_PROV_CTRY_NM"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.PHN_NO").alias("PHN_NO"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.SVC_PROV_ID").alias("SVC_PROV_ID"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.TAX_ID").alias("TAX_ID"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.CLM_EXTRNL_PROV_CTRY_CD_SK").alias("CLM_EXTRNL_PROV_CTRY_CD_SK"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.CLM_EXTRNL_PROV_ST_CD_SK").alias("CLM_EXTRNL_PROV_ST_CD_SK"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.CLM_SK").alias("CLM_SK"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.PROV_NPI").alias("CLM_EXTRNL_PROV_NPI"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.POSTAL_CD").alias("POSTAL_CD"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.SVC_PROV_NPI").alias("CLM_EXTRNL_PROV_SVC_PROV_NPI"),
    F.col("Ink_IdsEdwClmExtrnlProvDExtr_inABC.SVC_PROV_NM").alias("CLM_EXTRNL_PROV_SVC_PROV_NM")
)

# --------------------------------------------------------------------------------
# Stage: xmf_businessLogic (CTransformerStage)
# Stage variables and output links
# --------------------------------------------------------------------------------
df_xmf_businessLogic = (
    df_Lkp_CdmaCodes_out
    .withColumn("svFmtdPostalCd", FORMAT.POSTALCD.EE(F.col("POSTAL_CD")))
    .withColumn("svFirst5", F.expr("substring(svFmtdPostalCd,1,5)"))
    .withColumn("svLast4", F.expr("substring(svFmtdPostalCd,6,4)"))
    .withColumn("svZip5", F.when(F.length(F.col("svFmtdPostalCd")) >= 5, F.col("svFirst5")).otherwise(F.lit("     ")))
    .withColumn("svZip4", F.when(F.length(F.col("svFmtdPostalCd")) >= 9, F.col("svLast4")).otherwise(F.lit("    ")))
)

# -----------------------
# Link: Ink_IdsEdwClmExtrnlProvDExtr_Main
# Constraint: CLM_EXTRNL_PROV_SK <> 0 and CLM_EXTRNL_PROV_SK <> 1
# Columns with expressions
# -----------------------
df_main = df_xmf_businessLogic.filter(
    (F.col("CLM_EXTRNL_PROV_SK") != 0) & (F.col("CLM_EXTRNL_PROV_SK") != 1)
).select(
    F.col("CLM_EXTRNL_PROV_SK").alias("CLM_EXTRNL_PROV_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PROV_NM").alias("CLM_EXTRNL_PROV_NM"),
    F.col("ADDR_LN_1").alias("CLM_EXTRNL_PROV_ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("CLM_EXTRNL_PROV_ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("CLM_EXTRNL_PROV_ADDR_LN_3"),
    F.col("CITY_NM").alias("CLM_EXTRNL_PROV_CITY_NM"),
    F.col("CLM_EXTRNL_PROV_ST_CD").alias("CLM_EXTRNL_PROV_ST_CD"),
    F.col("CLM_EXTRNL_PROV_ST_NM").alias("CLM_EXTRNL_PROV_ST_NM"),
    F.col("CNTY_NM").alias("CLM_EXTRNL_PROV_CNTY_NM"),
    F.col("CLM_EXTRNL_PROV_CTRY_CD").alias("CLM_EXTRNL_PROV_CTRY_CD"),
    F.col("CLM_EXTRNL_PROV_CTRY_NM").alias("CLM_EXTRNL_PROV_CTRY_NM"),
    F.col("PHN_NO").alias("CLM_EXTRNL_PROV_PHN_NO"),
    F.col("SVC_PROV_ID").alias("CLM_EXTRNL_PROV_SVC_PROV_ID"),
    F.col("TAX_ID").alias("CLM_EXTRNL_PROV_TAX_ID"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_EXTRNL_PROV_CTRY_CD_SK").alias("CLM_EXTRNL_PROV_CTRY_CD_SK"),
    F.col("CLM_EXTRNL_PROV_ST_CD_SK").alias("CLM_EXTRNL_PROV_ST_CD_SK"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("svZip5").alias("CLM_EXTRNL_PROV_ZIP_CD_5"),
    F.col("svZip4").alias("CLM_EXTRNL_PROV_ZIP_CD_4"),
    F.when(
        F.col("CLM_EXTRNL_PROV_NPI").isNull() | (F.length(F.col("CLM_EXTRNL_PROV_NPI")) == 0), 
        F.lit(None)
    ).otherwise(F.col("CLM_EXTRNL_PROV_NPI")).alias("CLM_EXTRNL_PROV_NPI"),
    F.when(
        F.col("CLM_EXTRNL_PROV_SVC_PROV_NPI").isNull() | (F.length(F.col("CLM_EXTRNL_PROV_SVC_PROV_NPI")) == 0), 
        F.lit(None)
    ).otherwise(F.col("CLM_EXTRNL_PROV_SVC_PROV_NPI")).alias("CLM_EXTRNL_PROV_SVC_PROV_NPI"),
    F.when(
        F.col("CLM_EXTRNL_PROV_SVC_PROV_NM").isNull() | (F.length(F.col("CLM_EXTRNL_PROV_SVC_PROV_NM")) == 0), 
        F.lit(None)
    ).otherwise(F.col("CLM_EXTRNL_PROV_SVC_PROV_NM")).alias("CLM_EXTRNL_PROV_SVC_PROV_NM")
)

# -----------------------
# Link: UNKRow
# Constraint: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1
# Generates a single UNK record
# -----------------------
temp_single_row_unk = {
    "CLM_EXTRNL_PROV_SK": 0,
    "SRC_SYS_CD": "UNK",
    "CLM_ID": "UNK",
    "CRT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
    "CLM_EXTRNL_PROV_NM": "UNK",
    "CLM_EXTRNL_PROV_ADDR_LN_1": None,
    "CLM_EXTRNL_PROV_ADDR_LN_2": None,
    "CLM_EXTRNL_PROV_ADDR_LN_3": None,
    "CLM_EXTRNL_PROV_CITY_NM": "UNK",
    "CLM_EXTRNL_PROV_ST_CD": "UNK",
    "CLM_EXTRNL_PROV_ST_NM": "UNK",
    "CLM_EXTRNL_PROV_CNTY_NM": "UNK",
    "CLM_EXTRNL_PROV_CTRY_CD": "UNK",
    "CLM_EXTRNL_PROV_CTRY_NM": "UNK",
    "CLM_EXTRNL_PROV_PHN_NO": None,
    "CLM_EXTRNL_PROV_SVC_PROV_ID": "UNK",
    "CLM_EXTRNL_PROV_TAX_ID": "UNK",
    "CRT_RUN_CYC_EXCTN_SK": 100,
    "LAST_UPDT_RUN_CYC_EXCTN_SK": 100,
    "CLM_EXTRNL_PROV_CTRY_CD_SK": 0,
    "CLM_EXTRNL_PROV_ST_CD_SK": 0,
    "CLM_SK": 0,
    "CLM_EXTRNL_PROV_ZIP_CD_5": " ",
    "CLM_EXTRNL_PROV_ZIP_CD_4": " ",
    "CLM_EXTRNL_PROV_NPI": None,
    "CLM_EXTRNL_PROV_SVC_PROV_NPI": None,
    "CLM_EXTRNL_PROV_SVC_PROV_NM": "UNK"
}
df_UNKRow = spark.createDataFrame([temp_single_row_unk])

# -----------------------
# Link: NARow
# Constraint: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1
# Generates a single NA record
# -----------------------
temp_single_row_na = {
    "CLM_EXTRNL_PROV_SK": 1,
    "SRC_SYS_CD": "NA",
    "CLM_ID": "NA",
    "CRT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": "1753-01-01",
    "CLM_EXTRNL_PROV_NM": "NA",
    "CLM_EXTRNL_PROV_ADDR_LN_1": None,
    "CLM_EXTRNL_PROV_ADDR_LN_2": None,
    "CLM_EXTRNL_PROV_ADDR_LN_3": None,
    "CLM_EXTRNL_PROV_CITY_NM": "NA",
    "CLM_EXTRNL_PROV_ST_CD": "NA",
    "CLM_EXTRNL_PROV_ST_NM": "NA",
    "CLM_EXTRNL_PROV_CNTY_NM": "NA",
    "CLM_EXTRNL_PROV_CTRY_CD": "NA",
    "CLM_EXTRNL_PROV_CTRY_NM": "NA",
    "CLM_EXTRNL_PROV_PHN_NO": None,
    "CLM_EXTRNL_PROV_SVC_PROV_ID": "NA",
    "CLM_EXTRNL_PROV_TAX_ID": "NA",
    "CRT_RUN_CYC_EXCTN_SK": 100,
    "LAST_UPDT_RUN_CYC_EXCTN_SK": 100,
    "CLM_EXTRNL_PROV_CTRY_CD_SK": 1,
    "CLM_EXTRNL_PROV_ST_CD_SK": 1,
    "CLM_SK": 1,
    "CLM_EXTRNL_PROV_ZIP_CD_5": " ",
    "CLM_EXTRNL_PROV_ZIP_CD_4": " ",
    "CLM_EXTRNL_PROV_NPI": None,
    "CLM_EXTRNL_PROV_SVC_PROV_NPI": None,
    "CLM_EXTRNL_PROV_SVC_PROV_NM": "NA"
}
df_NARow = spark.createDataFrame([temp_single_row_na])

# --------------------------------------------------------------------------------
# Stage: fnl_dataLinks (PxFunnel)
# Union the three links into one DataFrame
# --------------------------------------------------------------------------------
funnel_cols = [
    "CLM_EXTRNL_PROV_SK",
    "SRC_SYS_CD",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLM_EXTRNL_PROV_NM",
    "CLM_EXTRNL_PROV_ADDR_LN_1",
    "CLM_EXTRNL_PROV_ADDR_LN_2",
    "CLM_EXTRNL_PROV_ADDR_LN_3",
    "CLM_EXTRNL_PROV_CITY_NM",
    "CLM_EXTRNL_PROV_ST_CD",
    "CLM_EXTRNL_PROV_ST_NM",
    "CLM_EXTRNL_PROV_CNTY_NM",
    "CLM_EXTRNL_PROV_CTRY_CD",
    "CLM_EXTRNL_PROV_CTRY_NM",
    "CLM_EXTRNL_PROV_PHN_NO",
    "CLM_EXTRNL_PROV_SVC_PROV_ID",
    "CLM_EXTRNL_PROV_TAX_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_EXTRNL_PROV_CTRY_CD_SK",
    "CLM_EXTRNL_PROV_ST_CD_SK",
    "CLM_SK",
    "CLM_EXTRNL_PROV_ZIP_CD_5",
    "CLM_EXTRNL_PROV_ZIP_CD_4",
    "CLM_EXTRNL_PROV_NPI",
    "CLM_EXTRNL_PROV_SVC_PROV_NPI",
    "CLM_EXTRNL_PROV_SVC_PROV_NM"
]

df_fnl_dataLinks_main = df_main.select(funnel_cols)
df_fnl_dataLinks_unk = df_UNKRow.select(funnel_cols)
df_fnl_dataLinks_na = df_NARow.select(funnel_cols)

df_fnl_dataLinks = df_fnl_dataLinks_main.unionByName(df_fnl_dataLinks_unk).unionByName(df_fnl_dataLinks_na)

# --------------------------------------------------------------------------------
# Stage: seq_CLM_EXTRNL_PROV_D_csv_load (PxSequentialFile)
# Writing the final DataFrame to CLM_EXTRNL_PROV_D.dat
# Apply rpad to char columns in final select, preserve column order
# --------------------------------------------------------------------------------
df_final = df_fnl_dataLinks.select(
    F.col("CLM_EXTRNL_PROV_SK"),
    F.col("SRC_SYS_CD"),
    F.col("CLM_ID"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLM_EXTRNL_PROV_NM"),
    F.col("CLM_EXTRNL_PROV_ADDR_LN_1"),
    F.col("CLM_EXTRNL_PROV_ADDR_LN_2"),
    F.col("CLM_EXTRNL_PROV_ADDR_LN_3"),
    F.col("CLM_EXTRNL_PROV_CITY_NM"),
    F.col("CLM_EXTRNL_PROV_ST_CD"),
    F.col("CLM_EXTRNL_PROV_ST_NM"),
    F.col("CLM_EXTRNL_PROV_CNTY_NM"),
    F.col("CLM_EXTRNL_PROV_CTRY_CD"),
    F.col("CLM_EXTRNL_PROV_CTRY_NM"),
    F.rpad(F.col("CLM_EXTRNL_PROV_PHN_NO").cast("string"), 20, " ").alias("CLM_EXTRNL_PROV_PHN_NO"),
    F.rpad(F.col("CLM_EXTRNL_PROV_SVC_PROV_ID").cast("string"), 13, " ").alias("CLM_EXTRNL_PROV_SVC_PROV_ID"),
    F.rpad(F.col("CLM_EXTRNL_PROV_TAX_ID").cast("string"), 9, " ").alias("CLM_EXTRNL_PROV_TAX_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_EXTRNL_PROV_CTRY_CD_SK"),
    F.col("CLM_EXTRNL_PROV_ST_CD_SK"),
    F.col("CLM_SK"),
    F.rpad(F.col("CLM_EXTRNL_PROV_ZIP_CD_5").cast("string"), 5, " ").alias("CLM_EXTRNL_PROV_ZIP_CD_5"),
    F.rpad(F.col("CLM_EXTRNL_PROV_ZIP_CD_4").cast("string"), 4, " ").alias("CLM_EXTRNL_PROV_ZIP_CD_4"),
    F.col("CLM_EXTRNL_PROV_NPI"),
    F.col("CLM_EXTRNL_PROV_SVC_PROV_NPI"),
    F.col("CLM_EXTRNL_PROV_SVC_PROV_NM")
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_EXTRNL_PROV_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)