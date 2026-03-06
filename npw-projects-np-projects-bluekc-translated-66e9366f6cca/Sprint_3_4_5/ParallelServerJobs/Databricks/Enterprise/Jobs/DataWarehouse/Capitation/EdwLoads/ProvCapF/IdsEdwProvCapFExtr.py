# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwProvCapExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from IDS by BeginCycle for sebsequent loading into the EDW.
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS:  PROV_CAP
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes         - Load the CDMA codes to get cd and cd_nm from cd_sk
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Ralph Tucker  -  08/02/2005  -   Originally Programmed
# MAGIC 
# MAGIC Modifications:                               
# MAGIC                                                                          Project/                                                                                                                                                                                  Code                  Date
# MAGIC Developer                                   Date              Altiris #        Change Description                                                                                                               Project                  Reviewer            Reviewed
# MAGIC -------------------------                        -------------------   -------------      --------------------------------------------------------------------------------------------------------------------------------------------  ---------------------------  -------------------------  -------------------
# MAGIC Ralph Tucker                              2011-04-13    TTR-1058    Bring up to current standards                                                                                            IntegrateCurDevl        SAndrew             2011-04-20
# MAGIC 
# MAGIC Lee Moore                                   2013-07-26     5114           Rewrite in parallel                                                                                                              EnterpriseWrhsDevl   Bhoomi Dasari     9/6/2013    
# MAGIC 
# MAGIC Karthik Chintalapani                     2014-09-09     4917           Added BCBSKC Runcyle to load Historical data into PROV_CAP_F table.                                                       Kalyan Neelam     2014-09-11

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CD_MPPNG_SK
# MAGIC refSrcSys
# MAGIC refPymtCapTypCd
# MAGIC refPymtLobCd
# MAGIC refPymtMethCd
# MAGIC Write PROV_CAP_F  Data into a Sequential file for Load Job IdsEdwCapFundDLoad.
# MAGIC Read all the Data from IDS PROV CAP Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwCapFundDExtr
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write this into a Dataset if the data goes into a PKEY job otherwise write this info into a Sequential file
# MAGIC 
# MAGIC Please use Metadata available in Table definitions to support data Lineage.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
IDSBCBSKCRunCycle = get_widget_value('IDSBCBSKCRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# db2_PROV_CAP_in
query_db2_PROV_CAP_in = f"""
SELECT 
PROV_CAP.PROV_CAP_SK,
PROV_CAP.SRC_SYS_CD_SK,
PROV_CAP.PD_DT_SK,
PROV_CAP.CAP_PROV_ID,
PROV_CAP.PD_PROV_ID,
PROV_CAP.PROV_CAP_PAYMT_LOB_CD_SK,
PROV_CAP.PROV_CAP_PAYMT_METH_CD_SK,
PROV_CAP.PROV_CAP_PAYMT_CAP_TYP_CD_SK,
PROV_CAP.CRT_RUN_CYC_EXCTN_SK,
PROV_CAP.LAST_UPDT_RUN_CYC_EXCTN_SK,
PROV_CAP.CAP_PROV_SK,
PROV_CAP.PD_PROV_SK,
PROV_CAP.PAYMT_SUM_SK,
PROV_CAP.AUTO_ADJ_AMT,
PROV_CAP.CUR_CAP_AMT,
MNL_ADJ_AMT,
PROV_CAP.NET_AMT,
PROV_CAP.AUTO_ADJ_MBR_MO_CT,
PROV_CAP.CUR_MBR_MO_CT,
PROV_CAP.MNL_ADJ_MBR_MO_CT,
RUN_CYC.STRT_DT CREATE_IDS_REC_PROCESS_DT
FROM {IDSOwner}.PROV_CAP PROV_CAP,
     {IDSOwner}.P_RUN_CYC RUN_CYC,
     {IDSOwner}.CD_MPPNG CD
WHERE
    PROV_CAP.CRT_RUN_CYC_EXCTN_SK = RUN_CYC.RUN_CYC_NO
    AND PROV_CAP.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
    AND ((CD.TRGT_CD = 'FACETS' AND PROV_CAP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle})
         OR (CD.TRGT_CD = 'BCBSKC' AND PROV_CAP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSBCBSKCRunCycle}))
    AND RUN_CYC.SUBJ_CD = 'CAPITATION'
    AND RUN_CYC.TRGT_SYS_CD = 'IDS'
"""
df_db2_PROV_CAP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_PROV_CAP_in)
    .load()
)

# db2_CD_MPPNG_in
query_db2_CD_MPPNG_in = f"""
SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_CD_MPPNG_in)
    .load()
)

# db2_Paymt_Sum
query_db2_Paymt_Sum = f"""
SELECT
PAYMT_SUM_SK,
PAYMT_REF_ID
FROM {IDSOwner}.PAYMT_SUM
"""
df_db2_Paymt_Sum = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_Paymt_Sum)
    .load()
)

# db2_Clndr_Dt
query_db2_Clndr_Dt = f"""
SELECT
CLNDR_DT_SK,
YR_MO_SK
FROM {IDSOwner}.CLNDR_DT
"""
df_db2_Clndr_Dt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_Clndr_Dt)
    .load()
)

# db2_refProv
query_db2_refProv = f"""
SELECT
PROV_SK,
PROV_ID,
PROV_NM
FROM {IDSOwner}.PROV
"""
df_db2_refProv = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_refProv)
    .load()
)

# cpy_cd_mppng
df_cpy_cd_mppng = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)
df_lnk_refPymtMethCd = df_cpy_cd_mppng
df_lnk_refPymtLobCd = df_cpy_cd_mppng
df_lnk_refSrcSys = df_cpy_cd_mppng
df_lnk_refPymtCapTypCd = df_cpy_cd_mppng

# cpy_Prov
df_cpy_Prov = df_db2_refProv.select(
    F.col("PROV_SK"),
    F.col("PROV_ID"),
    F.col("PROV_NM")
)
df_lnk_Pd_Prov_Lookup = df_cpy_Prov
df_lnk_Cap_Prov_Lookup = df_cpy_Prov

# lkp_FKeys
df_lkp_FKeys_joined = (
    df_db2_PROV_CAP_in.alias("A")
    .join(df_db2_Clndr_Dt.alias("B"), F.col("A.PD_DT_SK") == F.col("B.CLNDR_DT_SK"), "left")
    .join(df_db2_Paymt_Sum.alias("C"), F.col("A.PAYMT_SUM_SK") == F.col("C.PAYMT_SUM_SK"), "left")
    .join(df_lnk_Pd_Prov_Lookup.alias("D"), F.col("A.PD_PROV_SK") == F.col("D.PROV_SK"), "left")
    .join(df_lnk_Cap_Prov_Lookup.alias("E"), F.col("A.CAP_PROV_SK") == F.col("E.PROV_SK"), "left")
)
df_lkp_FKeys = df_lkp_FKeys_joined.select(
    F.col("A.PROV_CAP_SK").alias("PROV_CAP_SK"),
    F.col("B.YR_MO_SK").alias("PD_YR_MO_SK"),
    F.col("C.PAYMT_REF_ID").alias("PAYMENT_SUM_PAYMT_REF_ID"),
    F.col("D.PROV_NM").alias("PD_PROV_NM"),
    F.col("E.PROV_NM").alias("CAP_PROV_NM"),
    F.col("A.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("A.PD_DT_SK").alias("PD_DT_SK"),
    F.col("A.CAP_PROV_ID").alias("CAP_PROV_ID"),
    F.col("A.PD_PROV_ID").alias("PD_PROV_ID"),
    F.col("A.PROV_CAP_PAYMT_LOB_CD_SK").alias("PROV_CAP_PAYMT_LOB_CD_SK"),
    F.col("A.PROV_CAP_PAYMT_METH_CD_SK").alias("PROV_CAP_PAYMT_METH_CD_SK"),
    F.col("A.PROV_CAP_PAYMT_CAP_TYP_CD_SK").alias("PROV_CAP_PAYMT_CAP_TYP_CD_SK"),
    F.col("A.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("A.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("A.CAP_PROV_SK").alias("CAP_PROV_SK"),
    F.col("A.PD_PROV_SK").alias("PD_PROV_SK"),
    F.col("A.PAYMT_SUM_SK").alias("PAYMT_SUM_SK"),
    F.col("A.AUTO_ADJ_AMT").alias("AUTO_ADJ_AMT"),
    F.col("A.CUR_CAP_AMT").alias("CUR_CAP_AMT"),
    F.col("A.MNL_ADJ_AMT").alias("MNL_ADJ_AMT"),
    F.col("A.NET_AMT").alias("NET_AMT"),
    F.col("A.AUTO_ADJ_MBR_MO_CT").alias("AUTO_ADJ_MBR_MO_CT"),
    F.col("A.CUR_MBR_MO_CT").alias("CUR_MBR_MO_CT"),
    F.col("A.MNL_ADJ_MBR_MO_CT").alias("MNL_ADJ_MBR_MO_CT"),
    F.col("A.CREATE_IDS_REC_PROCESS_DT").alias("CREATE_IDS_REC_PROCESS_DT")
)

# lkp_Codes
df_lkp_Codes_joined = (
    df_lkp_FKeys.alias("A")
    .join(df_lnk_refPymtLobCd.alias("B"), F.col("A.PROV_CAP_PAYMT_LOB_CD_SK") == F.col("B.CD_MPPNG_SK"), "left")
    .join(df_lnk_refSrcSys.alias("C"), F.col("A.SRC_SYS_CD_SK") == F.col("C.CD_MPPNG_SK"), "left")
    .join(df_lnk_refPymtMethCd.alias("D"), F.col("A.PROV_CAP_PAYMT_METH_CD_SK") == F.col("D.CD_MPPNG_SK"), "left")
    .join(df_lnk_refPymtCapTypCd.alias("E"), F.col("A.PROV_CAP_PAYMT_CAP_TYP_CD_SK") == F.col("E.CD_MPPNG_SK"), "left")
)
df_lkp_Codes = df_lkp_Codes_joined.select(
    F.col("A.PROV_CAP_SK").alias("PROV_CAP_SK"),
    F.col("B.TRGT_CD").alias("LOB_CD"),
    F.col("C.TRGT_CD").alias("SRC_SYS_CD"),
    F.col("D.TRGT_CD").alias("METH_CD"),
    F.col("E.TRGT_CD").alias("CAP_PAY_CD"),
    F.col("A.PD_YR_MO_SK").alias("PD_YR_MO_SK"),
    F.col("A.PAYMENT_SUM_PAYMT_REF_ID").alias("PAYMENT_SUM_PAYMT_REF_ID"),
    F.col("A.PD_PROV_NM").alias("PD_PROV_NM"),
    F.col("A.CAP_PROV_NM").alias("CAP_PROV_NM"),
    F.col("A.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("A.PD_DT_SK").alias("PD_DT_SK"),
    F.col("A.CAP_PROV_ID").alias("CAP_PROV_ID"),
    F.col("A.PD_PROV_ID").alias("PD_PROV_ID"),
    F.col("A.PROV_CAP_PAYMT_LOB_CD_SK").alias("PROV_CAP_PAYMT_LOB_CD_SK"),
    F.col("A.PROV_CAP_PAYMT_METH_CD_SK").alias("PROV_CAP_PAYMT_METH_CD_SK"),
    F.col("A.PROV_CAP_PAYMT_CAP_TYP_CD_SK").alias("PROV_CAP_PAYMT_CAP_TYP_CD_SK"),
    F.col("A.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("A.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("A.CAP_PROV_SK").alias("CAP_PROV_SK"),
    F.col("A.PD_PROV_SK").alias("PD_PROV_SK"),
    F.col("A.PAYMT_SUM_SK").alias("PAYMT_SUM_SK"),
    F.col("A.AUTO_ADJ_AMT").alias("AUTO_ADJ_AMT"),
    F.col("A.CUR_CAP_AMT").alias("CUR_CAP_AMT"),
    F.col("A.MNL_ADJ_AMT").alias("MNL_ADJ_AMT"),
    F.col("A.NET_AMT").alias("NET_AMT"),
    F.col("A.AUTO_ADJ_MBR_MO_CT").alias("AUTO_ADJ_MBR_MO_CT"),
    F.col("A.CUR_MBR_MO_CT").alias("CUR_MBR_MO_CT"),
    F.col("A.MNL_ADJ_MBR_MO_CT").alias("MNL_ADJ_MBR_MO_CT"),
    F.col("A.CREATE_IDS_REC_PROCESS_DT").alias("CREATE_IDS_REC_PROCESS_DT")
)

# xfrm_BusinessLogic primary link
df_xfrm_input = df_lkp_Codes

# lin_Detail
df_lin_Detail_filtered = df_xfrm_input.filter(
    (F.col("PROV_CAP_SK") != 0) & (F.col("PROV_CAP_SK") != 1)
)
df_lin_Detail = df_lin_Detail_filtered.select(
    F.col("PROV_CAP_SK").alias("PROV_CAP_SK"),
    F.when(F.col("SRC_SYS_CD").isNull(), F.lit(" ")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.when(F.col("PD_YR_MO_SK").isNull(), F.lit("UNK")).otherwise(trim("PD_YR_MO_SK")).alias("PD_YR_MO_SK"),
    trim("CAP_PROV_ID").alias("CAP_PROV_ID"),
    trim("PD_PROV_ID").alias("PD_PROV_ID"),
    F.when(F.col("LOB_CD").isNull(), F.lit(" ")).otherwise(trim("LOB_CD")).alias("PROV_CAP_PAYMT_LOB_CD"),
    F.when(F.col("METH_CD").isNull(), F.lit(" ")).otherwise(trim("METH_CD")).alias("PROV_CAP_PAYMT_METH_CD"),
    F.when(F.col("CAP_PAY_CD").isNull(), F.lit(" ")).otherwise(trim("CAP_PAY_CD")).alias("PROV_CAP_PAYMT_TYP_CD"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.when(F.col("CAP_PROV_SK").isNull(), F.lit(" ")).otherwise(trim("CAP_PROV_SK")).alias("CAP_PROV_SK"),
    F.when(F.col("PD_PROV_SK").isNull(), F.lit("UNK")).otherwise(trim("PD_PROV_SK")).alias("PD_PROV_SK"),
    F.col("PD_DT_SK").alias("PD_DT_SK"),
    F.col("AUTO_ADJ_AMT").alias("PROV_CAP_AUTO_ADJ_AMT"),
    F.col("CUR_CAP_AMT").alias("PROV_CAP_CUR_CAP_AMT"),
    F.col("MNL_ADJ_AMT").alias("PROV_CAP_MNL_ADJ_AMT"),
    F.col("NET_AMT").alias("PROV_CAP_NET_AMT"),
    F.col("AUTO_ADJ_MBR_MO_CT").alias("PROV_CAP_AUTO_ADJ_MBR_MO_CT"),
    F.col("CUR_MBR_MO_CT").alias("PROV_CAP_CUR_MBR_MO_CT"),
    F.col("MNL_ADJ_MBR_MO_CT").alias("PROV_CAP_MNL_ADJ_MBR_MO_CT"),
    F.col("CAP_PROV_NM").alias("CAP_PROV_NM"),
    F.col("PD_PROV_NM").alias("PD_PROV_NM"),
    F.when(F.col("PAYMT_SUM_SK") == 1, F.lit("NA"))
     .otherwise(
        F.when(F.col("PAYMENT_SUM_PAYMT_REF_ID").isNull(), F.lit("UNK"))
        .otherwise(trim("PAYMENT_SUM_PAYMT_REF_ID"))
     ).alias("PAYMT_SUM_PAYMT_REF_ID"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_CAP_PAYMT_LOB_CD_SK").alias("PROV_CAP_LOB_CD_SK"),
    F.col("PROV_CAP_PAYMT_METH_CD_SK").alias("PROV_CAP_PAYMT_METH_CD_SK"),
    F.col("PROV_CAP_PAYMT_CAP_TYP_CD_SK").alias("PROV_CAP_TYP_CD_SK")
)

# lnk_Na (single row)
schema_na = StructType([
    StructField("PROV_CAP_SK", IntegerType()),
    StructField("SRC_SYS_CD", StringType()),
    StructField("PD_YR_MO_SK", StringType()),
    StructField("CAP_PROV_ID", StringType()),
    StructField("PD_PROV_ID", StringType()),
    StructField("PROV_CAP_PAYMT_LOB_CD", StringType()),
    StructField("PROV_CAP_PAYMT_METH_CD", StringType()),
    StructField("PROV_CAP_PAYMT_TYP_CD", StringType()),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType()),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType()),
    StructField("CAP_PROV_SK", StringType()),
    StructField("PD_PROV_SK", StringType()),
    StructField("PD_DT_SK", StringType()),
    StructField("PROV_CAP_AUTO_ADJ_AMT", IntegerType()),
    StructField("PROV_CAP_CUR_CAP_AMT", IntegerType()),
    StructField("PROV_CAP_MNL_ADJ_AMT", IntegerType()),
    StructField("PROV_CAP_NET_AMT", IntegerType()),
    StructField("PROV_CAP_AUTO_ADJ_MBR_MO_CT", IntegerType()),
    StructField("PROV_CAP_CUR_MBR_MO_CT", IntegerType()),
    StructField("PROV_CAP_MNL_ADJ_MBR_MO_CT", IntegerType()),
    StructField("CAP_PROV_NM", StringType()),
    StructField("PD_PROV_NM", StringType()),
    StructField("PAYMT_SUM_PAYMT_REF_ID", StringType()),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType()),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType()),
    StructField("PROV_CAP_LOB_CD_SK", StringType()),
    StructField("PROV_CAP_PAYMT_METH_CD_SK", StringType()),
    StructField("PROV_CAP_TYP_CD_SK", StringType())
])
df_lnk_Na = spark.createDataFrame([
    (
        1,
        "NA",
        "1",
        "NA",
        "NA",
        "NA",
        "NA",
        "NA",
        "1753-01-01",
        EDWRunCycleDate,
        "1",
        "1",
        "1",
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        "NA",
        "NA",
        "NA",
        100,
        EDWRunCycle,
        "1",
        "1",
        "1"
    )
], schema=schema_na)

# lnk_Unk (single row)
schema_unk = StructType([
    StructField("PROV_CAP_SK", IntegerType()),
    StructField("SRC_SYS_CD", StringType()),
    StructField("PD_YR_MO_SK", StringType()),
    StructField("CAP_PROV_ID", StringType()),
    StructField("PD_PROV_ID", StringType()),
    StructField("PROV_CAP_PAYMT_LOB_CD", StringType()),
    StructField("PROV_CAP_PAYMT_METH_CD", StringType()),
    StructField("PROV_CAP_PAYMT_TYP_CD", StringType()),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType()),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType()),
    StructField("CAP_PROV_SK", StringType()),
    StructField("PD_PROV_SK", StringType()),
    StructField("PD_DT_SK", StringType()),
    StructField("PROV_CAP_AUTO_ADJ_AMT", IntegerType()),
    StructField("PROV_CAP_CUR_CAP_AMT", IntegerType()),
    StructField("PROV_CAP_MNL_ADJ_AMT", IntegerType()),
    StructField("PROV_CAP_NET_AMT", IntegerType()),
    StructField("PROV_CAP_AUTO_ADJ_MBR_MO_CT", IntegerType()),
    StructField("PROV_CAP_CUR_MBR_MO_CT", IntegerType()),
    StructField("PROV_CAP_MNL_ADJ_MBR_MO_CT", IntegerType()),
    StructField("CAP_PROV_NM", StringType()),
    StructField("PD_PROV_NM", StringType()),
    StructField("PAYMT_SUM_PAYMT_REF_ID", StringType()),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType()),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType()),
    StructField("PROV_CAP_LOB_CD_SK", StringType()),
    StructField("PROV_CAP_PAYMT_METH_CD_SK", StringType()),
    StructField("PROV_CAP_TYP_CD_SK", StringType())
])
df_lnk_Unk = spark.createDataFrame([
    (
        0,
        "UNK",
        "0",
        "UNK",
        "UNK",
        "UNK",
        "UNK",
        "UNK",
        "1753-01-01",
        EDWRunCycleDate,
        "0",
        "0",
        "0",
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        "UNK",
        "UNK",
        "UNK",
        100,
        EDWRunCycle,
        "0",
        "0",
        "0"
    )
], schema=schema_unk)

# PxFunnel equivalent
df_Fnl_Prov_Cap_F = df_lin_Detail.unionByName(df_lnk_Na).unionByName(df_lnk_Unk)

# Final select with rpad for char columns
df_final = df_Fnl_Prov_Cap_F.select(
    F.col("PROV_CAP_SK"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("PD_YR_MO_SK"), 6, " ").alias("PD_YR_MO_SK"),
    F.col("CAP_PROV_ID"),
    F.col("PD_PROV_ID"),
    F.col("PROV_CAP_PAYMT_LOB_CD"),
    F.col("PROV_CAP_PAYMT_METH_CD"),
    F.col("PROV_CAP_PAYMT_TYP_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CAP_PROV_SK"),
    F.col("PD_PROV_SK"),
    F.rpad(F.col("PD_DT_SK"), 10, " ").alias("PD_DT_SK"),
    F.col("PROV_CAP_AUTO_ADJ_AMT"),
    F.col("PROV_CAP_CUR_CAP_AMT"),
    F.col("PROV_CAP_MNL_ADJ_AMT"),
    F.col("PROV_CAP_NET_AMT"),
    F.col("PROV_CAP_AUTO_ADJ_MBR_MO_CT"),
    F.col("PROV_CAP_CUR_MBR_MO_CT"),
    F.col("PROV_CAP_MNL_ADJ_MBR_MO_CT"),
    F.col("CAP_PROV_NM"),
    F.col("PD_PROV_NM"),
    F.col("PAYMT_SUM_PAYMT_REF_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_CAP_LOB_CD_SK"),
    F.col("PROV_CAP_PAYMT_METH_CD_SK"),
    F.col("PROV_CAP_TYP_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/PROV_CAP_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)