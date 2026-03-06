# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                    Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------             ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                 2012-06-28         4784                            Original Programming                                    EnterpriseWrhsDevl    Brent Leland                 07-10-2012
# MAGIC Kalyan Neelam                 2012-08-06         4784                            Added                                                           EnterpriseWrhsDevl    Sharon Andrew             2012-08-07
# MAGIC                                                                                                        CLM_F2.CLM_ID <> DSLink523.PRI_REL_CLM_ID_1 
# MAGIC                                                                                                        constraint in the ClmGrp_lkup transformer
# MAGIC Kalyan Neelam                 2012-08-13         4784                           Added CLM_F join for new ITS_CLM_IN       EnterpriseWrhsDevl     Bhoomi Dasari             08/14/2012
# MAGIC                                                                                                        constraint in CLM_F2 link in CLM_F2_lkup ODBC.
# MAGIC                                                                                                        Removed FACETS srcsyscd condition in CLM_F2 ODBC
# MAGIC Kalyan Neelam                 2012-11-02         4784                           Added new ExistingClm_lkup to the               EnterpriseWrhsDevl    Bhoomi Dasari             11/6/2012
# MAGIC                                                                                                        ClmGrp_Lkup Transformer to not extract the claims 
# MAGIC                                                                                                        which are already being processed in the below route
# MAGIC 
# MAGIC Rama Kamjula                 2013-11-04            5114                        Rewritten from Server to Parallel version         EnterpriseWrhsDEvl    Jag Yelavarthi               2013-12-22
# MAGIC                                                                                                        (IdsClmFact2ITSRelClmsUpdt)

# MAGIC JobName:IdsClmF2ITSRelClmsUpDt
# MAGIC 
# MAGIC Updates the Primary ITS Claim Related Claims fields.
# MAGIC Extract the Secondary Claims which are loaded during the current run and compare them with the primary claims to update their related claims fields.
# MAGIC Creates datasets to update the relClms in CLM_F2 table - IdsClmF2ITSRelClmsUpdt job
# MAGIC creates dataset with relclm to use in IdsClmF2ITSrelClmsUpdt job to update the CLM_F table.
# MAGIC Removes duplicates for CLM_SK based on the recent run cycles
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad, substring, coalesce
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunDtCycle = get_widget_value('EDWRunDtCycle','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_CLM_F2_PRI (EDW)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query_db2_CLM_F2_PRI = f"""
SELECT 
CLM_F2.CLM_SK,
CLM_F2.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
CLM_F2.LAST_UPDT_RUN_CYC_EXCTN_SK,
CLM_F2.REL_ITS_CLM_IN,
CLM_F2.REL_ITS_CLM_SK,
CLM_F2.REL_ITS_CLM_SRC_SYS_CD,
CLM_F2.REL_ITS_CLM_ID,
CLM_F2.REL_ITS_CLM_MBR_SK,
'Y' as PRI_IND
FROM {EDWOwner}.CLM_F2 CLM_F2
WHERE CLM_F2.SRC_SYS_CD = 'FACETS'
""".strip()
df_db2_CLM_F2_PRI = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_CLM_F2_PRI)
    .load()
)

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_IDS_CLM_REL_CLM (IDS)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_IDS_CLM_REL_CLM = f"""
SELECT
CLM_REL_CLM.SEC_REL_CLM_SK,
CLM_REL_CLM.PRI_REL_CLM_SK,
CLM_REL_CLM.PRI_REL_CLM_ID,
CLM_REL_CLM.PRI_REL_CLM_SRC_SYS_CD_SK,
CLM_REL_CLM.CLM_RELSHP_TYP_CD,
CLM_REL_CLM.SEC_REL_CLM_ID,
CLM_REL_CLM.SEC_REL_CLM_SRC_SYS_CD_SK,
CD.TRGT_CD PRI_SRC_SYS_CD,
CD1.TRGT_CD SEC_SRC_SYS_CD,
CLM_REL_CLM.LAST_UPDT_RUN_CYC_EXCTN_SK,
'Y' as SEC_REL_IND
FROM {IDSOwner}.CLM_REL_CLM CLM_REL_CLM,
     {IDSOwner}.CD_MPPNG CD,
     {IDSOwner}.CD_MPPNG CD1
WHERE
CLM_REL_CLM.CLM_RELSHP_TYP_CD = 'ITSBCBSKCPRI' AND
CLM_REL_CLM.PRI_REL_CLM_SRC_SYS_CD_SK = CD.CD_MPPNG_SK AND
CLM_REL_CLM.SEC_REL_CLM_SRC_SYS_CD_SK = CD1.CD_MPPNG_SK
""".strip()
df_db2_IDS_CLM_REL_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_IDS_CLM_REL_CLM)
    .load()
)

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_CLM_F2 (EDW)
extract_query_db2_CLM_F2 = f"""
SELECT 
CLM_SK
FROM {EDWOwner}.CLM_F2
WHERE
LAST_UPDT_RUN_CYC_EXCTN_DT_SK = '{EDWRunDtCycle}'
AND SRC_SYS_CD IN ('BCBSSC')
""".strip()
df_db2_CLM_F2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_CLM_F2)
    .load()
)

# --------------------------------------------------------------------------------
# PxLookup: lkp_Codes (PrimaryLink: db2_CLM_F2, LookupLink: db2_IDS_CLM_REL_CLM)
df_DSLink6 = (
    df_db2_CLM_F2.alias("lnk_ClmF2RelClms")
    .join(
        df_db2_IDS_CLM_REL_CLM.alias("DSLink4"),
        col("lnk_ClmF2RelClms.CLM_SK") == col("DSLink4.SEC_REL_CLM_SK"),
        "left"
    )
    .select(
        col("lnk_ClmF2RelClms.CLM_SK").alias("CLM_SK"),
        col("DSLink4.PRI_REL_CLM_ID").alias("PRI_REL_CLM_ID"),
        col("DSLink4.PRI_REL_CLM_SRC_SYS_CD_SK").alias("PRI_REL_CLM_SRC_SYS_CD_SK"),
        col("DSLink4.CLM_RELSHP_TYP_CD").alias("CLM_RELSHP_TYP_CD"),
        col("DSLink4.SEC_REL_CLM_ID").alias("SEC_REL_CLM_ID"),
        col("DSLink4.SEC_REL_CLM_SRC_SYS_CD_SK").alias("SEC_REL_CLM_SRC_SYS_CD_SK"),
        col("DSLink4.PRI_REL_CLM_SK").alias("PRI_REL_CLM_SK"),
        col("DSLink4.SEC_REL_IND").alias("SEC_REL_IND"),
        col("DSLink4.PRI_SRC_SYS_CD").alias("PRI_SRC_SYS_CD"),
        col("DSLink4.SEC_SRC_SYS_CD").alias("SEC_SRC_SYS_CD"),
        col("DSLink4.SEC_REL_CLM_SK").alias("SEC_REL_CLM_SK"),
        col("DSLink4.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# --------------------------------------------------------------------------------
# CTransformerStage: xfm_BusinessLogic (Input: DSLink6)
df_xfm_BusinessLogic_lnk_RelClm = (
    df_DSLink6
    .filter(col("SEC_REL_IND") == 'Y')
    .select(
        col("CLM_SK").alias("CLM_SK"),
        col("PRI_REL_CLM_ID").alias("PRI_REL_CLM_ID"),
        col("PRI_REL_CLM_SRC_SYS_CD_SK").alias("PRI_REL_CLM_SRC_SYS_CD_SK"),
        col("CLM_RELSHP_TYP_CD").alias("CLM_RELSHP_TYP_CD"),
        col("SEC_REL_CLM_ID").alias("SEC_REL_CLM_ID"),
        col("SEC_REL_CLM_SRC_SYS_CD_SK").alias("SEC_REL_CLM_SRC_SYS_CD_SK"),
        col("PRI_REL_CLM_SK").alias("PRI_REL_CLM_SK"),
        when(col("PRI_SRC_SYS_CD").isNull(), lit(None)).otherwise(col("PRI_SRC_SYS_CD")).alias("PRI_SRC_SYS_CD"),
        when(col("SEC_SRC_SYS_CD").isNull(), lit(None)).otherwise(col("SEC_SRC_SYS_CD")).alias("SEC_SRC_SYS_CD"),
        col("SEC_REL_CLM_SK").alias("SEC_REL_CLM_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        when(col("SEC_REL_CLM_SK") < 0, -1 * col("SEC_REL_CLM_SK")).otherwise(col("SEC_REL_CLM_SK")).alias("SEC_REL_CLM_SK_srt")
    )
)

# --------------------------------------------------------------------------------
# PxLookup: lkp_codes2 (PrimaryLink: xfm_BusinessLogic -> lnk_RelClm, LookupLink: db2_CLM_F2_PRI)
df_lkp_codes2 = (
    df_xfm_BusinessLogic_lnk_RelClm.alias("lnk_RelClm")
    .join(
        df_db2_CLM_F2_PRI.alias("lnk_Clm_F2_Pri"),
        col("lnk_RelClm.PRI_REL_CLM_SK") == col("lnk_Clm_F2_Pri.CLM_SK"),
        "left"
    )
    .select(
        col("lnk_Clm_F2_Pri.CLM_SK").alias("CLM_SK_Clm_F2"),
        col("lnk_Clm_F2_Pri.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("lnk_Clm_F2_Pri.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("lnk_Clm_F2_Pri.REL_ITS_CLM_IN").alias("REL_ITS_CLM_IN"),
        col("lnk_Clm_F2_Pri.PRI_IND").alias("PRI_IND_Clm_F2"),
        col("lnk_RelClm.SEC_REL_CLM_ID").alias("SEC_REL_CLM_ID_RelClm"),
        col("lnk_RelClm.SEC_SRC_SYS_CD").alias("SEC_SRC_SYS_CD_RelClm"),
        col("lnk_RelClm.SEC_REL_CLM_SK").alias("SEC_REL_CLM_SK_RelClm"),
        col("lnk_Clm_F2_Pri.REL_ITS_CLM_MBR_SK").alias("REL_ITS_CLM_MBR_SK_Clm_F2"),
        col("lnk_RelClm.PRI_REL_CLM_ID").alias("PRI_REL_CLM_ID_RelClm"),
        col("lnk_RelClm.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK_srt"),
        col("lnk_RelClm.SEC_REL_CLM_SK_srt").alias("SEC_REL_CLM_SK_srt")
    )
)

# --------------------------------------------------------------------------------
# CTransformerStage: xfm_BusinessLogic2 (Input: lnk_RelClmF2)
df_xfm_BusinessLogic2_lnk_ClmGrp = (
    df_lkp_codes2
    .filter(col("PRI_IND_Clm_F2") == 'Y')
    .select(
        substring(col("PRI_REL_CLM_ID_RelClm"), 1, 10).alias("PRI_REL_CLM_ID"),
        col("CLM_SK_Clm_F2").alias("CLM_SK"),
        lit(EDWRunDtCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit("Y").alias("REL_ITS_CLM_IN"),
        col("SEC_REL_CLM_SK_RelClm").alias("REL_ITS_CLM_SK"),
        coalesce(col("SEC_SRC_SYS_CD_RelClm"), lit("NA")).alias("REL_ITS_CLM_SRC_SYS_CD"),
        coalesce(col("SEC_REL_CLM_ID_RelClm"), lit("NA")).alias("REL_ITS_CLM_ID"),
        col("REL_ITS_CLM_MBR_SK_Clm_F2").alias("REL_ITS_CLM_MBR_SK"),
        col("PRI_REL_CLM_ID_RelClm").alias("PRI_REL_CLM_ID_1"),
        lit("Y").alias("ClmGrp_Ind")
    )
)
df_xfm_BusinessLogic2_lnk_RelClmUpdt = (
    df_lkp_codes2
    .filter(col("PRI_IND_Clm_F2") == 'Y')
    .select(
        col("CLM_SK_Clm_F2").alias("CLM_SK"),
        lit(EDWRunDtCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit("Y").alias("REL_ITS_CLM_IN"),
        coalesce(col("SEC_REL_CLM_ID_RelClm"), lit("NA")).alias("REL_ITS_CLM_ID"),
        coalesce(col("SEC_SRC_SYS_CD_RelClm"), lit("NA")).alias("REL_ITS_CLM_SRC_SYS_CD"),
        col("SEC_REL_CLM_SK_RelClm").alias("REL_ITS_CLM_SK"),
        col("REL_ITS_CLM_MBR_SK_Clm_F2").alias("REL_ITS_CLM_MBR_SK"),
        col("SEC_REL_CLM_SK_RelClm").alias("REL_ITS_CLM_SK_SEC"),
        lit("Y").alias("ExistingClms_Ind"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK_srt"),
        col("SEC_REL_CLM_SK_srt")
    )
)

# --------------------------------------------------------------------------------
# PxDataSet: ds_ClmGrp (Input: lnk_ClmGrp)
df_final_ds_ClmGrp = df_xfm_BusinessLogic2_lnk_ClmGrp.select(
    "PRI_REL_CLM_ID",
    "CLM_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "REL_ITS_CLM_IN",
    "REL_ITS_CLM_SK",
    "REL_ITS_CLM_SRC_SYS_CD",
    "REL_ITS_CLM_ID",
    "REL_ITS_CLM_MBR_SK",
    "PRI_REL_CLM_ID_1",
    "ClmGrp_Ind"
)
df_final_ds_ClmGrp = df_final_ds_ClmGrp \
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("REL_ITS_CLM_IN", rpad(col("REL_ITS_CLM_IN"), 1, " "))

write_files(
    df_final_ds_ClmGrp,
    f"{adls_path}/ds/ClmGrp.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# PxRemDup: rd_Clm_Sk (Input: lnk_RelClmUpdt) 
df_rd_Clm_Sk_dedup = dedup_sort(
    df_xfm_BusinessLogic2_lnk_RelClmUpdt,
    ["CLM_SK"],
    [("CLM_SK","A"), ("LAST_UPDT_RUN_CYC_EXCTN_SK_srt","D"), ("SEC_REL_CLM_SK_srt","D")]
).select(
    col("CLM_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("REL_ITS_CLM_IN"),
    col("REL_ITS_CLM_ID"),
    col("REL_ITS_CLM_SRC_SYS_CD"),
    col("REL_ITS_CLM_SK"),
    col("REL_ITS_CLM_MBR_SK"),
    col("REL_ITS_CLM_SK_SEC")
)

# --------------------------------------------------------------------------------
# PxDataSet: ds_RelClmUpdt (Input: lnk_RelClmUpdt_Out)
df_final_ds_RelClmUpdt = df_rd_Clm_Sk_dedup.select(
    "CLM_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "REL_ITS_CLM_IN",
    "REL_ITS_CLM_ID",
    "REL_ITS_CLM_SRC_SYS_CD",
    "REL_ITS_CLM_SK",
    "REL_ITS_CLM_MBR_SK",
    "REL_ITS_CLM_SK_SEC"
)
df_final_ds_RelClmUpdt = df_final_ds_RelClmUpdt \
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
    .withColumn("REL_ITS_CLM_IN", rpad(col("REL_ITS_CLM_IN"), 1, " "))

write_files(
    df_final_ds_RelClmUpdt,
    f"{adls_path}/ds/RelClmUpdt.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)