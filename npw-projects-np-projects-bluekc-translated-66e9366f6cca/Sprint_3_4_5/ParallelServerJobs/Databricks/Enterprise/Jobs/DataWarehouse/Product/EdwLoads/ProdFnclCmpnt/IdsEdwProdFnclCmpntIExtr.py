# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC   
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     EdwProdFnclCmpntIntExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC 	Pulls data from the IDS to update the EDW
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS:  PROD_FNCL_CMPNT
# MAGIC                          EXPRNC_CAT
# MAGIC                          FNCL_LOB
# MAGIC                          PROD_SH_NM
# MAGIC 
# MAGIC HASH FILES:
# MAGIC 	hf_cdma_codes         - Load the CDMA codes to get cd a nd cd_nm from cd_sk
# MAGIC 3;hf_prod_fncl_cmpnt_exp_cat;hf_prod_fncl_cmpnt_fncl_lob;hf_prod_fncl_cmpnt_prod_sh_nm
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC 	TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 	IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 	Each of the stream output hash files
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 	Hugh Sisson    09-20-2005   Original Program
# MAGIC                 SAndrew          12-14-2005   Removed unnessary parameters for when designing sequencer.
# MAGIC                SAndrew          11-14-2006   Prodject 1756 - standardized Run Cycle logix.    Changed direct reference lookups to hash file lookups.   added 3 hash files
# MAGIC 
# MAGIC 
# MAGIC                                                                                                                                                                                                                DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Leandrew Moore     05/19/2013       5114                              update to parallel                                                                                     Enterprisewarehouse     Pete Marshall           2013-08-08

# MAGIC Pull the matched set of products fron IDS and move to EDW.
# MAGIC K table - Use K table when all we need from this look up is Natural key from FKey denormalization. 
# MAGIC 
# MAGIC Base table - Use Base table when we need more than Natural key values from FKey denormalization
# MAGIC 
# MAGIC Once the SK is used for denormalization, do not carry oevr that information if it is not needed in the target table.
# MAGIC Read from source table PROD_FNCL_CMPNT from IDS.  Apply Run Cycle filters when applicable to get just the needed rows forward
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Once the SK is used for denormalization, do not carry over that information if it is not needed in the target table.
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
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate', '')
EDWRunCycle = get_widget_value('EDWRunCycle', '')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_db2_CD_MPPNG_Extr = f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from {IDSOwner}.CD_MPPNG"
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

extract_query_db2_PROD_FNCL_CMPNT = f"""SELECT 
  PROD_FNCL.PROD_FNCL_CMPNT_SK,
  COALESCE(CD2.TRGT_CD,'UNK') AS SRC_SYS_CD,
  PROD_FNCL.PROD_ID,
  PROD_FNCL.EFF_DT_SK,
  PROD_FNCL.CRT_RUN_CYC_EXCTN_SK,
  PROD_FNCL.LAST_UPDT_RUN_CYC_EXCTN_SK,
  PROD_FNCL.EXPRNC_CAT_SK,
  PROD_FNCL.FNCL_LOB_SK,
  PROD_FNCL.PROD_SK,
  PROD_LOB_CD_SK,
  PROD_FNCL.TERM_DT_SK
FROM {IDSOwner}.PROD_FNCL_CMPNT PROD_FNCL
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CD2
ON PROD_FNCL.SRC_SYS_CD_SK = CD2.CD_MPPNG_SK
"""
df_db2_PROD_FNCL_CMPNT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PROD_FNCL_CMPNT)
    .load()
)

extract_query_db2_EXPRNC_CAT = f"SELECT EXPRNC_CAT_SK,EXPRNC_CAT_CD FROM {IDSOwner}.EXPRNC_CAT"
df_db2_EXPRNC_CAT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_EXPRNC_CAT)
    .load()
)

extract_query_db2_FNCL_LOB = f"SELECT FNCL_LOB_SK,FNCL_LOB_CD FROM {IDSOwner}.FNCL_LOB"
df_db2_FNCL_LOB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_FNCL_LOB)
    .load()
)

extract_query_db2_PROD_SH_NM = f"SELECT PROD_SK,PROD_SH_NM FROM {IDSOwner}.PROD PROD, {IDSOwner}.PROD_SH_NM PROD_SH_NM WHERE PROD.PROD_SH_NM_SK = PROD_SH_NM.PROD_SH_NM_SK"
df_db2_PROD_SH_NM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PROD_SH_NM)
    .load()
)

df_lkp_FKeys = (
    df_db2_PROD_FNCL_CMPNT.alias("IdsEdwProdFnclCmpntExtrInAbc")
    .join(
        df_db2_EXPRNC_CAT.alias("lnk_IdsExprncCatLkup"),
        F.col("IdsEdwProdFnclCmpntExtrInAbc.EXPRNC_CAT_SK")
        == F.col("lnk_IdsExprncCatLkup.EXPRNC_CAT_SK"),
        how="left",
    )
    .join(
        df_db2_FNCL_LOB.alias("lnk_IdsFnclLobLkup"),
        F.col("IdsEdwProdFnclCmpntExtrInAbc.FNCL_LOB_SK")
        == F.col("lnk_IdsFnclLobLkup.FNCL_LOB_SK"),
        how="left",
    )
    .join(
        df_db2_PROD_SH_NM.alias("lnk_IdsProdShNmLkup"),
        F.col("IdsEdwProdFnclCmpntExtrInAbc.PROD_SK")
        == F.col("lnk_IdsProdShNmLkup.PROD_SK"),
        how="left",
    )
    .select(
        F.col("IdsEdwProdFnclCmpntExtrInAbc.PROD_FNCL_CMPNT_SK").alias("PROD_FNCL_CMPNT_SK"),
        F.col("IdsEdwProdFnclCmpntExtrInAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("IdsEdwProdFnclCmpntExtrInAbc.PROD_ID").alias("PROD_ID"),
        F.col("IdsEdwProdFnclCmpntExtrInAbc.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("IdsEdwProdFnclCmpntExtrInAbc.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
        F.col("IdsEdwProdFnclCmpntExtrInAbc.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        F.col("IdsEdwProdFnclCmpntExtrInAbc.PROD_SK").alias("PROD_SK"),
        F.col("IdsEdwProdFnclCmpntExtrInAbc.PROD_LOB_CD_SK").alias("PROD_LOB_CD_SK"),
        F.col("lnk_IdsExprncCatLkup.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
        F.col("lnk_IdsFnclLobLkup.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
        F.col("lnk_IdsProdShNmLkup.PROD_SH_NM").alias("PROD_SH_NM"),
        F.col("IdsEdwProdFnclCmpntExtrInAbc.TERM_DT_SK").alias("TERM_DT_SK"),
    )
)

df_lkp_Codes = (
    df_lkp_FKeys.alias("lnk_lkup_Fkeys")
    .join(
        df_db2_CD_MPPNG_Extr.alias("lnk_CdMppngOut"),
        F.col("lnk_lkup_Fkeys.PROD_LOB_CD_SK")
        == F.col("lnk_CdMppngOut.CD_MPPNG_SK"),
        how="left",
    )
    .select(
        F.col("lnk_CdMppngOut.TRGT_CD").alias("PROD_LOB_CD"),
        F.col("lnk_lkup_Fkeys.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_lkup_Fkeys.PROD_FNCL_CMPNT_SK").alias("PROD_FNCL_CMPNT_SK"),
        F.col("lnk_lkup_Fkeys.PROD_ID").alias("PROD_ID"),
        F.col("lnk_lkup_Fkeys.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("lnk_lkup_Fkeys.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
        F.col("lnk_lkup_Fkeys.EXPRNC_CAT_CD").alias("EXPRNC_CAT_CD"),
        F.col("lnk_lkup_Fkeys.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        F.col("lnk_lkup_Fkeys.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
        F.col("lnk_lkup_Fkeys.PROD_SK").alias("PROD_SK"),
        F.col("lnk_lkup_Fkeys.PROD_LOB_CD_SK").alias("PROD_LOB_CD_SK"),
        F.col("lnk_lkup_Fkeys.PROD_SH_NM").alias("PROD_SH_NM"),
        F.col("lnk_lkup_Fkeys.TERM_DT_SK").alias("TERM_DT_SK"),
    )
)

EXPRNC_CAT_CD_expr = F.when(
    F.col("lnk_CODES.EXPRNC_CAT_SK").isNull(), F.lit("UNK")
).otherwise(
    F.when(
        F.col("lnk_CODES.EXPRNC_CAT_CD").isNull(),
        F.lit("UNK"),
    ).otherwise(
        F.when(
            F.length(F.col("lnk_CODES.EXPRNC_CAT_CD")) == 0,
            F.lit("UNK"),
        ).otherwise(trim(F.col("lnk_CODES.EXPRNC_CAT_CD")))
    )
)

FNCL_LOB_CD_expr = F.when(
    F.col("lnk_CODES.FNCL_LOB_SK").isNull(), F.lit("UNK")
).otherwise(
    F.when(
        F.col("lnk_CODES.FNCL_LOB_CD").isNull(),
        F.lit("UNK"),
    ).otherwise(
        F.when(
            F.length(F.col("lnk_CODES.FNCL_LOB_CD")) == 0,
            F.lit("UNK"),
        ).otherwise(trim(F.col("lnk_CODES.FNCL_LOB_CD")))
    )
)

PROD_LOB_CD_expr = F.when(
    F.col("lnk_CODES.PROD_LOB_CD").isNull(), F.lit("UNK")
).otherwise(
    F.when(
        F.col("lnk_CODES.PROD_LOB_CD").isNull(),
        F.lit("UNK"),
    ).otherwise(
        F.when(
            F.length(F.col("lnk_CODES.PROD_LOB_CD")) == 0,
            F.lit("UNK"),
        ).otherwise(F.col("lnk_CODES.PROD_LOB_CD"))
    )
)

PROD_SH_NM_expr = F.when(
    F.col("lnk_CODES.PROD_SK").isNull(), F.lit("UNK")
).otherwise(
    F.when(
        F.col("lnk_CODES.PROD_SH_NM").isNull(),
        F.lit("UNK"),
    ).otherwise(
        F.when(
            F.length(F.col("lnk_CODES.PROD_SH_NM")) == 0,
            F.lit("UNK"),
        ).otherwise(trim(F.col("lnk_CODES.PROD_SH_NM")))
    )
)

df_xfrm_Business_Logic = (
    df_lkp_Codes.alias("lnk_CODES")
    .select(
        F.col("lnk_CODES.PROD_FNCL_CMPNT_SK").alias("PROD_FNCL_CMPNT_SK"),
        F.col("lnk_CODES.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_CODES.PROD_ID").alias("PROD_ID"),
        F.col("lnk_CODES.EFF_DT_SK").alias("EFF_DT_SK"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        EXPRNC_CAT_CD_expr.alias("EXPRNC_CAT_CD"),
        FNCL_LOB_CD_expr.alias("FNCL_LOB_CD"),
        PROD_LOB_CD_expr.alias("PROD_LOB_CD"),
        PROD_SH_NM_expr.alias("PROD_SH_NM"),
        F.col("lnk_CODES.TERM_DT_SK").alias("TERM_DT_SK"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_CODES.EXPRNC_CAT_SK").alias("EXPRNC_CAT_SK"),
        F.col("lnk_CODES.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        F.col("lnk_CODES.PROD_SK").alias("PROD_SK"),
        F.col("lnk_CODES.PROD_LOB_CD_SK").alias("PROD_LOB_CD_SK"),
    )
    .withColumn("EFF_DT_SK", F.rpad(F.col("EFF_DT_SK"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", F.rpad(F.col("TERM_DT_SK"), 10, " "))
)

df_final = df_xfrm_Business_Logic.select(
    "PROD_FNCL_CMPNT_SK",
    "SRC_SYS_CD",
    "PROD_ID",
    "EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "EXPRNC_CAT_CD",
    "FNCL_LOB_CD",
    "PROD_LOB_CD",
    "PROD_SH_NM",
    "TERM_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "EXPRNC_CAT_SK",
    "FNCL_LOB_SK",
    "PROD_SK",
    "PROD_LOB_CD_SK",
)

write_files(
    df_final,
    f"{adls_path}/load/PROD_FNCL_CMPNT_I.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None,
)