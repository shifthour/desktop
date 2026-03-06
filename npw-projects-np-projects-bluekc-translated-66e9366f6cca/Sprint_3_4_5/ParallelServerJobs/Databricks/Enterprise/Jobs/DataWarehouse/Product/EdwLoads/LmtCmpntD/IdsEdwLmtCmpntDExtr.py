# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                11/28/2007        Product/                       Originally Programmed                        devlEDW10                    Steph Goddard           12/05/2007
# MAGIC Pete Cundy                    09/16/2009         3556                            Added IDSRunCycle                          devlEDWnew                  Steph Goddard           10/07/2009
# MAGIC                                                                                                        Modified Where Clause
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               05/22/2013        5114                              Create Load File for EDW Table MMT_CMPNT_D                             EnterpriseWhseDevl   Pete Marshall               8/8/2013

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwLmtCmpntDExtr
# MAGIC Read from source table LMT_CMPNT from IDS.  Apply Run Cycle filters when applicable to get just the needed rows forward
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC LMT_CMPNT_ACCUM_CD_SK,
# MAGIC LMT_CMPNT_ACCUM_PERD_CD_SK,
# MAGIC LMT_CMPNT_INCLD_EXCL_CD_SK,
# MAGIC LMT_CMPNT_INCLD_EXCL_TYP_CD_SK,
# MAGIC LMT_CMPNT_LVL_CD_SK,
# MAGIC LMT_CMPNT_PRCS_CD_SK,
# MAGIC Write LMT_CMPNT_D Data into a Sequential file for Load Job IdsEdwLmtCmpmtDLoad.
# MAGIC Add Defaults and Null Handling.
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
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

fetch_query_db2_IDSLmtCmpnt_in = f"""
SELECT
LMT_CMPNT.LMT_CMPNT_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
LMT_CMPNT.LMT_CMPNT_ID,
LMT_CMPNT.ACCUM_NO,
LMT_CMPNT.EXCD_SK,
LMT_CMPNT.LMT_CMPNT_ACCUM_CD_SK,
LMT_CMPNT.LMT_CMPNT_ACCUM_PERD_CD_SK,
LMT_CMPNT.LMT_CMPNT_INCLD_EXCL_CD_SK,
LMT_CMPNT.LMT_CMPNT_INCLD_EXCL_TYP_CD_SK,
LMT_CMPNT.LMT_CMPNT_LVL_CD_SK,
LMT_CMPNT.LMT_CMPNT_PRCS_CD_SK,
LMT_CMPNT.LMT_CMPNT_CAROVR_REINST_AMT,
LMT_CMPNT.LMT_CMPNT_MAX_ACCUM_AMT,
LMT_CMPNT.LMT_CMPNT_EXCL_DAYS_CT,
LMT_CMPNT.LMT_CMPNT_DESC
FROM {IDSOwner}.LMT_CMPNT LMT_CMPNT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON LMT_CMPNT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE LMT_CMPNT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
"""

df_db2_IDSLmtCmpnt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", fetch_query_db2_IDSLmtCmpnt_in)
    .load()
)

fetch_query_db2_CD_MPPNG_Extr = f"""
SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", fetch_query_db2_CD_MPPNG_Extr)
    .load()
)

df_Ref_CmpntLvlCdLkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_AccumCdLkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_IncldExclTypLkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_CmpntPrcsCdLkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_AccumPerdCd_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_IncldExclCdLkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes = (
    df_db2_IDSLmtCmpnt_in.alias("lnk_IdsEdwLmtCmpnt_InAbc")
    .join(
        df_Ref_AccumPerdCd_Lkp.alias("Ref_AccumPerdCd_Lkp"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_ACCUM_PERD_CD_SK") == F.col("Ref_AccumPerdCd_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_AccumCdLkp.alias("Ref_AccumCdLkp"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_ACCUM_CD_SK") == F.col("Ref_AccumCdLkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_IncldExclCdLkp.alias("Ref_IncldExclCdLkp"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_INCLD_EXCL_CD_SK") == F.col("Ref_IncldExclCdLkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_IncldExclTypLkp.alias("Ref_IncldExclTypLkp"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_INCLD_EXCL_TYP_CD_SK") == F.col("Ref_IncldExclTypLkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_CmpntLvlCdLkp.alias("Ref_CmpntLvlCdLkp"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_LVL_CD_SK") == F.col("Ref_CmpntLvlCdLkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_CmpntPrcsCdLkp.alias("Ref_CmpntPrcsCdLkp"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_PRCS_CD_SK") == F.col("Ref_CmpntPrcsCdLkp.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_SK").alias("LMT_CMPNT_SK"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_ID").alias("LMT_CMPNT_ID"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.ACCUM_NO").alias("ACCUM_NO"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.EXCD_SK").alias("EXCD_SK"),
        F.col("Ref_AccumPerdCd_Lkp.TRGT_CD").alias("LMT_CMPNT_ACCUM_PERD_CD"),
        F.col("Ref_AccumPerdCd_Lkp.TRGT_CD_NM").alias("LMT_CMPNT_ACCUM_PERD_DESC"),
        F.col("Ref_AccumCdLkp.TRGT_CD").alias("LMT_CMPNT_ACCUM_CD"),
        F.col("Ref_AccumCdLkp.TRGT_CD_NM").alias("LMT_CMPNT_ACCUM_DESC"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_CAROVR_REINST_AMT").alias("LMT_CMPNT_CAROVR_REINST_AMT"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_DESC").alias("LMT_CMPNT_DESC"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_EXCL_DAYS_CT").alias("LMT_CMPNT_EXCL_DAYS_CT"),
        F.col("Ref_IncldExclCdLkp.TRGT_CD").alias("LMT_CMPNT_INCLD_EXCL_CD"),
        F.col("Ref_IncldExclCdLkp.TRGT_CD_NM").alias("LMT_CMPNT_INCLD_EXCL_DSCE"),
        F.col("Ref_IncldExclTypLkp.TRGT_CD").alias("LMT_CMPNT_INCLD_EXCL_TYP_CD"),
        F.col("Ref_IncldExclTypLkp.TRGT_CD_NM").alias("LMT_CMPNT_INCLD_EXCL_TYP_DSCE"),
        F.col("Ref_CmpntLvlCdLkp.TRGT_CD").alias("LMT_CMPNT_LVL_CD"),
        F.col("Ref_CmpntLvlCdLkp.TRGT_CD_NM").alias("LMT_CMPNT_LVL_DSCE"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_MAX_ACCUM_AMT").alias("LMT_CMPNT_MAX_ACCUM_AMT"),
        F.col("Ref_CmpntPrcsCdLkp.TRGT_CD").alias("LMT_CMPNT_PRCS_CD"),
        F.col("Ref_CmpntPrcsCdLkp.TRGT_CD_NM").alias("LMT_CMPNT_PRCS_DESC"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_ACCUM_PERD_CD_SK").alias("LMT_CMPNT_ACCUM_PERD_CD_SK"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_ACCUM_CD_SK").alias("LMT_CMPNT_ACCUM_CD_SK"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_INCLD_EXCL_CD_SK").alias("LMT_CMPNT_INCLD_EXCL_CD_SK"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_INCLD_EXCL_TYP_CD_SK").alias("LMT_CMPNT_INCLD_EXCL_TYP_CD_SK"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_LVL_CD_SK").alias("LMT_CMPNT_LVL_CD_SK"),
        F.col("lnk_IdsEdwLmtCmpnt_InAbc.LMT_CMPNT_PRCS_CD_SK").alias("LMT_CMPNT_PRCS_CD_SK")
    )
)

df_xfm_BusinessLogic = df_lkp_Codes.select(
    F.col("LMT_CMPNT_SK").alias("LMT_CMPNT_SK"),
    F.when(
        F.col("SRC_SYS_CD").isNull() | (F.trim(F.col("SRC_SYS_CD")) == ''),
        'UNK'
    ).otherwise(F.trim(F.col("SRC_SYS_CD"))).alias("SRC_SYS_CD"),
    F.col("LMT_CMPNT_ID").alias("LMT_CMPNT_ID"),
    F.col("ACCUM_NO").alias("LMT_CMPNT_ACCUM_NO"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("EXCD_SK").alias("EXCD_SK"),
    F.when(
        F.col("LMT_CMPNT_ACCUM_PERD_CD").isNull() | (F.trim(F.col("LMT_CMPNT_ACCUM_PERD_CD")) == ''),
        'UNK'
    ).otherwise(F.trim(F.col("LMT_CMPNT_ACCUM_PERD_CD"))).alias("LMT_CMPNT_ACCUM_PERD_CD"),
    F.when(
        F.col("LMT_CMPNT_ACCUM_PERD_DESC").isNull() | (F.trim(F.col("LMT_CMPNT_ACCUM_PERD_DESC")) == ''),
        'UNK'
    ).otherwise(F.trim(F.col("LMT_CMPNT_ACCUM_PERD_DESC"))).alias("LMT_CMPNT_ACCUM_PERD_DESC"),
    F.when(
        F.col("LMT_CMPNT_ACCUM_CD").isNull() | (F.trim(F.col("LMT_CMPNT_ACCUM_CD")) == ''),
        'UNK'
    ).otherwise(F.trim(F.col("LMT_CMPNT_ACCUM_CD"))).alias("LMT_CMPNT_ACCUM_CD"),
    F.when(
        F.col("LMT_CMPNT_ACCUM_DESC").isNull() | (F.trim(F.col("LMT_CMPNT_ACCUM_DESC")) == ''),
        'UNK'
    ).otherwise(F.trim(F.col("LMT_CMPNT_ACCUM_DESC"))).alias("LMT_CMPNT_ACCUM_DESC"),
    F.col("LMT_CMPNT_CAROVR_REINST_AMT").alias("LMT_CMPNT_CAROVR_REINST_AMT"),
    F.col("LMT_CMPNT_DESC").alias("LMT_CMPNT_DESC"),
    F.col("LMT_CMPNT_EXCL_DAYS_CT").alias("LMT_CMPNT_EXCL_DAYS_CT"),
    F.when(
        F.col("LMT_CMPNT_INCLD_EXCL_CD").isNull() | (F.trim(F.col("LMT_CMPNT_INCLD_EXCL_CD")) == ''),
        'UNK'
    ).otherwise(F.trim(F.col("LMT_CMPNT_INCLD_EXCL_CD"))).alias("LMT_CMPNT_INCLD_EXCL_CD"),
    F.when(
        F.col("LMT_CMPNT_INCLD_EXCL_DSCE").isNull() | (F.trim(F.col("LMT_CMPNT_INCLD_EXCL_DSCE")) == ''),
        'UNK'
    ).otherwise(F.trim(F.col("LMT_CMPNT_INCLD_EXCL_DSCE"))).alias("LMT_CMPNT_INCLD_EXCL_DESC"),
    F.when(
        F.col("LMT_CMPNT_INCLD_EXCL_TYP_CD").isNull() | (F.trim(F.col("LMT_CMPNT_INCLD_EXCL_TYP_CD")) == ''),
        'UNK'
    ).otherwise(F.trim(F.col("LMT_CMPNT_INCLD_EXCL_TYP_CD"))).alias("LMT_CMPNT_INCLD_EXCL_TYP_CD"),
    F.when(
        F.col("LMT_CMPNT_INCLD_EXCL_TYP_DSCE").isNull() | (F.trim(F.col("LMT_CMPNT_INCLD_EXCL_TYP_DSCE")) == ''),
        'UNK'
    ).otherwise(F.trim(F.col("LMT_CMPNT_INCLD_EXCL_TYP_DSCE"))).alias("LMT_CMPNT_INCLD_EXCL_TYP_DESC"),
    F.when(
        F.col("LMT_CMPNT_LVL_CD").isNull() | (F.trim(F.col("LMT_CMPNT_LVL_CD")) == ''),
        'UNK'
    ).otherwise(F.trim(F.col("LMT_CMPNT_LVL_CD"))).alias("LMT_CMPNT_LVL_CD"),
    F.when(
        F.col("LMT_CMPNT_LVL_DSCE").isNull() | (F.trim(F.col("LMT_CMPNT_LVL_DSCE")) == ''),
        'UNK'
    ).otherwise(F.trim(F.col("LMT_CMPNT_LVL_DSCE"))).alias("LMT_CMPNT_LVL_DESC"),
    F.col("LMT_CMPNT_MAX_ACCUM_AMT").alias("LMT_CMPNT_MAX_ACCUM_AMT"),
    F.when(
        F.col("LMT_CMPNT_PRCS_CD").isNull() | (F.trim(F.col("LMT_CMPNT_PRCS_CD")) == ''),
        'UNK'
    ).otherwise(F.trim(F.col("LMT_CMPNT_PRCS_CD"))).alias("LMT_CMPNT_PRCS_CD"),
    F.when(
        F.col("LMT_CMPNT_PRCS_DESC").isNull() | (F.trim(F.col("LMT_CMPNT_PRCS_DESC")) == ''),
        'UNK'
    ).otherwise(F.trim(F.col("LMT_CMPNT_PRCS_DESC"))).alias("LMT_CMPNT_PRCS_DESC"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LMT_CMPNT_ACCUM_PERD_CD_SK").alias("LMT_CMPNT_ACCUM_PERD_CD_SK"),
    F.col("LMT_CMPNT_ACCUM_CD_SK").alias("LMT_CMPNT_ACCUM_CD_SK"),
    F.col("LMT_CMPNT_INCLD_EXCL_CD_SK").alias("LMT_CMPNT_INCLD_EXCL_CD_SK"),
    F.col("LMT_CMPNT_INCLD_EXCL_TYP_CD_SK").alias("LMT_CMPNT_INCLD_EXCL_TYP_CD_SK"),
    F.col("LMT_CMPNT_LVL_CD_SK").alias("LMT_CMPNT_LVL_CD_SK"),
    F.col("LMT_CMPNT_PRCS_CD_SK").alias("LMT_CMPNT_PRCS_CD_SK")
)

df_seq_LMT_CMPNT_D_Load = df_xfm_BusinessLogic.select(
    "LMT_CMPNT_SK",
    "SRC_SYS_CD",
    "LMT_CMPNT_ID",
    "LMT_CMPNT_ACCUM_NO",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "EXCD_SK",
    "LMT_CMPNT_ACCUM_PERD_CD",
    "LMT_CMPNT_ACCUM_PERD_DESC",
    "LMT_CMPNT_ACCUM_CD",
    "LMT_CMPNT_ACCUM_DESC",
    "LMT_CMPNT_CAROVR_REINST_AMT",
    "LMT_CMPNT_DESC",
    "LMT_CMPNT_EXCL_DAYS_CT",
    "LMT_CMPNT_INCLD_EXCL_CD",
    "LMT_CMPNT_INCLD_EXCL_DESC",
    "LMT_CMPNT_INCLD_EXCL_TYP_CD",
    "LMT_CMPNT_INCLD_EXCL_TYP_DESC",
    "LMT_CMPNT_LVL_CD",
    "LMT_CMPNT_LVL_DESC",
    "LMT_CMPNT_MAX_ACCUM_AMT",
    "LMT_CMPNT_PRCS_CD",
    "LMT_CMPNT_PRCS_DESC",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "LMT_CMPNT_ACCUM_PERD_CD_SK",
    "LMT_CMPNT_ACCUM_CD_SK",
    "LMT_CMPNT_INCLD_EXCL_CD_SK",
    "LMT_CMPNT_INCLD_EXCL_TYP_CD_SK",
    "LMT_CMPNT_LVL_CD_SK",
    "LMT_CMPNT_PRCS_CD_SK"
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

write_files(
    df_seq_LMT_CMPNT_D_Load,
    f"{adls_path}/load/LMT_CMPNT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)