# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Ralph Tucker 11/17/2004 -   Originally Programmed
# MAGIC Brent Leland   08/30/2005 -   Changed column output order to match table.
# MAGIC  BJ Luce          11/21/2005  -   add DSALW_TYP_CAT_CD and DSALW_TYP_CAT_CD_SK, default to NA and 1
# MAGIC BJ Luce          1/17/2006        pull DSALW_TYP_CAT_CD and DSALW_TYP_CAT_CD_SK from IDS 
# MAGIC Brent Leland   04/04/2006      Changed parameters to environment parameters
# MAGIC                                                               Removed trim() off SK values.
# MAGIC Brent Leland    05/11/2006    Removed trim() on codes and IDs
# MAGIC 
# MAGIC Rama Kamjula  10/15/2013    Rewritten from Server to parallel version                                                                                                EnterpriseWrhsDevl        Jag Yelavarthi              2013-12-21
# MAGIC 
# MAGIC Nathan Reynolds   31 Oct 2016       tfs-13129                                   Add new field CLM_LN_CLNCL_EDIT_EXCD_TYP_CD_SK     EnterpriseDev2               Jag Yelavarthi              2016-11-16

# MAGIC JobName: IdsEdwClmLnClnclEditDExtr
# MAGIC 
# MAGIC Job creates loadfile for CLM_LN_CLNCL_EDIT_D  to load into EDW
# MAGIC Extracts data from CLM_LN_CLNCL_EDIT table from IDS.
# MAGIC Business Logic
# MAGIC Creates load file for CLM_LN_CLNCL_EDIT_D - EDW table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_db2_CLS_LN_CLNCL_EDIT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT CLCE.CLM_LN_CLNCL_EDIT_SK, "
        "COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD, "
        "CLCE.CLM_ID, "
        "CLCE.CLM_LN_SEQ_NO, "
        "CLCE.CLM_LN_SK, "
        "CLCE.REF_CLM_LN_SK, "
        "CLCE.CLM_LN_CLNCL_EDIT_ACTN_CD_SK, "
        "CLCE.CLM_LN_CLNCLEDIT_FMT_CHG_CD_SK, "
        "CLCE.CLM_LN_CLNCL_EDIT_TYP_CD_SK, "
        "CLCE.COMBND_CHRG_IN, "
        "CLCE.REF_CLM_ID, "
        "CLCE.REF_CLM_LN_SEQ_NO, "
        "CLCE.CLM_LN_CLNCL_EDIT_EXCD_SK "
        f"FROM {IDSOwner}.CLM_LN_CLNCL_EDIT CLCE "
        f"INNER JOIN {IDSOwner}.W_EDW_ETL_DRVR DT "
        "ON CLCE.CLM_ID = DT.CLM_ID AND CLCE.SRC_SYS_CD_SK = DT.SRC_SYS_CD_SK "
        f"LEFT JOIN {IDSOwner}.CD_MPPNG CD "
        "ON CLCE.SRC_SYS_CD_SK = CD.CD_MPPNG_SK"
    )
    .load()
)

df_db2_EXCD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT EXCD_SK, EXCD_ID, EXCD_SH_TX FROM {IDSOwner}.EXCD"
    )
    .load()
)

df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT CD_MPPNG_SK, COALESCE(TRGT_CD, 'UNK') TRGT_CD, TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
    )
    .load()
)

df_lnk_ClnclEditTyp = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lnk_ClnclEditActn = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lnk_ClnclEditFmtChg = df_db2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes = (
    df_db2_CLS_LN_CLNCL_EDIT.alias("lnk_PreExtr")
    .join(
        df_lnk_ClnclEditTyp.alias("lnk_ClnclEditTyp"),
        F.col("lnk_PreExtr.CLM_LN_CLNCL_EDIT_TYP_CD_SK") == F.col("lnk_ClnclEditTyp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_lnk_ClnclEditActn.alias("lnk_ClnclEditActn"),
        F.col("lnk_PreExtr.CLM_LN_CLNCL_EDIT_ACTN_CD_SK") == F.col("lnk_ClnclEditActn.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_lnk_ClnclEditFmtChg.alias("lnk_ClnclEditFmtChg"),
        F.col("lnk_PreExtr.CLM_LN_CLNCLEDIT_FMT_CHG_CD_SK") == F.col("lnk_ClnclEditFmtChg.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_EXCD.alias("Ink_EXCD_CD"),
        F.col("lnk_PreExtr.CLM_LN_CLNCL_EDIT_EXCD_SK") == F.col("Ink_EXCD_CD.EXCD_SK"),
        "left"
    )
    .select(
        F.col("lnk_PreExtr.CLM_LN_CLNCL_EDIT_SK").alias("CLM_LN_CLNCL_EDIT_SK"),
        F.col("lnk_PreExtr.CLM_ID").alias("CLM_ID"),
        F.col("lnk_PreExtr.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("lnk_PreExtr.CLM_LN_SK").alias("CLM_LN_SK"),
        F.col("lnk_PreExtr.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_PreExtr.CLM_LN_CLNCLEDIT_FMT_CHG_CD_SK").alias("CLM_LN_CLNCLEDIT_FMT_CHG_CD_SK"),
        F.col("lnk_PreExtr.REF_CLM_LN_SK").alias("REF_CLM_LN_SK"),
        F.col("lnk_PreExtr.CLM_LN_CLNCL_EDIT_ACTN_CD_SK").alias("CLM_LN_CLNCL_EDIT_ACTN_CD_SK"),
        F.col("lnk_PreExtr.COMBND_CHRG_IN").alias("COMBND_CHRG_IN"),
        F.col("lnk_PreExtr.REF_CLM_ID").alias("REF_CLM_ID"),
        F.col("lnk_PreExtr.CLM_LN_CLNCL_EDIT_TYP_CD_SK").alias("CLM_LN_CLNCL_EDIT_TYP_CD_SK"),
        F.col("lnk_PreExtr.REF_CLM_LN_SEQ_NO").alias("REF_CLM_LN_SEQ_NO"),
        F.col("lnk_ClnclEditActn.TRGT_CD").alias("TRGT_CD_ClnclEditActn"),
        F.col("lnk_ClnclEditActn.TRGT_CD_NM").alias("TRGT_CD_NM_ClnclEditActn"),
        F.col("lnk_ClnclEditFmtChg.TRGT_CD").alias("TRGT_CD_ClnclEditFmtChg"),
        F.col("lnk_ClnclEditFmtChg.TRGT_CD_NM").alias("TRGT_CD_NMClnclEditFmtChg"),
        F.col("lnk_ClnclEditTyp.TRGT_CD").alias("TRGT_CD_ClnclEditTypCd"),
        F.col("lnk_ClnclEditTyp.TRGT_CD_NM").alias("TRGT_CD_NM_ClnclEditTypCd"),
        F.col("lnk_PreExtr.CLM_LN_CLNCL_EDIT_EXCD_SK").alias("CLM_LN_CLNCL_EDIT_EXCD_SK"),
        F.col("Ink_EXCD_CD.EXCD_ID").alias("EXCD_ID"),
        F.col("Ink_EXCD_CD.EXCD_SH_TX").alias("EXCD_SH_TX")
    )
)

window_all = Window.orderBy(F.lit(1))
df_temp = df_lkp_Codes.withColumn("rn", F.row_number().over(window_all))

df_lnk_ClsLnClnclEditDOut = df_temp.filter(
    (F.col("CLM_LN_CLNCL_EDIT_SK") != 0) & (F.col("CLM_LN_CLNCL_EDIT_SK") != 1)
)

df_lnk_ClsLnClnclEditDOut = (
    df_lnk_ClsLnClnclEditDOut
    .withColumn(
        "CLM_LN_CLNCL_EDIT_ACTN_CD",
        F.when(
            F.col("TRGT_CD_ClnclEditActn").isNull() | (F.col("TRGT_CD_ClnclEditActn") == ""),
            F.lit("UNK")
        ).otherwise(F.col("TRGT_CD_ClnclEditActn"))
    )
    .withColumn(
        "CLM_LN_CLNCL_EDIT_ACTN_NM",
        F.when(
            F.col("TRGT_CD_NM_ClnclEditActn").isNull() | (F.col("TRGT_CD_NM_ClnclEditActn") == ""),
            F.lit("UNK")
        ).otherwise(F.col("TRGT_CD_NM_ClnclEditActn"))
    )
    .withColumn(
        "CLM_LN_CLNCL_EDIT_CMBD_CHRG_IN",
        F.rpad(F.col("COMBND_CHRG_IN"), 1, " ")
    )
    .withColumn(
        "CLM_LN_CLNCL_EDIT_FMT_CHG_CD",
        F.when(
            F.col("TRGT_CD_ClnclEditFmtChg").isNull() | (F.col("TRGT_CD_ClnclEditFmtChg") == ""),
            F.lit("UNK")
        ).otherwise(F.col("TRGT_CD_ClnclEditFmtChg"))
    )
    .withColumn(
        "CLM_LN_CLNCL_EDIT_FMT_CHG_NM",
        F.when(
            F.col("TRGT_CD_NMClnclEditFmtChg").isNull() | (F.col("TRGT_CD_NMClnclEditFmtChg") == ""),
            F.lit("UNK")
        ).otherwise(F.col("TRGT_CD_NMClnclEditFmtChg"))
    )
    .withColumn(
        "CLM_LN_CLNCL_EDIT_TYP_CD",
        F.when(
            (F.col("CLM_LN_CLNCL_EDIT_EXCD_SK") > 1) | (F.col("CLM_LN_CLNCL_EDIT_EXCD_SK") < 1),
            F.when(
                F.col("EXCD_ID").isNull() | (F.col("EXCD_ID") == ""),
                F.lit("UNK")
            ).otherwise(F.col("EXCD_ID"))
        ).otherwise(
            F.when(
                F.col("TRGT_CD_ClnclEditTypCd").isNull() | (F.col("TRGT_CD_ClnclEditTypCd") == ""),
                F.lit("UNK")
            ).otherwise(F.col("TRGT_CD_ClnclEditTypCd"))
        )
    )
    .withColumn(
        "CLM_LN_CLNCL_EDIT_TYP_NM",
        F.when(
            (F.col("CLM_LN_CLNCL_EDIT_EXCD_SK") > 1) | (F.col("CLM_LN_CLNCL_EDIT_EXCD_SK") < 1),
            F.when(
                F.col("EXCD_SH_TX").isNull() | (F.col("EXCD_SH_TX") == ""),
                F.lit("UNK")
            ).otherwise(F.col("EXCD_SH_TX"))
        ).otherwise(
            F.when(
                F.col("TRGT_CD_NM_ClnclEditTypCd").isNull() | (F.col("TRGT_CD_NM_ClnclEditTypCd") == ""),
                F.lit("UNK")
            ).otherwise(F.col("TRGT_CD_NM_ClnclEditTypCd"))
        )
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.lit(CurrRunCycleDate), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.lit(CurrRunCycleDate), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
)

df_lnk_ClsLnClnclEditDOut = df_lnk_ClsLnClnclEditDOut.select(
    F.col("CLM_LN_CLNCL_EDIT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLM_LN_SK"),
    F.col("REF_CLM_LN_SK"),
    F.col("CLM_LN_CLNCL_EDIT_ACTN_CD"),
    F.col("CLM_LN_CLNCL_EDIT_ACTN_NM"),
    F.col("CLM_LN_CLNCL_EDIT_CMBD_CHRG_IN"),
    F.col("CLM_LN_CLNCL_EDIT_FMT_CHG_CD"),
    F.col("CLM_LN_CLNCL_EDIT_FMT_CHG_NM"),
    F.col("CLM_LN_CLNCL_EDIT_TYP_CD"),
    F.col("CLM_LN_CLNCL_EDIT_TYP_NM"),
    F.col("REF_CLM_ID"),
    F.col("REF_CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_CLNCL_EDIT_ACTN_CD_SK"),
    F.col("CLM_LN_CLNCLEDIT_FMT_CHG_CD_SK"),
    F.col("CLM_LN_CLNCL_EDIT_TYP_CD_SK"),
    F.col("CLM_LN_CLNCL_EDIT_EXCD_SK")
)

df_lnk_NA = df_temp.filter(F.col("rn") == 1)
df_lnk_NA = df_lnk_NA.select(
    F.lit(1).alias("CLM_LN_CLNCL_EDIT_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(0).alias("CLM_LN_SEQ_NO"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("CLM_LN_SK"),
    F.lit(1).alias("REF_CLM_LN_SK"),
    F.lit("NA").alias("CLM_LN_CLNCL_EDIT_ACTN_CD"),
    F.lit("NA").alias("CLM_LN_CLNCL_EDIT_ACTN_NM"),
    F.rpad(F.lit("N"), 1, " ").alias("CLM_LN_CLNCL_EDIT_CMBD_CHRG_IN"),
    F.lit("NA").alias("CLM_LN_CLNCL_EDIT_FMT_CHG_CD"),
    F.lit("NA").alias("CLM_LN_CLNCL_EDIT_FMT_CHG_NM"),
    F.lit("NA").alias("CLM_LN_CLNCL_EDIT_TYP_CD"),
    F.lit("NA").alias("CLM_LN_CLNCL_EDIT_TYP_NM"),
    F.lit("NA").alias("REF_CLM_ID"),
    F.lit(0).alias("REF_CLM_LN_SEQ_NO"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLM_LN_CLNCL_EDIT_ACTN_CD_SK"),
    F.lit(1).alias("CLM_LN_CLNCLEDIT_FMT_CHG_CD_SK"),
    F.lit(1).alias("CLM_LN_CLNCL_EDIT_TYP_CD_SK"),
    F.lit(1).alias("CLM_LN_CLNCL_EDIT_EXCD_SK")
)

df_lnk_UNK = df_temp.filter(F.col("rn") == 1)
df_lnk_UNK = df_lnk_UNK.select(
    F.lit(0).alias("CLM_LN_CLNCL_EDIT_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("CLM_LN_SEQ_NO"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("CLM_LN_SK"),
    F.lit(0).alias("REF_CLM_LN_SK"),
    F.lit("UNK").alias("CLM_LN_CLNCL_EDIT_ACTN_CD"),
    F.lit("UNK").alias("CLM_LN_CLNCL_EDIT_ACTN_NM"),
    F.rpad(F.lit("N"), 1, " ").alias("CLM_LN_CLNCL_EDIT_CMBD_CHRG_IN"),
    F.lit("UNK").alias("CLM_LN_CLNCL_EDIT_FMT_CHG_CD"),
    F.lit("UNK").alias("CLM_LN_CLNCL_EDIT_FMT_CHG_NM"),
    F.lit("UNK").alias("CLM_LN_CLNCL_EDIT_TYP_CD"),
    F.lit("UNK").alias("CLM_LN_CLNCL_EDIT_TYP_NM"),
    F.lit("UNK").alias("REF_CLM_ID"),
    F.lit(0).alias("REF_CLM_LN_SEQ_NO"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_LN_CLNCL_EDIT_ACTN_CD_SK"),
    F.lit(0).alias("CLM_LN_CLNCLEDIT_FMT_CHG_CD_SK"),
    F.lit(0).alias("CLM_LN_CLNCL_EDIT_TYP_CD_SK"),
    F.lit(0).alias("CLM_LN_CLNCL_EDIT_EXCD_SK")
)

df_funnel = df_lnk_ClsLnClnclEditDOut.unionByName(df_lnk_NA).unionByName(df_lnk_UNK)

df_final = df_funnel.select(
    "CLM_LN_CLNCL_EDIT_SK",
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLM_LN_SK",
    "REF_CLM_LN_SK",
    "CLM_LN_CLNCL_EDIT_ACTN_CD",
    "CLM_LN_CLNCL_EDIT_ACTN_NM",
    "CLM_LN_CLNCL_EDIT_CMBD_CHRG_IN",
    "CLM_LN_CLNCL_EDIT_FMT_CHG_CD",
    "CLM_LN_CLNCL_EDIT_FMT_CHG_NM",
    "CLM_LN_CLNCL_EDIT_TYP_CD",
    "CLM_LN_CLNCL_EDIT_TYP_NM",
    "REF_CLM_ID",
    "REF_CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_CLNCL_EDIT_ACTN_CD_SK",
    "CLM_LN_CLNCLEDIT_FMT_CHG_CD_SK",
    "CLM_LN_CLNCL_EDIT_TYP_CD_SK",
    "CLM_LN_CLNCL_EDIT_EXCD_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_LN_CLNCL_EDIT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)