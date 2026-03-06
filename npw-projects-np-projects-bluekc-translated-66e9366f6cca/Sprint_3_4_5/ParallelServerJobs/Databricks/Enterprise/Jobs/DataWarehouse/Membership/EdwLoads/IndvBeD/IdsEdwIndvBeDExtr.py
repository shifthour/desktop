# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Archana Palivela     07/03/2013        5114                              Originally Programmed  (In Parallel)                                                      EnterpriseWhseDevl

# MAGIC Read Before data from  table INDV_BE_D
# MAGIC Reads After  data from source table INDV_BE
# MAGIC Funnel coulmns for Update and Insert
# MAGIC Create a load seq file for load job.
# MAGIC Job Name: IdsEdwIndvBeDExtr
# MAGIC 
# MAGIC Records are created in INDV_BE_D table whenever they are changes in Source INDV_BE
# MAGIC Applys transformation rules for the columns:
# MAGIC INDV_BE_PRCS_STRT_DT_SK
# MAGIC INDV_BE_CUR_RCRD_IN
# MAGIC INDV_BE_PRCS_END_DT_SK
# MAGIC  acc to the cdma document.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
EDWOwner = get_widget_value('EDWOwner','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
PrevEdwRunCycleDt = get_widget_value('PrevEdwRunCycleDt','')
ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
query_DB2_INDV_BE_D = (
    f"SELECT \n\nINDV_BE_KEY,\nINDV_BE_KEY AS INDV_BE_KEY_H,\nINDV_BE_PRCS_STRT_DT_SK,\n"
    f"CRT_RUN_CYC_EXCTN_DT_SK,\nINDV_BE_BRTH_DT_SK,\nINDV_BE_CUR_RCRD_IN,\nINDV_BE_COV_STTUS_CD,\n"
    f"INDV_BE_COV_STTUS_NM,\nINDV_BE_DCSD_DT_SK,\nINDV_BE_GNDR_CD,\nINDV_BE_GNDR_NM,\nINDV_BE_FIRST_NM,\n"
    f"INDV_BE_MIDINIT,\nINDV_BE_LAST_NM,\nINDV_BE_ORIG_EFF_DT_SK,\nINDV_BE_PRCS_END_DT_SK,\nINDV_BE_SSN,\n"
    f"MBR_DIM_IN,\nSRC_SYS_CD,\nCRT_RUN_CYC_EXCTN_SK\n\nFROM {EDWOwner}.INDV_BE_D \n\nWHERE INDV_BE_CUR_RCRD_IN = 'Y'"
)
df_DB2_INDV_BE_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", query_DB2_INDV_BE_D)
    .load()
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
query_DB2_INDV_BE = (
    f"SELECT \n\nINDV_BE.INDV_BE_KEY,\nINDV_BE.INDV_BE_COV_STTUS_CD_SK,\nINDV_BE.INDV_BE_GNDR_CD_SK,\n"
    f"COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,\nINDV_BE.MBR_IN,\nINDV_BE.BRTH_DT_SK,\nINDV_BE.DCSD_DT_SK,\n"
    f"INDV_BE.ORIG_EFF_DT_SK,\nINDV_BE.FIRST_NM,\nINDV_BE.MIDINIT,\nINDV_BE.LAST_NM,\nINDV_BE.SSN \n\n"
    f"FROM {IDSOwner}.INDV_BE INDV_BE\n\n"
    f"LEFT JOIN {IDSOwner}.CD_MPPNG CD\n"
    f"ON INDV_BE.SRC_SYS_CD_SK = CD.CD_MPPNG_SK\n\n"
    f"WHERE INDV_BE.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}"
)
df_DB2_INDV_BE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_DB2_INDV_BE)
    .load()
)

query_db2_CD_MPPNG_Extr = (
    f"SELECT CD_MPPNG_SK,\nCOALESCE(TRGT_CD,'UNK') TRGT_CD,\nCOALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM\n"
    f" from {IDSOwner}.CD_MPPNG"
)
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_CD_MPPNG_Extr)
    .load()
)

df_Lnk_Indv_Be_Cov_Sttus_Cd = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)

df_Lnk_Indv_Be_Gndr_cd = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)

df_lkp_Codes = (
    df_DB2_INDV_BE.alias("IdsEdw_IndvBeD_After_outABC")
    .join(
        df_Lnk_Indv_Be_Cov_Sttus_Cd.alias("Lnk_Indv_Be_Cov_Sttus_Cd"),
        F.col("IdsEdw_IndvBeD_After_outABC.INDV_BE_COV_STTUS_CD_SK") == F.col("Lnk_Indv_Be_Cov_Sttus_Cd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Lnk_Indv_Be_Gndr_cd.alias("Lnk_Indv_Be_Gndr_cd"),
        F.col("IdsEdw_IndvBeD_After_outABC.INDV_BE_GNDR_CD_SK") == F.col("Lnk_Indv_Be_Gndr_cd.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("IdsEdw_IndvBeD_After_outABC.INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("Lnk_Indv_Be_Cov_Sttus_Cd.TRGT_CD").alias("INDV_BE_COV_STTUS_CD_S"),
        F.col("Lnk_Indv_Be_Cov_Sttus_Cd.TRGT_CD_NM").alias("INDV_BE_COV_STTUS_NM_S"),
        F.col("Lnk_Indv_Be_Gndr_cd.TRGT_CD").alias("INDV_BE_GNDR_CD_S"),
        F.col("Lnk_Indv_Be_Gndr_cd.TRGT_CD_NM").alias("INDV_BE_GNDR_NM_S"),
        F.col("IdsEdw_IndvBeD_After_outABC.SRC_SYS_CD").alias("SRC_SYS_CD_S"),
        F.col("IdsEdw_IndvBeD_After_outABC.MBR_IN").alias("MBR_DIM_IN_S"),
        F.col("IdsEdw_IndvBeD_After_outABC.BRTH_DT_SK").alias("INDV_BE_BRTH_DT_SK_S"),
        F.col("IdsEdw_IndvBeD_After_outABC.DCSD_DT_SK").alias("INDV_BE_DCSD_DT_SK_S"),
        F.col("IdsEdw_IndvBeD_After_outABC.ORIG_EFF_DT_SK").alias("INDV_BE_ORIG_EFF_DT_SK_S"),
        F.col("IdsEdw_IndvBeD_After_outABC.FIRST_NM").alias("INDV_BE_FIRST_NM_S"),
        F.col("IdsEdw_IndvBeD_After_outABC.MIDINIT").alias("INDV_BE_MIDINIT_S"),
        F.col("IdsEdw_IndvBeD_After_outABC.LAST_NM").alias("INDV_BE_LAST_NM_S"),
        F.col("IdsEdw_IndvBeD_After_outABC.SSN").alias("INDV_BE_SSN_S")
    )
)

df_Jn_Indv_Be_D = (
    df_lkp_Codes.alias("lnk_Lkp_Codes")
    .join(
        df_DB2_INDV_BE_D.alias("IdsEdw_IndvBeD_Before_InABC"),
        F.col("lnk_Lkp_Codes.INDV_BE_KEY") == F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_KEY"),
        "left"
    )
    .select(
        F.col("lnk_Lkp_Codes.INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_KEY_H").alias("INDV_BE_KEY_H"),
        F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_PRCS_STRT_DT_SK").alias("INDV_BE_PRCS_STRT_DT_SK"),
        F.col("IdsEdw_IndvBeD_Before_InABC.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_BRTH_DT_SK").alias("INDV_BE_BRTH_DT_SK"),
        F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_CUR_RCRD_IN").alias("INDV_BE_CUR_RCRD_IN"),
        F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_FIRST_NM").alias("INDV_BE_FIRST_NM"),
        F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_MIDINIT").alias("INDV_BE_MIDINIT"),
        F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_LAST_NM").alias("INDV_BE_LAST_NM"),
        F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_PRCS_END_DT_SK").alias("INDV_BE_PRCS_END_DT_SK"),
        F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_SSN").alias("INDV_BE_SSN"),
        F.col("IdsEdw_IndvBeD_Before_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_Lkp_Codes.INDV_BE_BRTH_DT_SK_S").alias("INDV_BE_BRTH_DT_SK_S"),
        F.col("lnk_Lkp_Codes.INDV_BE_FIRST_NM_S").alias("INDV_BE_FIRST_NM_S"),
        F.col("lnk_Lkp_Codes.INDV_BE_MIDINIT_S").alias("INDV_BE_MIDINIT_S"),
        F.col("lnk_Lkp_Codes.INDV_BE_LAST_NM_S").alias("INDV_BE_LAST_NM_S"),
        F.col("lnk_Lkp_Codes.INDV_BE_SSN_S").alias("INDV_BE_SSN_S"),
        F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_COV_STTUS_CD").alias("INDV_BE_COV_STTUS_CD"),
        F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_COV_STTUS_NM").alias("INDV_BE_COV_STTUS_NM"),
        F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_GNDR_CD").alias("INDV_BE_GNDR_CD"),
        F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_GNDR_NM").alias("INDV_BE_GNDR_NM"),
        F.col("IdsEdw_IndvBeD_Before_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("IdsEdw_IndvBeD_Before_InABC.MBR_DIM_IN").alias("MBR_DIM_IN"),
        F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_DCSD_DT_SK").alias("INDV_BE_DCSD_DT_SK"),
        F.col("IdsEdw_IndvBeD_Before_InABC.INDV_BE_ORIG_EFF_DT_SK").alias("INDV_BE_ORIG_EFF_DT_SK"),
        F.col("lnk_Lkp_Codes.INDV_BE_COV_STTUS_CD_S").alias("INDV_BE_COV_STTUS_CD_S"),
        F.col("lnk_Lkp_Codes.INDV_BE_COV_STTUS_NM_S").alias("INDV_BE_COV_STTUS_NM_S"),
        F.col("lnk_Lkp_Codes.INDV_BE_GNDR_CD_S").alias("INDV_BE_GNDR_CD_S"),
        F.col("lnk_Lkp_Codes.INDV_BE_GNDR_NM_S").alias("INDV_BE_GNDR_NM_S"),
        F.col("lnk_Lkp_Codes.SRC_SYS_CD_S").alias("SRC_SYS_CD_S"),
        F.col("lnk_Lkp_Codes.MBR_DIM_IN_S").alias("MBR_DIM_IN_S"),
        F.col("lnk_Lkp_Codes.INDV_BE_DCSD_DT_SK_S").alias("INDV_BE_DCSD_DT_SK_S"),
        F.col("lnk_Lkp_Codes.INDV_BE_ORIG_EFF_DT_SK_S").alias("INDV_BE_ORIG_EFF_DT_SK_S")
    )
)

df_xfrm = (
    df_Jn_Indv_Be_D
    .withColumn(
        "svNewrowIn",
        F.when(F.col("INDV_BE_KEY_H").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "svLkpMin",
        F.when(F.col("INDV_BE_MIDINIT").isNull(), F.lit(" ")).otherwise(F.col("INDV_BE_MIDINIT"))
    )
    .withColumn(
        "svPrev",
        F.concat(
            trim(
                F.when(F.col("INDV_BE_FIRST_NM").isNotNull(), F.col("INDV_BE_FIRST_NM")).otherwise(F.lit(""))
            ),
            trim(
                F.when(F.col("INDV_BE_LAST_NM").isNotNull(), F.col("INDV_BE_LAST_NM")).otherwise(F.lit(""))
            ),
            trim(F.col("svLkpMin")),
            trim(F.col("INDV_BE_BRTH_DT_SK"))
        )
    )
    .withColumn(
        "svSMINT",
        F.when(F.col("INDV_BE_MIDINIT_S").isNull(), F.lit(" ")).otherwise(F.col("INDV_BE_MIDINIT_S"))
    )
    .withColumn(
        "svCurr",
        F.concat(
            trim(
                F.when(F.col("INDV_BE_FIRST_NM_S").isNotNull(), F.col("INDV_BE_FIRST_NM_S")).otherwise(F.lit(""))
            ),
            trim(
                F.when(F.col("INDV_BE_LAST_NM_S").isNotNull(), F.col("INDV_BE_LAST_NM_S")).otherwise(F.lit(""))
            ),
            trim(F.col("svSMINT")),
            trim(F.col("INDV_BE_BRTH_DT_SK_S"))
        )
    )
    .withColumn(
        "svSSN",
        F.when(
            trim(F.col("INDV_BE_SSN")) != trim(F.col("INDV_BE_SSN_S")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svValueCompare",
        F.when(
            (F.col("svPrev") != F.col("svCurr")) | (F.col("svSSN") == "Y"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

df_Lnk_Hist_Rec = (
    df_xfrm
    .filter(
        (F.col("svNewrowIn") == "N")
        & (F.col("svValueCompare") == "Y")
        & (F.col("INDV_BE_KEY") != 0)
        & (F.col("INDV_BE_KEY") != 1)
    )
    .select(
        F.col("INDV_BE_KEY"),
        rpad(F.col("INDV_BE_PRCS_STRT_DT_SK"), 10, " ").alias("INDV_BE_PRCS_STRT_DT_SK"),
        rpad(F.col("INDV_BE_BRTH_DT_SK"), 10, " ").alias("INDV_BE_BRTH_DT_SK"),
        F.when(
            trim(F.col("INDV_BE_COV_STTUS_CD")) == "",
            F.lit("UNK")
        ).otherwise(F.col("INDV_BE_COV_STTUS_CD")).alias("INDV_BE_COV_STTUS_CD"),
        F.when(
            trim(F.col("INDV_BE_COV_STTUS_NM")) == "",
            F.lit("UNK")
        ).otherwise(F.col("INDV_BE_COV_STTUS_NM")).alias("INDV_BE_COV_STTUS_NM"),
        rpad(F.col("INDV_BE_DCSD_DT_SK"), 10, " ").alias("INDV_BE_DCSD_DT_SK"),
        F.when(
            trim(F.col("INDV_BE_GNDR_CD")) == "",
            F.lit("UNK")
        ).otherwise(F.col("INDV_BE_GNDR_CD")).alias("INDV_BE_GNDR_CD"),
        F.when(
            trim(F.col("INDV_BE_GNDR_NM")) == "",
            F.lit("UNK")
        ).otherwise(F.col("INDV_BE_GNDR_NM")).alias("INDV_BE_GNDR_NM"),
        F.col("INDV_BE_FIRST_NM").alias("INDV_BE_FIRST_NM"),
        F.col("svLkpMin").alias("INDV_BE_MIDINIT"),
        F.col("INDV_BE_LAST_NM").alias("INDV_BE_LAST_NM"),
        rpad(F.col("INDV_BE_ORIG_EFF_DT_SK"), 10, " ").alias("INDV_BE_ORIG_EFF_DT_SK"),
        F.col("INDV_BE_SSN").alias("INDV_BE_SSN"),
        rpad(F.col("MBR_DIM_IN"), 1, " ").alias("MBR_DIM_IN"),
        F.when(
            trim(F.col("SRC_SYS_CD")) == "",
            F.lit("UNK")
        ).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        rpad(F.lit(PrevEdwRunCycleDt), 10, " ").alias("INDV_BE_PRCS_END_DT_SK"),
        rpad(F.lit("N"), 1, " ").alias("INDV_BE_CUR_RCRD_IN"),
        F.col("EDWRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        rpad(F.col("EDWRunCycleDate"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("EDWRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
        rpad(F.col("EDWRunCycleDate"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK")
    )
)

df_Lnk_Curr_rec = (
    df_xfrm
    .filter(
        (F.col("INDV_BE_KEY") != 0)
        & (F.col("INDV_BE_KEY") != 1)
    )
    .select(
        F.col("INDV_BE_KEY"),
        rpad(
            F.when(
                F.col("INDV_BE_PRCS_STRT_DT_SK").isNull()
                | (F.col("svValueCompare") == "Y")
                | (F.col("INDV_BE_KEY_H").isNull()),
                F.col("EDWRunCycleDate")
            ).otherwise(F.col("INDV_BE_PRCS_STRT_DT_SK")),
            10, " "
        ).alias("INDV_BE_PRCS_STRT_DT_SK"),
        rpad(F.col("INDV_BE_BRTH_DT_SK_S"), 10, " ").alias("INDV_BE_BRTH_DT_SK"),
        F.when(
            trim(F.col("INDV_BE_COV_STTUS_CD_S")) == "",
            F.lit("UNK")
        ).otherwise(F.col("INDV_BE_COV_STTUS_CD_S")).alias("INDV_BE_COV_STTUS_CD"),
        F.when(
            trim(F.col("INDV_BE_COV_STTUS_NM_S")) == "",
            F.lit("UNK")
        ).otherwise(F.col("INDV_BE_COV_STTUS_NM_S")).alias("INDV_BE_COV_STTUS_NM"),
        rpad(F.col("INDV_BE_DCSD_DT_SK_S"), 10, " ").alias("INDV_BE_DCSD_DT_SK"),
        F.when(
            trim(F.col("INDV_BE_GNDR_CD_S")) == "",
            F.lit("UNK")
        ).otherwise(F.col("INDV_BE_GNDR_CD_S")).alias("INDV_BE_GNDR_CD"),
        F.when(
            trim(F.col("INDV_BE_GNDR_NM_S")) == "",
            F.lit("UNK")
        ).otherwise(F.col("INDV_BE_GNDR_NM_S")).alias("INDV_BE_GNDR_NM"),
        F.col("INDV_BE_FIRST_NM_S").alias("INDV_BE_FIRST_NM"),
        F.col("svSMINT").alias("INDV_BE_MIDINIT"),
        F.col("INDV_BE_LAST_NM_S").alias("INDV_BE_LAST_NM"),
        rpad(F.col("INDV_BE_ORIG_EFF_DT_SK_S"), 10, " ").alias("INDV_BE_ORIG_EFF_DT_SK"),
        F.col("INDV_BE_SSN_S").alias("INDV_BE_SSN"),
        rpad(F.col("MBR_DIM_IN_S"), 1, " ").alias("MBR_DIM_IN"),
        F.when(
            trim(F.col("SRC_SYS_CD_S")) == "",
            F.lit("UNK")
        ).otherwise(F.col("SRC_SYS_CD_S")).alias("SRC_SYS_CD"),
        rpad(
            F.when(
                F.col("INDV_BE_PRCS_END_DT_SK").isNull()
                | (F.col("svValueCompare") == "Y"),
                F.lit("2199-12-31")
            ).otherwise(F.col("INDV_BE_PRCS_END_DT_SK")),
            10, " "
        ).alias("INDV_BE_PRCS_END_DT_SK"),
        rpad(F.lit("Y"), 1, " ").alias("INDV_BE_CUR_RCRD_IN"),
        F.col("EDWRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        rpad(F.col("EDWRunCycleDate"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("EDWRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
        rpad(F.col("EDWRunCycleDate"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK")
    )
)

df_NA_Row = (
    df_xfrm
    .filter(
        ((F.col("@INROWNUM") - F.lit(1)) * F.col("@NUMPARTITIONS") + F.col("@PARTITIONNUM") + F.lit(1)) == 1
    )
    .select(
        F.lit(1).alias("INDV_BE_KEY"),
        rpad(F.lit("1753-01-01"), 10, " ").alias("INDV_BE_PRCS_STRT_DT_SK"),
        rpad(F.lit("1753-01-01"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        rpad(F.col("EDWRunCycleDate"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        rpad(F.lit("1753-01-01"), 10, " ").alias("INDV_BE_BRTH_DT_SK"),
        rpad(F.lit("N"), 1, " ").alias("INDV_BE_CUR_RCRD_IN"),
        F.lit("NA").alias("INDV_BE_COV_STTUS_CD"),
        F.lit("NA").alias("INDV_BE_COV_STTUS_NM"),
        rpad(F.lit("1753-01-01"), 10, " ").alias("INDV_BE_DCSD_DT_SK"),
        F.lit("NA").alias("INDV_BE_GNDR_CD"),
        F.lit("NA").alias("INDV_BE_GNDR_NM"),
        F.lit("NA").alias("INDV_BE_FIRST_NM"),
        F.lit("NA").alias("INDV_BE_MIDINIT"),
        F.lit("NA").alias("INDV_BE_LAST_NM"),
        rpad(F.lit("1753-01-01"), 10, " ").alias("INDV_BE_ORIG_EFF_DT_SK"),
        rpad(F.lit("1753-01-01"), 10, " ").alias("INDV_BE_PRCS_END_DT_SK"),
        F.lit("NA").alias("INDV_BE_SSN"),
        rpad(F.lit("N"), 1, " ").alias("MBR_DIM_IN"),
        F.lit("NA").alias("SRC_SYS_CD"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("EDWRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_UNK_Row = (
    df_xfrm
    .filter(
        ((F.col("@INROWNUM") - F.lit(1)) * F.col("@NUMPARTITIONS") + F.col("@PARTITIONNUM") + F.lit(1)) == 1
    )
    .select(
        F.lit(0).alias("INDV_BE_KEY"),
        rpad(F.lit("1753-01-01"), 10, " ").alias("INDV_BE_PRCS_STRT_DT_SK"),
        rpad(F.lit("1753-01-01"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        rpad(F.col("EDWRunCycleDate"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        rpad(F.lit("1753-01-01"), 10, " ").alias("INDV_BE_BRTH_DT_SK"),
        rpad(F.lit("N"), 1, " ").alias("INDV_BE_CUR_RCRD_IN"),
        F.lit("UNK").alias("INDV_BE_COV_STTUS_CD"),
        F.lit("UNK").alias("INDV_BE_COV_STTUS_NM"),
        rpad(F.lit("1753-01-01"), 10, " ").alias("INDV_BE_DCSD_DT_SK"),
        F.lit("UNK").alias("INDV_BE_GNDR_CD"),
        F.lit("UNK").alias("INDV_BE_GNDR_NM"),
        F.lit("UNK").alias("INDV_BE_FIRST_NM"),
        F.lit("UNK").alias("INDV_BE_MIDINIT"),
        F.lit("UNK").alias("INDV_BE_LAST_NM"),
        rpad(F.lit("1753-01-01"), 10, " ").alias("INDV_BE_ORIG_EFF_DT_SK"),
        rpad(F.lit("1753-01-01"), 10, " ").alias("INDV_BE_PRCS_END_DT_SK"),
        F.lit("UNK").alias("INDV_BE_SSN"),
        rpad(F.lit("N"), 1, " ").alias("MBR_DIM_IN"),
        F.lit("UNK").alias("SRC_SYS_CD"),
        F.lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("EDWRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_Fnl_Update_Insert = df_Lnk_Hist_Rec.union(df_Lnk_Curr_rec).union(df_NA_Row).union(df_UNK_Row)

df_Seq_INDV_BE_D = df_Fnl_Update_Insert.select(
    F.col("INDV_BE_KEY"),
    F.col("INDV_BE_PRCS_STRT_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("INDV_BE_BRTH_DT_SK"),
    F.col("INDV_BE_CUR_RCRD_IN"),
    F.col("INDV_BE_COV_STTUS_CD"),
    F.col("INDV_BE_COV_STTUS_NM"),
    F.col("INDV_BE_DCSD_DT_SK"),
    F.col("INDV_BE_GNDR_CD"),
    F.col("INDV_BE_GNDR_NM"),
    F.col("INDV_BE_FIRST_NM"),
    F.col("INDV_BE_MIDINIT"),
    F.col("INDV_BE_LAST_NM"),
    F.col("INDV_BE_ORIG_EFF_DT_SK"),
    F.col("INDV_BE_PRCS_END_DT_SK"),
    F.col("INDV_BE_SSN"),
    F.col("MBR_DIM_IN"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_Seq_INDV_BE_D,
    f"{adls_path}/load/INDV_BE_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)