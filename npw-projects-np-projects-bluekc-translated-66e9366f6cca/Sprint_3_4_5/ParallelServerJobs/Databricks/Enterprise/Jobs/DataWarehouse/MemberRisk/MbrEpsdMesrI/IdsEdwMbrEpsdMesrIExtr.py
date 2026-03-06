# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extracting data from MBR_EPSD_MESR
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                1/03/2008         CLINICALS/3044            Originally Programmed                        devlEDW10                   Steph Goddard            01/07/2008
# MAGIC 
# MAGIC Steph Goddard              04/04/2009                                               populated hash file hf_mbr_risk_uniq_key  testEDWnew
# MAGIC 
# MAGIC 
# MAGIC Pooja Sunkara               10/28/2013        5114                                Rewrite in Parallel                              EnterpriseWrhsDevl       Jag Yelavarthi            2013-12-12

# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK and LAST_UPDT_RUN_CYC_EXCTN_DT_SK
# MAGIC Job name:
# MAGIC IdsEdwMbrEpsdMesrIExtr
# MAGIC EDW Mbr Epsd Mesr I extract from IDS
# MAGIC Write MBR_EPSD_MESR_I Data into a Sequential file for Load Ready Job.
# MAGIC Read data from source table MBR_EPSD_MESR
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"SELECT MBR_UNIQ_KEY, INDV_BE_KEY FROM {IDSOwner}.MBR"
df_db2_MBR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = (
    f"SELECT MBR_EPSD_MESR_SK, SRC_SYS_CD_SK, MBR_UNIQ_KEY, EPSD_TREAT_GRP_CD, "
    f"PRCS_YR_MO_SK, CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, EPSD_TREAT_GRP_SK, "
    f"MBR_SK, MBR_MED_MESRS_SK, ER_DT_SK, IP_DT_SK "
    f"FROM {IDSOwner}.MBR_EPSD_MESR WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}"
)
df_db2_MBR_EPSD_MESR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = (
    f"SELECT EPSD_TREAT_GRP_SK, MAJ_PRCTC_CAT_CD_SK, EPSD_TREAT_GRP_DESC "
    f"FROM {IDSOwner}.EPSD_TREAT_GRP"
)
df_db2_EPSD_TREAT_GRP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Lkp_Cd = (
    df_db2_MBR_EPSD_MESR_in.alias("Ink_IdsEdwMbrEpsdMesrIExtr_inABC")
    .join(
        df_db2_EPSD_TREAT_GRP_in.alias("ref_GrpCd"),
        F.col("Ink_IdsEdwMbrEpsdMesrIExtr_inABC.EPSD_TREAT_GRP_SK")
        == F.col("ref_GrpCd.EPSD_TREAT_GRP_SK"),
        how="left",
    )
    .select(
        F.col("Ink_IdsEdwMbrEpsdMesrIExtr_inABC.MBR_EPSD_MESR_SK").alias("MBR_EPSD_MESR_SK"),
        F.col("Ink_IdsEdwMbrEpsdMesrIExtr_inABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Ink_IdsEdwMbrEpsdMesrIExtr_inABC.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Ink_IdsEdwMbrEpsdMesrIExtr_inABC.EPSD_TREAT_GRP_CD").alias("EPSD_TREAT_GRP_CD"),
        F.col("Ink_IdsEdwMbrEpsdMesrIExtr_inABC.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        F.col("Ink_IdsEdwMbrEpsdMesrIExtr_inABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Ink_IdsEdwMbrEpsdMesrIExtr_inABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias(
            "LAST_UPDT_RUN_CYC_EXCTN_SK"
        ),
        F.col("Ink_IdsEdwMbrEpsdMesrIExtr_inABC.EPSD_TREAT_GRP_SK").alias("EPSD_TREAT_GRP_SK"),
        F.col("Ink_IdsEdwMbrEpsdMesrIExtr_inABC.MBR_SK").alias("MBR_SK"),
        F.col("Ink_IdsEdwMbrEpsdMesrIExtr_inABC.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
        F.col("Ink_IdsEdwMbrEpsdMesrIExtr_inABC.ER_DT_SK").alias("ER_DT_SK"),
        F.col("Ink_IdsEdwMbrEpsdMesrIExtr_inABC.IP_DT_SK").alias("IP_DT_SK"),
        F.col("ref_GrpCd.MAJ_PRCTC_CAT_CD_SK").alias("MAJ_PRCTC_CAT_CD_SK"),
        F.col("ref_GrpCd.EPSD_TREAT_GRP_DESC").alias("EPSD_TREAT_GRP_DESC"),
    )
)

extract_query = f"SELECT CD_MPPNG_SK, TRGT_CD, TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Cpy_CdMppng_ref_SrcSysCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
)

df_Cpy_CdMppng_ref_MajPrctcCatCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
)

df_Lkp_Codes = (
    df_Lkp_Cd.alias("lnk_IdsEdwMbrEpsdMesrIExtr_lkpIn")
    .join(
        df_db2_MBR_in.alias("ref_UniqKey"),
        F.col("lnk_IdsEdwMbrEpsdMesrIExtr_lkpIn.MBR_UNIQ_KEY")
        == F.col("ref_UniqKey.MBR_UNIQ_KEY"),
        how="left",
    )
    .join(
        df_Cpy_CdMppng_ref_SrcSysCd.alias("ref_SrcSysCd"),
        F.col("lnk_IdsEdwMbrEpsdMesrIExtr_lkpIn.SRC_SYS_CD_SK")
        == F.col("ref_SrcSysCd.CD_MPPNG_SK"),
        how="left",
    )
    .join(
        df_Cpy_CdMppng_ref_MajPrctcCatCd.alias("ref_MajPrctcCatCd"),
        F.col("lnk_IdsEdwMbrEpsdMesrIExtr_lkpIn.MAJ_PRCTC_CAT_CD_SK")
        == F.col("ref_MajPrctcCatCd.CD_MPPNG_SK"),
        how="left",
    )
    .select(
        F.col("lnk_IdsEdwMbrEpsdMesrIExtr_lkpIn.MBR_EPSD_MESR_SK").alias("MBR_EPSD_MESR_SK"),
        F.col("ref_SrcSysCd.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwMbrEpsdMesrIExtr_lkpIn.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("lnk_IdsEdwMbrEpsdMesrIExtr_lkpIn.EPSD_TREAT_GRP_CD").alias("EPSD_TREAT_GRP_CD"),
        F.col("lnk_IdsEdwMbrEpsdMesrIExtr_lkpIn.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        F.col("lnk_IdsEdwMbrEpsdMesrIExtr_lkpIn.EPSD_TREAT_GRP_SK").alias("EPSD_TREAT_GRP_SK"),
        F.col("lnk_IdsEdwMbrEpsdMesrIExtr_lkpIn.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
        F.col("lnk_IdsEdwMbrEpsdMesrIExtr_lkpIn.ER_DT_SK").alias("ER_DT_SK"),
        F.col("lnk_IdsEdwMbrEpsdMesrIExtr_lkpIn.IP_DT_SK").alias("IP_DT_SK"),
        F.col("ref_UniqKey.INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("lnk_IdsEdwMbrEpsdMesrIExtr_lkpIn.MBR_SK").alias("MBR_SK"),
        F.col("lnk_IdsEdwMbrEpsdMesrIExtr_lkpIn.EPSD_TREAT_GRP_DESC").alias("EPSD_TREAT_GRP_DESC"),
        F.col("ref_MajPrctcCatCd.TRGT_CD").alias("MAJ_PRCTC_CAT_CD"),
        F.col("ref_MajPrctcCatCd.TRGT_CD_NM").alias("MAJ_PRCTC_CAT_DESC"),
    )
)

windowSpec = Window.orderBy(F.lit(1))

df_xfrm_BusinessLogic_main = df_Lkp_Codes.filter(
    (F.col("MBR_EPSD_MESR_SK") != 0) & (F.col("MBR_EPSD_MESR_SK") != 1)
).select(
    F.col("MBR_EPSD_MESR_SK").alias("MBR_EPSD_MESR_SK"),
    F.when(
        F.col("SRC_SYS_CD").isNull() | (F.length(trim(F.col("SRC_SYS_CD"))) == 0), F.lit("NA")
    ).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("EPSD_TREAT_GRP_CD").alias("EPSD_TREAT_GRP_CD"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("EPSD_TREAT_GRP_SK").alias("EPSD_TREAT_GRP_SK"),
    F.col("MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
    F.col("ER_DT_SK").alias("ER_DT_SK"),
    F.col("IP_DT_SK").alias("IP_DT_SK"),
    F.when(
        F.col("INDV_BE_KEY").isNull() | (F.length(trim(F.col("INDV_BE_KEY"))) == 0),
        F.lit(0),
    ).otherwise(F.col("INDV_BE_KEY")).alias("INDV_BE_KEY"),
    F.when(
        F.col("EPSD_TREAT_GRP_DESC").isNull() | (F.length(F.col("EPSD_TREAT_GRP_DESC")) == 0),
        F.lit("NA"),
    ).otherwise(F.col("EPSD_TREAT_GRP_DESC")).alias("EPSD_TREAT_GRP_DESC"),
    F.when(
        F.col("MAJ_PRCTC_CAT_CD").isNull() | (F.length(trim(F.col("MAJ_PRCTC_CAT_CD"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("MAJ_PRCTC_CAT_CD")).alias("MAJ_PRCTC_CAT_CD"),
    F.when(
        F.col("MAJ_PRCTC_CAT_DESC").isNull() | (F.length(trim(F.col("MAJ_PRCTC_CAT_DESC"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("MAJ_PRCTC_CAT_DESC")).alias("MAJ_PRCTC_CAT_DESC"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
)

df_with_rn = df_Lkp_Codes.withColumn("rn", F.row_number().over(windowSpec))

df_xfrm_BusinessLogic_NA = df_with_rn.filter(F.col("rn") == 1).select(
    F.lit(1).alias("MBR_EPSD_MESR_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit(1).alias("MBR_UNIQ_KEY"),
    F.lit("NA").alias("EPSD_TREAT_GRP_CD"),
    F.lit("175301").alias("PRCS_YR_MO_SK"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("EPSD_TREAT_GRP_SK"),
    F.lit(1).alias("MBR_MED_MESRS_SK"),
    F.lit("1753-01-01").alias("ER_DT_SK"),
    F.lit("1753-01-01").alias("IP_DT_SK"),
    F.lit(1).alias("INDV_BE_KEY"),
    F.lit("").alias("EPSD_TREAT_GRP_DESC"),
    F.lit("NA").alias("MAJ_PRCTC_CAT_CD"),
    F.lit("").alias("MAJ_PRCTC_CAT_DESC"),
    F.lit(1).alias("MBR_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
)

df_xfrm_BusinessLogic_UNK = df_with_rn.filter(F.col("rn") == 1).select(
    F.lit(0).alias("MBR_EPSD_MESR_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit(0).alias("MBR_UNIQ_KEY"),
    F.lit("UNK").alias("EPSD_TREAT_GRP_CD"),
    F.lit("175301").alias("PRCS_YR_MO_SK"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("EPSD_TREAT_GRP_SK"),
    F.lit(0).alias("MBR_MED_MESRS_SK"),
    F.lit("1753-01-01").alias("ER_DT_SK"),
    F.lit("1753-01-01").alias("IP_DT_SK"),
    F.lit(0).alias("INDV_BE_KEY"),
    F.lit("").alias("EPSD_TREAT_GRP_DESC"),
    F.lit("UNK").alias("MAJ_PRCTC_CAT_CD"),
    F.lit("").alias("MAJ_PRCTC_CAT_DESC"),
    F.lit(0).alias("MBR_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
)

df_Fnl_Data = (
    df_xfrm_BusinessLogic_main.unionByName(df_xfrm_BusinessLogic_NA)
    .unionByName(df_xfrm_BusinessLogic_UNK)
    .select(
        F.col("MBR_EPSD_MESR_SK"),
        F.col("SRC_SYS_CD"),
        F.col("MBR_UNIQ_KEY"),
        F.col("EPSD_TREAT_GRP_CD"),
        F.rpad(F.col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
        F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("EPSD_TREAT_GRP_SK"),
        F.col("MBR_MED_MESRS_SK"),
        F.rpad(F.col("ER_DT_SK"), 10, " ").alias("ER_DT_SK"),
        F.rpad(F.col("IP_DT_SK"), 10, " ").alias("IP_DT_SK"),
        F.col("INDV_BE_KEY"),
        F.col("EPSD_TREAT_GRP_DESC"),
        F.col("MAJ_PRCTC_CAT_CD"),
        F.col("MAJ_PRCTC_CAT_DESC"),
        F.col("MBR_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    )
)

write_files(
    df_Fnl_Data,
    f"{adls_path}/load/MBR_EPSD_MESR_I.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)