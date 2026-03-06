# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                 Code                  Date
# MAGIC Developer        Date              Altiris #           Change Description                                                        Reviewer            Reviewed
# MAGIC ----------------------  -------------------   -------------------   ------------------------------------------------------------------------------------   ----------------------     -------------------   
# MAGIC Hugh Sisson    6/26/2007                           Original program                                                              Steph Goddard   8/30/07
# MAGIC 
# MAGIC Rama Kamjula   11/06/2013     5114           Rewritten to parallel from Server version                        Peter Marshall     12/11/2013

# MAGIC JobName: IdsEdwMbrMrkrMesrIExtr
# MAGIC 
# MAGIC Job creates loadfile for MBR_Mrkr_Mesr_I  in EDW
# MAGIC Extracts MRKR  from IDS table
# MAGIC Load file for  MBR_MRKR_MESR_I - EDW table
# MAGIC Null Handling and Business Logic
# MAGIC Extracts MBR_MRKR_MESR data from IDS table
# MAGIC Extracts MBR data from IDS table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, trim, lit, length, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

jdbc_url_db2_MBR, jdbc_props_db2_MBR = get_db_config(ids_secret_name)
query_db2_MBR = "SELECT \n\nMBR_UNIQ_KEY,\nINDV_BE_KEY \n\nFROM " + IDSOwner + ".MBR"
df_db2_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_MBR)
    .options(**jdbc_props_db2_MBR)
    .option("query", query_db2_MBR)
    .load()
)

jdbc_url_db2_CD_MPPNG, jdbc_props_db2_CD_MPPNG = get_db_config(ids_secret_name)
query_db2_CD_MPPNG = (
    "SELECT \n\nCD_MPPNG_SK,\nCOALESCE(TRGT_CD,'UNK') TRGT_CD,\nTRGT_CD_NM\n\nFROM  "
    + IDSOwner
    + ".CD_MPPNG"
)
df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG)
    .options(**jdbc_props_db2_CD_MPPNG)
    .option("query", query_db2_CD_MPPNG)
    .load()
)

df_cpy_CD_MPPNG_1 = df_db2_CD_MPPNG.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)
df_cpy_CD_MPPNG_2 = df_db2_CD_MPPNG.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

jdbc_url_db2_MRKR_CAT, jdbc_props_db2_MRKR_CAT = get_db_config(ids_secret_name)
query_db2_MRKR_CAT = (
    "SELECT \n\nMRKR_CAT_SK,\nMAJ_PRCTC_CAT_CD_SK,\nMRKR_CAT_CD,\nMRKR_CAT_DESC\n\nFROM "
    + IDSOwner
    + ".MRKR_CAT ;"
)
df_db2_MRKR_CAT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_MRKR_CAT)
    .options(**jdbc_props_db2_MRKR_CAT)
    .option("query", query_db2_MRKR_CAT)
    .load()
)

jdbc_url_db2_MBR_MRKR_MESR, jdbc_props_db2_MBR_MRKR_MESR = get_db_config(ids_secret_name)
query_db2_MBR_MRKR_MESR = (
    "SELECT \n\nMESR.MBR_MRKR_MESR_SK,\nCOALESCE(CD.TRGT_CD, 'UNK')  SRC_SYS_CD,\nMESR.MBR_UNIQ_KEY,\nMESR.MRKR_ID,\nMESR.PRCS_YR_MO_SK,\nMESR.CRT_RUN_CYC_EXCTN_SK,\nMESR.LAST_UPDT_RUN_CYC_EXCTN_SK,\nMESR.MRKR_SK,\nMESR.MRKR_CAT_SK,\nMESR.MBR_SK,\nMESR.MBR_MED_MESRS_SK\n\nFROM "
    + IDSOwner
    + ".MBR_MRKR_MESR  MESR  LEFT JOIN  "
    + IDSOwner
    + ".CD_MPPNG CD\n\nON  MESR.SRC_SYS_CD_SK  =  CD.CD_MPPNG_SK\n\nWHERE MESR.LAST_UPDT_RUN_CYC_EXCTN_SK >= "
    + ExtractRunCycle
    + ";"
)
df_db2_MBR_MRKR_MESR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_MBR_MRKR_MESR)
    .options(**jdbc_props_db2_MBR_MRKR_MESR)
    .option("query", query_db2_MBR_MRKR_MESR)
    .load()
)

jdbc_url_db2_MRKR, jdbc_props_db2_MRKR = get_db_config(ids_secret_name)
query_db2_MRKR = (
    "SELECT \n\nMRKR_SK,\nMRKR_TYP_CD_SK \n\nFROM " + IDSOwner + ".MRKR ;"
)
df_db2_MRKR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_MRKR)
    .options(**jdbc_props_db2_MRKR)
    .option("query", query_db2_MRKR)
    .load()
)

df_lkp_Codes = (
    df_db2_MBR_MRKR_MESR.alias("lnk_Mbr_Mrkr_Mesr")
    .join(
        df_db2_MRKR_CAT.alias("lnk_Mrkr_Cat"),
        col("lnk_Mbr_Mrkr_Mesr.MRKR_CAT_SK") == col("lnk_Mrkr_Cat.MRKR_CAT_SK"),
        "left"
    )
    .join(
        df_db2_MRKR.alias("lnk_Mrkr"),
        col("lnk_Mbr_Mrkr_Mesr.MRKR_SK") == col("lnk_Mrkr.MRKR_SK"),
        "left"
    )
    .select(
        col("lnk_Mbr_Mrkr_Mesr.MBR_MRKR_MESR_SK").alias("MBR_MRKR_MESR_SK"),
        col("lnk_Mbr_Mrkr_Mesr.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_Mbr_Mrkr_Mesr.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("lnk_Mbr_Mrkr_Mesr.MRKR_ID").alias("MRKR_ID"),
        col("lnk_Mbr_Mrkr_Mesr.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        col("lnk_Mbr_Mrkr_Mesr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnk_Mbr_Mrkr_Mesr.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("lnk_Mbr_Mrkr_Mesr.MRKR_SK").alias("MRKR_SK"),
        col("lnk_Mbr_Mrkr_Mesr.MRKR_CAT_SK").alias("MRKR_CAT_SK"),
        col("lnk_Mbr_Mrkr_Mesr.MBR_SK").alias("MBR_SK"),
        col("lnk_Mbr_Mrkr_Mesr.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
        col("lnk_Mrkr_Cat.MRKR_CAT_CD").alias("MRKR_CAT_CD"),
        col("lnk_Mrkr_Cat.MAJ_PRCTC_CAT_CD_SK").alias("MAJ_PRCTC_CAT_CD_SK"),
        col("lnk_Mrkr_Cat.MRKR_CAT_DESC").alias("MRKR_CAT_DESC"),
        col("lnk_Mrkr.MRKR_TYP_CD_SK").alias("MRKR_TYP_CD_SK")
    )
)

df_xfm_BusinessLogic = (
    df_lkp_Codes
    .withColumn(
        "MRKR_CAT_CD",
        when(
            col("MRKR_CAT_CD").isNull() | (trim(col("MRKR_CAT_CD")) == ""),
            lit("NA")
        ).otherwise(col("MRKR_CAT_CD"))
    )
    .withColumn(
        "MAJ_PRCTC_CAT_CD_SK",
        when(
            col("MAJ_PRCTC_CAT_CD_SK").isNull(),
            lit(0)
        ).otherwise(col("MAJ_PRCTC_CAT_CD_SK"))
    )
    .withColumn(
        "MRKR_CAT_DESC",
        when(
            col("MRKR_CAT_DESC").isNull() | (trim(col("MRKR_CAT_DESC")) == ""),
            lit("NA")
        ).otherwise(col("MRKR_CAT_DESC"))
    )
    .withColumn(
        "MRKR_TYP_CD_SK",
        when(
            col("MRKR_TYP_CD_SK").isNull(),
            lit(0)
        ).otherwise(col("MRKR_TYP_CD_SK"))
    )
)

df_lkp_Codes2 = (
    df_xfm_BusinessLogic.alias("lnk_MbrMrkrMesr_In")
    .join(
        df_db2_MBR.alias("lnk_Uniqkey"),
        col("lnk_MbrMrkrMesr_In.MBR_UNIQ_KEY") == col("lnk_Uniqkey.MBR_UNIQ_KEY"),
        "left"
    )
    .join(
        df_cpy_CD_MPPNG_1.alias("lnk_MrkrTypCd"),
        col("lnk_MbrMrkrMesr_In.MRKR_TYP_CD_SK") == col("lnk_MrkrTypCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_CD_MPPNG_2.alias("lnk_MajPrctcCatCdLkup"),
        col("lnk_MbrMrkrMesr_In.MAJ_PRCTC_CAT_CD_SK") == col("lnk_MajPrctcCatCdLkup.CD_MPPNG_SK"),
        "left"
    )
    .select(
        col("lnk_MbrMrkrMesr_In.MBR_MRKR_MESR_SK").alias("MBR_MRKR_MESR_SK"),
        col("lnk_MbrMrkrMesr_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_MbrMrkrMesr_In.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("lnk_MbrMrkrMesr_In.MRKR_ID").alias("MRKR_ID"),
        col("lnk_MbrMrkrMesr_In.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        col("lnk_MbrMrkrMesr_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnk_MbrMrkrMesr_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("lnk_MbrMrkrMesr_In.MRKR_SK").alias("MRKR_SK"),
        col("lnk_MbrMrkrMesr_In.MRKR_CAT_SK").alias("MRKR_CAT_SK"),
        col("lnk_MbrMrkrMesr_In.MBR_SK").alias("MBR_SK"),
        col("lnk_MbrMrkrMesr_In.MBR_MED_MESRS_SK").alias("MBR_MED_MESRS_SK"),
        col("lnk_MbrMrkrMesr_In.MRKR_CAT_CD").alias("MRKR_CAT_CD"),
        col("lnk_MbrMrkrMesr_In.MAJ_PRCTC_CAT_CD_SK").alias("MAJ_PRCTC_CAT_CD_SK"),
        col("lnk_MbrMrkrMesr_In.MRKR_CAT_DESC").alias("MRKR_CAT_DESC"),
        col("lnk_MbrMrkrMesr_In.MRKR_TYP_CD_SK").alias("MRKR_TYP_CD_SK"),
        col("lnk_Uniqkey.INDV_BE_KEY").alias("INDV_BE_KEY"),
        col("lnk_MrkrTypCd.TRGT_CD").alias("TRGT_CD_MrkrTypCd"),
        col("lnk_MrkrTypCd.TRGT_CD_NM").alias("TRGT_CD_NM_MrkrTypCd"),
        col("lnk_MajPrctcCatCdLkup.TRGT_CD").alias("TRGT_CD_MajPrctcCat"),
        col("lnk_MajPrctcCatCdLkup.TRGT_CD_NM").alias("TRGT_CD_NM_MajprctcCat")
    )
)

df_xfm_BusinessLogic2_main = (
    df_lkp_Codes2
    .select(
        col("MBR_MRKR_MESR_SK"),
        col("SRC_SYS_CD"),
        col("MBR_UNIQ_KEY"),
        col("MRKR_ID"),
        col("PRCS_YR_MO_SK"),
        lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("MRKR_SK"),
        col("MRKR_CAT_SK"),
        col("MBR_MED_MESRS_SK"),
        when(
            col("INDV_BE_KEY").isNull() | (length(col("INDV_BE_KEY")) == 0),
            lit(0)
        ).otherwise(col("INDV_BE_KEY")).alias("INDV_BE_KEY"),
        col("MRKR_CAT_CD"),
        col("MRKR_CAT_DESC"),
        when(
            col("TRGT_CD_MajPrctcCat").isNull() | (trim(col("TRGT_CD_MajPrctcCat")) == ""),
            lit("NA")
        ).otherwise(col("TRGT_CD_MajPrctcCat")).alias("MAJ_PRCTC_CAT_CD"),
        when(
            col("TRGT_CD_NM_MajprctcCat").isNull() | (trim(col("TRGT_CD_NM_MajprctcCat")) == ""),
            lit("NA")
        ).otherwise(col("TRGT_CD_NM_MajprctcCat")).alias("MAJ_PRCTC_CAT_DESC"),
        when(
            col("TRGT_CD_MrkrTypCd").isNull() | (trim(col("TRGT_CD_MrkrTypCd")) == ""),
            lit("NA")
        ).otherwise(col("TRGT_CD_MrkrTypCd")).alias("MRKR_TYP_CD"),
        when(
            col("TRGT_CD_NM_MrkrTypCd").isNull() | (trim(col("TRGT_CD_NM_MrkrTypCd")) == ""),
            lit("NA")
        ).otherwise(col("TRGT_CD_NM_MrkrTypCd")).alias("MRKR_TYP_DESC"),
        col("MBR_SK"),
        lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
    .filter(
        (col("MBR_MRKR_MESR_SK") != 0) & (col("MBR_MRKR_MESR_SK") != 1)
    )
)

df_lnk_NA = spark.createDataFrame(
    [
        (
            1, 'NA', 1, 'NA', '175301', '1753-01-01', '1753-01-01', 1, 1, 1, 1,
            'NA', '', 'NA', '', 'NA', '', 1, 100, 100
        )
    ],
    [
        "MBR_MRKR_MESR_SK","SRC_SYS_CD","MBR_UNIQ_KEY","MRKR_ID","PRCS_YR_MO_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK","MRKR_SK","MRKR_CAT_SK",
        "MBR_MED_MESRS_SK","INDV_BE_KEY","MRKR_CAT_CD","MRKR_CAT_DESC","MAJ_PRCTC_CAT_CD",
        "MAJ_PRCTC_CAT_DESC","MRKR_TYP_CD","MRKR_TYP_DESC","MBR_SK","CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    ]
)

df_lnk_UNK = spark.createDataFrame(
    [
        (
            0, 'UNK', 0, 'UNK', '175301', '1753-01-01', '1753-01-01', 0, 0, 0, 0,
            'UNK', '', 'UNK', '', 'UNK', '', 0, 100, 100
        )
    ],
    [
        "MBR_MRKR_MESR_SK","SRC_SYS_CD","MBR_UNIQ_KEY","MRKR_ID","PRCS_YR_MO_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK","MRKR_SK","MRKR_CAT_SK",
        "MBR_MED_MESRS_SK","INDV_BE_KEY","MRKR_CAT_CD","MRKR_CAT_DESC","MAJ_PRCTC_CAT_CD",
        "MAJ_PRCTC_CAT_DESC","MRKR_TYP_CD","MRKR_TYP_DESC","MBR_SK","CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    ]
)

df_funnel = df_xfm_BusinessLogic2_main.union(df_lnk_NA).union(df_lnk_UNK)

df_final = df_funnel.select(
    col("MBR_MRKR_MESR_SK"),
    col("SRC_SYS_CD"),
    col("MBR_UNIQ_KEY"),
    col("MRKR_ID"),
    rpad(col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("MRKR_SK"),
    col("MRKR_CAT_SK"),
    col("MBR_MED_MESRS_SK"),
    col("INDV_BE_KEY"),
    col("MRKR_CAT_CD"),
    col("MRKR_CAT_DESC"),
    col("MAJ_PRCTC_CAT_CD"),
    col("MAJ_PRCTC_CAT_DESC"),
    col("MRKR_TYP_CD"),
    col("MRKR_TYP_DESC"),
    col("MBR_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/MBR_MRKR_MESR_I.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)