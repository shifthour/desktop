# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from (idsdevl.ClmLnDiag, idsdevl.DIAG_CD) and
# MAGIC                            edwdevl.ClmLnDiag_d
# MAGIC   Loads the data from EDW and IDS into two hash files for subsequent comparison.
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC       
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source and dump to hf. Do nothing but trim the fields.
# MAGIC                   /dev/null - no records, just used to re-init the IUD hf
# MAGIC   
# MAGIC MODIFICATIONS:
# MAGIC               Judy Reynolds 11/15/2004-   Originally Programmed
# MAGIC               Brent leland     03/24/2006    Changed parameters to evironment variables.
# MAGIC                                                              Took out trim() on SK values.
# MAGIC               Brent Leland   07/21/2006     Changed SQL lookup to a hash lookup for DIAG_CD
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer          Date                 Project/Altiris #         Change Description                                                                                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------        --------------------     ------------------------        -----------------------------------------------------------------------------------------------------------------               --------------------------------       -------------------------------   ----------------------------       
# MAGIC SAndrew           2008-12-22         #3494 DRG           Add business rules for 3 new fieldss  CLM_DIAG_POA_CD_SK, 
# MAGIC                                                                                      CLM_DIAG_POA_CD, and CLM_DIAG_POA_NM  on CLM_DIAG_I                             devlIDS                 Brent Leland              12-29-2008
# MAGIC                                                                                      Added reference lookup refPOA
# MAGIC 
# MAGIC Rick Henry        2012-05-08         4896                      Added DIAG_CD_TYP_CD, DIAG_CD_TYP_NM, DIAG_CD_TYPE_CD_SK          EnterpriseNewDevl        Sanderw                    2012-05-18
# MAGIC  
# MAGIC Lee  Moore        09/11/2013       p5114                      rewrite in parallel                                                                                                      EnterpriseWrhsDevl      Peter Marshall            12/19/2013

# MAGIC Job Name:   IdsEdwClmDiagIExtr
# MAGIC Read from source table 
# MAGIC CLM_DIAG_I.
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write CLM_DIAG_I Data into a Sequential file for Load Job.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling
# MAGIC Code SK lookups for Denormalization
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql.functions import col, when, lit, coalesce, row_number, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_CLM_DIAG_in = (
    "SELECT dg.CLM_DIAG_SK,\n"
    "dg.CLM_ID,\n"
    "COALESCE(CD.TRGT_CD, 'UNK') SRC_SYS_CD,\n"
    "dg.CLM_DIAG_ORDNL_CD_SK,\n"
    "dg.CLM_SK,\n"
    "dg.DIAG_CD_SK,\n"
    "dg.CLM_DIAG_POA_CD_SK,\n"
    "dg.CRT_RUN_CYC_EXCTN_SK\n"
    "FROM " + IDSOwner + ".CLM_DIAG dg\n"
    "INNER JOIN " + IDSOwner + ".W_EDW_ETL_DRVR dr\n"
    "ON dg.CLM_ID = dr.CLM_ID AND dg.SRC_SYS_CD_SK = dr.SRC_SYS_CD_SK\n"
    "LEFT JOIN " + IDSOwner + ".CD_MPPNG CD\n"
    "ON CD.CD_MPPNG_SK = dg.SRC_SYS_CD_SK"
)
df_db2_CLM_DIAG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_DIAG_in)
    .load()
)

extract_query_db2_DIAG_CD_in = (
    "SELECT DIAG_CD_SK,\n"
    "DIAG_CD,\n"
    "DIAG_CD_TYP_CD,\n"
    "DIAG_CD_TYP_CD_SK,\n"
    "DIAG_CD_DESC,\n"
    "TRGT_CD_NM\n"
    "FROM " + IDSOwner + ".DIAG_CD A,\n"
    + IDSOwner + ".CD_MPPNG B\n"
    "WHERE A.DIAG_CD_TYP_CD_SK = B.CD_MPPNG_SK"
)
df_db2_DIAG_CD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_DIAG_CD_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = (
    "SELECT CD_MPPNG_SK,\n"
    "COALESCE(TRGT_CD,'UNK') TRGT_CD,\n"
    "TRGT_CD_NM\n"
    "FROM " + IDSOwner + ".CD_MPPNG"
)
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_cpy_cd_mppng = df_db2_CD_MPPNG_Extr
df_lnl_refclmdiagiordnl = df_cpy_cd_mppng.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_lnk_refPOA = df_cpy_cd_mppng.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lnk_IdsEdwClmDiagIExtr_InABC = df_db2_CLM_DIAG_in
df_lnk_DIAG_CD = df_db2_DIAG_CD_in

df_lkp_temp1 = df_lnk_IdsEdwClmDiagIExtr_InABC.alias("lnk_IdsEdwClmDiagIExtr_InABC").join(
    df_lnk_refPOA.alias("lnk_refPOA"),
    [col("lnk_IdsEdwClmDiagIExtr_InABC.CLM_DIAG_POA_CD_SK") == col("lnk_refPOA.CD_MPPNG_SK")],
    "left"
)
df_lkp_temp2 = df_lkp_temp1.alias("lkp_temp1").join(
    df_lnl_refclmdiagiordnl.alias("lnl_refclmdiagiordnl"),
    [col("lnk_IdsEdwClmDiagIExtr_InABC.CLM_DIAG_ORDNL_CD_SK") == col("lnl_refclmdiagiordnl.CD_MPPNG_SK")],
    "left"
)
df_lkp_temp3 = df_lkp_temp2.alias("lkp_temp2").join(
    df_lnk_DIAG_CD.alias("lnk_DIAG_CD"),
    [col("lnk_IdsEdwClmDiagIExtr_InABC.DIAG_CD_SK") == col("lnk_DIAG_CD.DIAG_CD_SK")],
    "left"
)

df_lkp_Codes = df_lkp_temp3.select(
    col("lnk_IdsEdwClmDiagIExtr_InABC.CLM_DIAG_SK").alias("CLM_DIAG_SK"),
    col("lnk_IdsEdwClmDiagIExtr_InABC.CLM_ID").alias("CLM_ID"),
    col("lnk_IdsEdwClmDiagIExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_IdsEdwClmDiagIExtr_InABC.CLM_DIAG_ORDNL_CD_SK").alias("CLM_DIAG_ORDNL_CD_SK"),
    col("lnk_IdsEdwClmDiagIExtr_InABC.CLM_SK").alias("CLM_SK"),
    col("lnk_IdsEdwClmDiagIExtr_InABC.DIAG_CD_SK").alias("DIAG_CD_SK"),
    col("lnk_IdsEdwClmDiagIExtr_InABC.CLM_DIAG_POA_CD_SK").alias("CLM_DIAG_POA_CD_SK"),
    col("lnk_IdsEdwClmDiagIExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnl_refclmdiagiordnl.TRGT_CD").alias("ORDNL_CD"),
    col("lnl_refclmdiagiordnl.TRGT_CD_NM").alias("ORDNL_CD_NM"),
    col("lnk_refPOA.TRGT_CD").alias("POA_CD"),
    col("lnk_refPOA.TRGT_CD_NM").alias("POA_CD_NM"),
    col("lnk_DIAG_CD.DIAG_CD").alias("DIAG_CD"),
    col("lnk_DIAG_CD.DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD"),
    col("lnk_DIAG_CD.DIAG_CD_TYP_CD_SK").alias("DIAG_CD_TYP_CD_SK"),
    col("lnk_DIAG_CD.DIAG_CD_DESC").alias("DIAG_CD_DESC"),
    col("lnk_DIAG_CD.TRGT_CD_NM").alias("DIAG_CD_TYP_NM")
)

df_xfm_BusinessLogic_input = df_lkp_Codes

df_xfm_BusinessLogic_detail = (
    df_xfm_BusinessLogic_input
    .withColumn("SRC_SYS_CD", when(col("SRC_SYS_CD").isNull(), lit("UNK")).otherwise(col("SRC_SYS_CD")))
    .withColumn("CLM_DIAG_ORDNL_CD", when(col("ORDNL_CD").isNull(), lit("UNK")).otherwise(col("ORDNL_CD")))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("CLM_DIAG_ORDNL_NM", when(col("ORDNL_CD_NM").isNull(), lit("UNKNOWN")).otherwise(col("ORDNL_CD_NM")))
    .withColumn("DIAG_CD", when(col("DIAG_CD").isNull(), lit("UNK")).otherwise(col("DIAG_CD")))
    .withColumn("DIAG_CD_DESC", when(col("DIAG_CD_DESC").isNull(), lit("UNKNOWN")).otherwise(col("DIAG_CD_DESC")))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    .withColumn("CLM_DIAG_ORDNL_CD_SK", col("CLM_DIAG_ORDNL_CD_SK"))
    .withColumn("CLM_SK", col("CLM_SK"))
    .withColumn("DIAG_CD_D_SK", col("DIAG_CD_SK"))
    .withColumn("CLM_DIAG_POA_CD_SK", col("CLM_DIAG_POA_CD_SK"))
    .withColumn("CLM_DIAG_POA_CD", when(col("POA_CD").isNull(), lit("NA")).otherwise(trim(col("POA_CD"))))
    .withColumn("CLM_DIAG_POA_NM", when(col("POA_CD_NM").isNull(), lit("NA")).otherwise(trim(col("POA_CD_NM"))))
    .withColumn("DIAG_CD_TYP_CD", when(col("DIAG_CD_TYP_CD").isNull(), lit("UNK")).otherwise(col("DIAG_CD_TYP_CD")))
    .withColumn("DIAG_CD_TYP_NM", when(col("DIAG_CD_TYP_NM").isNull(), lit("UNKNOWN")).otherwise(col("DIAG_CD_TYP_NM")))
    .withColumn("DIAG_CD_TYP_CD_SK", when(col("DIAG_CD_TYP_CD_SK").isNull(), lit(0)).otherwise(col("DIAG_CD_TYP_CD_SK")))
    .select(
        col("CLM_DIAG_SK"),
        col("SRC_SYS_CD"),
        col("CLM_ID"),
        col("CLM_DIAG_ORDNL_CD"),
        col("CRT_RUN_CYC_EXCTN_DT_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("CLM_DIAG_ORDNL_NM"),
        col("DIAG_CD"),
        col("DIAG_CD_DESC"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CLM_DIAG_ORDNL_CD_SK"),
        col("CLM_SK"),
        col("DIAG_CD_D_SK"),
        col("CLM_DIAG_POA_CD_SK"),
        col("CLM_DIAG_POA_CD"),
        col("CLM_DIAG_POA_NM"),
        col("DIAG_CD_TYP_CD"),
        col("DIAG_CD_TYP_NM"),
        col("DIAG_CD_TYP_CD_SK")
    )
)

df_temp_for_na_unk = df_xfm_BusinessLogic_input.withColumn("row_num", row_number().over(Window.orderBy(col("CLM_DIAG_SK")))).filter("row_num = 1")

df_xfm_BusinessLogic_na = df_temp_for_na_unk.select(
    lit(1).alias("CLM_DIAG_SK"),
    lit("NA").alias("SRC_SYS_CD"),
    lit("NA").alias("CLM_ID"),
    lit("NA").alias("CLM_DIAG_ORDNL_CD"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit("NA").alias("CLM_DIAG_ORDNL_NM"),
    lit("NA").alias("DIAG_CD"),
    lit("").alias("DIAG_CD_DESC"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("CLM_DIAG_ORDNL_CD_SK"),
    lit(1).alias("CLM_SK"),
    lit(1).alias("DIAG_CD_D_SK"),
    lit(1).alias("CLM_DIAG_POA_CD_SK"),
    lit("NA").alias("CLM_DIAG_POA_CD"),
    lit("NA").alias("CLM_DIAG_POA_NM"),
    lit("NA").alias("DIAG_CD_TYP_CD"),
    lit("NA").alias("DIAG_CD_TYP_NM"),
    lit(1).alias("DIAG_CD_TYP_CD_SK")
)

df_xfm_BusinessLogic_unk = df_temp_for_na_unk.select(
    lit(0).alias("CLM_DIAG_SK"),
    lit("UNK").alias("SRC_SYS_CD"),
    lit("UNK").alias("CLM_ID"),
    lit("UNK").alias("CLM_DIAG_ORDNL_CD"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit("UNK").alias("CLM_DIAG_ORDNL_NM"),
    lit("UNK").alias("DIAG_CD"),
    lit("").alias("DIAG_CD_DESC"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLM_DIAG_ORDNL_CD_SK"),
    lit(0).alias("CLM_SK"),
    lit(0).alias("DIAG_CD_D_SK"),
    lit(0).alias("CLM_DIAG_POA_CD_SK"),
    lit("UNK").alias("CLM_DIAG_POA_CD"),
    lit("UNK").alias("CLM_DIAG_POA_NM"),
    lit("UNK").alias("DIAG_CD_TYP_CD"),
    lit("UNK").alias("DIAG_CD_TYP_NM"),
    lit(0).alias("DIAG_CD_TYP_CD_SK")
)

df_Fnl_Clm_DiagI = (
    df_xfm_BusinessLogic_detail
    .unionByName(df_xfm_BusinessLogic_na)
    .unionByName(df_xfm_BusinessLogic_unk)
)

df_Fnl_Clm_DiagI_final = df_Fnl_Clm_DiagI.select(
    col("CLM_DIAG_SK"),
    col("SRC_SYS_CD"),
    col("CLM_ID"),
    col("CLM_DIAG_ORDNL_CD"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CLM_DIAG_ORDNL_NM"),
    col("DIAG_CD"),
    col("DIAG_CD_DESC"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_DIAG_ORDNL_CD_SK"),
    col("CLM_SK"),
    col("DIAG_CD_D_SK"),
    col("CLM_DIAG_POA_CD_SK"),
    col("CLM_DIAG_POA_CD"),
    col("CLM_DIAG_POA_NM"),
    col("DIAG_CD_TYP_CD"),
    col("DIAG_CD_TYP_NM"),
    col("DIAG_CD_TYP_CD_SK")
)
write_files(
    df_Fnl_Clm_DiagI_final,
    f"{adls_path}/load/CLM_DIAG_I.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)

df_xfm_BusinessLogic_BClmF2 = df_xfm_BusinessLogic_input.select(
    when(col("SRC_SYS_CD").isNull(), lit("UNK")).otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID")
)

df_xfm_BusinessLogic_ClmDiagSk = df_xfm_BusinessLogic_input.select(
    when(col("SRC_SYS_CD").isNull(), lit("UNK")).otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("ORDNL_CD").alias("CLM_DIAG_ORDNL_CD"),
    col("CLM_DIAG_SK").alias("CLM_DIAG_SK")
)

df_Rmd_Sk = dedup_sort(
    df_xfm_BusinessLogic_ClmDiagSk,
    ["SRC_SYS_CD","CLM_ID","CLM_DIAG_ORDNL_CD"],
    [("SRC_SYS_CD","A"),("CLM_ID","A"),("CLM_DIAG_ORDNL_CD","A")]
)
df_cpy_DiagSk = df_Rmd_Sk.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD"),
    col("CLM_DIAG_SK").alias("CLM_DIAG_SK")
)
write_files(
    df_cpy_DiagSk,
    f"{adls_path}/load/CLM_DIAG_SK.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)

df_Rmd_B_Clm_F2 = dedup_sort(
    df_xfm_BusinessLogic_BClmF2,
    ["SRC_SYS_CD","CLM_ID"],
    [("SRC_SYS_CD","A"),("CLM_ID","A")]
)
df_cpy_BClmF2 = df_Rmd_B_Clm_F2.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID")
)
write_files(
    df_cpy_BClmF2,
    f"{adls_path}/load/B_CLM_F2.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)