# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  IDSReferenceSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   This job runs monthly.  This job pulls the state/county fips codes to be used as a reference lookup.  If the state, county, and county FIPs number (logical key) do not change, do not update 
# MAGIC \(9)the Create Date or the Create Execution SK by not including the record in the file to be loaded.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      \(9)Change Description                                                 Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      \(9)-----------------------------------------------------------------------         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Laurel Kindley          2007-02-26       Project 3279  \(9)\(9)Original Programming.                                             \(9)CDS Sunset                    Steph Goddard          02/28/2007
# MAGIC 
# MAGIC Pooja Sunkara         04/25/2014     5345                           Converted job from server to parallel version.             EnterpriseWrhsDevl

# MAGIC Pull the fips data that is already in EDW.
# MAGIC If the state/county fips data is in EDW, do not include it in the load file.
# MAGIC Job name:
# MAGIC IdsEdwFipsDExtr
# MAGIC Write FIPS_D Data into a Sequential file for Load Ready Job.
# MAGIC Pull all county and state fips data from IDS
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
IDSOwner = IDSOwner  # Preserve parameter
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
EDWOwner = EDWOwner  # Preserve parameter
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_db2_FIPS_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT FIPS.FIPS_SK,FIPS.ST_CD,FIPS.CNTY_FIPS_NO,FIPS.CRT_RUN_CYC_EXCTN_SK,FIPS.LAST_UPDT_RUN_CYC_EXCTN_SK,FIPS.ST_CD_SK,FIPS.CNTY_NM FROM #$IDSOwner#.fips FIPS")
    .load()
)

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
df_db2_FIPS_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", f"SELECT FIPS_D.ST_CD,\nFIPS_D.CNTY_FIPS_NO,\nFIPS_D.CRT_RUN_CYC_EXCTN_DT_SK,\nFIPS_D.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,\nFIPS_D.CNTY_NM,\nFIPS_D.CRT_RUN_CYC_EXCTN_SK,\nFIPS_D.LAST_UPDT_RUN_CYC_EXCTN_SK,\nFIPS_D.ST_CD_SK\n\nFROM #$EDWOwner#.fips_d FIPS_D")
    .load()
)

df_ref_fips_D_Trim = df_db2_FIPS_D_in
df_Xfrm_Trim = df_ref_fips_D_Trim.withColumn("ST_CD", Upcase(trim(col("ST_CD")))) \
    .withColumn("CNTY_FIPS_NO", trim(col("CNTY_FIPS_NO"))) \
    .withColumn("CNTY_NM", Upcase(trim(col("CNTY_NM"))))

df_Xfrm_Trim = df_Xfrm_Trim.select(
    "ST_CD",
    "CNTY_FIPS_NO",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CNTY_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ST_CD_SK"
)

df_Lkup_Cnty = (
    df_db2_FIPS_in.alias("Ink_IdsEdwFipsDExtr_inABC")
    .join(
        df_Xfrm_Trim.alias("ref_FipsD"),
        (
            (col("Ink_IdsEdwFipsDExtr_inABC.ST_CD") == col("ref_FipsD.ST_CD")) &
            (col("Ink_IdsEdwFipsDExtr_inABC.CNTY_FIPS_NO") == col("ref_FipsD.CNTY_FIPS_NO")) &
            (col("Ink_IdsEdwFipsDExtr_inABC.CNTY_NM") == col("ref_FipsD.CNTY_NM")) &
            (col("Ink_IdsEdwFipsDExtr_inABC.ST_CD_SK") == col("ref_FipsD.ST_CD_SK"))
        ),
        "left"
    )
    .select(
        col("Ink_IdsEdwFipsDExtr_inABC.FIPS_SK").alias("FIPS_SK"),
        col("Ink_IdsEdwFipsDExtr_inABC.ST_CD").alias("ST_CD"),
        col("Ink_IdsEdwFipsDExtr_inABC.CNTY_FIPS_NO").alias("CNTY_FIPS_NO"),
        col("Ink_IdsEdwFipsDExtr_inABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("Ink_IdsEdwFipsDExtr_inABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("Ink_IdsEdwFipsDExtr_inABC.ST_CD_SK").alias("ST_CD_SK"),
        col("Ink_IdsEdwFipsDExtr_inABC.CNTY_NM").alias("CNTY_NM"),
        col("ref_FipsD.ST_CD").alias("edw_Lkup_ST_CD"),
        col("ref_FipsD.CNTY_FIPS_NO").alias("edw_Lkup_CNTY_FIPS_NO"),
        col("ref_FipsD.CNTY_NM").alias("edw_Lkup_CNTY_NM"),
        col("ref_FipsD.ST_CD_SK").alias("edw_Lkup_ST_CD_SK"),
        col("ref_FipsD.CRT_RUN_CYC_EXCTN_DT_SK").alias("edw_Lkup_CRT_RUN_CYC_EXCTN_DT_SK"),
        col("ref_FipsD.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("edw_Lkup_LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("ref_FipsD.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("edw_Lkup_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("ref_FipsD.CRT_RUN_CYC_EXCTN_SK").alias("edw_Lkup_CRT_RUN_CYC_EXCTN_SK")
    )
)

df_in_xmf_businessLogic = df_Lkup_Cnty

df_xmf_businessLogic_lnk_Main = df_in_xmf_businessLogic.filter(
    (col("FIPS_SK") != 0) & (col("FIPS_SK") != 1)
).select(
    col("FIPS_SK").alias("FIPS_SK"),
    col("ST_CD").alias("ST_CD"),
    col("CNTY_FIPS_NO").alias("CNTY_FIPS_NO"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CNTY_NM").alias("CNTY_NM"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(col("ST_CD_SK").isNull(), lit(1)).otherwise(col("ST_CD_SK")).alias("ST_CD_SK")
)

df_xmf_businessLogic_UNKLink = df_in_xmf_businessLogic.limit(1).select(
    lit(0).alias("FIPS_SK"),
    lit("UNK").alias("ST_CD"),
    lit("").alias("CNTY_FIPS_NO"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit("UNK").alias("CNTY_NM"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("ST_CD_SK")
)

df_xmf_businessLogic_NALink = df_in_xmf_businessLogic.limit(1).select(
    lit(1).alias("FIPS_SK"),
    lit("NA").alias("ST_CD"),
    lit("").alias("CNTY_FIPS_NO"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit("NA").alias("CNTY_NM"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("ST_CD_SK")
)

df_Fnl_All = df_xmf_businessLogic_NALink.unionByName(df_xmf_businessLogic_lnk_Main).unionByName(df_xmf_businessLogic_UNKLink)

df_Fnl_All = df_Fnl_All.select(
    col("FIPS_SK"),
    col("ST_CD"),
    col("CNTY_FIPS_NO"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CNTY_NM"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ST_CD_SK")
)

write_files(
    df_Fnl_All,
    f"{adls_path}/load/FIPS_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)