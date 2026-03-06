# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: FctsIdsWorkCompGrpCaseSeq
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_GRP table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                          Change Description                                                   Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------                                        -----------------------------------------------------------------------                  ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2016-12-07\(9)5628 WORK_COMPNSTN_GRP \(9)    Original Programming\(9)\(9)\(9)          Integratedev2                           Kalyan Neelam          2017-01-09
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC 
# MAGIC 
# MAGIC Akhila Manickavelu\(9)  2016-05-11\(9)5628 WORK_COMPNSTN_GRP \(9)    Update for GRP_TERM_RSN_CD_SK                        Integratedev2                           Kalyan Neelam           2017-05-17
# MAGIC                                                                 (Facets to IDS) ETL Report                         Lookup for cd_mppng
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Akhila Manickavelu\(9)  2017-06-08\(9)5628 WORK_COMPNSTN_GRP \(9)    Additional field PRNT_GRP_ID included\(9)        Integratedev2                           Kalyan Neelam           2017-06-12
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC Prabhu ES                 2022-03-02            S2S Remediation                                        MSSQL ODBC conn added and other param changes IntegrateDev5                         Goutham Kalidindi      6/3/2022

# MAGIC This job Extracts the Claims data from Facets tables to generate the load file WORK_COMPNSTN_GRP to be send to IDS WORK_COMPNSTN_GRP table
# MAGIC Join CMC_GRGR_GROUP, CMC_MEME_MEMBER  for master extract and Look up on P_SEL_PRCS_CRITR for GRGR_ID
# MAGIC Generate the Seq. file to load into the DB2 table WORK_COMPNSTN_GRP
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, TimestampType, DateType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
WorkCompOwner = get_widget_value('WorkCompOwner','')
workcomp_secret_name = get_widget_value('workcomp_secret_name','')
RunCycle = get_widget_value('RunCycle','')
Env = get_widget_value('Env','')
RunID = get_widget_value('RunID','')

# FacetsDB_Input (ODBCConnectorPX)
jdbc_url_FacetsDB_Input, jdbc_props_FacetsDB_Input = get_db_config(facets_secret_name)
extract_query_FacetsDB_Input = f"""
SELECT DISTINCT
   CMC_GRGR_GROUP.GRGR_CK
   ,CMC_GRGR_GROUP.GRGR_ID
   ,CMC_GRGR_GROUP.GRGR_NAME
   ,CMC_GRGR_GROUP.GRGR_STS
   ,CMC_GRGR_GROUP.GRGR_ORIG_EFF_DT
   ,CMC_GRGR_GROUP.GRGR_TERM_DT
   ,CAST(CMC_GRGR_GROUP.GRGR_MCTR_TRSN AS VARCHAR(4)) AS GRGR_MCTR_TRSN
   ,CMC_GRGR_GROUP.GRGR_RNST_DT
   ,CMC_GRGR_GROUP.GRGR_RENEW_MMDD
   ,CMC_GRGR_GROUP.GRGR_CURR_ANNV_DT
   ,CMC_GRGR_GROUP.GRGR_NEXT_ANNV_DT
   ,CMC_GRGR_GROUP.GRGR_TOTAL_EMPL
   ,CMC_GRGR_GROUP.GRGR_TOTAL_CONTR
   ,CASE WHEN CMC_GRGR_GROUP.SYS_LAST_UPD_DTM IS NULL THEN GETDATE() ELSE CMC_GRGR_GROUP.SYS_LAST_UPD_DTM END as SYS_LAST_UPD_DTM
   ,CMC_PAGR_PARENT_GR.PAGR_ID
FROM
   {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR_GROUP
INNER JOIN {FacetsOwner}.CMC_PAGR_PARENT_GR CMC_PAGR_PARENT_GR ON
   CMC_PAGR_PARENT_GR.PAGR_CK=CMC_GRGR_GROUP.PAGR_CK
WHERE EXISTS 
   (
     SELECT 'Y'
     FROM 
       {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
       {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR
     WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
       AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
       AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
       AND CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
       AND CMC_GRGR.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
       AND CMC_GRGR.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK
   )
"""
df_FacetsDB_Input = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_FacetsDB_Input)
    .options(**jdbc_props_FacetsDB_Input)
    .option("query", extract_query_FacetsDB_Input)
    .load()
)

# CD_MPPNG (DB2ConnectorPX) - Database=IDS
jdbc_url_CD_MPPNG, jdbc_props_CD_MPPNG = get_db_config(ids_secret_name)
extract_query_CD_MPPNG = f"""
SELECT DISTINCT
   SRC_DRVD_LKUP_VAL,
   CD_MPPNG_SK,
   SRC_DOMAIN_NM,
   TRGT_DOMAIN_NM
FROM {IDSOwner}.CD_MPPNG
WHERE 
   SRC_SYS_CD = 'FACETS'
   AND SRC_CLCTN_CD = 'FACETS DBO'
   AND SRC_DOMAIN_NM IN ( 'GROUP STATUS' , 'GROUP TERMINATION REASON' )
   AND TRGT_CLCTN_CD = 'IDS'
   AND TRGT_DOMAIN_NM  IN ( 'GROUP STATUS' , 'GROUP TERMINATION REASON' )
"""
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CD_MPPNG)
    .options(**jdbc_props_CD_MPPNG)
    .option("query", extract_query_CD_MPPNG)
    .load()
)

# Flt_CdMppng (PxFilter)
df_LnkStatus = df_CD_MPPNG.filter(
    "(SRC_DOMAIN_NM = 'GROUP STATUS') AND (TRGT_DOMAIN_NM = 'GROUP STATUS')"
).select("CD_MPPNG_SK", "SRC_DRVD_LKUP_VAL")

df_LnkTermination = df_CD_MPPNG.filter(
    "(SRC_DOMAIN_NM = 'GROUP TERMINATION REASON') AND (TRGT_DOMAIN_NM = 'GROUP TERMINATION REASON')"
).select("CD_MPPNG_SK", "SRC_DRVD_LKUP_VAL")

# LkpTwoSets (PxLookup)
df_LkpTwoSets = (
    df_FacetsDB_Input.alias("Lnk_Facets")
    .join(
        df_LnkTermination.alias("LnkTermination"),
        F.col("Lnk_Facets.GRGR_MCTR_TRSN") == F.col("LnkTermination.SRC_DRVD_LKUP_VAL"),
        "left",
    )
    .join(
        df_LnkStatus.alias("LnkStatus"),
        F.col("Lnk_Facets.GRGR_STS") == F.col("LnkStatus.SRC_DRVD_LKUP_VAL"),
        "left",
    )
    .select(
        F.col("Lnk_Facets.GRGR_CK").alias("GRGR_CK"),
        F.col("Lnk_Facets.GRGR_ID").alias("GRGR_ID"),
        F.col("Lnk_Facets.GRGR_NAME").alias("GRGR_NAME"),
        F.col("Lnk_Facets.GRGR_STS").alias("GRGR_STS"),
        F.col("Lnk_Facets.GRGR_ORIG_EFF_DT").alias("GRGR_ORIG_EFF_DT"),
        F.col("Lnk_Facets.GRGR_TERM_DT").alias("GRGR_TERM_DT"),
        F.col("Lnk_Facets.GRGR_MCTR_TRSN").alias("GRGR_MCTR_TRSN"),
        F.col("Lnk_Facets.GRGR_RNST_DT").alias("GRGR_RNST_DT"),
        F.col("Lnk_Facets.GRGR_RENEW_MMDD").alias("GRGR_RENEW_MMDD"),
        F.col("Lnk_Facets.GRGR_CURR_ANNV_DT").alias("GRGR_CURR_ANNV_DT"),
        F.col("Lnk_Facets.GRGR_NEXT_ANNV_DT").alias("GRGR_NEXT_ANNV_DT"),
        F.col("Lnk_Facets.GRGR_TOTAL_EMPL").alias("GRGR_TOTAL_EMPL"),
        F.col("Lnk_Facets.GRGR_TOTAL_CONTR").alias("GRGR_TOTAL_CONTR"),
        F.col("Lnk_Facets.SYS_LAST_UPD_DTM").alias("SYS_LAST_UPD_DTM"),
        F.col("LnkStatus.CD_MPPNG_SK").alias("GRP_STTUS_CD_SK"),
        F.col("LnkTermination.CD_MPPNG_SK").alias("GRP_TERM_RSN_CD_SK"),
        F.col("Lnk_Facets.PAGR_ID").alias("PAGR_ID"),
    )
)

# BusinessLogic (CTransformerStage)
df_BusinessLogic = (
    df_LkpTwoSets
    .withColumn("svTermDate", F.date_format(F.col("GRGR_TERM_DT"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("tmp_year", F.year(current_date()).cast(StringType()))
    .withColumn("tmp_year_plus1", (F.year(current_date()) + F.lit(1)).cast(StringType()))
    .withColumn("tmp_len_renew", F.length(F.col("GRGR_RENEW_MMDD")))
    .withColumn(
        "svRenewDate",
        F.when(
            F.col("tmp_len_renew") == 3,
            F.concat(F.col("tmp_year"), F.lit("0"), F.col("GRGR_RENEW_MMDD"))
        ).otherwise(
            F.concat(F.col("tmp_year"), F.col("GRGR_RENEW_MMDD"))
        )
    )
    .withColumn("svRenewDate_yyyy", F.substring(F.col("svRenewDate"), 1, 4))
    .withColumn("svRenewDate_mm", F.substring(F.col("svRenewDate"), 5, 2))
    .withColumn("svRenewDate_dd", F.substring(F.col("svRenewDate"), 7, 2))
    .withColumn(
        "svRenewDate_date",
        F.to_date(
            F.concat_ws("-", F.col("svRenewDate_yyyy"), F.col("svRenewDate_mm"), F.col("svRenewDate_dd")),
            "yyyy-MM-dd"
        )
    )
    .withColumn("daysSince_renew", F.datediff(current_date(), F.col("svRenewDate_date")))
    .withColumn(
        "svFinalRenew",
        F.when(
            F.col("daysSince_renew") < 0,
            F.concat(F.col("tmp_year_plus1"), F.substring(F.col("svRenewDate"), 5, 4))
        ).otherwise(F.col("svRenewDate"))
    )
)

df_WorkCompnstnGrp = (
    df_BusinessLogic
    .select(
        F.col("GRGR_ID").alias("GRP_ID"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(F.col("GRP_STTUS_CD_SK").isNull(), F.lit(0)).otherwise(F.col("GRP_STTUS_CD_SK")).alias("GRP_STTUS_CD_SK"),
        F.when(F.col("GRP_TERM_RSN_CD_SK").isNull(), F.lit(0)).otherwise(F.col("GRP_TERM_RSN_CD_SK")).alias("GRP_TERM_RSN_CD_SK"),
        F.to_date(F.col("GRGR_CURR_ANNV_DT")).alias("CUR_ANNV_DT"),
        F.to_date(F.col("GRGR_NEXT_ANNV_DT")).alias("NEXT_ANNV_DT"),
        F.to_date(F.col("GRGR_ORIG_EFF_DT")).alias("ORIG_EFF_DT"),
        F.to_date(F.col("GRGR_RNST_DT")).alias("REINST_DT"),
        F.when(
            F.col("GRGR_STS") == F.lit("TM"),
            F.concat(
                F.substring(F.col("svTermDate"), 1, 4), F.lit("-"),
                F.substring(F.col("svTermDate"), 6, 2), F.lit("-"),
                F.substring(F.col("svTermDate"), 9, 2)
            )
        ).otherwise(
            F.concat(
                F.substring(F.col("svFinalRenew"), 1, 4), F.lit("-"),
                F.substring(F.col("svFinalRenew"), 5, 2), F.lit("-"),
                F.substring(F.col("svFinalRenew"), 7, 2)
            )
        ).alias("RNWL_DT"),
        F.when(
            F.col("SYS_LAST_UPD_DTM").isNull(),
            current_date()
        ).otherwise(
            F.to_date(F.col("SYS_LAST_UPD_DTM"))
        ).alias("SRC_SYS_LAST_UPDT_DT"),
        F.to_date(F.col("GRGR_TERM_DT")).alias("TERM_DT"),
        F.col("GRGR_CK").alias("GRP_UNIQ_KEY"),
        F.col("GRGR_TOTAL_CONTR").alias("TOT_CNTR_CT"),
        F.col("GRGR_TOTAL_EMPL").alias("TOT_EMPL_CT"),
        F.col("GRGR_RENEW_MMDD").alias("RNWL_DT_MO_DAY"),
        F.col("GRGR_NAME").alias("GRP_NM"),
        F.when(F.trim(F.col("PAGR_ID")) == F.lit(""), F.lit(None)).otherwise(F.trim(F.col("PAGR_ID"))).alias("PRNT_GRP_ID"),
    )
)

df_BalWorkCompnstnGrp = (
    df_BusinessLogic
    .select(
        F.col("GRGR_ID").alias("GRP_ID"),
        F.lit("FACETS").alias("SRC_SYS_CD")
    )
)

df_WorkCompnstnGrp_rpad = (
    df_WorkCompnstnGrp
    .withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), 8, " "))
    .withColumn("RNWL_DT_MO_DAY", F.rpad(F.col("RNWL_DT_MO_DAY"), 4, " "))
    .withColumn("GRP_NM", F.rpad(F.col("GRP_NM"), 50, " "))
    .withColumn("PRNT_GRP_ID", F.rpad(F.col("PRNT_GRP_ID"), 9, " "))
)

df_BalWorkCompnstnGrp_rpad = (
    df_BalWorkCompnstnGrp
    .withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), 8, " "))
)

write_files(
    df_WorkCompnstnGrp_rpad,
    f"{adls_path}/load/WORK_COMPNSTN_GRP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)

write_files(
    df_BalWorkCompnstnGrp_rpad,
    f"{adls_path}/load/B_WORK_COMPNSTN_GRP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)