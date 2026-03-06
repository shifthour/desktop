# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsIdsWorkCompClmLnProcCdModExtr
# MAGIC CALLED BY: 
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM_LN_PROC_CD_MOD table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                                                    Change Description                                                 Development Project\(9)      Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)---------------------------------------------------------------------------------------       ----------------------------------------------                                   ----------------------------------\(9)     -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2017-01-17\(9)5628 WORK_COMPNSTN_CLM_LN_PROC_CD_MOD       Original Programming\(9)                                      Integratedev2                          Kalyan Neelam          2017-02-17
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC 
# MAGIC Akhila Manickavelu\(9)  2017-05-09\(9)5628 WORK_COMPNSTN_CLM                                          Reversal Logic updated\(9)\(9)\(9) Integratedev2                               Kalyan Neelam          2017-05-18
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC Prabhu ES                 2022-03-02            S2S Remediation                                                                  MSSQL ODBC conn added and other param changes  IntegrateDev5\(9)Ken Bradmon\(9)2022-06-03

# MAGIC This job Extracts the Claims data from Facets tables to generate the load file WORK_COMPNSTN_CLM_LN_PROC_CD_MOD.dat to be send to WORK_COMPNSTN_CLM_LN_PROC_CD_MOD table
# MAGIC Join CMC_CDML_CL_LINE, CMC_GRGR_GROUP, CMC_MEME_MEMBER for master extract and Look up on P_SEL_PRCS_CRITR for GRGR_ID
# MAGIC Seq. file to load into the DB2 table B_WORK_COMPNSTN_CLM_LN_PROC_CD_MOD for Balancing Report
# MAGIC Seq. file to load into the DB2 table WORK_COMPNSTN_CLM_LN_PROC_CD_MOD
# MAGIC Reversal Logic
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
facets_secret_name = get_widget_value('facets_secret_name','')
workcomp_secret_name = get_widget_value('workcomp_secret_name','')
RunCycle = get_widget_value('RunCycle','')
DrivTable = get_widget_value('DrivTable','')
SrcSysCd = get_widget_value('SrcSysCd','')
FacetsOwner = get_widget_value('FacetsOwner','')
BCBSOwner = get_widget_value('BCBSOwner','')
IDSOwner = get_widget_value('IDSOwner','')
WorkCompOwner = get_widget_value('WorkCompOwner','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_WorkCompnstnClmLnProcCdMod = f"""SELECT
CLM_ID,
CLM_LN_SEQ_NO,
CLM_LN_PROC_CD_MOD_ORDNL_CD,
CRT_RUN_CYC_EXCTN_SK
FROM {WorkCompOwner}.WORK_COMPNSTN_CLM_LN_PROC_CD_MOD
WHERE SRC_SYS_CD = 'FACETS'
"""
df_WorkCompnstnClmLnProcCdMod = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_WorkCompnstnClmLnProcCdMod)
    .load()
)

df_Ds_ReversalClaim = spark.read.parquet(f"{adls_path}/ds/Clm_Reversals.parquet")

extract_query_CD_MPPNG = f"""SELECT DISTINCT
SRC_CD,
CD_MPPNG_SK,
TRGT_CD
FROM {IDSOwner}.CD_MPPNG
WHERE SRC_SYS_CD = 'FACETS'
 AND SRC_CLCTN_CD = 'FACETS DBO'
 AND SRC_DOMAIN_NM = 'CUSTOM DOMAIN MAPPING'
 AND TRGT_CLCTN_CD = 'IDS'
 AND TRGT_DOMAIN_NM = 'PROCEDURE ORDINAL'
"""
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CD_MPPNG)
    .load()
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_FacetsDB_Input = f"""SELECT DISTINCT
      CMC_CDML_CL_LINE.CLCL_ID,
      CMC_CDML_CL_LINE.CDML_SEQ_NO,
      CMC_CDML_CL_LINE.IPCD_ID,
      CMC_CDML_CL_LINE.CDML_IPCD_MOD2,
      CMC_CDML_CL_LINE.CDML_IPCD_MOD3,
      CMC_CDML_CL_LINE.CDML_IPCD_MOD4
FROM {FacetsOwner}.CMC_CDML_CL_LINE CMC_CDML_CL_LINE
INNER JOIN tempdb..#{DrivTable} Drvr
ON Drvr.CLM_ID = CMC_CDML_CL_LINE.CLCL_ID
WHERE LEN(LTRIM(RTRIM(CMC_CDML_CL_LINE.IPCD_ID))) = 7
"""
df_FacetsDB_Input = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_FacetsDB_Input)
    .load()
)

df_Pvt_PROC_CD = (
    df_FacetsDB_Input.select(
        F.col("CLCL_ID"),
        F.col("CDML_SEQ_NO"),
        F.explode(
            F.array(
                F.struct(F.col("IPCD_ID").alias("PROC_CD"), F.lit(0).alias("pivot_index")),
                F.struct(F.col("CDML_IPCD_MOD2").alias("PROC_CD"), F.lit(1).alias("pivot_index")),
                F.struct(F.col("CDML_IPCD_MOD3").alias("PROC_CD"), F.lit(2).alias("pivot_index")),
                F.struct(F.col("CDML_IPCD_MOD4").alias("PROC_CD"), F.lit(3).alias("pivot_index"))
            )
        ).alias("pivoted")
    )
    .select(
        F.col("CLCL_ID"),
        F.col("CDML_SEQ_NO"),
        F.col("pivoted.PROC_CD").alias("PROC_CD"),
        F.col("pivoted.pivot_index").alias("Pivot_index")
    )
)

df_Xfm_Trim_pre = df_Pvt_PROC_CD.filter(
    (F.col("PROC_CD").isNotNull()) & (trim(F.col("PROC_CD")) != "")
)
df_Xfm_Trim = df_Xfm_Trim_pre.select(
    F.col("CLCL_ID"),
    F.col("CDML_SEQ_NO"),
    F.col("PROC_CD"),
    F.when(F.col("Pivot_index") == 0, "1")
     .when(F.col("Pivot_index") == 1, "2")
     .when(F.col("Pivot_index") == 2, "3")
     .when(F.col("Pivot_index") == 3, "4")
     .otherwise("")
     .alias("LKUP_CLM_PROV_ROLE_TYP_CD_SK")
)

df_Lookup_DBs = (
    df_Xfm_Trim.alias("Lnk_FacetsLkp")
    .join(
        df_CD_MPPNG.alias("Lnk_CustDomain"),
        F.col("Lnk_FacetsLkp.LKUP_CLM_PROV_ROLE_TYP_CD_SK") == F.col("Lnk_CustDomain.SRC_CD"),
        "inner"
    )
    .select(
        F.col("Lnk_FacetsLkp.CLCL_ID").alias("CLCL_ID"),
        F.col("Lnk_FacetsLkp.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        F.col("Lnk_FacetsLkp.PROC_CD").alias("PROC_CD"),
        F.col("Lnk_CustDomain.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("Lnk_CustDomain.TRGT_CD").alias("TRGT_CD")
    )
)

df_Lkp_RunCycle = (
    df_Lookup_DBs.alias("Lnk_BusinessLogic")
    .join(
        df_WorkCompnstnClmLnProcCdMod.alias("Lnk_ClmLnProcCdMod"),
        (
            (F.col("Lnk_BusinessLogic.CLCL_ID") == F.col("Lnk_ClmLnProcCdMod.CLM_ID")) &
            (F.col("Lnk_BusinessLogic.CDML_SEQ_NO") == F.col("Lnk_ClmLnProcCdMod.CLM_LN_SEQ_NO")) &
            (F.col("Lnk_BusinessLogic.TRGT_CD") == F.col("Lnk_ClmLnProcCdMod.CLM_LN_PROC_CD_MOD_ORDNL_CD"))
        ),
        "left"
    )
    .join(
        df_Ds_ReversalClaim.alias("Lnk_ReversalDs"),
        F.col("Lnk_BusinessLogic.CLCL_ID") == F.col("Lnk_ReversalDs.CLCL_ID"),
        "left"
    )
    .select(
        F.col("Lnk_BusinessLogic.CLCL_ID").alias("CLCL_ID"),
        F.col("Lnk_BusinessLogic.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        F.col("Lnk_BusinessLogic.PROC_CD").alias("PROC_CD"),
        F.col("Lnk_BusinessLogic.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("Lnk_BusinessLogic.TRGT_CD").alias("TRGT_CD"),
        F.col("Lnk_ClmLnProcCdMod.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_ReversalDs.CLCL_ID").alias("CLCL_ID_Reverse")
    )
)

df_Xfm_Reversal_All = df_Lkp_RunCycle.select(
    F.col("CLCL_ID"),
    F.col("CDML_SEQ_NO"),
    F.col("PROC_CD"),
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK")
)

df_Xfm_Reversal_Reversal = (
    df_Lkp_RunCycle.filter(
        (F.col("CLCL_ID_Reverse").isNotNull()) & (trim(F.col("CLCL_ID_Reverse")) != "")
    )
    .select(
        F.concat(trim(F.col("CLCL_ID")), F.lit("R")).alias("CLCL_ID"),
        F.col("CDML_SEQ_NO"),
        F.col("PROC_CD"),
        F.col("CD_MPPNG_SK"),
        F.col("TRGT_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK")
    )
)

df_Fnl_All_Reverse = df_Xfm_Reversal_All.unionByName(df_Xfm_Reversal_Reversal)

df_Xfm_BusinessLogic = df_Fnl_All_Reverse

df_B_WorkCompnstnClmLnProcCdMod = df_Xfm_BusinessLogic.select(
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("TRGT_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD")
)

df_WorkCompnstnClmLnProcCdMod_out = df_Xfm_BusinessLogic.select(
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("TRGT_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.when(
        (F.col("CRT_RUN_CYC_EXCTN_SK").isNull()) | 
        (F.col("CRT_RUN_CYC_EXCTN_SK") == 0),
        F.lit(RunCycle)
    ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("CD_MPPNG_SK").isNull(), F.lit(0)).otherwise(F.col("CD_MPPNG_SK")).alias("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK"),
    F.when(
        F.length(trim(F.col("PROC_CD"))) == 7,
        F.upper(F.substring(trim(F.col("PROC_CD")), 6, 2))
    ).otherwise(F.upper(trim(F.col("PROC_CD")))).alias("CLM_LN_PROC_CD_MOD_TX")
)

write_files(
    df_WorkCompnstnClmLnProcCdMod_out.select(
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_PROC_CD_MOD_ORDNL_CD",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_PROC_CD_MOD_ORDNL_CD_SK",
        "CLM_LN_PROC_CD_MOD_TX"
    ),
    f"{adls_path}/load/WORK_COMPNSTN_CLM_LN_PROC_CD_MOD.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)

write_files(
    df_B_WorkCompnstnClmLnProcCdMod.select(
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_PROC_CD_MOD_ORDNL_CD",
        "SRC_SYS_CD"
    ),
    f"{adls_path}/load/B_WORK_COMPNSTN_CLM_LN_PROC_CD_MOD.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)