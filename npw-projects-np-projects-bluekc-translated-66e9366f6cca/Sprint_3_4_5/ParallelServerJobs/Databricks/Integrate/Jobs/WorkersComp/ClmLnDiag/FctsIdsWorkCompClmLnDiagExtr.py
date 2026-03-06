# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsIdsWorkCompClmLnDiagExtr
# MAGIC CALLED BY:     FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM_LN_DIAG table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                                    Change Description                                                   Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------                                                      -----------------------------------------------------------------------            ----------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2017-01-04\(9)5628 WORK_COMPNSTN_CLM_LN_DIAG          Original Programming\(9)\(9)\(9) Integratedev2                                   Kalyan Neelam           2017-02-17               
# MAGIC 
# MAGIC 
# MAGIC Akhila Manickavelu\(9)  2017-05-09\(9)5628 WORK_COMPNSTN_CLM \(9)              Reversal Logic updated\(9)\(9)\(9) Integratedev2                                   Kalyan Neelam           2017-05-18
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC Prabhu ES                 2022-03-02            S2S Remediation                                        MSSQL ODBC conn added and other param changes  IntegrateDev5\(9)Ken Bradmon\(9)2022-06-03

# MAGIC This job Extracts the Claims data from Facets tables to generate the load file WORK_COMPNSTN_CLM_LN_DIAG.dat to be send to WORK_COMPNSTN_CLM_LN_DIAG table
# MAGIC Source extract from CMC_CDML_CL_LINE
# MAGIC Seq. file to load into the DB2 table B_WORK_COMPNSTN_CLM_LN_DIAG for Balancing Report
# MAGIC Seq. file to load into the DB2 table WORK_COMPNSTN_CLM_LN_DIAG
# MAGIC get the field CRT_RUN_CYC_EXCTN_SK from 
# MAGIC IDS DB2 table  WORK_COMPNSTN_CLM_LN_DIAG
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


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
RunCycle = get_widget_value('RunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
WorkCompOwner = get_widget_value('WorkCompOwner','')
workcomp_secret_name = get_widget_value('workcomp_secret_name','')
DrivTable = get_widget_value('DrivTable','')
SrcSysCd = get_widget_value('SrcSysCd','')

# DB credentials for IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Read from IDS (WorkCompnstnClmLnDiag)
query_WorkCompnstnClmLnDiag = (
    f"SELECT CLM_ID, CLM_LN_SEQ_NO, CLM_LN_DIAG_ORDNL_CD, CRT_RUN_CYC_EXCTN_SK "
    f"FROM {WorkCompOwner}.WORK_COMPNSTN_CLM_LN_DIAG "
    f"WHERE SRC_SYS_CD = 'FACETS'"
)
df_WorkCompnstnClmLnDiag = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_WorkCompnstnClmLnDiag)
    .load()
)

# Read from PxDataSet (Ds_ReversalClaim => Clm_Reversals.ds => read parquet)
df_Ds_ReversalClaim = spark.read.parquet(f"{adls_path}/ds/Clm_Reversals.parquet")

# Read from IDS (CD_MPPNG)
query_CD_MPPNG = (
    f"SELECT DISTINCT SRC_CD, CD_MPPNG_SK, TRGT_CD "
    f"FROM {IDSOwner}.CD_MPPNG "
    f"WHERE SRC_SYS_CD = 'FACETS' "
    f"  AND SRC_CLCTN_CD = 'FACETS DBO' "
    f"  AND SRC_DOMAIN_NM = 'DIAGNOSIS ORDINAL' "
    f"  AND TRGT_CLCTN_CD = 'IDS' "
    f"  AND TRGT_DOMAIN_NM = 'DIAGNOSIS ORDINAL'"
)
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_CD_MPPNG)
    .load()
)

# Read from IDS (DiagCD)
query_DiagCD = (
    f"SELECT DIAG_CD, DIAG_CD_SK "
    f"FROM {IDSOwner}.DIAG_CD "
    f"WHERE DIAG_CD_TYP_CD = 'ICD10'"
)
df_DiagCD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_DiagCD)
    .load()
)

# DB credentials for Facets (ODBCConnectorPX)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

# Read from FacetsDB_Diag
query_FacetsDB_Diag = (
    f"SELECT DISTINCT CMC_CLMD_DIAG.CLCL_ID, "
    f"       CAST(CMC_CLMD_DIAG.CLMD_TYPE AS VARCHAR(20)) AS CLMD_TYPE, "
    f"       CMC_CLMD_DIAG.IDCD_ID "
    f"FROM {FacetsOwner}.CMC_CLMD_DIAG CMC_CLMD_DIAG "
    f"INNER JOIN tempdb..#{DrivTable} Drvr "
    f"     ON Drvr.CLM_ID = CMC_CLMD_DIAG.CLCL_ID"
)
df_FacetsDB_Diag = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_FacetsDB_Diag)
    .load()
)

# Read from FacetsDB_Input
query_FacetsDB_Input = (
    f"SELECT DISTINCT CMC_CDML_CL_LINE.CLCL_ID, "
    f"       CMC_CDML_CL_LINE.CDML_SEQ_NO, "
    f"       CMC_CDML_CL_LINE.IDCD_ID, "
    f"       CMC_CDML_CL_LINE.CDML_CLMD_TYPE2, "
    f"       CMC_CDML_CL_LINE.CDML_CLMD_TYPE3, "
    f"       CMC_CDML_CL_LINE.CDML_CLMD_TYPE4, "
    f"       CMC_CDML_CL_LINE.CDML_CLMD_TYPE5, "
    f"       CMC_CDML_CL_LINE.CDML_CLMD_TYPE6, "
    f"       CMC_CDML_CL_LINE.CDML_CLMD_TYPE7, "
    f"       CMC_CDML_CL_LINE.CDML_CLMD_TYPE8 "
    f"FROM {FacetsOwner}.CMC_CDML_CL_LINE CMC_CDML_CL_LINE "
    f"INNER JOIN tempdb..#{DrivTable} Drvr "
    f"     ON Drvr.CLM_ID = CMC_CDML_CL_LINE.CLCL_ID"
)
df_FacetsDB_Input = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_FacetsDB_Input)
    .load()
)

# Pivot Stage (Pvt_DIAG_CD)
df_Pvt_DIAG_CD = (
    df_FacetsDB_Input.select(
        F.col("CLCL_ID"),
        F.col("CDML_SEQ_NO"),
        F.array(
            "IDCD_ID",
            "CDML_CLMD_TYPE2",
            "CDML_CLMD_TYPE3",
            "CDML_CLMD_TYPE4",
            "CDML_CLMD_TYPE5",
            "CDML_CLMD_TYPE6",
            "CDML_CLMD_TYPE7",
            "CDML_CLMD_TYPE8"
        ).alias("pivot_array")
    )
    .select(
        F.col("CLCL_ID"),
        F.col("CDML_SEQ_NO"),
        F.posexplode(F.col("pivot_array")).alias("Pivot_index", "DIAG_CD")
    )
)

# Xfm_Trim
df_Xfm_Trim = (
    df_Pvt_DIAG_CD
    .filter(
        (F.col("DIAG_CD").isNotNull()) &
        (trim(F.col("DIAG_CD")) != "")
    )
    .select(
        F.col("CLCL_ID").alias("CLCL_ID"),
        trim(F.col("DIAG_CD")).alias("DIAG_CD"),
        F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        F.when(F.col("Pivot_index") == 0, '1')
         .when(F.col("Pivot_index") == 1, '2')
         .when(F.col("Pivot_index") == 2, '3')
         .when(F.col("Pivot_index") == 3, '4')
         .when(F.col("Pivot_index") == 4, '5')
         .when(F.col("Pivot_index") == 5, '6')
         .when(F.col("Pivot_index") == 6, '7')
         .when(F.col("Pivot_index") == 7, '8')
         .otherwise('')
         .alias("LKUP_ORDNL_CD")
    )
)

# Lkup_Diag (PxLookup) with no join condition on Lnk_FctsInp => cross join for left join
df_Lnk_Diag_left = df_Xfm_Trim.alias("Lnk_Diag")
df_FacetsDB_Diag_left = df_FacetsDB_Diag.alias("Lnk_FctsInp")
# Cross join to replicate DataStage's left-join-with-no-condition approach
df_Lkup_Diag_temp = df_Lnk_Diag_left.crossJoin(df_FacetsDB_Diag_left)
df_Lkup_Diag = df_Lkup_Diag_temp.select(
    F.col("Lnk_Diag.CLCL_ID").alias("CLCL_ID"),
    F.col("Lnk_Diag.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.col("Lnk_Diag.DIAG_CD").alias("DIAG_CD"),
    F.col("Lnk_Diag.LKUP_ORDNL_CD").alias("LKUP_ORDNL_CD"),
    F.col("Lnk_FctsInp.IDCD_ID").alias("IDCD_ID")
)

# Xfm_DiagCd
df_Xfm_DiagCd = (
    df_Lkup_Diag.select(
        F.col("CLCL_ID").alias("CLCL_ID"),
        F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        F.when(F.col("LKUP_ORDNL_CD") == '1', F.col("DIAG_CD"))
         .otherwise(F.col("IDCD_ID"))
         .alias("DIAG_CD"),
        F.col("LKUP_ORDNL_CD").alias("LKUP_ORDNL_CD")
    )
)

# Lookup_DBs => multi-lookup:
#   1) left join with df_DiagCD => on DIAG_CD
#   2) inner join with df_CD_MPPNG => on LKUP_ORDNL_CD = SRC_CD
df_Lookup_DBs_temp = (
    df_Xfm_DiagCd.alias("Lnk_FacetsLkp")
    .join(
        df_DiagCD.alias("Lnk_DiagCD"),
        F.col("Lnk_FacetsLkp.DIAG_CD") == F.col("Lnk_DiagCD.DIAG_CD"),
        "left"
    )
    .join(
        df_CD_MPPNG.alias("Lnk_CdMppng"),
        F.col("Lnk_FacetsLkp.LKUP_ORDNL_CD") == F.col("Lnk_CdMppng.SRC_CD"),
        "inner"
    )
)
df_Lookup_DBs = df_Lookup_DBs_temp.select(
    F.col("Lnk_FacetsLkp.CLCL_ID").alias("CLCL_ID"),
    F.col("Lnk_FacetsLkp.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.col("Lnk_DiagCD.DIAG_CD_SK").alias("DIAG_CD_SK"),
    F.col("Lnk_CdMppng.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("Lnk_CdMppng.TRGT_CD").alias("TRGT_CD")
)

# Lkp_TrgtTble => multi-lookup:
#   1) left join df_WorkCompnstnClmLnDiag => condition => CLCL_ID=CLM_ID, CDML_SEQ_NO=CLM_LN_SEQ_NO, TRGT_CD=CLM_LN_DIAG_ORDNL_CD
#   2) left join df_Ds_ReversalClaim => condition => CLCL_ID=CLCL_ID
df_Lkp_TrgtTble_temp = (
    df_Lookup_DBs.alias("Lnk_BusinessLogic")
    .join(
        df_WorkCompnstnClmLnDiag.alias("Lnk_WorkCompClmLnDiag"),
        (
            (F.col("Lnk_BusinessLogic.CLCL_ID") == F.col("Lnk_WorkCompClmLnDiag.CLM_ID")) &
            (F.col("Lnk_BusinessLogic.CDML_SEQ_NO") == F.col("Lnk_WorkCompClmLnDiag.CLM_LN_SEQ_NO")) &
            (F.col("Lnk_BusinessLogic.TRGT_CD") == F.col("Lnk_WorkCompClmLnDiag.CLM_LN_DIAG_ORDNL_CD"))
        ),
        "left"
    )
    .join(
        df_Ds_ReversalClaim.alias("Lnk_ReversalDs"),
        F.col("Lnk_BusinessLogic.CLCL_ID") == F.col("Lnk_ReversalDs.CLCL_ID"),
        "left"
    )
)
df_Lkp_TrgtTble = df_Lkp_TrgtTble_temp.select(
    F.col("Lnk_BusinessLogic.CLCL_ID").alias("CLCL_ID"),
    F.col("Lnk_BusinessLogic.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.col("Lnk_BusinessLogic.DIAG_CD_SK").alias("DIAG_CD_SK"),
    F.col("Lnk_BusinessLogic.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("Lnk_BusinessLogic.TRGT_CD").alias("TRGT_CD"),
    F.col("Lnk_WorkCompClmLnDiag.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_ReversalDs.CLCL_ID").alias("CLCL_ID_Reverse")
)

# Xfm_Reversal => two output links
# Link All
df_Xfm_Reversal_Lnk_All = df_Lkp_TrgtTble.select(
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.col("DIAG_CD_SK").alias("DIAG_CD_SK"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

# Link Reversal => constraint => trim( if IsNotNull(CLCL_ID_Reverse) then CLCL_ID_Reverse else "" ) <> ""
df_Xfm_Reversal_Lnk_Reversal = (
    df_Lkp_TrgtTble
    .filter(
        trim(
            F.when(F.col("CLCL_ID_Reverse").isNotNull(), F.col("CLCL_ID_Reverse")).otherwise(F.lit(""))
        ) != ""
    )
    .select(
        (trim(F.col("CLCL_ID")) + F.lit("R")).alias("CLCL_ID"),
        F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        F.col("DIAG_CD_SK").alias("DIAG_CD_SK"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("TRGT_CD").alias("TRGT_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
    )
)

# Fnl_All_Reverse => Funnel => union
df_Fnl_All_Reverse = df_Xfm_Reversal_Lnk_All.unionByName(df_Xfm_Reversal_Lnk_Reversal)

# Xfm_BusinessLogicFinal =>
df_BLF_alias = df_Fnl_All_Reverse.alias("Lnk_BusinessLogicFinal")

df_Lnk_WorkCompnstnClmLnDiagFinal = df_BLF_alias.select(
    F.col("Lnk_BusinessLogicFinal.CLCL_ID").alias("CLM_ID"),
    F.col("Lnk_BusinessLogicFinal.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.col("Lnk_BusinessLogicFinal.TRGT_CD").alias("CLM_LN_DIAG_ORDNL_CD"),
    F.when(
        (F.col("Lnk_BusinessLogicFinal.CRT_RUN_CYC_EXCTN_SK").isNotNull()) &
        (F.col("Lnk_BusinessLogicFinal.CRT_RUN_CYC_EXCTN_SK") != 0),
        F.col("Lnk_BusinessLogicFinal.CRT_RUN_CYC_EXCTN_SK")
    ).otherwise(F.lit(RunCycle)).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_BusinessLogicFinal.DIAG_CD_SK").alias("DIAG_CD_SK"),
    F.col("Lnk_BusinessLogicFinal.CD_MPPNG_SK").alias("CLM_LN_DIAG_ORDNL_CD_SK")
)

df_Lnk_B_WorkCompnstnClmLnDiag = df_BLF_alias.select(
    F.col("Lnk_BusinessLogicFinal.CLCL_ID").alias("CLM_ID"),
    F.col("Lnk_BusinessLogicFinal.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("Lnk_BusinessLogicFinal.TRGT_CD").alias("CLM_LN_DIAG_ORDNL_CD"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD")
)

# For final file outputs, apply rpad to char/varchar columns where lengths are known or inferred.
df_Lnk_B_WorkCompnstnClmLnDiag_out = (
    df_Lnk_B_WorkCompnstnClmLnDiag
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 12, " "))
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.rpad(F.col("CLM_LN_DIAG_ORDNL_CD"), 2, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 10, " "))
    .select(
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_DIAG_ORDNL_CD",
        "SRC_SYS_CD"
    )
)

df_Lnk_WorkCompnstnClmLnDiagFinal_out = (
    df_Lnk_WorkCompnstnClmLnDiagFinal
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 12, " "))
    .withColumn("CLM_LN_DIAG_ORDNL_CD", F.rpad(F.col("CLM_LN_DIAG_ORDNL_CD"), 2, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 10, " "))
    .select(
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "SRC_SYS_CD",
        "CLM_LN_DIAG_ORDNL_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "DIAG_CD_SK",
        "CLM_LN_DIAG_ORDNL_CD_SK"
    )
)

# Write B_WORK_COMPNSTN_CLM_LN_DIAG.dat
write_files(
    df_Lnk_B_WorkCompnstnClmLnDiag_out,
    f"{adls_path}/load/B_WORK_COMPNSTN_CLM_LN_DIAG.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)

# Write WORK_COMPNSTN_CLM_LN_DIAG.dat
write_files(
    df_Lnk_WorkCompnstnClmLnDiagFinal_out,
    f"{adls_path}/load/WORK_COMPNSTN_CLM_LN_DIAG.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)