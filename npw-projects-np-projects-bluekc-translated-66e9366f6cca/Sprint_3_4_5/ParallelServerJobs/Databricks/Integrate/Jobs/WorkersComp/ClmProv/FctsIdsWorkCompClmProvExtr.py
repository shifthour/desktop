# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsIdsWorkCompClmProvExtr
# MAGIC CALLED BY:         FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM_PROV table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                                  Change Description                                                   Development Project\(9)Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------                                                   -----------------------------------------------------------------------              ------------------------------\(9)                -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2017-01-05\(9)5628 WORK_COMPNSTN_CLM_PROV \(9)    Original Programming\(9)\(9)\(9)Integratedev2                            Kalyan Neelam         2017-02-22    
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC 
# MAGIC 
# MAGIC Akhila Manickavelu\(9)  2017-05-11\(9)5628 WORK_COMPNSTN_CLM \(9)          Reversal Logic updated\(9)\(9)\(9)            Integratedev2                               Kalyan Neelam          2017-05-18
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC Prabhu ES                 2022-03-02            S2S Remediation                                           MSSQL ODBC conn added and other param changes  IntegrateDev5\(9)Ken Bradmon\(9)2022-06-03

# MAGIC This job Extracts the Claims data from Facets tables to generate the load file WORK_COMPNSTN_CLM_PROV.dat to be send to WORK_COMPNSTN_CLM_PROV table
# MAGIC Join CMC_CLCL_CLAIM, CMC_GRGR_GROUP for master extract and Look up on P_SEL_PRCS_CRITR for GRGR_ID
# MAGIC Generate the Seq. file to load into the DB2 table WORK_COMPNSTN_CLM_PROV
# MAGIC Generate the Seq. file to load into the DB2 table B_WORK_COMPNSTN_CLM_PROV
# MAGIC get the field CRT_RUN_CYC_EXCTN_SK from 
# MAGIC IDS DB2 table  WORK_COMPNSTN_CLM_PROV
# MAGIC Lookup IDS DB2 table  WORK_COMPNSTN_CLM_PROV and get the field CRT_RUN_CYC_EXCTN_SK
# MAGIC Reversal Logic
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

 
# -------------------------------------------------------------------------
# Retrieve job parameters
# -------------------------------------------------------------------------
FacetsOwner = get_widget_value('FacetsOwner','')
facetsowner_secret_name = get_widget_value('facetsowner_secret_name','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbsowner_secret_name = get_widget_value('bcbsowner_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
WorkCompOwner = get_widget_value('WorkCompOwner','')
workcompowner_secret_name = get_widget_value('workcompowner_secret_name','')
RunCycle = get_widget_value('RunCycle','')
Env = get_widget_value('Env','')
RunID = get_widget_value('RunID','')
DrivTable = get_widget_value('DrivTable','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

# -------------------------------------------------------------------------
# Stage: WorkCompnstnClmProv (DB2ConnectorPX) - Database: IDS
# -------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
query_WorkCompnstnClmProv = f"""
SELECT
    CLM_ID, 
    CLM_PROV_ROLE_TYP_CD,
    CRT_RUN_CYC_EXCTN_SK
FROM
    {WorkCompOwner}.WORK_COMPNSTN_CLM_PROV
WHERE 
    SRC_SYS_CD = 'FACETS'
"""
df_WorkCompnstnClmProv = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_WorkCompnstnClmProv)
    .load()
)

# -------------------------------------------------------------------------
# Stage: Ds_ReversalClaim (PxDataSet) -> read .ds as parquet
# -------------------------------------------------------------------------
df_Ds_ReversalClaim = spark.read.parquet(f"{adls_path}/ds/Clm_Reversals.parquet")
df_Ds_ReversalClaim = df_Ds_ReversalClaim.select("CLCL_ID","CLCL_CUR_STS","CLCL_PAID_DT")

# -------------------------------------------------------------------------
# Stage: CD_MPPNG (DB2ConnectorPX) - Database: IDS
# -------------------------------------------------------------------------
query_CdMppng = f"""
SELECT DISTINCT
    SRC_CD,
    TRGT_CD,
    CD_MPPNG_SK
FROM 
    {IDSOwner}.CD_MPPNG
WHERE
    SRC_SYS_CD = 'FACETS'
    AND SRC_CLCTN_CD = 'FACETS DBO'
    AND SRC_DOMAIN_NM IN ( 'CLAIM PROVIDER ROLE TYPE' )
    AND TRGT_CLCTN_CD = 'IDS'
    AND TRGT_DOMAIN_NM  IN ( 'CLAIM PROVIDER ROLE TYPE' )
"""
df_CdMppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_CdMppng)
    .load()
)

# -------------------------------------------------------------------------
# Stage: Prov (DB2ConnectorPX) - Database: IDS
# -------------------------------------------------------------------------
query_Prov = f"""
SELECT DISTINCT
     PROV.PROV_ID
    ,PROV.PROV_SK
    ,PROV.TAX_ID
FROM {IDSOwner}.PROV PROV
WHERE PROV.SRC_SYS_CD_SK = {SrcSysCdSk}
"""
df_Prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_Prov)
    .load()
)

# -------------------------------------------------------------------------
# Stage: FacetsDB_Input (ODBCConnectorPX)
# -------------------------------------------------------------------------
jdbc_url_facets, jdbc_props_facets = get_db_config(facetsowner_secret_name)
query_FacetsDB_Input = f"""
SELECT DISTINCT
    CMC_CLCL_CLAIM.CLCL_ID,
    CMC_CLCL_CLAIM.CLCL_PRPR_ID_PCP,
    CMC_CLCL_CLAIM.CLCL_PAYEE_PR_ID,
    CMC_CLCL_CLAIM.CLCL_PAY_PR_IND,
    CMC_CLCL_CLAIM.PRPR_ID
FROM {FacetsOwner}.CMC_CLCL_CLAIM CMC_CLCL_CLAIM
INNER JOIN tempdb..{DrivTable} Drvr
    ON Drvr.CLM_ID = CMC_CLCL_CLAIM.CLCL_ID
"""
df_FacetsDB_Input = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_FacetsDB_Input)
    .load()
)

# -------------------------------------------------------------------------
# Stage: Pivot_ProvID (PxPivot)
# -------------------------------------------------------------------------
df_Pivot_ProvID = df_FacetsDB_Input.select(
    "CLCL_ID",
    "CLCL_PAY_PR_IND",
    "CLCL_PRPR_ID_PCP",
    "CLCL_PAYEE_PR_ID",
    "PRPR_ID"
).select(
    "CLCL_ID",
    "CLCL_PAY_PR_IND",
    F.posexplode(F.array("CLCL_PRPR_ID_PCP","CLCL_PAYEE_PR_ID","PRPR_ID")).alias("Pivot_index","PROV_ID")
)

# -------------------------------------------------------------------------
# Stage: Xfm_Trim (CTransformerStage)
# -------------------------------------------------------------------------
df_Xfm_Trim = (
    df_Pivot_ProvID
    .withColumn("PROV_ID", F.when((F.col("PROV_ID").isNotNull()) | (trim(F.col("PROV_ID")) != ''), trim(F.col("PROV_ID"))).otherwise(''))
    .withColumn(
        "LKUP_CLM_PROV_ROLE_TYP_CD_SK",
        F.when(
            ((F.col("PROV_ID").isNotNull()) | (trim(F.col("PROV_ID")) != '')) & (F.col("Pivot_index") == 0),
            F.lit("PCP")
        )
        .when(
            ((F.col("PROV_ID").isNotNull()) | (trim(F.col("PROV_ID")) != ''))
            & (F.col("Pivot_index") == 1)
            & (F.col("CLCL_PAY_PR_IND") == 'P'),
            F.lit("PD")
        )
        .when(
            ((F.col("PROV_ID").isNotNull()) | (trim(F.col("PROV_ID")) != ''))
            & (F.col("Pivot_index") == 1)
            & (F.col("CLCL_PAY_PR_IND") != 'P'),
            F.lit("BILL")
        )
        .when(
            ((F.col("PROV_ID").isNotNull()) | (trim(F.col("PROV_ID")) != ''))
            & (F.col("Pivot_index") == 2),
            F.lit("SVC")
        )
        .otherwise('')
    )
    .select(
        F.col("CLCL_ID"),
        F.col("CLCL_PAY_PR_IND"),
        F.col("PROV_ID"),
        F.col("Pivot_index"),
        F.col("LKUP_CLM_PROV_ROLE_TYP_CD_SK")
    )
)

# -------------------------------------------------------------------------
# Stage: LkpTwoSets (PxLookup)
# -------------------------------------------------------------------------
df_LkpTwoSets = (
    df_Xfm_Trim.alias("Lnk_FacetsLkp")
    .join(
        df_Prov.alias("Lnk_Prov"),
        F.col("Lnk_FacetsLkp.PROV_ID") == F.col("Lnk_Prov.PROV_ID"),
        how="left"
    )
    .join(
        df_CdMppng.alias("Lnk_CdMppng"),
        F.col("Lnk_FacetsLkp.LKUP_CLM_PROV_ROLE_TYP_CD_SK") == F.col("Lnk_CdMppng.SRC_CD"),
        how="inner"
    )
    .selectExpr(
        "Lnk_FacetsLkp.CLCL_ID as CLCL_ID",
        "Lnk_CdMppng.TRGT_CD as CLM_PROV_ROLE_TYP_CD",
        "Lnk_Prov.PROV_SK as PROV_SK",
        "Lnk_CdMppng.CD_MPPNG_SK as CLM_PROV_ROLE_TYP_CD_SK",
        "Lnk_FacetsLkp.PROV_ID as PROV_ID",
        "Lnk_Prov.TAX_ID as TAX_ID"
    )
)

# -------------------------------------------------------------------------
# Stage: BusinessLogic (CTransformerStage)
# -------------------------------------------------------------------------
df_BusinessLogic_temp = (
    df_LkpTwoSets
    .withColumn("CLM_ID", trim(F.col("CLCL_ID")))
    .withColumn("CLM_PROV_ROLE_TYP_CD", F.when(F.col("CLM_PROV_ROLE_TYP_CD").isNull(), F.lit("")).otherwise(F.col("CLM_PROV_ROLE_TYP_CD")))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(RunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(RunCycle))
    .withColumn("PROV_SK", F.when(F.col("PROV_SK").isNull(), F.lit(0)).otherwise(F.col("PROV_SK")))
    .withColumn("CLM_PROV_ROLE_TYP_CD_SK", F.when(F.col("CLM_PROV_ROLE_TYP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("CLM_PROV_ROLE_TYP_CD_SK")))
    .withColumn("PROV_ID", F.col("PROV_ID"))
    .withColumn("TAX_ID", F.when(F.col("TAX_ID").isNull() | (trim(F.col("TAX_ID")) == ''), F.lit("Unknown")).otherwise(F.col("TAX_ID")))
)

df_BusinessLogic = df_BusinessLogic_temp.filter(
    (F.col("PROV_ID").isNotNull()) & (trim(F.col("PROV_ID")) != '')
).select(
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_SK",
    "CLM_PROV_ROLE_TYP_CD_SK",
    "PROV_ID",
    "TAX_ID"
)

# -------------------------------------------------------------------------
# Stage: Lkp_IDS (PxLookup)
# -------------------------------------------------------------------------
df_Lkp_IDS = (
    df_BusinessLogic.alias("Lnk_WorkCompnstnProv")
    .join(
        df_WorkCompnstnClmProv.alias("Lnk_WorkCompClmProv"),
        [
            F.col("Lnk_WorkCompnstnProv.CLM_ID") == F.col("Lnk_WorkCompClmProv.CLM_ID"),
            F.col("Lnk_WorkCompnstnProv.CLM_PROV_ROLE_TYP_CD") == F.col("Lnk_WorkCompClmProv.CLM_PROV_ROLE_TYP_CD")
        ],
        how="left"
    )
    .join(
        df_Ds_ReversalClaim.alias("Lnk_ReversalDs"),
        F.col("Lnk_WorkCompnstnProv.CLM_ID") == F.col("Lnk_ReversalDs.CLCL_ID"),
        how="left"
    )
    .selectExpr(
        "Lnk_WorkCompnstnProv.CLM_ID as CLM_ID",
        "Lnk_WorkCompnstnProv.CLM_PROV_ROLE_TYP_CD as CLM_PROV_ROLE_TYP_CD",
        "Lnk_WorkCompnstnProv.SRC_SYS_CD as SRC_SYS_CD",
        "Lnk_WorkCompClmProv.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK",
        "Lnk_WorkCompnstnProv.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK",
        "Lnk_WorkCompnstnProv.PROV_SK as PROV_SK",
        "Lnk_WorkCompnstnProv.CLM_PROV_ROLE_TYP_CD_SK as CLM_PROV_ROLE_TYP_CD_SK",
        "Lnk_WorkCompnstnProv.PROV_ID as PROV_ID",
        "Lnk_WorkCompnstnProv.TAX_ID as TAX_ID",
        "Lnk_ReversalDs.CLCL_ID as CLCL_ID"
    )
)

# -------------------------------------------------------------------------
# Stage: Xfm_Reversal (CTransformerStage)
# -------------------------------------------------------------------------
df_Xfm_Reversal_temp = df_Lkp_IDS.withColumn("CLM_ID_Out", F.col("CLM_ID")) \
    .withColumn("CLM_PROV_ROLE_TYP_CD_Out", F.col("CLM_PROV_ROLE_TYP_CD")) \
    .withColumn("SRC_SYS_CD_Out", F.col("SRC_SYS_CD")) \
    .withColumn("CRT_RUN_CYC_EXCTN_SK_Out", F.col("CRT_RUN_CYC_EXCTN_SK")) \
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK_Out", F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")) \
    .withColumn("PROV_SK_Out", F.col("PROV_SK")) \
    .withColumn("CLM_PROV_ROLE_TYP_CD_SK_Out", F.col("CLM_PROV_ROLE_TYP_CD_SK")) \
    .withColumn("PROV_ID_Out", F.col("PROV_ID")) \
    .withColumn("TAX_ID_Out", F.col("TAX_ID"))

df_Xfm_Reversal_Lnk_All = df_Xfm_Reversal_temp.selectExpr(
    "CLM_ID_Out as CLM_ID",
    "CLM_PROV_ROLE_TYP_CD_Out as CLM_PROV_ROLE_TYP_CD",
    "SRC_SYS_CD_Out as SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK_Out as CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK_Out as LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_SK_Out as PROV_SK",
    "CLM_PROV_ROLE_TYP_CD_SK_Out as CLM_PROV_ROLE_TYP_CD_SK",
    "PROV_ID_Out as PROV_ID",
    "TAX_ID_Out as TAX_ID"
)

df_Xfm_Reversal_Lnk_Reversed_temp = df_Xfm_Reversal_temp.filter(
    (F.col("CLCL_ID").isNotNull()) & (trim(F.col("CLCL_ID")) != '')
)

df_Xfm_Reversal_Lnk_Reversed = df_Xfm_Reversal_Lnk_Reversed_temp.withColumn(
    "CLM_ID_Reversed", F.concat(trim(F.col("CLCL_ID")), F.lit("R"))
).selectExpr(
    "CLM_ID_Reversed as CLM_ID",
    "CLM_PROV_ROLE_TYP_CD_Out as CLM_PROV_ROLE_TYP_CD",
    "SRC_SYS_CD_Out as SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK_Out as CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK_Out as LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_SK_Out as PROV_SK",
    "CLM_PROV_ROLE_TYP_CD_SK_Out as CLM_PROV_ROLE_TYP_CD_SK",
    "PROV_ID_Out as PROV_ID",
    "TAX_ID_Out as TAX_ID"
)

# -------------------------------------------------------------------------
# Stage: Fnl_All_Reverse (PxFunnel)
# -------------------------------------------------------------------------
df_Xfm_Reversal_Lnk_All_sel = df_Xfm_Reversal_Lnk_All.select(
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_SK",
    "CLM_PROV_ROLE_TYP_CD_SK",
    "PROV_ID",
    "TAX_ID"
)

df_Xfm_Reversal_Lnk_Reversed_sel = df_Xfm_Reversal_Lnk_Reversed.select(
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_SK",
    "CLM_PROV_ROLE_TYP_CD_SK",
    "PROV_ID",
    "TAX_ID"
)

df_Fnl_All_Reverse = df_Xfm_Reversal_Lnk_All_sel.unionByName(df_Xfm_Reversal_Lnk_Reversed_sel)

# -------------------------------------------------------------------------
# Stage: Xfm_BusinessLogic (CTransformerStage)
# -------------------------------------------------------------------------
df_Xfm_BusinessLogic_temp = df_Fnl_All_Reverse.withColumn(
    "CRT_RUN_CYC_EXCTN_SK_new",
    F.when(
        (F.col("CRT_RUN_CYC_EXCTN_SK").isNull()) | (F.col("CRT_RUN_CYC_EXCTN_SK") == 0),
        F.lit(RunCycle)
    ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
)

df_Xfm_BusinessLogic_Lnk_WorkCompnstnProv = df_Xfm_BusinessLogic_temp.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK_new").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_SK").alias("PROV_SK"),
    F.col("CLM_PROV_ROLE_TYP_CD_SK").alias("CLM_PROV_ROLE_TYP_CD_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("TAX_ID").alias("TAX_ID")
)

df_Xfm_BusinessLogic_LnkBalWorkCompnstnProv = df_Fnl_All_Reverse.select(
    F.rpad(trim(F.col("CLM_ID")),12," ").alias("CLM_ID"),
    F.when(F.col("CLM_PROV_ROLE_TYP_CD").isNull(),F.lit("")).otherwise(F.col("CLM_PROV_ROLE_TYP_CD")).alias("CLM_PROV_ROLE_TYP_CD"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD")
)

# -------------------------------------------------------------------------
# Stage: B_WORK_COMPNSTN_CLM_PROV (PxSequentialFile) - Write
# -------------------------------------------------------------------------
df_B_WORK_COMPNSTN_CLM_PROV_write = df_Xfm_BusinessLogic_LnkBalWorkCompnstnProv.select(
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "SRC_SYS_CD"
)
write_files(
    df_B_WORK_COMPNSTN_CLM_PROV_write,
    f"{adls_path}/load/B_WORK_COMPNSTN_CLM_PROV.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)

# -------------------------------------------------------------------------
# Stage: WORK_COMPNSTN_CLM_PROV (PxSequentialFile) - Write
# -------------------------------------------------------------------------
df_WORK_COMPNSTN_CLM_PROV_write = (
    df_Xfm_BusinessLogic_Lnk_WorkCompnstnProv
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"),12," "))
    .withColumn("PROV_ID", F.rpad(F.col("PROV_ID"),12," "))
    .select(
        "CLM_ID",
        "CLM_PROV_ROLE_TYP_CD",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PROV_SK",
        "CLM_PROV_ROLE_TYP_CD_SK",
        "PROV_ID",
        "TAX_ID"
    )
)
write_files(
    df_WORK_COMPNSTN_CLM_PROV_write,
    f"{adls_path}/load/WORK_COMPNSTN_CLM_PROV.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)