# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2017 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsIdsWorkCompClmLnDsalwExtr
# MAGIC CALLED BY:       FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:   Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM_LN_DSALW  table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer          Date              Project                                                                 Change Description                                           Development Project    Code Reviewer          Date Reviewed       
# MAGIC ------------------        -------------------   --------------------------------------------------------------------------  -----------------------------------------------------------------------   ----------------------------------   -------------------------------   ----------------------------       
# MAGIC Kailashnath J     2017-01-18\(9)5628 WORK_COMPNSTN_CLM_LN_DSALW  Original Programming\(9)\(9)\(9)      IntegrateDev2               Hugh Sisson              2017-03-17
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC 
# MAGIC Akhila M            2017-05-09\(9)5628 WORK_COMPNSTN_CLM \(9)              Reversal Logic updated\(9)\(9)\(9) Integratedev2       Kalyan Neelam        2017-05-22                     
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC Prabhu ES         2022-03-02            S2S Remediation                                        MSSQL ODBC conn added and other param changes  IntegrateDev5\(9)Ken Bradmon\(9)2022-06-03

# MAGIC This job Extracts the Claims data from Facets tables to generate the load file WORK_COMPNSTN_CLM_LN_DSALW.dat  to to load WORK_COMPNSTN_CLM_LN_DSALW  table
# MAGIC Join CMC_CDMD_LI_DISALL, CMC_CLCL_CLAIM , CMC_GRGR_GROUP, CMC_MEME_MEMBER for master extract and Look up on P_SEL_PRCS_CRITR for GRGR_ID
# MAGIC Seq. file to load into the DB2 table B_WORK_COMPNSTN_CLM_LN_DSALW for Balancing Report.
# MAGIC Seq. file to load into the DB2 table WORK_COMPNSTN_CLM_LN_DSALW
# MAGIC Reversal Logic
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, trim, coalesce, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

jdbc_url = None
jdbc_props = None

FacetsOwner = get_widget_value('$FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
WorkCompOwner = get_widget_value('$WorkCompOwner','')
workcomp_secret_name = get_widget_value('workcomp_secret_name','')
RunCycle = get_widget_value('RunCycle','')
Env = get_widget_value('Env','')
DrivTable = get_widget_value('DrivTable','')
SrcSysCd = get_widget_value('SrcSysCd','')

# Stage: IDS_EXCD
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT DISTINCT TRIM(EXCD_ID) AS EXCD_ID, EXCD_SK FROM {IDSOwner}.EXCD"
df_IDS_EXCD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: Ds_ReversalClaim (.ds -> .parquet)
schema_Ds_ReversalClaim = StructType([
    StructField("CLCL_ID", StringType(), True),
    StructField("CLCL_CUR_STS", StringType(), True),
    StructField("CLCL_PAID_DT", StringType(), True)
])
df_Ds_ReversalClaim = spark.read.schema(schema_Ds_ReversalClaim).parquet(f"{adls_path}/ds/Clm_Reversals.parquet")

# Stage: IDS_WORK_COMPNSTN_CLM_LN_DSALW
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT DISTINCT CLM_ID, CRT_RUN_CYC_EXCTN_SK FROM {WorkCompOwner}.WORK_COMPNSTN_CLM_LN_DSALW WHERE SRC_SYS_CD='FACETS'"
df_IDS_WORK_COMPNSTN_CLM_LN_DSALW = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: Facets_CMC_CDMD_LI_DISALL (ODBC)
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = (
    f"SELECT DISTINCT CMC_CDMD_LI_DISALL.CLCL_ID, CMC_CDMD_LI_DISALL.CDML_SEQ_NO, "
    f"CMC_CDMD_LI_DISALL.CDMD_TYPE, CMC_CDMD_LI_DISALL.CDMD_DISALL_AMT, CMC_CDMD_LI_DISALL.EXCD_ID "
    f"FROM {FacetsOwner}.CMC_CDMD_LI_DISALL CMC_CDMD_LI_DISALL "
    f"INNER JOIN tempdb..{DrivTable} Drvr ON Drvr.CLM_ID=CMC_CDMD_LI_DISALL.CLCL_ID"
)
df_Facets_CMC_CDMD_LI_DISALL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: IDS_CD_MPPNG
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    f"SELECT DISTINCT SRC_CD, TRGT_CD, CD_MPPNG_SK, SRC_DOMAIN_NM, TRGT_DOMAIN_NM "
    f"FROM {IDSOwner}.CD_MPPNG "
    f"WHERE SRC_SYS_CD = 'FACETS' "
    f"AND SRC_CLCTN_CD = 'FACETS DBO' "
    f"AND SRC_DOMAIN_NM IN ( 'CLAIM LINE DISALLOW TYPE' , 'DISALLOW TYPE CATEGORY') "
    f"AND TRGT_CLCTN_CD = 'IDS' "
    f"AND TRGT_DOMAIN_NM IN ( 'CLAIM LINE DISALLOW TYPE' , 'DISALLOW TYPE CATEGORY')"
)
df_IDS_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: Flt_CdMppng
df_Flt_CdMppng_input = df_IDS_CD_MPPNG
df_Lnk_ClmLnDsalwTypCd = df_Flt_CdMppng_input.filter(
    (col("SRC_DOMAIN_NM") == "CLAIM LINE DISALLOW TYPE") & (col("TRGT_DOMAIN_NM") == "CLAIM LINE DISALLOW TYPE")
).select(
    col("SRC_CD").alias("SRC_CD"),
    col("TRGT_CD").alias("TRGT_CD")
)
df_Lnk_ClmLnDsalwTypCatCdSk = df_Flt_CdMppng_input.filter(
    (col("SRC_DOMAIN_NM") == "DISALLOW TYPE CATEGORY") & (col("TRGT_DOMAIN_NM") == "DISALLOW TYPE CATEGORY")
).select(
    col("SRC_CD").alias("SRC_CD"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# Stage: Lkp_Cd_Mppng
df_Lkp_Cd_Mppng = (
    df_Facets_CMC_CDMD_LI_DISALL.alias("Lnk_FctsInp")
    .join(
        df_IDS_WORK_COMPNSTN_CLM_LN_DSALW.alias("LnkWorkCompnstnClmLnDsalw"),
        col("Lnk_FctsInp.CLCL_ID") == col("LnkWorkCompnstnClmLnDsalw.CLM_ID"),
        how="left"
    )
    .join(
        df_Lnk_ClmLnDsalwTypCd.alias("Lnk_ClmLnDsalwTypCd"),
        col("Lnk_FctsInp.CDMD_TYPE") == col("Lnk_ClmLnDsalwTypCd.SRC_CD"),
        how="inner"
    )
    .join(
        df_Lnk_ClmLnDsalwTypCatCdSk.alias("Lnk_ClmLnDsalwTypCatCdSk"),
        col("Lnk_FctsInp.CDMD_TYPE") == col("Lnk_ClmLnDsalwTypCatCdSk.SRC_CD"),
        how="left"
    )
    .select(
        col("Lnk_FctsInp.EXCD_ID").alias("EXCD_ID"),
        col("Lnk_FctsInp.CLCL_ID").alias("CLCL_ID"),
        col("Lnk_FctsInp.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        col("Lnk_FctsInp.CDMD_TYPE").alias("CDMD_TYPE"),
        col("Lnk_FctsInp.CDMD_DISALL_AMT").alias("CDMD_DISALL_AMT"),
        col("Lnk_ClmLnDsalwTypCd.TRGT_CD").alias("TRGT_CD"),
        col("Lnk_ClmLnDsalwTypCatCdSk.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        col("LnkWorkCompnstnClmLnDsalw.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
    )
)

# Stage: Lkp_Excd
df_FinalLkp_Excd = (
    df_Lkp_Cd_Mppng.alias("Lnk_To_Excd")
    .join(
        df_IDS_EXCD.alias("Lnk_IDS_Excd"),
        col("Lnk_To_Excd.EXCD_ID") == col("Lnk_IDS_Excd.EXCD_ID"),
        how="left"
    )
    .join(
        df_Ds_ReversalClaim.alias("Lnk_ReversalDs"),
        col("Lnk_To_Excd.CLCL_ID") == col("Lnk_ReversalDs.CLCL_ID"),
        how="left"
    )
    .select(
        col("Lnk_To_Excd.CLCL_ID").alias("CLCL_ID"),
        col("Lnk_To_Excd.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        col("Lnk_To_Excd.TRGT_CD").alias("TRGT_CD"),
        col("Lnk_To_Excd.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("Lnk_IDS_Excd.EXCD_SK").alias("EXCD_SK"),
        col("Lnk_To_Excd.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        col("Lnk_To_Excd.CDMD_DISALL_AMT").alias("CDMD_DISALL_AMT"),
        col("Lnk_To_Excd.EXCD_ID").alias("EXCD_ID"),
        col("Lnk_ReversalDs.CLCL_ID").alias("CLCL_ID_Reverse"),
        col("Lnk_To_Excd.EXCD_ID").alias("EXCD_ID_original"),
        col("Lnk_To_Excd.CLCL_ID").alias("CLCL_ID_original"),
        col("Lnk_To_Excd.CDML_SEQ_NO").alias("CDML_SEQ_NO_original"),
        col("Lnk_To_Excd.CDMD_TYPE").alias("CDMD_TYPE_original"),
        col("Lnk_To_Excd.CDMD_DISALL_AMT").alias("CDMD_DISALL_AMT_original"),
        col("Lnk_To_Excd.TRGT_CD").alias("TRGT_CD_original"),
        col("Lnk_To_Excd.CD_MPPNG_SK").alias("CD_MPPNG_SK_original"),
        col("Lnk_To_Excd.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK_original")
    )
)

df_Lkp_Excd_Lnk_ReverseLogic = df_FinalLkp_Excd.select(
    col("CLCL_ID").alias("CLM_ID"),
    col("CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("TRGT_CD").alias("CLM_LN_DSALW_TYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("EXCD_SK").alias("CLM_LN_DSALW_EXCD_SK"),
    col("CD_MPPNG_SK").alias("CLM_LN_DSALW_TYP_CAT_CD_SK"),
    col("CDMD_DISALL_AMT").alias("DSALW_AMT"),
    col("EXCD_ID").alias("EXCD_ID"),
    col("CLCL_ID_Reverse").alias("CLCL_ID_Reverse")
)

df_Lkp_Excd_Lnk_From_Excd = df_FinalLkp_Excd.select(
    col("EXCD_ID_original").alias("EXCD_ID"),
    col("CLCL_ID_original").alias("CLCL_ID"),
    col("CDML_SEQ_NO_original").alias("CDML_SEQ_NO"),
    col("CDMD_TYPE_original").alias("CDMD_TYPE"),
    col("CDMD_DISALL_AMT_original").alias("CDMD_DISALL_AMT"),
    col("TRGT_CD_original").alias("TRGT_CD"),
    col("CD_MPPNG_SK_original").alias("CD_MPPNG_SK"),
    col("CRT_RUN_CYC_EXCTN_SK_original").alias("CRT_RUN_CYC_EXCTN_SK")
)

# Stage: Seq_Excd_Rej
df_Seq_Excd_Rej_final = df_Lkp_Excd_Lnk_From_Excd.select(
    col("EXCD_ID"),
    col("CLCL_ID"),
    col("CDML_SEQ_NO"),
    col("CDMD_TYPE"),
    col("CDMD_DISALL_AMT"),
    col("TRGT_CD"),
    col("CD_MPPNG_SK"),
    col("CRT_RUN_CYC_EXCTN_SK")
)
df_Seq_Excd_Rej_final = df_Seq_Excd_Rej_final.withColumn("EXCD_ID", rpad(col("EXCD_ID"), 3, " "))
df_Seq_Excd_Rej_final = df_Seq_Excd_Rej_final.withColumn("CLCL_ID", rpad(col("CLCL_ID"), 12, " "))
df_Seq_Excd_Rej_final = df_Seq_Excd_Rej_final.withColumn("CDMD_TYPE", rpad(col("CDMD_TYPE"), 2, " "))
write_files(
    df_Seq_Excd_Rej_final,
    f"{adls_path_publish}/external/IDS_EXCD_REJ.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)

# Stage: Xfm_Reversal
df_Xfm_Reversal_input = df_Lkp_Excd_Lnk_ReverseLogic

df_Xfm_Reversal_Lnk_All = df_Xfm_Reversal_input.select(
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("CLM_LN_DSALW_EXCD_SK"),
    col("CLM_LN_DSALW_TYP_CAT_CD_SK"),
    col("DSALW_AMT"),
    col("EXCD_ID"),
    lit("").alias("CLCL_ID_Reverse")
)

df_Xfm_Reversal_Lnk_Reversal = df_Xfm_Reversal_input.filter(
    trim(coalesce(col("CLCL_ID_Reverse"), lit(""))) != ""
).select(
    rpad(trim(col("CLM_ID")), 0, "").alias("CLM_ID")  # to ensure trim before concat
    .alias("CLM_ID"),  # will replace below
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("CLM_LN_DSALW_EXCD_SK"),
    col("CLM_LN_DSALW_TYP_CAT_CD_SK"),
    col("DSALW_AMT"),
    col("EXCD_ID"),
    col("CLCL_ID_Reverse")
)

# Fixing "Trim(Lnk_ReverseLogic.CLM_ID) : 'R'"
# Replace with direct expression:
df_Xfm_Reversal_Lnk_Reversal = df_Xfm_Reversal_Lnk_Reversal.select(
    trim(col("CLM_ID")).alias("CLM_ID_temp"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("CLM_LN_DSALW_EXCD_SK"),
    col("CLM_LN_DSALW_TYP_CAT_CD_SK"),
    col("DSALW_AMT"),
    col("EXCD_ID"),
    col("CLCL_ID_Reverse")
).withColumn("CLM_ID", col("CLM_ID_temp").substr(1, 999999) + lit("R")).drop("CLM_ID_temp")

# Stage: Fnl_All_Reverse (Funnel)
commonCols = [
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_DSALW_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "CLM_LN_DSALW_EXCD_SK",
    "CLM_LN_DSALW_TYP_CAT_CD_SK",
    "DSALW_AMT",
    "EXCD_ID",
    "CLCL_ID_Reverse"
]
df_Fnl_All_Reverse_Lnk_All = df_Xfm_Reversal_Lnk_All.select(commonCols)
df_Fnl_All_Reverse_Lnk_Reversal = df_Xfm_Reversal_Lnk_Reversal.select(commonCols)
df_Fnl_All_Reverse = df_Fnl_All_Reverse_Lnk_All.unionByName(df_Fnl_All_Reverse_Lnk_Reversal)

df_Fnl_All_Reverse_Lnk_BusinessLogic = df_Fnl_All_Reverse

# Stage: Xfm_BusinessLogic
df_Xfm_BusinessLogic_input = df_Fnl_All_Reverse_Lnk_BusinessLogic

df_Lnk_B_WorkCompnstnClmLnDsalw = df_Xfm_BusinessLogic_input.select(
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
    lit(SrcSysCd).alias("SRC_SYS_CD")
)

df_Lnk_WorkCompnstnClmLnDsalw = df_Xfm_BusinessLogic_input.select(
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    when(coalesce(col("CRT_RUN_CYC_EXCTN_SK"), lit(0)) == 0, lit(RunCycle))
    .otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
    .alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(col("CLM_LN_DSALW_EXCD_SK").isNull(), lit(0))
    .otherwise(col("CLM_LN_DSALW_EXCD_SK"))
    .alias("CLM_LN_DSALW_EXCD_SK"),
    when(
        col("CLM_LN_DSALW_TYP_CAT_CD_SK").isNull() | (trim(col("CLM_LN_DSALW_TYP_CAT_CD_SK")) == ""),
        lit(1)
    )
    .otherwise(col("CLM_LN_DSALW_TYP_CAT_CD_SK"))
    .alias("CLM_LN_DSALW_TYP_CAT_CD_SK"),
    when(trim(coalesce(col("CLCL_ID_Reverse"), lit(""))) != "", -col("DSALW_AMT"))
    .otherwise(col("DSALW_AMT"))
    .alias("DSALW_AMT")
)

# Stage: WORK_COMPNSTN_CLM_LN_DSALW
df_WORK_COMPNSTN_CLM_LN_DSALW_final = df_Lnk_WorkCompnstnClmLnDsalw.select(
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_CD"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_LN_DSALW_EXCD_SK"),
    col("CLM_LN_DSALW_TYP_CAT_CD_SK"),
    col("DSALW_AMT")
)
write_files(
    df_WORK_COMPNSTN_CLM_LN_DSALW_final,
    f"{adls_path}/load/WORK_COMPNSTN_CLM_LN_DSALW.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)

# Stage: B_WORK_COMPNSTN_CLM_LN_DSALW
df_B_WORK_COMPNSTN_CLM_LN_DSALW_final = df_Lnk_B_WorkCompnstnClmLnDsalw.select(
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_CD"),
    col("SRC_SYS_CD")
)
write_files(
    df_B_WORK_COMPNSTN_CLM_LN_DSALW_final,
    f"{adls_path}/load/B_WORK_COMPNSTN_CLM_LN_DSALW.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)