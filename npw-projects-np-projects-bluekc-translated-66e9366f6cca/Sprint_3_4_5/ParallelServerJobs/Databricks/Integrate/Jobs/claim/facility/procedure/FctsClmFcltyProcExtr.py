# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CLHI_PROC to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CMC_CLHI_PROC
# MAGIC                 Joined With
# MAGIC                 CMC_CLCL_CLAIM to pull rows based on dates.
# MAGIC   
# MAGIC HASH FILES:  hf_clm_nasco_dup_bypass
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   STRIP.FIELD
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC  
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Steph Goddard  04/2004-   Originally Programmed
# MAGIC              Steph Goddard  07/21/2004    Added strip of decimal point for procedure code - it wasn't matching up with proc code table to find SK
# MAGIC               Steph Goddard  02/14/2006  Added transform, primary key for sequencer
# MAGIC          BJ Luce            03/20/2006     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC               Steph Goddard 03/30/2006  select first five characters of ipcd_id for proc code instead of all of them
# MAGIC              Sanderw  12/08/2006   Project 1756  - Reversal logix added for new status codes 89 and  and 99
# MAGIC              BhoomiD  - 04/10/2007 Added new field PROC_CD_MOD_TX, retrieving last 2 characters of IPCD_CD. Made change to container also.
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Oliver Nielsen          08/15/2007       Balancing              Added Snapshot extract for balancing                                        devlIDS30                       Steph Goddard          8/30/07
# MAGIC 
# MAGIC 
# MAGIC Bhoomi Dasari         2008-07-31      3567(Primary Key)    Changed primary key process from hash file to DB2 table            devlIDS                          Steph Goddard         08/07/2008 
# MAGIC 
# MAGIC Steph Goddard       10/13/2010      TTR-974                 added check for length for modifier, removed fields not needed  IntegrateNewDevl
# MAGIC                                                                                         added hash files between transforms & collector
# MAGIC Rick Henry              04/27/2012      4896                       Changed source for Proc_Cd_Typ_Cd for SK lookup                  IntegrateNewDevl            Sharon Adnrew     2012-05-20
# MAGIC 
# MAGIC Raja Gummadi        12/17/2012       TTR-1507              Fixed code to handle Null Values for PROC_CD_MOD_TX         IntegrateNewDevl           Bhoomi Dasari        12/20/2012
# MAGIC                                                                                         in reversals
# MAGIC 
# MAGIC Manasa Andru        09/17/2013       TTR - 1413            Updated the logic in PROC_CD and PROC_CD_MOD_TX         IntegrateNewDevl          Bhoomi Dasari        9/23/2013
# MAGIC                                                                                                      fields as per the new mapping rules.
# MAGIC 
# MAGIC Shanmugam A        2018-03-08       TFS-21148            Updated the logic in PROC_CD_TYPE_CD                                    IntegrateDev1	   Jaideep Mankala    03/13/2018
# MAGIC Prabhu ES              2022-02-28        S2S Remediation  MSSQL connection parameters added                                           IntegrateDev5          Manasa Andru           2022-06-10

# MAGIC Run STRIP.FIELD transform on fields that might have Carriage Return, Line Feed, and  Tab
# MAGIC Pulling Facets Claim Procedure Data for a specific Time Period
# MAGIC Hash file (hf_fclty_proc_allcol) cleared from the shared container FcltyClmProcPK
# MAGIC This container is used in:
# MAGIC FctsClmFcltyProcExtr
# MAGIC NascoClmFcltyProcTrns
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/FcltyClmProcPK
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_PROC_CD_ProcCd = f"SELECT PROC_CD, PROC_CD_TYP_CD FROM {IDSOwner}.PROC_CD WHERE PROC_CD_CAT_CD = 'MED'"
df_PROC_CD_ProcCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_PROC_CD_ProcCd)
    .load()
)
df_PROC_CD_ProcCd = df_PROC_CD_ProcCd.select(
    F.rpad(F.col("PROC_CD"), <...>, " ").alias("PROC_CD"),
    F.rpad(F.col("PROC_CD_TYP_CD"), <...>, " ").alias("PROC_CD_TYP_CD")
)

extract_query_PROC_CD_ProcCd2 = f"SELECT PROC_CD, PROC_CD_TYP_CD FROM {IDSOwner}.PROC_CD WHERE PROC_CD_CAT_CD = 'MED' AND PROC_CD_TYP_CD in ('CPT4','HCPCS')"
df_PROC_CD_ProcCd2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_PROC_CD_ProcCd2)
    .load()
)
df_PROC_CD_ProcCd2 = df_PROC_CD_ProcCd2.select(
    F.rpad(F.col("PROC_CD"), <...>, " ").alias("PROC_CD"),
    F.rpad(F.col("PROC_CD_TYP_CD"), <...>, " ").alias("PROC_CD_TYP_CD")
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_CMC_CLHI_PROC = (
    f"SELECT A.CLCL_ID, A.CLHI_TYPE, A.IPCD_ID, A.CLHI_IP_DT, B.IPCD_TYPE "
    f"FROM {FacetsOwner}.CMC_CLHI_PROC A, tempdb..{DriverTable} TMP, {FacetsOwner}.CMC_IPCD_PROC_CD B "
    f"WHERE TMP.CLM_ID = A.CLCL_ID AND A.IPCD_ID = B.IPCD_ID"
)
df_CMC_CLHI_PROC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CLHI_PROC)
    .load()
)

df_CMC_CLHI_PROC = df_CMC_CLHI_PROC.select(
    F.col("CLCL_ID"),
    F.col("CLHI_TYPE"),
    F.col("IPCD_ID"),
    F.col("CLHI_IP_DT"),
    F.col("IPCD_TYPE")
)

df_CMC_CLHI_PROC = (
    df_CMC_CLHI_PROC
    .withColumn("CLCL_ID", strip_field(F.col("CLCL_ID")))
    .withColumn("CLHI_TYPE", strip_field(F.col("CLHI_TYPE")))
    .withColumn("IPCD_ID", strip_field(F.col("IPCD_ID")))
    .withColumn("IPCD_TYPE", strip_field(F.col("IPCD_TYPE")))
    .withColumn("CLCL_ID", F.rpad(F.col("CLCL_ID"), 12, " "))
    .withColumn("CLHI_TYPE", F.rpad(F.col("CLHI_TYPE"), 2, " "))
    .withColumn("IPCD_ID", F.rpad(F.col("IPCD_ID"), 7, " "))
    .withColumn("IPCD_TYPE", F.rpad(F.col("IPCD_TYPE"), 1, " "))
)

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_hf_clm_fcts_reversals = df_hf_clm_fcts_reversals.select(
    F.rpad(F.col("CLCL_ID"),12," ").alias("CLCL_ID"),
    F.rpad(F.col("CLCL_CUR_STS"),2," ").alias("CLCL_CUR_STS"),
    F.col("CLCL_PAID_DT").alias("CLCL_PAID_DT"),
    F.rpad(F.col("CLCL_ID_ADJ_TO"),12," ").alias("CLCL_ID_ADJ_TO"),
    F.rpad(F.col("CLCL_ID_ADJ_FROM"),12," ").alias("CLCL_ID_ADJ_FROM")
)

df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
df_clm_nasco_dup_bypass = df_clm_nasco_dup_bypass.select(
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID")
)

df_hf_fcltyproc_proccdcatcd = spark.read.parquet(f"{adls_path}/hf_fcltyproc_proccdcatcd.parquet")
df_hf_fcltyproc_proccdcatcd = df_hf_fcltyproc_proccdcatcd.select(
    F.rpad(F.col("PROC_CD"), <...>, " ").alias("PROC_CD"),
    F.rpad(F.col("PROC_CD_TYP_CD"), <...>, " ").alias("PROC_CD_TYP_CD")
)

df_hf_fcltyproc_proccdtypcd = spark.read.parquet(f"{adls_path}/hf_fcltyproc_proccdtypcd.parquet")
df_hf_fcltyproc_proccdtypcd = df_hf_fcltyproc_proccdtypcd.select(
    F.rpad(F.col("PROC_CD"), <...>, " ").alias("PROC_CD"),
    F.rpad(F.col("PROC_CD_TYP_CD"), <...>, " ").alias("PROC_CD_TYP_CD")
)

df_StripField = df_CMC_CLHI_PROC

df_business_logic = (
    df_StripField.alias("Strip")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
    .join(
        df_hf_fcltyproc_proccdcatcd.alias("ProcCdCatCd"),
        F.trim(F.col("Strip.IPCD_ID")) == F.col("ProcCdCatCd.PROC_CD"),
        "left"
    )
    .join(
        df_hf_fcltyproc_proccdtypcd.alias("ProcCdTypCd"),
        F.trim(F.col("Strip.IPCD_ID").substr(1, 5)) == F.col("ProcCdTypCd.PROC_CD"),
        "left"
    )
)

df_business_logic = df_business_logic.withColumn("svIpcdcd", trim(F.col("Strip.IPCD_ID")))

df_business_logic = df_business_logic.withColumn(
    "ProcCdMod",
    F.substring(F.col("svIpcdcd"), F.length(F.col("svIpcdcd")) - 1, 2)
)

df_business_logic = df_business_logic.withColumn(
    "ClmId",
    trim(F.col("Strip.CLCL_ID"))
)

df_business_logic = df_business_logic.withColumn(
    "NoMEDMatch",
    F.when(
        (F.length(trim(F.col("Strip.IPCD_ID"))) > 5)
        & (F.col("ProcCdTypCd.PROC_CD").isNotNull()),
        F.col("ProcCdTypCd.PROC_CD")
    ).otherwise("UNK")
)

df_business_logic = df_business_logic.withColumn(
    "MEDMatch",
    F.when(
        F.col("ProcCdCatCd.PROC_CD").isNotNull(),
        F.col("ProcCdCatCd.PROC_CD")
    ).otherwise(F.col("NoMEDMatch"))
)

df_business_logic = df_business_logic.withColumn(
    "ModTx2",
    F.when(
        (F.col("Strip.IPCD_ID").isNull()) | (F.length(F.col("Strip.IPCD_ID")) == 0),
        F.lit("  ")
    ).otherwise(
        F.when(
            (F.col("ProcCdCatCd.PROC_CD_TYP_CD") == F.lit("ICD10")) | (F.col("NoMEDMatch") == F.lit("UNK")),
            F.lit("  ")
        ).otherwise(F.col("Strip.IPCD_ID"))
    )
)

df_FctsClmProc = df_business_logic.filter(
    "nasco_dup_lkup.CLM_ID IS NULL"
)

df_FctsClmProc = df_FctsClmProc.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"),1," ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"),1," ").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"),F.lit(";"),F.col("ClmId"),F.lit(";"),trim(F.col("Strip.CLHI_TYPE"))).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROC_SK"),
    F.col("ClmId").alias("CLM_ID"),
    F.rpad(trim(F.col("Strip.CLHI_TYPE")),2," ").alias("FCLTY_CLM_PROC_ORDNL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.when(
        (F.col("Strip.IPCD_ID").isNull()) | (F.length(F.col("Strip.IPCD_ID")) == 0),
        F.lit("NA")
    ).otherwise(F.col("MEDMatch")).alias("PROC_CD"),
    F.expr(
        """FORMAT_DATE('yyyy-MM-dd', Strip.CLHI_IP_DT)"""
    ).alias("PROC_DT"),
    F.when(
        (F.col("Strip.IPCD_TYPE").isNull()) | (F.length(trim(F.col("Strip.IPCD_TYPE"))) == 0),
        F.when(
            (F.col("ProcCdCatCd.PROC_CD_TYP_CD").isNull()) | (F.length(trim(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))) == 0),
            F.col("ProcCdTypCd.PROC_CD_TYP_CD")
        ).otherwise(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))
    ).otherwise(
        F.when(
            F.col("Strip.IPCD_TYPE") == F.lit("Y"),
            F.lit("ICD10")
        ).otherwise(
            F.when(
                (F.col("ProcCdCatCd.PROC_CD_TYP_CD").isNull()) | (F.length(trim(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))) == 0),
                F.col("ProcCdTypCd.PROC_CD_TYP_CD")
            ).otherwise(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))
        )
    ).alias("PROC_TYPE_CD"),
    F.rpad(
        F.when(
            F.col("ModTx2") == F.lit("  "),
            F.lit("  ")
        ).otherwise(
            F.when(F.length(F.col("Strip.IPCD_ID")) >= 7, F.col("Strip.IPCD_ID").substr(6,2)).otherwise(F.lit("  "))
        ),2," "
    ).alias("PROC_CD_MOD_TX"),
    F.lit("MED").alias("PROC_CD_CAT_CD")
)

df_reversals = df_business_logic.filter(
    "(fcts_reversals.CLCL_ID IS NOT NULL) AND (fcts_reversals.CLCL_CUR_STS IN ('89','91','99'))"
)

df_reversals = df_reversals.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"),1," ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"),1," ").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"),F.lit(";"),F.col("ClmId"),F.lit("R;"),trim(F.col("Strip.CLHI_TYPE"))).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROC_SK"),
    F.concat(F.col("ClmId"),F.lit("R")).alias("CLM_ID"),
    F.rpad(trim(F.col("Strip.CLHI_TYPE")),2," ").alias("FCLTY_CLM_PROC_ORDNL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.when(
        (F.col("Strip.IPCD_ID").isNull()) | (F.length(F.col("Strip.IPCD_ID")) == 0),
        F.lit("NA")
    ).otherwise(F.col("MEDMatch")).alias("PROC_CD"),
    F.expr(
        """FORMAT_DATE('yyyy-MM-dd', Strip.CLHI_IP_DT)"""
    ).alias("PROC_DT"),
    F.when(
        (F.col("Strip.IPCD_TYPE").isNull()) | (F.length(trim(F.col("Strip.IPCD_TYPE"))) == 0),
        F.when(
            (F.col("ProcCdCatCd.PROC_CD_TYP_CD").isNull()) | (F.length(trim(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))) == 0),
            F.col("ProcCdTypCd.PROC_CD_TYP_CD")
        ).otherwise(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))
    ).otherwise(
        F.when(
            F.col("Strip.IPCD_TYPE") == F.lit("Y"),
            F.lit("ICD10")
        ).otherwise(
            F.when(
                (F.col("ProcCdCatCd.PROC_CD_TYP_CD").isNull()) | (F.length(trim(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))) == 0),
                F.col("ProcCdTypCd.PROC_CD_TYP_CD")
            ).otherwise(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))
        )
    ).alias("PROC_TYPE_CD"),
    F.rpad(
        F.when(
            F.col("ModTx2") == F.lit("  "),
            F.lit("  ")
        ).otherwise(
            F.when(F.length(F.col("Strip.IPCD_ID")) >= 7, F.col("Strip.IPCD_ID").substr(6,2)).otherwise(F.lit("  "))
        ),2," "
    ).alias("PROC_CD_MOD_TX"),
    F.lit("MED").alias("PROC_CD_CAT_CD")
)

df_hf_fcltyclmproc_col1 = dedup_sort(
    df_reversals,
    ["PRI_KEY_STRING"],
    []
)
df_hf_fcltyclmproc_col2 = dedup_sort(
    df_FctsClmProc,
    ["PRI_KEY_STRING"],
    []
)

df_col1 = df_hf_fcltyclmproc_col1.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_PROC_SK",
    "CLM_ID",
    "FCLTY_CLM_PROC_ORDNL_CD",
    "CRT_RUN_CYC_EXTCN_SK",
    "LAST_UPDT_RUN_CYC_EXTCN_SK",
    "PROC_CD",
    "PROC_DT",
    "PROC_TYPE_CD",
    "PROC_CD_MOD_TX",
    "PROC_CD_CAT_CD"
)

df_col2 = df_hf_fcltyclmproc_col2.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_PROC_SK",
    "CLM_ID",
    "FCLTY_CLM_PROC_ORDNL_CD",
    "CRT_RUN_CYC_EXTCN_SK",
    "LAST_UPDT_RUN_CYC_EXTCN_SK",
    "PROC_CD",
    "PROC_DT",
    "PROC_TYPE_CD",
    "PROC_CD_MOD_TX",
    "PROC_CD_CAT_CD"
)

df_collector = df_col1.unionByName(df_col2)

df_transform_in = df_collector.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_PROC_SK",
    "CLM_ID",
    "FCLTY_CLM_PROC_ORDNL_CD",
    "CRT_RUN_CYC_EXTCN_SK",
    "LAST_UPDT_RUN_CYC_EXTCN_SK",
    "PROC_CD",
    "PROC_DT",
    "PROC_TYPE_CD",
    "PROC_CD_MOD_TX",
    "PROC_CD_CAT_CD"
)

df_transform_allcol = df_transform_in.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    "CLM_ID",
    F.rpad(F.col("FCLTY_CLM_PROC_ORDNL_CD"),2," ").alias("FCLTY_CLM_PROC_ORDNL_CD"),
    "JOB_EXCTN_RCRD_ERR_SK",
    F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_PROC_SK",
    "CRT_RUN_CYC_EXTCN_SK",
    "LAST_UPDT_RUN_CYC_EXTCN_SK",
    "PROC_CD",
    F.rpad(F.col("PROC_DT"),10," ").alias("PROC_DT"),
    "PROC_TYPE_CD",
    F.rpad(F.col("PROC_CD_MOD_TX"),2," ").alias("PROC_CD_MOD_TX"),
    "PROC_CD_CAT_CD"
)

df_transform_snapshot = df_transform_in.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.rpad(F.col("FCLTY_CLM_PROC_ORDNL_CD"),2," ").alias("FCLTY_CLM_PROC_ORDNL_CD")
)

df_transform_transform = df_transform_in.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    "CLM_ID",
    F.col("FCLTY_CLM_PROC_ORDNL_CD").alias("FCLTY_CLM_PROC_ORDNL_CD")
)

df_transformer_in = df_transform_snapshot.select(
    F.lit(SrcSysCdSk).alias("SrcSysCdSk"),
    "CLM_ID",
    F.col("FCLTY_CLM_PROC_ORDNL_CD")
)

df_transformer_in = df_transformer_in.withColumn(
    "svOrdnlCdSk",
    F.expr("GetFkeyCodes('FACETS', 0, 'PROCEDURE ORDINAL', FCLTY_CLM_PROC_ORDNL_CD, 'X')")
)

df_transformer_out = df_transformer_in.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("svOrdnlCdSk").alias("FCLTY_CLM_PROC_ORDNL_CD_SK")
)

df_b_fclty_clm_proc = df_transformer_out

write_files(
    df_b_fclty_clm_proc.select("SRC_SYS_CD_SK","CLM_ID","FCLTY_CLM_PROC_ORDNL_CD_SK"),
    f"{adls_path}/load/B_FCLTY_CLM_PROC.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "$IDSOwner": IDSOwner,
    "SrcSysCdSk": SrcSysCdSk
}
df_fcltyclmprocpk = FcltyClmProcPK(df_transform_allcol, df_transform_transform, params)

write_files(
    df_fcltyclmprocpk.select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
        F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("CLM_PROC_SK"),
        F.col("CLM_ID"),
        F.rpad(F.col("FCLTY_CLM_PROC_ORDNL_CD"),2," ").alias("FCLTY_CLM_PROC_ORDNL_CD"),
        F.col("CRT_RUN_CYC_EXTCN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        F.col("PROC_CD"),
        F.rpad(F.col("PROC_DT"),10," ").alias("PROC_DT"),
        F.col("PROC_TYPE_CD"),
        F.rpad(F.col("PROC_CD_MOD_TX"),2," ").alias("PROC_CD_MOD_TX"),
        F.col("PROC_CD_CAT_CD")
    ),
    f"{adls_path}/key/FctsClmFcltyProcExtr.FctsClmFcltyProc.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)