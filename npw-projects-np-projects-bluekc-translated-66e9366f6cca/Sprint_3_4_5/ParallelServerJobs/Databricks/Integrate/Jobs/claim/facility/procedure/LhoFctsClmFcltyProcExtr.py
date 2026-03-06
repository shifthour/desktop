# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004, 2007, 2008, 2012, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmFcltyProcExtr
# MAGIC CALLED BY:  LhoFctsClmOnDmdExtr1Seq
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
# MAGIC 
# MAGIC Reddy Sanam        2020-08-12         263446                 Changed the Env variables from Facets to LhoFacetsStg
# MAGIC                                                                                        Chnaged the value "FACETS" to "SrcSysCd" in the 
# MAGIC                                                                                        transformer stage "Business_Logic" and Transformer
# MAGIC                                                                                        Traget sequential file name changed to reflect "LhoFacets"
# MAGIC Prabhu ES               2022-03-29       S2S                      MSSQL ODBC conn params added                                                 IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC Run STRIP.FIELD transform on fields that might have Carriage Return, Line Feed, and  Tab
# MAGIC Pulling Facets Claim Procedure Data for a specific Time Period
# MAGIC Hash file (hf_fclty_proc_allcol) cleared from the shared container FcltyClmProcPK
# MAGIC This container is used in:
# MAGIC FctsClmFcltyProcExtr
# MAGIC LhoFctsClmFcltyProcExtr
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
from pyspark.sql import functions as F, types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Widgets (Parameters)
DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')
ids_secret_name = get_widget_value('ids_secret_name','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/FcltyClmProcPK
# COMMAND ----------

# Read from DB2Connector (PROC_CD) - first output
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_PROC_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT PROC_CD, PROC_CD_TYP_CD FROM {IDSOwner}.PROC_CD WHERE PROC_CD_CAT_CD = 'MED'")
    .load()
)

# Read from DB2Connector (PROC_CD) - second output
df_PROC_CD2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT PROC_CD, PROC_CD_TYP_CD FROM {IDSOwner}.PROC_CD WHERE PROC_CD_CAT_CD = 'MED' AND PROC_CD_TYP_CD in ('CPT4','HCPCS')")
    .load()
)

# Deduplicate for hf_fcltyproc_proccdcatcd (scenario A) on key = PROC_CD
df_ProcCdCatCd_temp = df_PROC_CD
df_ProcCdCatCd = dedup_sort(df_ProcCdCatCd_temp, ["PROC_CD"], [])

# Deduplicate for hf_fcltyproc_proccdtypcd (scenario A) on key = PROC_CD
df_ProcCdTypCd_temp = df_PROC_CD2
df_ProcCdTypCd = dedup_sort(df_ProcCdTypCd_temp, ["PROC_CD"], [])

# Read hashed file hf_clm_fcts_reversals (scenario C)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# Read hashed file clm_nasco_dup_bypass (scenario C)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# CMC_CLHI_PROC (ODBCConnector)
jdbc_url_stg, jdbc_props_stg = get_db_config(lhofacetsstg_secret_name)
df_CMC_CLHI_PROC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_stg)
    .options(**jdbc_props_stg)
    .option(
        "query",
        f"SELECT A.CLCL_ID, A.CLHI_TYPE, A.IPCD_ID, A.CLHI_IP_DT, B.IPCD_TYPE "
        f"FROM {LhoFacetsStgOwner}.CMC_CLHI_PROC A, tempdb..{DriverTable} TMP, {LhoFacetsStgOwner}.CMC_IPCD_PROC_CD B "
        f"WHERE TMP.CLM_ID = A.CLCL_ID AND A.IPCD_ID = B.IPCD_ID"
    )
    .load()
)

# StripField Transformer
df_StripField = (
    df_CMC_CLHI_PROC
    .withColumn("CLCL_ID", strip_field(F.col("CLCL_ID")))
    .withColumn("CLHI_TYPE", strip_field(F.col("CLHI_TYPE")))
    .withColumn("IPCD_ID", strip_field(F.col("IPCD_ID")))
    .withColumn("CLHI_IP_DT", F.col("CLHI_IP_DT"))
    .withColumn("IPCD_TYPE", strip_field(F.col("IPCD_TYPE")))
)

# Business_Logic - build joined DataFrame
df_main = (
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
        df_ProcCdCatCd.alias("ProcCdCatCd"),
        trim(F.col("Strip.IPCD_ID")) == F.col("ProcCdCatCd.PROC_CD"),
        "left"
    )
    .join(
        df_ProcCdTypCd.alias("ProcCdTypCd"),
        trim(F.col("Strip.IPCD_ID").substr(1, 5)) == F.col("ProcCdTypCd.PROC_CD"),
        "left"
    )
    .withColumn("svIpcdcd", trim(F.col("Strip.IPCD_ID")))
    .withColumn(
        "ProcCdMod",
        F.substring(F.col("svIpcdcd"), -1 * F.least(F.lit(2), F.length("svIpcdcd")), 2)
    )
    .withColumn("ClmId", trim(F.col("Strip.CLCL_ID")))
    .withColumn(
        "NoMEDMatch",
        F.when(
            (F.length(trim(F.col("Strip.IPCD_ID"))) > 5) & (F.col("ProcCdTypCd.PROC_CD").isNotNull()),
            F.col("ProcCdTypCd.PROC_CD")
        ).otherwise("UNK")
    )
    .withColumn(
        "MEDMatch",
        F.when(
            F.col("ProcCdCatCd.PROC_CD").isNotNull(),
            F.col("ProcCdCatCd.PROC_CD")
        ).otherwise(F.col("NoMEDMatch"))
    )
    .withColumn(
        "ModTx2",
        F.when(
            (F.col("Strip.IPCD_ID").isNull()) | (F.length(F.col("Strip.IPCD_ID")) == 0),
            "  "
        ).otherwise(
            F.when(
                (F.col("ProcCdCatCd.PROC_CD_TYP_CD") == "ICD10") | (F.col("NoMEDMatch") == "UNK"),
                "  "
            ).otherwise(F.col("Strip.IPCD_ID"))
        )
    )
)

# Split into FctsClmProc
df_FctsClmProc = df_main.filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
df_FctsClmProc_sel = df_FctsClmProc.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.concat(F.col("SrcSysCd"), F.lit(";"), F.col("ClmId"), F.lit(";"), trim(F.col("Strip.CLHI_TYPE"))).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROC_SK"),
    F.col("ClmId").alias("CLM_ID"),
    trim(F.col("Strip.CLHI_TYPE")).alias("FCLTY_CLM_PROC_ORDNL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.when(
        (F.col("Strip.IPCD_ID").isNull()) | (F.length(F.col("Strip.IPCD_ID")) == 0),
        "NA"
    ).otherwise(F.col("MEDMatch")).alias("PROC_CD"),
    F.date_format(F.col("Strip.CLHI_IP_DT"), "yyyy-MM-dd").alias("PROC_DT"),
    F.when(
        (F.col("Strip.IPCD_TYPE").isNull()) | (F.length(trim(F.col("Strip.IPCD_TYPE"))) == 0),
        F.when(
            (F.col("ProcCdCatCd.PROC_CD_TYP_CD").isNull()) | (F.length(trim(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))) == 0),
            F.col("ProcCdTypCd.PROC_CD_TYP_CD")
        ).otherwise(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))
    ).otherwise(
        F.when(
            F.col("Strip.IPCD_TYPE") == "Y",
            "ICD10"
        ).otherwise(
            F.when(
                (F.col("ProcCdCatCd.PROC_CD_TYP_CD").isNull()) | (F.length(trim(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))) == 0),
                F.col("ProcCdTypCd.PROC_CD_TYP_CD")
            ).otherwise(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))
        )
    ).alias("PROC_TYPE_CD"),
    F.when(
        F.col("ModTx2") == "  ",
        "  "
    ).otherwise(
        F.when(F.length(F.col("Strip.IPCD_ID")) >= 7, F.substring(F.col("Strip.IPCD_ID"), 6, 2)).otherwise("  ")
    ).alias("PROC_CD_MOD_TX"),
    F.lit("MED").alias("PROC_CD_CAT_CD")
)

# Split into reversals
df_reversals = df_main.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (
        (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
        (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
        (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
    )
)
df_reversals_sel = df_reversals.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.concat(F.col("SrcSysCd"), F.lit(";"), F.col("ClmId"), F.lit("R;"), trim(F.col("Strip.CLHI_TYPE"))).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROC_SK"),
    F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
    trim(F.col("Strip.CLHI_TYPE")).alias("FCLTY_CLM_PROC_ORDNL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.when(
        (F.col("Strip.IPCD_ID").isNull()) | (F.length(F.col("Strip.IPCD_ID")) == 0),
        "NA"
    ).otherwise(F.col("MEDMatch")).alias("PROC_CD"),
    F.date_format(F.col("Strip.CLHI_IP_DT"), "yyyy-MM-dd").alias("PROC_DT"),
    F.when(
        (F.col("Strip.IPCD_TYPE").isNull()) | (F.length(trim(F.col("Strip.IPCD_TYPE"))) == 0),
        F.when(
            (F.col("ProcCdCatCd.PROC_CD_TYP_CD").isNull()) | (F.length(trim(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))) == 0),
            F.col("ProcCdTypCd.PROC_CD_TYP_CD")
        ).otherwise(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))
    ).otherwise(
        F.when(
            F.col("Strip.IPCD_TYPE") == "Y",
            "ICD10"
        ).otherwise(
            F.when(
                (F.col("ProcCdCatCd.PROC_CD_TYP_CD").isNull()) | (F.length(trim(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))) == 0),
                F.col("ProcCdTypCd.PROC_CD_TYP_CD")
            ).otherwise(F.col("ProcCdCatCd.PROC_CD_TYP_CD"))
        )
    ).alias("PROC_TYPE_CD"),
    F.when(
        F.col("ModTx2") == "  ",
        "  "
    ).otherwise(
        F.when(F.length(F.col("Strip.IPCD_ID")) >= 7, F.substring(F.col("Strip.IPCD_ID"), 6, 2)).otherwise("  ")
    ).alias("PROC_CD_MOD_TX"),
    F.lit("MED").alias("PROC_CD_CAT_CD")
)

# hf_fcltyclmproc_col1 (scenario A)
df_reversals_dedup = dedup_sort(df_reversals_sel, ["PRI_KEY_STRING"], [])
# hf_fcltyclmproc_col2 (scenario A)
df_FctsClmProc_dedup = dedup_sort(df_FctsClmProc_sel, ["PRI_KEY_STRING"], [])

# Collector (union the two inputs)
df_collect = df_reversals_dedup.unionByName(df_FctsClmProc_dedup)

# Next Transformer (Transform)
df_Transform = df_collect.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_PROC_SK").alias("CLM_PROC_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_PROC_ORDNL_CD").alias("FCLTY_CLM_PROC_ORDNL_CD"),
    F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("PROC_CD").alias("PROC_CD"),
    F.col("PROC_DT").alias("PROC_DT"),
    F.col("PROC_TYPE_CD").alias("PROC_TYPE_CD"),
    F.col("PROC_CD_MOD_TX").alias("PROC_CD_MOD_TX"),
    F.col("PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD")
)

df_AllCol = df_Transform.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_PROC_ORDNL_CD").alias("FCLTY_CLM_PROC_ORDNL_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_PROC_SK").alias("CLM_PROC_SK"),
    F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("PROC_CD").alias("PROC_CD"),
    F.col("PROC_DT").alias("PROC_DT"),
    F.col("PROC_TYPE_CD").alias("PROC_TYPE_CD"),
    F.col("PROC_CD_MOD_TX").alias("PROC_CD_MOD_TX"),
    F.col("PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD")
)

df_SnapShot = df_Transform.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_PROC_ORDNL_CD").alias("FCLTY_CLM_PROC_ORDNL_CD")
)

df_TransformOut = df_Transform.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_PROC_ORDNL_CD").alias("FCLTY_CLM_PROC_ORDNL_CD")
)

# Next Transformer (Transformer)
df_SnapShot_enriched = df_SnapShot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_PROC_ORDNL_CD").alias("FCLTY_CLM_PROC_ORDNL_CD")
)

df_enriched = df_SnapShot_enriched.withColumn(
    "FCLTY_CLM_PROC_ORDNL_CD_SK",
    F.expr("GetFkeyCodes(SrcSysCd, 0, 'PROCEDURE ORDINAL', FCLTY_CLM_PROC_ORDNL_CD, 'X')")
)

df_RowCount = df_enriched.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("FCLTY_CLM_PROC_ORDNL_CD_SK")
)

# B_FCLTY_CLM_PROC (CSeqFileStage)
df_B_FCLTY_CLM_PROC_write = df_RowCount.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "FCLTY_CLM_PROC_ORDNL_CD_SK"
)

write_files(
    df_B_FCLTY_CLM_PROC_write,
    f"{adls_path}/load/B_FCLTY_CLM_PROC.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Shared Container FcltyClmProcPK
params_container = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "IDSOwner": IDSOwner,
    "SrcSysCdSk": SrcSysCdSk
}
df_FcltyClmProcPK_out = FcltyClmProcPK(df_AllCol, df_TransformOut, params_container)

# FctsClmFcltyProcExtr (CSeqFileStage)
df_FctsClmFcltyProcExtr = df_FcltyClmProcPK_out.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_PROC_SK"),
    F.col("CLM_ID"),
    F.rpad(F.col("FCLTY_CLM_PROC_ORDNL_CD"), 2, " ").alias("FCLTY_CLM_PROC_ORDNL_CD"),
    F.col("CRT_RUN_CYC_EXTCN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("PROC_CD"),
    F.rpad(F.col("PROC_DT"), 10, " ").alias("PROC_DT"),
    F.col("PROC_TYPE_CD"),
    F.rpad(F.col("PROC_CD_MOD_TX"), 2, " ").alias("PROC_CD_MOD_TX"),
    F.col("PROC_CD_CAT_CD")
)

write_files(
    df_FctsClmFcltyProcExtr,
    f"{adls_path}/key/LhoFctsClmFcltyProcExtr.LhoFctsClmFcltyProc.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)