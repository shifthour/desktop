# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 09/22/09 15:32:09 Batch  15241_56279 PROMOTE bckcetl:31540 ids20 dsadm bls for sa
# MAGIC ^1_2 09/22/09 15:22:02 Batch  15241_55325 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_1 09/22/09 09:19:44 Batch  15241_33586 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_2 07/06/09 11:52:09 Batch  15163_42760 PROMOTE bckcett:31540 testIDSnew u150906 3833-RemitAlternateCharge_Sharon_testIDSnew             Maddy
# MAGIC ^1_2 07/06/09 11:37:10 Batch  15163_41931 INIT bckcett:31540 devlIDSnew u150906 3833-RemitAlternateCharge_Sharon_devlIDSnew                  Maddy
# MAGIC 
# MAGIC ************************************************************************************
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC ************************************************************************************
# MAGIC 
# MAGIC JOB NAME:  BcbsClmLnAltChrgRemitExtr
# MAGIC CALLED BY:  BCBSClmLnRemitExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  This job extracts data from BCBS Medical Line Alternate Charge Remit table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer           Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------         --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------    
# MAGIC SAndrew             2009-06-10      3833 Remit             new program                                                                                  devlIDSnew                  Steph Goddard           07/02/2009                                 
# MAGIC                                                    Alternate Chrg 
# MAGIC Prabhu ES          2022-02-26       S2S Remediation  MSSQL connection parameters added                                            IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC CLM_LN_ALT_CHRG_REMIT
# MAGIC BCBS tbl 
# MAGIC Extraction
# MAGIC Reversals
# MAGIC Business Logic
# MAGIC Data Collection
# MAGIC Primary Keying
# MAGIC This container uses the claim line hash file for lookups
# MAGIC Hash files loaded in FctsClmRemitDriverBuild
# MAGIC Do not delete - all extract programs called by BCBSClmLnRemitExtrSeq also uses
# MAGIC Reissued checks with on orig claims and no adjusting-to claims on this file.  
# MAGIC Amts on orig claim will be positive
# MAGIC Reissued checks on orig claims where adjusting-to is now paying.  
# MAGIC Amts on reissued chk will be neg.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnAltChrgRemitPK
# COMMAND ----------

CurrRunCycle = get_widget_value('CurrRunCycle','')
DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')

jdbc_url, jdbc_props = get_db_config(bcbs_secret_name)

df_hf_clm_remit_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_remit_fcts_reversals.parquet")
df_clm_remit_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_remit_nasco_dup_bypass.parquet")
df_hf_clm_remit_check_reissue_only = spark.read.parquet(f"{adls_path}/hf_clm_remit_check_reissue_only.parquet")
df_hf_clm_remit_chk_w_pd_adj = spark.read.parquet(f"{adls_path}/hf_clm_remit_chk_w_pd_adj.parquet")

extract_query = f"""
SELECT 
  MED_CLM_LN.CLCL_ID,
  MED_CLM_LN.CDML_SEQ_NO,
  MED_CLM_LN.PRPR_WRITEOFF
FROM
  tempdb..{DriverTable} DRVR,
  {BCBSOwner}.PD_MED_CLM_LN_ALT_CHRG MED_CLM_LN
WHERE
  DRVR.CLM_ID = MED_CLM_LN.CLCL_ID
"""

df_PD_MED_CLM_DTL_ALT_CHRG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_med_ln_alt_chrgs = df_PD_MED_CLM_DTL_ALT_CHRG.select(
    strip_field(F.col("CLCL_ID")).alias("CLCL_ID"),
    F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.col("PRPR_WRITEOFF").alias("PRPR_WRITEOFF")
)

df_businessLogicJoined = (
    df_med_ln_alt_chrgs.alias("med_ln_alt_chrgs")
    .join(
        df_hf_clm_remit_fcts_reversals.alias("fcts_reversals"),
        F.col("med_ln_alt_chrgs.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_clm_remit_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("med_ln_alt_chrgs.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
    .join(
        df_hf_clm_remit_chk_w_pd_adj.alias("chk_w_pd_adj"),
        F.col("med_ln_alt_chrgs.CLCL_ID") == F.col("chk_w_pd_adj.CLCL_ID"),
        "left"
    )
    .join(
        df_hf_clm_remit_check_reissue_only.alias("reissue_only"),
        F.col("med_ln_alt_chrgs.CLCL_ID") == F.col("reissue_only.CLCL_ID"),
        "left"
    )
)

df_businessLogicEnriched = (
    df_businessLogicJoined
    .withColumn("PassThru", F.lit("Y"))
    .withColumn("ClmId", trim(F.col("med_ln_alt_chrgs.CLCL_ID")))
    .withColumn(
        "ThisIsAnAdjustedClaim",
        F.when(
            F.col("fcts_reversals.CLCL_ID").isNotNull() & (F.col("fcts_reversals.CLCL_CUR_STS") == "91"),
            "Y"
        ).otherwise("N")
    )
    .withColumn(
        "ThisIsACheckReissueOnly",
        F.when(
            F.col("reissue_only.CLCL_ID").isNotNull() & F.col("chk_w_pd_adj.CLCL_ID").isNull(),
            "Y"
        ).otherwise("N")
    )
    .withColumn(
        "ThisIsPdAdjWithOrighCheckReissue",
        F.when(
            F.col("reissue_only.CLCL_ID").isNull() & F.col("chk_w_pd_adj.CLCL_ID").isNotNull(),
            "Y"
        ).otherwise("N")
    )
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
)

df_Regular = (
    df_businessLogicEnriched
    .filter(
        (F.col("nasco_dup_lkup.CLM_ID").isNull())
        & (F.col("ThisIsAnAdjustedClaim") == "N")
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.col("PassThru"), 1, " ").alias("ROW_PASS_THRU"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("ClmId"), F.lit(";"), F.col("CDML_SEQ_NO")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_SK"),
        F.col("ClmId").alias("CLM_ID"),
        F.col("CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PRPR_WRITEOFF").alias("REMIT_PROV_WRT_OFF_AMT")
    )
)

df_ReversalA08 = (
    df_businessLogicEnriched
    .filter(
        F.col("fcts_reversals.CLCL_ID").isNotNull()
        & (F.col("ThisIsAnAdjustedClaim") == "Y")
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.col("PassThru"), 1, " ").alias("ROW_PASS_THRU"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("ClmId"), F.lit("R;"), F.col("CDML_SEQ_NO")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_SK"),
        F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
        F.col("CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(F.col("ThisIsACheckReissueOnly") == "Y", -1 * F.col("PRPR_WRITEOFF"))
         .otherwise(F.col("PRPR_WRITEOFF"))
         .alias("REMIT_PROV_WRT_OFF_AMT")
    )
)

df_ReversalA09 = (
    df_businessLogicEnriched
    .filter(
        F.col("fcts_reversals.CLCL_ID").isNotNull()
        & (F.col("ThisIsAnAdjustedClaim") == "Y")
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.col("PassThru"), 1, " ").alias("ROW_PASS_THRU"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("ClmId"), F.lit(";"), F.col("CDML_SEQ_NO")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_SK"),
        F.col("ClmId").alias("CLM_ID"),
        F.col("CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(F.col("ThisIsACheckReissueOnly") == "Y", F.col("PRPR_WRITEOFF"))
         .otherwise(-1 * F.col("PRPR_WRITEOFF"))
         .alias("REMIT_PROV_WRT_OFF_AMT")
    )
)

df_Link_Collector = (
    df_Regular.unionByName(df_ReversalA08)
    .unionByName(df_ReversalA09)
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "ROW_PASS_THRU",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_LN_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "REMIT_PROV_WRT_OFF_AMT"
    )
)

df_Transform = df_Link_Collector.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "ROW_PASS_THRU",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LN_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "REMIT_PROV_WRT_OFF_AMT"
).alias("Out")

df_Balance = df_Link_Collector.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("REMIT_PROV_WRT_OFF_AMT").alias("REMIT_PROV_WRT_OFF_AMT")
)

write_files(
    df_Balance.select("SRC_SYS_CD_SK", "CLM_ID", "CLM_LN_SEQ_NO", "REMIT_PROV_WRT_OFF_AMT"),
    f"{adls_path}/load/B_CLM_LN_ALT_CHRG_REMIT.{SrcSysCd}.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

params_container_ClmLnAltChrgRemitPK = {
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "CurrentDate": CurrentDate,
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk
}

df_ClmLnAltChrgRemitPK = ClmLnAltChrgRemitPK(df_Transform, params_container_ClmLnAltChrgRemitPK)

df_final_IdsClmLnAltChrgRemitExtr = df_ClmLnAltChrgRemitPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("ROW_PASS_THRU"), 1, " ").alias("ROW_PASS_THRU"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("REMIT_PROV_WRT_OFF_AMT")
)

write_files(
    df_final_IdsClmLnAltChrgRemitExtr,
    f"{adls_path}/key/BcbsClmLnAltChrgRemitExtr.ClmLnAltChrgRemit.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)