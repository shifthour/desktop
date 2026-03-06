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
# MAGIC      
# MAGIC JOB NAME:  BcbsClmLnAltChrgRemitDsalwExtr
# MAGIC CALLED BY:  BCBSClmLnRemitExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  This job extracts data from BCBS Medical Detail Alternate Charge Remit table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer           Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------         --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------    
# MAGIC SAndrew             2009-06-10      3833 Remit             new program                                                                                  devlIDSnew                  Steph Goddard          07/02/2009                                 
# MAGIC                                                    Alternate Chrg 
# MAGIC Prabhu ES          2022-02-26       S2S Remediation   MSSQL connection parameters added                                           IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC BCBS Claim Line Remit Disallow Extract and Primary Key
# MAGIC update primary key table (K_CLM_LN_REMIT_DSALW) with new keys created today
# MAGIC Writing Sequential File to ../key
# MAGIC primary key hash file only contains current run keys and is cleared before writing
# MAGIC Strip fields
# MAGIC Extraction
# MAGIC Reissued checks on orig claims where adjusting-to is now paying.  Amts on reissued chk will be neg.
# MAGIC Reissued checks with on orig claims and no adjusting-to claims are on this pd file.  Amts on orig claim will be positive
# MAGIC Data Collection
# MAGIC Business Logic
# MAGIC Reversals
# MAGIC Hash files loaded in FctsClmRemitDriverBuild
# MAGIC Hash files loaded in FctsClmRemitDriverBuild
# MAGIC Do not delete - BcbsClmLnRemitDsalowExtr also uses
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnRemitDsalwPK
# COMMAND ----------

# Parameter Retrieval
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Read Hashed Files as Parquet (Scenario C)
df_hf_clm_remit_chk_w_pd_adj = spark.read.parquet(f"{adls_path}/hf_clm_remit_chk_w_pd_adj.parquet")
df_hf_clm_remit_check_reissue_only = spark.read.parquet(f"{adls_path}/hf_clm_remit_check_reissue_only.parquet")
df_hf_clm_remit_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_remit_nasco_dup_bypass.parquet")
df_hf_clm_remit_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_remit_fcts_reversals.parquet")

# ODBC Connector to BCBS
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)
extract_query_pd_med = (
    f"SELECT \n"
    f"MED_CLM_DTL.CLCL_ID,\n"
    f"MED_CLM_DTL.CDML_SEQ_NO,\n"
    f"MED_CLM_DTL.DISALLOW_TYPE,\n"
    f"MED_CLM_DTL.BYPASS_IN,\n"
    f"MED_CLM_DTL.CDML_DISALL_AMT,\n"
    f"MED_CLM_DTL.CDML_DISALL_EXCD,\n"
    f"MED_CLM_DTL.EXCD_RSPNSB_IN\n"
    f"FROM tempdb..{DriverTable} DRIVER,\n"
    f"{SrcSysCd}.PD_MED_CLM_DTL_ALT_CHRG MED_CLM_DTL\n"
    f"WHERE DRIVER.CLM_ID = MED_CLM_DTL.CLCL_ID"
)

df_PD_MED_CLM_DTL_ALT_CHRG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", extract_query_pd_med)
    .load()
)

# StripFields Transformer
df_StripFields = df_PD_MED_CLM_DTL_ALT_CHRG.select(
    strip_field(F.col("CLCL_ID")).alias("CLCL_ID"),
    F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    strip_field(F.col("DISALLOW_TYPE")).alias("DISALLOW_TYPE"),
    strip_field(F.col("BYPASS_IN")).alias("BYPASS_IN"),
    F.col("CDML_DISALL_AMT").alias("CDML_DISALL_AMT"),
    strip_field(F.col("CDML_DISALL_EXCD")).alias("CDML_DISALL_EXCD"),
    strip_field(F.col("EXCD_RSPNSB_IN")).alias("EXCD_RSPNSB_IN")
)

# BusinessLogic Transformer (joins + stage variables)
df_BusinessLogic = (
    df_StripFields.alias("MedClmDtlAltChrg")
    .join(
        df_hf_clm_remit_fcts_reversals.alias("fcts_reversals"),
        F.col("MedClmDtlAltChrg.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_hf_clm_remit_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("MedClmDtlAltChrg.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
    .join(
        df_hf_clm_remit_chk_w_pd_adj.alias("chk_w_pd_adj"),
        F.col("MedClmDtlAltChrg.CLCL_ID") == F.col("chk_w_pd_adj.CLCL_ID"),
        "left"
    )
    .join(
        df_hf_clm_remit_check_reissue_only.alias("reissue_only"),
        F.col("MedClmDtlAltChrg.CLCL_ID") == F.col("reissue_only.CLCL_ID"),
        "left"
    )
    .withColumn("PassThru", F.lit("Y"))
    .withColumn("svClmId", trim(F.col("MedClmDtlAltChrg.CLCL_ID")))
    .withColumn(
        "svDsalwType",
        F.when(
            F.col("MedClmDtlAltChrg.DISALLOW_TYPE").isNull() |
            (F.length(trim(F.col("MedClmDtlAltChrg.DISALLOW_TYPE"))) == 0),
            F.lit("UNK")
        ).otherwise(trim(F.col("MedClmDtlAltChrg.DISALLOW_TYPE")))
    )
    .withColumn(
        "ThisIsAnAdjustedClaim",
        F.when(
            (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "ThisIsACheckReissueOnly",
        F.when(
            (F.col("reissue_only.CLCL_ID").isNotNull()) &
            (F.col("chk_w_pd_adj.CLCL_ID").isNull()),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "ThisIsPdAdjWithOrighCheckReissue",
        F.when(
            (F.col("reissue_only.CLCL_ID").isNotNull()) &
            (F.col("chk_w_pd_adj.CLCL_ID").isNotNull()),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

# Paid Output
df_Paid = df_BusinessLogic.filter(
    (F.col("nasco_dup_lkup.CLM_ID").isNull()) &
    (F.col("ThisIsAnAdjustedClaim") == "N")
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("ROW_PASS_THRU"),
    F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(
        F.lit(SrcSysCd), F.lit(";"),
        F.col("svClmId"), F.lit(";"),
        F.col("MedClmDtlAltChrg.CDML_SEQ_NO"), F.lit(";"),
        F.col("MedClmDtlAltChrg.DISALLOW_TYPE"), F.lit(";"),
        F.col("MedClmDtlAltChrg.BYPASS_IN")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_REMIT_DSALW_SK"),
    F.col("svClmId").alias("CLM_ID"),
    F.col("MedClmDtlAltChrg.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("svDsalwType").alias("CLM_LN_DSALW_TYP_CD"),
    trim(F.col("MedClmDtlAltChrg.BYPASS_IN")).alias("BYPS_IN"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_LN_SK"),
    F.when(
        F.col("MedClmDtlAltChrg.CDML_DISALL_EXCD").isNull() |
        (F.length(trim(F.col("MedClmDtlAltChrg.CDML_DISALL_EXCD"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.col("MedClmDtlAltChrg.CDML_DISALL_EXCD"))).alias("CLM_LN_REMIT_DSALW_EXCD"),
    F.when(
        F.col("MedClmDtlAltChrg.EXCD_RSPNSB_IN").isNull() |
        (F.length(trim(F.col("MedClmDtlAltChrg.EXCD_RSPNSB_IN"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.col("MedClmDtlAltChrg.EXCD_RSPNSB_IN"))).alias("CLM_LN_RMT_DSW_EXCD_RESP_CD"),
    F.col("svDsalwType").alias("CLM_LN_RMT_DSALW_TYP_CAT_CD"),
    F.col("MedClmDtlAltChrg.CDML_DISALL_AMT").alias("REMIT_DSALW_AMT")
)

# ReversalA08 Output
df_ReversalA08 = df_BusinessLogic.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (F.col("ThisIsAnAdjustedClaim") == "Y")
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("ROW_PASS_THRU"),
    F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(
        F.lit(SrcSysCd), F.lit(";"),
        F.col("svClmId"), F.lit("R;"),
        F.col("MedClmDtlAltChrg.CDML_SEQ_NO"), F.lit(";"),
        F.col("MedClmDtlAltChrg.DISALLOW_TYPE"), F.lit(";"),
        F.col("MedClmDtlAltChrg.BYPASS_IN")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_REMIT_DSALW_SK"),
    F.concat(F.col("svClmId"), F.lit("R")).alias("CLM_ID"),
    F.col("MedClmDtlAltChrg.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("svDsalwType").alias("CLM_LN_DSALW_TYP_CD"),
    trim(F.col("MedClmDtlAltChrg.BYPASS_IN")).alias("BYPS_IN"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_LN_SK"),
    F.when(
        F.col("MedClmDtlAltChrg.CDML_DISALL_EXCD").isNull() |
        (F.length(trim(F.col("MedClmDtlAltChrg.CDML_DISALL_EXCD"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.col("MedClmDtlAltChrg.CDML_DISALL_EXCD"))).alias("CLM_LN_REMIT_DSALW_EXCD"),
    F.when(
        F.col("MedClmDtlAltChrg.EXCD_RSPNSB_IN").isNull() |
        (F.length(trim(F.col("MedClmDtlAltChrg.EXCD_RSPNSB_IN"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.col("MedClmDtlAltChrg.EXCD_RSPNSB_IN"))).alias("CLM_LN_RMT_DSW_EXCD_RESP_CD"),
    F.col("svDsalwType").alias("CLM_LN_RMT_DSALW_TYP_CAT_CD"),
    F.when(
        F.col("ThisIsACheckReissueOnly") == "Y",
        -1 * F.col("MedClmDtlAltChrg.CDML_DISALL_AMT")
    ).when(
        F.col("ThisIsPdAdjWithOrighCheckReissue") == "Y",
        F.col("MedClmDtlAltChrg.CDML_DISALL_AMT")
    ).otherwise(
        F.col("MedClmDtlAltChrg.CDML_DISALL_AMT")
    ).alias("REMIT_DSALW_AMT")
)

# ReversalA09 Output
df_ReversalA09 = df_BusinessLogic.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (F.col("ThisIsAnAdjustedClaim") == "Y")
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("ROW_PASS_THRU"),
    F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(
        F.lit(SrcSysCd), F.lit(";"),
        F.col("svClmId"), F.lit(";"),
        F.col("MedClmDtlAltChrg.CDML_SEQ_NO"), F.lit(";"),
        F.col("MedClmDtlAltChrg.DISALLOW_TYPE"), F.lit(";"),
        F.col("MedClmDtlAltChrg.BYPASS_IN")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_REMIT_DSALW_SK"),
    F.col("svClmId").alias("CLM_ID"),
    F.col("MedClmDtlAltChrg.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("svDsalwType").alias("CLM_LN_DSALW_TYP_CD"),
    trim(F.col("MedClmDtlAltChrg.BYPASS_IN")).alias("BYPS_IN"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_LN_SK"),
    F.when(
        F.col("MedClmDtlAltChrg.CDML_DISALL_EXCD").isNull() |
        (F.length(trim(F.col("MedClmDtlAltChrg.CDML_DISALL_EXCD"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.col("MedClmDtlAltChrg.CDML_DISALL_EXCD"))).alias("CLM_LN_REMIT_DSALW_EXCD"),
    F.when(
        F.col("MedClmDtlAltChrg.EXCD_RSPNSB_IN").isNull() |
        (F.length(trim(F.col("MedClmDtlAltChrg.EXCD_RSPNSB_IN"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.col("MedClmDtlAltChrg.EXCD_RSPNSB_IN"))).alias("CLM_LN_RMT_DSW_EXCD_RESP_CD"),
    F.col("svDsalwType").alias("CLM_LN_RMT_DSALW_TYP_CAT_CD"),
    F.when(
        F.col("ThisIsACheckReissueOnly") == "Y",
        F.col("MedClmDtlAltChrg.CDML_DISALL_AMT")
    ).when(
        F.col("ThisIsPdAdjWithOrighCheckReissue") == "Y",
        -1 * F.col("MedClmDtlAltChrg.CDML_DISALL_AMT")
    ).otherwise(
        -1 * F.col("MedClmDtlAltChrg.CDML_DISALL_AMT")
    ).alias("REMIT_DSALW_AMT")
)

# Link_Collector (Union)
df_link_collector = df_Paid.unionByName(df_ReversalA08).unionByName(df_ReversalA09)

# SnapShot Transformer
df_SnapShot = df_link_collector.withColumn(
    "svClmLnDsalwTypCdSk",
    GetFkeyCodes(
        F.col("SRC_SYS_CD"),
        F.lit(102),
        F.lit("CLAIM LINE DISALLOW TYPE"),
        F.col("CLM_LN_DSALW_TYP_CD"),
        F.lit("X")
    )
)

# Outputs from SnapShot
# "Trans" (ClmLnRemitDsalwPK input)
df_Trans = df_SnapShot.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("ROW_PASS_THRU").alias("ROW_PASS_THRU"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_LN_REMIT_DSALW_SK").alias("CLM_LN_REMIT_DSALW_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
    F.col("BYPS_IN").alias("BYPS_IN"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("CLM_LN_REMIT_DSALW_EXCD").alias("CLM_LN_REMIT_DSALW_EXCD"),
    F.col("CLM_LN_RMT_DSW_EXCD_RESP_CD").alias("CLM_LN_RMT_DSW_EXCD_RESP_CD"),
    F.col("CLM_LN_RMT_DSALW_TYP_CAT_CD").alias("CLM_LN_RMT_DSALW_TYP_CAT_CD"),
    F.col("REMIT_DSALW_AMT").alias("REMIT_DSALW_AMT")
)

# "balance" (B_CLM_LN_ALT_CHRG_REMIT_DSALW output)
df_balance = df_SnapShot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("svClmLnDsalwTypCdSk").alias("CLM_LN_DSALW_TYP_CD_SK"),
    F.col("BYPS_IN").alias("BYPS_IN"),
    F.col("REMIT_DSALW_AMT").alias("REMIT_DSALW_AMT")
)

# Write B_CLM_LN_ALT_CHRG_REMIT_DSALW
df_balance_final = df_balance.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DSALW_TYP_CD_SK"),
    F.rpad(F.col("BYPS_IN"), 1, " ").alias("BYPS_IN"),
    F.col("REMIT_DSALW_AMT")
)

write_files(
    df_balance_final,
    f"{adls_path}/load/B_CLM_LN_ALT_CHRG_REMIT_DSALW.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Call Shared Container "ClmLnRemitDsalwPK"
params_ClmLnRemitDsalwPK = {}
df_ClmLnRemitDsalwPK = ClmLnRemitDsalwPK(df_Trans, params_ClmLnRemitDsalwPK)

# Write IdsClmLnAltChrgRemitDsalwExtr
df_IdsClmLnAltChrgRemitDsalwExtr_final = df_ClmLnRemitDsalwPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("ROW_PASS_THRU"), 1, " ").alias("ROW_PASS_THRU"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_REMIT_DSALW_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.rpad(F.col("CLM_LN_DSALW_TYP_CD"), 10, " ").alias("CLM_LN_DSALW_TYP_CD"),
    F.rpad(F.col("BYPS_IN"), 1, " ").alias("BYPS_IN"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_SK"),
    F.rpad(F.col("CLM_LN_REMIT_DSALW_EXCD"), 10, " ").alias("CLM_LN_REMIT_DSALW_EXCD"),
    F.rpad(F.col("CLM_LN_RMT_DSW_EXCD_RESP_CD"), 10, " ").alias("CLM_LN_RMT_DSW_EXCD_RESP_CD"),
    F.rpad(F.col("CLM_LN_RMT_DSALW_TYP_CAT_CD"), 10, " ").alias("CLM_LN_RMT_DSALW_TYP_CAT_CD"),
    F.col("REMIT_DSALW_AMT")
)

write_files(
    df_IdsClmLnAltChrgRemitDsalwExtr_final,
    f"{adls_path}/key/BcbsClmLnAltChrgRemitDsalwExtr.ClmLnAltChrgRemitDsalw.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# AfterJobRoutine
params_after_job = {
    "EnvProjectPath": f"dap/<...>/Jobs/claim/ClaimLine/ClmLnAltChrgRemitDisallow",
    "File_Path": "<...>",
    "File_Name": "<...>"
}
dbutils.notebook.run("../../../../../sequencer_routines/Move_File", timeout_seconds=3600, arguments=params_after_job)