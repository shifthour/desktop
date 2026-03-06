# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_6 09/22/09 15:32:09 Batch  15241_56279 PROMOTE bckcetl:31540 ids20 dsadm bls for sa
# MAGIC ^1_6 09/22/09 15:22:02 Batch  15241_55325 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_4 09/22/09 09:19:44 Batch  15241_33586 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_1 09/21/09 12:20:43 Batch  15240_44447 PROMOTE bckcett:31540 testIDSnew u10157 sa
# MAGIC ^1_1 09/21/09 12:16:59 Batch  15240_44229 INIT bckcett:31540 devlIDSnew u10157 sa for 2009-09-21 prod deployment
# MAGIC ^1_3 04/24/09 09:20:30 Batch  15090_33635 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_3 04/24/09 09:14:25 Batch  15090_33296 INIT bckcett testIDS dsadm BLS FOR SA
# MAGIC ^1_2 03/10/09 11:34:35 Batch  15045_41695 PROMOTE bckcett devlIDS u10157 sa - Bringing ALL Claim code down from production
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_2 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/22/08 10:09:03 Batch  14845_36590 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 08/22/08 09:55:32 Batch  14845_35734 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_1 08/19/08 10:43:19 Batch  14842_38609 PROMOTE bckcett testIDS u03651 steph for Sharon 3057
# MAGIC ^1_1 08/19/08 10:38:02 Batch  14842_38285 INIT bckcett devlIDSnew u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC ************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC ************************************************************************************
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  claim / SeqClmLnRemit / BCBSClmLnRemitExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  This job extracts data from BCBS Medical and Dental Claim line tables for Claim Line Remit
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                            Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                                    --------------------------------       -------------------------------   ----------------------------    
# MAGIC Parik                       2008-07-29        3057(Web Remit)   Original Programming                                                                                                          devlIDSnew                   Steph Goddard           08/11/2008
# MAGIC SANdrew                2009-03-03        ProdSupport          Added balancing .                                                                                                               devlIDS                          Steph Goddard           03/24/2009
# MAGIC                                                                                       Added hash file lookups with files built in FctsClmLnRemitDriverBuild 
# MAGIC                                                                                      and provides foundation for determing new business rules for reissed checks and adjustment checks.
# MAGIC                                                                                      If ThisIsACheckReissueOnly="Y" then  -1 * #REMIT_AMT#   else
# MAGIC                                                                                      If ThisIsPdAdjWithOrighCheckReissue = "Y"  then   #REMIT_AMT# 
# MAGIC                                                                                      else #REMIT_AMT# 
# MAGIC 
# MAGIC SAndrew             2009-06-10      3833 Remit              updated documentation in job to denote many programs use hash file                                   devlIDSnew                                                     
# MAGIC                                                    Alternate Chrg          changed balance file to use SrcSysCd parameter and not hard coded
# MAGIC Prabhu ES          2022-02-26       S2S Remediation      MSSQL connection parameters added                                                                                 IntegrateDev5

# MAGIC CLM_LN_REMIT
# MAGIC Extraction
# MAGIC Strip fields
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnRemitPK
# COMMAND ----------

bcbs_secret_name = get_widget_value('bcbs_secret_name','')
DriverTable = get_widget_value('DriverTable','TMP_IDS_CLAIM_REMIT')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','155000')
CurrentDate = get_widget_value('CurrentDate','2009-07-01')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

df_fcts_reversals = spark.read.format("parquet").load(f"{adls_path}/hf_clm_remit_fcts_reversals.parquet")
df_nasco_dup_lkup = spark.read.format("parquet").load(f"{adls_path}/hf_clm_remit_nasco_dup_bypass.parquet")
df_reissue_only = spark.read.format("parquet").load(f"{adls_path}/hf_clm_remit_check_reissue_only.parquet")
df_chk_w_pd_adj = spark.read.format("parquet").load(f"{adls_path}/hf_clm_remit_chk_w_pd_adj.parquet")

jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)

extract_query_med = (
    "SELECT distinct CLCL_ID, CDML_SEQ_NO, PATIENT_RESP, PRPR_WRITEOFF, NO_RESP_AMT, CDML_DISALL_AMT "
    f"FROM tempdb..{DriverTable} DRVR, {bcbs_secret_name}.PD_MED_CLM_LN MED_CLM_LN "
    "WHERE DRVR.CLM_ID = MED_CLM_LN.CLCL_ID"
)
df_BcbsPdMedDntlClmLn_Med = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", extract_query_med)
    .load()
)

extract_query_dntl = (
    "SELECT distinct CLCL_ID, CDDL_SEQ_NO, PATIENT_RESP, PRPR_WRITEOFF, NO_RESP_AMT, CDDL_DISALL_AMT "
    f"FROM {bcbs_secret_name}.PD_DNTL_CLM_LN DNTL_CLM_LN, tempdb..{DriverTable} DRVR"
)
df_BcbsPdMedDntlClmLn_Dntl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", extract_query_dntl)
    .load()
)

df_StripFields_MedOut = (
    df_BcbsPdMedDntlClmLn_Med
    .withColumn("CLCL_ID", strip_field(F.col("CLCL_ID")))
    .select(
        F.col("CLCL_ID").alias("CLCL_ID"),
        F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
        F.col("PATIENT_RESP").alias("PATIENT_RESP"),
        F.col("PRPR_WRITEOFF").alias("PRPR_WRITEOFF"),
        (F.col("CDML_DISALL_AMT") - (F.col("PRPR_WRITEOFF") + F.col("NO_RESP_AMT"))).alias("MBR_OTHR_LIAB_AMT"),
        F.col("NO_RESP_AMT").alias("NO_RESP_AMT")
    )
)

df_StripFields2_DntlOut = (
    df_BcbsPdMedDntlClmLn_Dntl
    .withColumn("CLCL_ID", strip_field(F.col("CLCL_ID")))
    .select(
        F.col("CLCL_ID").alias("CLCL_ID"),
        F.col("CDDL_SEQ_NO").alias("CDML_SEQ_NO"),
        F.col("PATIENT_RESP").alias("PATIENT_RESP"),
        F.col("PRPR_WRITEOFF").alias("PRPR_WRITEOFF"),
        (F.col("CDDL_DISALL_AMT") - (F.col("PRPR_WRITEOFF") + F.col("NO_RESP_AMT"))).alias("MBR_OTHR_LIAB_AMT"),
        F.col("NO_RESP_AMT").alias("NO_RESP_AMT")
    )
)

df_Concat = df_StripFields_MedOut.unionByName(df_StripFields2_DntlOut)

df_Concat_dedup = dedup_sort(
    df_Concat,
    partition_cols=["CLCL_ID","CDML_SEQ_NO"],
    sort_cols=[]
)

df_BusinessLogic = (
    df_Concat_dedup.alias("co")
    .join(df_fcts_reversals.alias("fr"), F.col("co.CLCL_ID")==F.col("fr.CLCL_ID"), "left")
    .join(df_nasco_dup_lkup.alias("nl"), F.col("co.CLCL_ID")==F.col("nl.CLM_ID"), "left")
    .join(df_chk_w_pd_adj.alias("cp"), F.col("co.CLCL_ID")==F.col("cp.CLCL_ID"), "left")
    .join(df_reissue_only.alias("ro"), F.col("co.CLCL_ID")==F.col("ro.CLCL_ID"), "left")
)

df_BusinessLogic = df_BusinessLogic.withColumn("PassThru", F.lit("Y"))
df_BusinessLogic = df_BusinessLogic.withColumn("ClmId", trim(F.col("co.CLCL_ID")))
df_BusinessLogic = df_BusinessLogic.withColumn(
    "ThisIsAnAdjustedClaim",
    F.when((F.col("fr.CLCL_ID").isNotNull()) & (F.col("fr.CLCL_CUR_STS") == "91"), "Y").otherwise("N")
)
df_BusinessLogic = df_BusinessLogic.withColumn(
    "ThisIsACheckReissueOnly",
    F.when((F.col("ro.CLCL_ID").isNotNull()) & (F.col("cp.CLCL_ID").isNull()), "Y").otherwise("N")
)
df_BusinessLogic = df_BusinessLogic.withColumn(
    "ThisIsPdAdjWithOrighCheckReissue",
    F.when((F.col("ro.CLCL_ID").isNull()) & (F.col("cp.CLCL_ID").isNotNull()), "Y").otherwise("N")
)

df_Regular = df_BusinessLogic.filter(
    (F.col("nl.CLM_ID").isNull()) &
    (F.col("ThisIsAnAdjustedClaim") == "N")
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("ROW_PASS_THRU"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd),F.lit(";"),F.col("ClmId"),F.lit(";"),F.col("co.CDML_SEQ_NO")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_SK"),
    F.col("ClmId").alias("CLM_ID"),
    F.col("co.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("co.PATIENT_RESP").alias("REMIT_PATN_RESP_AMT"),
    F.col("co.PRPR_WRITEOFF").alias("REMIT_PROV_WRT_OFF_AMT"),
    F.col("co.MBR_OTHR_LIAB_AMT").alias("REMIT_MBR_OTHR_LIAB_AMT"),
    F.col("co.NO_RESP_AMT").alias("REMIT_NO_RESP_AMT")
)

df_ReversalA08 = df_BusinessLogic.filter(
    (F.col("fr.CLCL_ID").isNotNull()) &
    (F.col("ThisIsAnAdjustedClaim") == "Y")
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("ROW_PASS_THRU"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd),F.lit(";"),F.col("ClmId"),F.lit("R;"),F.col("co.CDML_SEQ_NO")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_SK"),
    F.concat(F.col("ClmId"),F.lit("R")).alias("CLM_ID"),
    F.col("co.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("ThisIsACheckReissueOnly")=="Y", -1*F.col("co.PATIENT_RESP"))
     .when(F.col("ThisIsPdAdjWithOrighCheckReissue")=="Y", F.col("co.PATIENT_RESP"))
     .otherwise(F.col("co.PATIENT_RESP")).alias("REMIT_PATN_RESP_AMT"),
    F.when(F.col("ThisIsACheckReissueOnly")=="Y", -1*F.col("co.PRPR_WRITEOFF"))
     .when(F.col("ThisIsPdAdjWithOrighCheckReissue")=="Y", F.col("co.PRPR_WRITEOFF"))
     .otherwise(F.col("co.PRPR_WRITEOFF")).alias("REMIT_PROV_WRT_OFF_AMT"),
    F.when(F.col("ThisIsACheckReissueOnly")=="Y", -1*F.col("co.MBR_OTHR_LIAB_AMT"))
     .when(F.col("ThisIsPdAdjWithOrighCheckReissue")=="Y", F.col("co.MBR_OTHR_LIAB_AMT"))
     .otherwise(F.col("co.MBR_OTHR_LIAB_AMT")).alias("REMIT_MBR_OTHR_LIAB_AMT"),
    F.when(F.col("ThisIsACheckReissueOnly")=="Y", -1*F.col("co.NO_RESP_AMT"))
     .when(F.col("ThisIsPdAdjWithOrighCheckReissue")=="Y", F.col("co.NO_RESP_AMT"))
     .otherwise(F.col("co.NO_RESP_AMT")).alias("REMIT_NO_RESP_AMT")
)

df_ReversalA09 = df_BusinessLogic.filter(
    (F.col("fr.CLCL_ID").isNotNull()) &
    (F.col("ThisIsAnAdjustedClaim") == "Y")
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("ROW_PASS_THRU"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd),F.lit(";"),F.col("ClmId"),F.lit(";"),F.col("co.CDML_SEQ_NO")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_SK"),
    F.col("ClmId").alias("CLM_ID"),
    F.col("co.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("ThisIsACheckReissueOnly")=="Y", F.col("co.PATIENT_RESP"))
     .when(F.col("ThisIsPdAdjWithOrighCheckReissue")=="Y", -1*F.col("co.PATIENT_RESP"))
     .otherwise(-1*F.col("co.PATIENT_RESP")).alias("REMIT_PATN_RESP_AMT"),
    F.when(F.col("ThisIsACheckReissueOnly")=="Y", F.col("co.PRPR_WRITEOFF"))
     .when(F.col("ThisIsPdAdjWithOrighCheckReissue")=="Y", -1*F.col("co.PRPR_WRITEOFF"))
     .otherwise(-1*F.col("co.PRPR_WRITEOFF")).alias("REMIT_PROV_WRT_OFF_AMT"),
    F.when(F.col("ThisIsACheckReissueOnly")=="Y", F.col("co.MBR_OTHR_LIAB_AMT"))
     .when(F.col("ThisIsPdAdjWithOrighCheckReissue")=="Y", -1*F.col("co.MBR_OTHR_LIAB_AMT"))
     .otherwise(-1*F.col("co.MBR_OTHR_LIAB_AMT")).alias("REMIT_MBR_OTHR_LIAB_AMT"),
    F.when(F.col("ThisIsACheckReissueOnly")=="Y", F.col("co.NO_RESP_AMT"))
     .when(F.col("ThisIsPdAdjWithOrighCheckReissue")=="Y", -1*F.col("co.NO_RESP_AMT"))
     .otherwise(-1*F.col("co.NO_RESP_AMT")).alias("REMIT_NO_RESP_AMT")
)

df_Link_Collector = df_Regular.unionByName(df_ReversalA08).unionByName(df_ReversalA09)

df_SnapShot_Out = df_Link_Collector.select(
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
    "REMIT_PATN_RESP_AMT",
    "REMIT_PROV_WRT_OFF_AMT",
    "REMIT_MBR_OTHR_LIAB_AMT",
    "REMIT_NO_RESP_AMT"
)

df_Transform = df_SnapShot_Out.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("ROW_PASS_THRU").alias("ROW_PASS_THRU"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("REMIT_PATN_RESP_AMT").alias("REMIT_PATN_RESP_AMT"),
    F.col("REMIT_PROV_WRT_OFF_AMT").alias("REMIT_PROV_WRT_OFF_AMT"),
    F.col("REMIT_MBR_OTHR_LIAB_AMT").alias("REMIT_MBR_OTHR_LIAB_AMT"),
    F.col("REMIT_NO_RESP_AMT").alias("REMIT_NO_RESP_AMT")
)

df_Balance = df_SnapShot_Out.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("REMIT_PATN_RESP_AMT").alias("REMIT_PATN_RESP_AMT"),
    F.col("REMIT_PROV_WRT_OFF_AMT").alias("REMIT_PROV_WRT_OFF_AMT"),
    F.col("REMIT_MBR_OTHR_LIAB_AMT").alias("REMIT_MBR_OTHR_LIAB_AMT"),
    F.col("REMIT_NO_RESP_AMT").alias("REMIT_NO_RESP_AMT")
)

params_ClmLnRemitPK = {
    "$BCBSOwner": bcbs_secret_name,
    "DriverTable": DriverTable,
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "CurrentDate": CurrentDate,
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk
}
df_ClmLnRemitPK_out = ClmLnRemitPK(df_Transform, params_ClmLnRemitPK)

df_IdsClmLnRemitExtr = df_ClmLnRemitPK_out.select(
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
    "REMIT_PATN_RESP_AMT",
    "REMIT_PROV_WRT_OFF_AMT",
    "REMIT_MBR_OTHR_LIAB_AMT",
    "REMIT_NO_RESP_AMT"
)

df_B_CLM_LN_REMIT = df_Balance

df_B_CLM_LN_REMIT_final = df_B_CLM_LN_REMIT.select(
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"),12," ").alias("CLM_ID"),
    "CLM_LN_SEQ_NO",
    "REMIT_PATN_RESP_AMT",
    "REMIT_PROV_WRT_OFF_AMT",
    "REMIT_MBR_OTHR_LIAB_AMT",
    "REMIT_NO_RESP_AMT"
)

write_files(
    df_B_CLM_LN_REMIT_final,
    f"{adls_path}/load/B_CLM_LN_REMIT.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_IdsClmLnRemitExtr_final = df_IdsClmLnRemitExtr.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("ROW_PASS_THRU"),1," ").alias("ROW_PASS_THRU"),
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LN_SK",
    F.rpad(F.col("CLM_ID"),12," ").alias("CLM_ID"),
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "REMIT_PATN_RESP_AMT",
    "REMIT_PROV_WRT_OFF_AMT",
    "REMIT_MBR_OTHR_LIAB_AMT",
    "REMIT_NO_RESP_AMT"
)

write_files(
    df_IdsClmLnRemitExtr_final,
    f"{adls_path}/key/BcbsClmLnRemitExtr.ClmLnRemit.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)