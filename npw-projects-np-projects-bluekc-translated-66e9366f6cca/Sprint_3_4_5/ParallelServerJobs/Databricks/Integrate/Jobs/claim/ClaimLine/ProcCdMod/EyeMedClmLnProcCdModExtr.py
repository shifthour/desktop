# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EyeMedClmLnExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the EyeMedClmLn_Landing.dat file and runs through primary key using shared container ClmLnPrcCdModPkey
# MAGIC     
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                            Date                            Project/Altiris #              Change Description                                                 Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                          --------------------                ------------------------         -----------------------------------------------------------------                --------------------------------              -------------------------------   ----------------------------      
# MAGIC Sethuraman Rajendran        2018-03-26                  5744 EyeMed                  Original Programming                                            IntegrateDev2                      Kalyan Neelam          2018-04-04

# MAGIC Transforming EyeMed ClmLn  to ClmLnProcCdMod
# MAGIC Hash file (hf_clm_ln_proc_cd_mod_allcol) cleared in calling program
# MAGIC Filter the Null Records
# MAGIC This container is used in:
# MAGIC FctsClmLnProcCdModExtr
# MAGIC NascoClmLnProcCdModExtr
# MAGIC BCBSSCClmLnProcCdModExtr
# MAGIC BCBSAClmLnProcCdModExtr
# MAGIC EyeMedClmLnProcCdModExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Sort and Pivot the Mod_Cd1 to Mod_Cd8
# MAGIC Remove Duplicates and assign Ordinality codes
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnProcCdModPK
# COMMAND ----------

schema_EyeMedClmLnLanding = T.StructType([
    T.StructField("CLM_ID", T.StringType(), False),
    T.StructField("CLM_LN_NO", T.StringType(), False),
    T.StructField("MBR_UNIQ_KEY", T.StringType(), False),
    T.StructField("SUB_UNIQ_KEY", T.StringType(), False),
    T.StructField("GRP_ID", T.StringType(), False),
    T.StructField("DOB", T.StringType(), True),
    T.StructField("GNDR_CD", T.StringType(), True),
    T.StructField("SUB_ID_IDS", T.StringType(), False),
    T.StructField("MBR_SFX_NO", T.StringType(), True),
    T.StructField("RCRD_TYP", T.StringType(), True),
    T.StructField("ADJ_VOID_FLAG", T.StringType(), True),
    T.StructField("EYEMED_GRP_ID", T.StringType(), True),
    T.StructField("EYEMED_SUBGRP_ID", T.StringType(), True),
    T.StructField("BILL_TYP_IN", T.StringType(), True),
    T.StructField("CLM_NO", T.StringType(), True),
    T.StructField("LN_CTR", T.StringType(), True),
    T.StructField("DT_OF_SVC", T.StringType(), True),
    T.StructField("INVC_NO", T.StringType(), True),
    T.StructField("INVC_DT", T.StringType(), True),
    T.StructField("BILL_AMT", T.StringType(), True),
    T.StructField("FFS_ADM_FEE", T.StringType(), True),
    T.StructField("RTL_AMT", T.StringType(), True),
    T.StructField("MBR_OOP", T.StringType(), True),
    T.StructField("3RD_PARTY_DSCNT", T.StringType(), True),
    T.StructField("COPAY_AMT", T.StringType(), True),
    T.StructField("COV_AMT", T.StringType(), True),
    T.StructField("FLR_1", T.StringType(), True),
    T.StructField("FLR_2", T.StringType(), True),
    T.StructField("NTWK_IN", T.StringType(), True),
    T.StructField("SVC_CD", T.StringType(), True),
    T.StructField("SVC_DESC", T.StringType(), True),
    T.StructField("MOD_CD_1", T.StringType(), True),
    T.StructField("MOD_CD_2", T.StringType(), True),
    T.StructField("MOD_CD_3", T.StringType(), True),
    T.StructField("MOD_CD_4", T.StringType(), True),
    T.StructField("MOD_CD_5", T.StringType(), True),
    T.StructField("MOD_CD_6", T.StringType(), True),
    T.StructField("MOD_CD_7", T.StringType(), True),
    T.StructField("MOD_CD_8", T.StringType(), True),
    T.StructField("ICD_CD_SET", T.StringType(), True),
    T.StructField("DIAG_CD_1", T.StringType(), True),
    T.StructField("DIAG_CD_2", T.StringType(), True),
    T.StructField("DIAG_CD_3", T.StringType(), True),
    T.StructField("DIAG_CD_4", T.StringType(), True),
    T.StructField("DIAG_CD_5", T.StringType(), True),
    T.StructField("DIAG_CD_6", T.StringType(), True),
    T.StructField("DIAG_CD_7", T.StringType(), True),
    T.StructField("DIAG_CD_8", T.StringType(), True),
    T.StructField("PATN_ID", T.StringType(), True),
    T.StructField("PATN_SSN", T.StringType(), True),
    T.StructField("PATN_FIRST_NM", T.StringType(), True),
    T.StructField("PATN_MIDINIT", T.StringType(), True),
    T.StructField("PATN_LAST_NM", T.StringType(), True),
    T.StructField("PATN_GNDR", T.StringType(), True),
    T.StructField("PATN_FMLY_RELSHP", T.StringType(), True),
    T.StructField("PATN_DOB", T.StringType(), True),
    T.StructField("PATN_ADDR", T.StringType(), True),
    T.StructField("PATN_ADDR_2", T.StringType(), True),
    T.StructField("PATN_CITY", T.StringType(), True),
    T.StructField("PATN_ST", T.StringType(), True),
    T.StructField("PATN_ZIP", T.StringType(), True),
    T.StructField("PATN_ZIP4", T.StringType(), True),
    T.StructField("CLNT_GRP_NO", T.StringType(), True),
    T.StructField("CO_CD", T.StringType(), True),
    T.StructField("DIV_CD", T.StringType(), True),
    T.StructField("LOC_CD", T.StringType(), True),
    T.StructField("CLNT_RPTNG_1", T.StringType(), True),
    T.StructField("CLNT_RPTNG_2", T.StringType(), True),
    T.StructField("CLNT_RPTNG_3", T.StringType(), True),
    T.StructField("CLNT_RPTNG_4", T.StringType(), True),
    T.StructField("CLNT_RPTNG_5", T.StringType(), True),
    T.StructField("CLS_PLN_ID", T.StringType(), True),
    T.StructField("SUB_ID", T.StringType(), True),
    T.StructField("SUB_SSN", T.StringType(), True),
    T.StructField("SUB_FIRST_NM", T.StringType(), True),
    T.StructField("SUB_MIDINIT", T.StringType(), True),
    T.StructField("SUB_LAST_NM", T.StringType(), True),
    T.StructField("SUB_GNDR", T.StringType(), True),
    T.StructField("SUB_DOB", T.StringType(), True),
    T.StructField("SUB_ADDR", T.StringType(), True),
    T.StructField("SUB_ADDR_2", T.StringType(), True),
    T.StructField("SUB_CITY", T.StringType(), True),
    T.StructField("SUB_ST", T.StringType(), True),
    T.StructField("SUB_ZIP", T.StringType(), True),
    T.StructField("SUB_ZIP_PLUS_4", T.StringType(), True),
    T.StructField("PROV_ID", T.StringType(), True),
    T.StructField("PROV_NPI", T.StringType(), True),
    T.StructField("TAX_ENTY_NPI", T.StringType(), True),
    T.StructField("PROV_FIRST_NM", T.StringType(), True),
    T.StructField("PROV_LAST_NM", T.StringType(), True),
    T.StructField("BUS_NM", T.StringType(), True),
    T.StructField("PROV_ADDR", T.StringType(), True),
    T.StructField("PROV_ADDR_2", T.StringType(), True),
    T.StructField("PROV_CITY", T.StringType(), True),
    T.StructField("PROV_ST", T.StringType(), True),
    T.StructField("PROV_ZIP", T.StringType(), True),
    T.StructField("PROV_ZIP_PLUS_4", T.StringType(), True),
    T.StructField("PROF_DSGTN", T.StringType(), True),
    T.StructField("TAX_ENTY_ID", T.StringType(), True),
    T.StructField("TXNMY_CD", T.StringType(), True),
    T.StructField("CLM_RCVD_DT", T.StringType(), True),
    T.StructField("ADJDCT_DT", T.StringType(), True),
    T.StructField("CHK_DT", T.StringType(), True),
    T.StructField("DENIAL_RSN_CD", T.StringType(), True),
    T.StructField("SVC_TYP", T.StringType(), True),
    T.StructField("UNIT_OF_SVC", T.StringType(), True),
    T.StructField("FLR", T.StringType(), True)
])

df_EyeMedClmLnLanding = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_EyeMedClmLnLanding)
    .load(f"{adls_path}/verified/EyeMedClm_ClmLnlanding.dat.{RunID}")
)

df_Lkup_Transformer = df_EyeMedClmLnLanding

df_Extract = df_Lkup_Transformer.select(
    F.rpad(F.col("CLM_ID"), 17, " ").alias("CLM_ID"),
    F.rpad(F.col("CLM_LN_NO"), 6, " ").alias("CLM_LN_NO"),
    F.col("MOD_CD_1").alias("MOD_CD_1"),
    F.col("MOD_CD_2").alias("MOD_CD_2"),
    F.col("MOD_CD_3").alias("MOD_CD_3"),
    F.col("MOD_CD_4").alias("MOD_CD_4"),
    F.col("MOD_CD_5").alias("MOD_CD_5"),
    F.col("MOD_CD_6").alias("MOD_CD_6"),
    F.col("MOD_CD_7").alias("MOD_CD_7"),
    F.col("MOD_CD_8").alias("MOD_CD_8")
)

df_Sort_Proc_Cd = df_Extract.orderBy("CLM_ID", "CLM_LN_NO")

df_ProcCd_Pivot = df_Sort_Proc_Cd.select(
    F.rpad(F.col("CLM_ID"), 17, " ").alias("CLM_ID"),
    F.rpad(F.col("CLM_LN_NO"), 6, " ").alias("CLM_LN_NO"),
    F.col("MOD_CD_1"),
    F.col("MOD_CD_2"),
    F.col("MOD_CD_3"),
    F.col("MOD_CD_4"),
    F.col("MOD_CD_5"),
    F.col("MOD_CD_6"),
    F.col("MOD_CD_7"),
    F.col("MOD_CD_8")
)

df_ProcCd_Pivot = df_ProcCd_Pivot.select(
    F.rpad(F.col("CLM_ID"), 17, " ").alias("CLM_ID"),
    # The pivot output requires CLM_LN_NO as char(2)
    F.rpad(F.col("CLM_LN_NO"), 2, " ").alias("CLM_LN_NO"),
    F.explode(F.array(
        "MOD_CD_1", "MOD_CD_2", "MOD_CD_3", "MOD_CD_4",
        "MOD_CD_5", "MOD_CD_6", "MOD_CD_7", "MOD_CD_8"
    )).alias("MOD_CD")
)

df_Trf_FilterNull = df_ProcCd_Pivot.filter(
    (F.col("MOD_CD").isNotNull()) & (trim(F.col("MOD_CD")) != "")
)

# Remove Hashed_File_126 (Scenario A) and deduplicate by (CLM_ID, CLM_LN_NO, MOD_CD)
df_dedup = dedup_sort(df_Trf_FilterNull, ["CLM_ID", "CLM_LN_NO", "MOD_CD"], [])

windowSpec = Window.partitionBy("CLM_ID","CLM_LN_NO").orderBy("CLM_ID","CLM_LN_NO")

dfTrfAssignOrdlCd = (
    df_dedup
    .withColumn("PKString", F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CLM_ID"), F.lit(";"), F.col("CLM_LN_NO")))
    .withColumn("CLM_LN_PROC_CD_MOD_ORDNL_CD", F.row_number().over(windowSpec))
)

df_Trans = (
    dfTrfAssignOrdlCd
    .filter((F.col("MOD_CD").isNotNull()) & (trim(F.col("MOD_CD")) != ""))
    .select(
        F.rpad(F.col("CLM_ID"), 17, " ").alias("CLM_ID"),
        # The transformer output uses CLM_LN_NO as char(2)
        F.rpad(F.col("CLM_LN_NO"), 2, " ").alias("CLM_LN_SEQ_NO"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        F.lit(current_date()).alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.concat(F.col("PKString"), F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MOD_CD").alias("PROC_CD_MOD_CD")
    )
)

# Snapshot stage has 3 output links
dfSnapshotAllCol = df_Trans.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_LN_PROC_CD_MOD_SK").alias("CLM_LN_PROC_CD_MOD_SK"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROC_CD_MOD_CD").alias("PROC_CD_MOD_CD")
)

dfSnapshotSnapshot = df_Trans.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD")
)

dfSnapshotTransform = df_Trans.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD")
)

# Transformer stage after Snapshot -> "Snapshot" link
dfTransformer = dfSnapshotSnapshot.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    GetFkeyCodes(
        SrcSysCd,
        0,
        "PROCEDURE ORDINAL",
        trim(F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD")),
        "X"
    ).alias("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
)

dfB_CLM_LN_PROC_CD_MOD = dfTransformer.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"), 17, " ").alias("CLM_ID"),
    F.rpad(F.col("CLM_LN_SEQ_NO"), 2, " ").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
)

write_files(
    dfB_CLM_LN_PROC_CD_MOD,
    f"{adls_path}/load/B_CLM_LN_PROC_CD_MOD.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

params_ClmLnProcCdModPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}

dfKey = ClmLnProcCdModPK(dfSnapshotTransform, dfSnapshotAllCol, params_ClmLnProcCdModPK)

# Final output file
# Columns from container output:
# [JOB_EXCTN_RCRD_ERR_SK, INSRT_UPDT_CD, DISCARD_IN, PASS_THRU_IN, FIRST_RECYC_DT,
#  ERR_CT, RECYCLE_CT, SRC_SYS_CD, PRI_KEY_STRING, CLM_LN_PROC_CD_MOD_SK,
#  CLM_ID, CLM_LN_SEQ_NO, CLM_LN_PROC_CD_MOD_ORDNL_CD, CRT_RUN_CYC_EXCTN_SK,
#  LAST_UPDT_RUN_CYC_EXCTN_SK, PROC_CD_MOD_CD]

dfFinal = dfKey.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_PROC_CD_MOD_SK"),
    F.rpad(F.col("CLM_ID"), 17, " ").alias("CLM_ID"),
    F.rpad(F.col("CLM_LN_SEQ_NO"), 2, " ").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROC_CD_MOD_CD")
)

write_files(
    dfFinal,
    f"{adls_path}/key/EyeMedClmLnProcCdModExtr.EyeMedClmLnProcCdMod.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)