# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:      Reads the EyeMed_Clm_landing.dat Extract file created from EyeMedClmPrepProcExtr job and adds the Recycle fields and values for the SK if possible.
# MAGIC 
# MAGIC PROCESSING:   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                        Date                 Project/Altiris #              Change Description                                                                  Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                      --------------------     ------------------------          -----------------------------------------------------------------------                            --------------------------------       -------------------------------   ----------------------------       
# MAGIC Sethuraman Rajendran    2018-03-22         5744                          Originally Programmed                                                                IntegrateDev2               Kalyan Neelam           2018-04-04
# MAGIC 
# MAGIC Goutham K                         2021-06-02         US-366403            New Provider file Change to include Loc and Svc loc id                  IntegrateDev1          Jeyaprasanna             2021-06-08

# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC Diag Cods file is used in 
# MAGIC BCAFEPClmDRGExtr 
# MAGIC EyeMedClmExtr job
# MAGIC Process to extract the Diag Codes
# MAGIC Apply business logic
# MAGIC This container is used in:
# MAGIC FctsClmDiagExtr
# MAGIC NascoClmDiagExtr
# MAGIC BCBSSCClmDiagExtr
# MAGIC BCBSAClmDiagExtr
# MAGIC EyeMedClmExtr 
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Hash file (hf_clm_diag_allcol) cleared in the calling program
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmDiagPK
# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, trim, length, row_number, when, rpad, expr
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')

# ----------------------------------------------------------------------------
# Stage: EyeMedClm_Landing (CSeqFileStage)
# ----------------------------------------------------------------------------
schema_EyeMedClm_Landing = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_NO", DecimalType(38,10), True),
    StructField("ADJ_FROM_CLM_ID", StringType(), True),
    StructField("ADJ_TO_CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("SUB_ID_IDS", StringType(), True),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("RCRD_TYP", StringType(), True),
    StructField("ADJ_VOID_FLAG", StringType(), True),
    StructField("EYEMED_GRP_ID", StringType(), True),
    StructField("EYEMED_SUBGRP_ID", StringType(), True),
    StructField("BILL_TYP_IN", StringType(), True),
    StructField("CLM_NO", StringType(), True),
    StructField("LN_CTR", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("INVC_NO", StringType(), True),
    StructField("INVC_DT", StringType(), True),
    StructField("BILL_AMT", StringType(), True),
    StructField("FFS_ADM_FEE", StringType(), True),
    StructField("RTL_AMT", StringType(), True),
    StructField("MBR_OOP", StringType(), True),
    StructField("3RD_PARTY_DSCNT", StringType(), True),
    StructField("COPAY_AMT", StringType(), True),
    StructField("COV_AMT", StringType(), True),
    StructField("FLR_1", StringType(), True),
    StructField("FLR_2", StringType(), True),
    StructField("NTWK_IN", StringType(), True),
    StructField("SVC_CD", StringType(), True),
    StructField("SVC_DESC", StringType(), True),
    StructField("MOD_CD_1", StringType(), True),
    StructField("MOD_CD_2", StringType(), True),
    StructField("MOD_CD_3", StringType(), True),
    StructField("MOD_CD_4", StringType(), True),
    StructField("MOD_CD_5", StringType(), True),
    StructField("MOD_CD_6", StringType(), True),
    StructField("MOD_CD_7", StringType(), True),
    StructField("MOD_CD_8", StringType(), True),
    StructField("ICD_CD_SET", StringType(), True),
    StructField("DIAG_CD_1", StringType(), True),
    StructField("DIAG_CD_2", StringType(), True),
    StructField("DIAG_CD_3", StringType(), True),
    StructField("DIAG_CD_4", StringType(), True),
    StructField("DIAG_CD_5", StringType(), True),
    StructField("DIAG_CD_6", StringType(), True),
    StructField("DIAG_CD_7", StringType(), True),
    StructField("DIAG_CD_8", StringType(), True),
    StructField("PATN_ID", StringType(), True),
    StructField("PATN_SSN", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_MIDINIT", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("PATN_GNDR", StringType(), True),
    StructField("PATN_FMLY_RELSHP", StringType(), True),
    StructField("PATN_DOB", StringType(), True),
    StructField("PATN_ADDR", StringType(), True),
    StructField("PATN_ADDR_2", StringType(), True),
    StructField("PATN_CITY", StringType(), True),
    StructField("PATN_ST", StringType(), True),
    StructField("PATN_ZIP", StringType(), True),
    StructField("PATN_ZIP4", StringType(), True),
    StructField("CLNT_GRP_NO", StringType(), True),
    StructField("CO_CD", StringType(), True),
    StructField("DIV_CD", StringType(), True),
    StructField("LOC_CD", StringType(), True),
    StructField("CLNT_RPTNG_1", StringType(), True),
    StructField("CLNT_RPTNG_2", StringType(), True),
    StructField("CLNT_RPTNG_3", StringType(), True),
    StructField("CLNT_RPTNG_4", StringType(), True),
    StructField("CLNT_RPTNG_5", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("SUB_SSN", StringType(), True),
    StructField("SUB_FIRST_NM", StringType(), True),
    StructField("SUB_MIDINIT", StringType(), True),
    StructField("SUB_LAST_NM", StringType(), True),
    StructField("SUB_GNDR", StringType(), True),
    StructField("SUB_DOB", StringType(), True),
    StructField("SUB_ADDR", StringType(), True),
    StructField("SUB_ADDR_2", StringType(), True),
    StructField("SUB_CITY", StringType(), True),
    StructField("SUB_ST", StringType(), True),
    StructField("SUB_ZIP", StringType(), True),
    StructField("SUB_ZIP_PLUS_4", StringType(), True),
    StructField("PROV_ID", StringType(), True),
    StructField("PROV_NPI", StringType(), True),
    StructField("TAX_ENTY_NPI", StringType(), True),
    StructField("PROV_FIRST_NM", StringType(), True),
    StructField("PROV_LAST_NM", StringType(), True),
    StructField("BUS_NM", StringType(), True),
    StructField("PROV_ADDR", StringType(), True),
    StructField("PROV_ADDR_2", StringType(), True),
    StructField("PROV_CITY", StringType(), True),
    StructField("PROV_ST", StringType(), True),
    StructField("PROV_ZIP", StringType(), True),
    StructField("PROV_ZIP_PLUS_4", StringType(), True),
    StructField("PROF_DSGTN", StringType(), True),
    StructField("TAX_ENTY_ID", StringType(), True),
    StructField("TXNMY_CD", StringType(), True),
    StructField("CLM_RCVD_DT", StringType(), True),
    StructField("ADJDCT_DT", StringType(), True),
    StructField("CHK_DT", StringType(), True),
    StructField("DENIAL_RSN_CD", StringType(), True),
    StructField("SVC_TYP", StringType(), True),
    StructField("UNIT_OF_SVC", StringType(), True),
    StructField("LOC_ID", StringType(), True),
    StructField("SVC_LOC_ID", StringType(), True),
    StructField("FLR", StringType(), True)
])

file_path_EyeMedClm_ClaimsLanding = f"{adls_path}/verified/EyeMedClm_ClaimsLanding.dat.{RunID}"

df_EyeMedClm_ClaimsLanding = (
    spark.read
    .format("csv")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_EyeMedClm_Landing)
    .load(file_path_EyeMedClm_ClaimsLanding)
)

df_EyeMed_Clm = df_EyeMedClm_ClaimsLanding.select(
    "CLM_ID","CLM_LN_NO","ADJ_FROM_CLM_ID","ADJ_TO_CLM_ID","CLM_STTUS_CD","MBR_UNIQ_KEY","SUB_UNIQ_KEY","GRP_ID","DOB",
    "GNDR_CD","SUB_ID_IDS","MBR_SFX_NO","RCRD_TYP","ADJ_VOID_FLAG","EYEMED_GRP_ID","EYEMED_SUBGRP_ID","BILL_TYP_IN","CLM_NO","LN_CTR",
    "DT_OF_SVC","INVC_NO","INVC_DT","BILL_AMT","FFS_ADM_FEE","RTL_AMT","MBR_OOP","3RD_PARTY_DSCNT","COPAY_AMT","COV_AMT","FLR_1","FLR_2",
    "NTWK_IN","SVC_CD","SVC_DESC","MOD_CD_1","MOD_CD_2","MOD_CD_3","MOD_CD_4","MOD_CD_5","MOD_CD_6","MOD_CD_7","MOD_CD_8","ICD_CD_SET",
    "DIAG_CD_1","DIAG_CD_2","DIAG_CD_3","DIAG_CD_4","DIAG_CD_5","DIAG_CD_6","DIAG_CD_7","DIAG_CD_8","PATN_ID","PATN_SSN","PATN_FIRST_NM",
    "PATN_MIDINIT","PATN_LAST_NM","PATN_GNDR","PATN_FMLY_RELSHP","PATN_DOB","PATN_ADDR","PATN_ADDR_2","PATN_CITY","PATN_ST","PATN_ZIP",
    "PATN_ZIP4","CLNT_GRP_NO","CO_CD","DIV_CD","LOC_CD","CLNT_RPTNG_1","CLNT_RPTNG_2","CLNT_RPTNG_3","CLNT_RPTNG_4","CLNT_RPTNG_5","CLS_PLN_ID",
    "SUB_ID","SUB_SSN","SUB_FIRST_NM","SUB_MIDINIT","SUB_LAST_NM","SUB_GNDR","SUB_DOB","SUB_ADDR","SUB_ADDR_2","SUB_CITY","SUB_ST","SUB_ZIP",
    "SUB_ZIP_PLUS_4","PROV_ID","PROV_NPI","TAX_ENTY_NPI","PROV_FIRST_NM","PROV_LAST_NM","BUS_NM","PROV_ADDR","PROV_ADDR_2","PROV_CITY","PROV_ST",
    "PROV_ZIP","PROV_ZIP_PLUS_4","PROF_DSGTN","TAX_ENTY_ID","TXNMY_CD","CLM_RCVD_DT","ADJDCT_DT","CHK_DT","DENIAL_RSN_CD","SVC_TYP",
    "UNIT_OF_SVC","LOC_ID","SVC_LOC_ID","FLR"
)

# ----------------------------------------------------------------------------
# Stage: Xfm_Diag_cd (CTransformerStage)
# ----------------------------------------------------------------------------
df_Xfm_Diag_cd = df_EyeMed_Clm.select(
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_NO").alias("CLM_LN_NO"),
    col("ICD_CD_SET").alias("ICD_CD_SET"),
    col("DIAG_CD_1").alias("DIAG_CD_1"),
    col("DIAG_CD_2").alias("DIAG_CD_2"),
    col("DIAG_CD_3").alias("DIAG_CD_3"),
    col("DIAG_CD_4").alias("DIAG_CD_4"),
    col("DIAG_CD_5").alias("DIAG_CD_5"),
    col("DIAG_CD_6").alias("DIAG_CD_6"),
    col("DIAG_CD_7").alias("DIAG_CD_7"),
    col("DIAG_CD_8").alias("DIAG_CD_8"),
    lit('1').alias("DIAGNO1"),
    lit('2').alias("DIAGNO2"),
    lit('3').alias("DIAGNO3"),
    lit('4').alias("DIAGNO4"),
    lit('5').alias("DIAGNO5"),
    lit('6').alias("DIAGNO6"),
    lit('7').alias("DIAGNO7"),
    lit('8').alias("DIAGNO8")
)

# ----------------------------------------------------------------------------
# Stage: DiagCd_Pivot (Pivot)
# ----------------------------------------------------------------------------
df_DiagCd_Pivot = (
    df_Xfm_Diag_cd.select(
        "CLM_ID",
        "CLM_LN_NO",
        "ICD_CD_SET",
        expr(
            """stack(
               8,
               DIAG_CD_1, DIAGNO1,
               DIAG_CD_2, DIAGNO2,
               DIAG_CD_3, DIAGNO3,
               DIAG_CD_4, DIAGNO4,
               DIAG_CD_5, DIAGNO5,
               DIAG_CD_6, DIAGNO6,
               DIAG_CD_7, DIAGNO7,
               DIAG_CD_8, DIAGNO8
            ) as (DIAG_CD, DIAG_NO)"""
        )
    )
    .select(
        col("CLM_ID"),
        col("CLM_LN_NO"),
        col("DIAG_CD"),
        col("DIAG_NO"),
        col("ICD_CD_SET").alias("ICD_VRSN_CD")
    )
)

# ----------------------------------------------------------------------------
# Stage: DiagCds (CTransformerStage)
#   Filter constraint: not null DIAG_CD and length(trim(DIAG_CD)) > 0
#   plus add DIAG_POA_IN => 'NA'
# ----------------------------------------------------------------------------
df_DiagCds = (
    df_DiagCd_Pivot
    .filter(
        (col("DIAG_CD").isNotNull()) &
        (length(trim(col("DIAG_CD"))) > 0)
    )
    .select(
        col("CLM_ID").alias("CLM_ID"),
        col("DIAG_CD").alias("DIAG_CD"),
        col("CLM_LN_NO").alias("CLM_LN_NO"),
        col("DIAG_NO").alias("DIAG_NO"),
        col("ICD_VRSN_CD").alias("ICD_VRSN_CD"),
        lit("NA").alias("DIAG_POA_IN")
    )
)

# ----------------------------------------------------------------------------
# Stage: SortDiagCds1 (sort)
#   SORTSPEC: CLM_ID asc, CLM_LN_NO D, DIAG_NO D
# ----------------------------------------------------------------------------
df_SortDiagCds1 = df_DiagCds.sort(
    col("CLM_ID").asc(),
    col("CLM_LN_NO").desc(),
    col("DIAG_NO").desc()
).select(
    "CLM_ID","DIAG_CD","CLM_LN_NO","DIAG_NO","ICD_VRSN_CD","DIAG_POA_IN"
)

# ----------------------------------------------------------------------------
# Stage: hf_eyemed_clm_diagcd_dedupe (CHashedFileStage)
#   Scenario A => drop duplicates on keys [CLM_ID, DIAG_CD]
# ----------------------------------------------------------------------------
df_hf_eyemed_clm_diagcd_dedupe = df_SortDiagCds1.dropDuplicates(["CLM_ID","DIAG_CD"])

# ----------------------------------------------------------------------------
# Stage: SortDiagCds2 (sort)
#   SORTSPEC: CLM_ID asc, CLM_LN_NO asc, DIAG_NO asc
# ----------------------------------------------------------------------------
df_SortDiagCds2 = df_hf_eyemed_clm_diagcd_dedupe.sort(
    col("CLM_ID").asc(),
    col("CLM_LN_NO").asc(),
    col("DIAG_NO").asc()
).select(
    "CLM_ID","DIAG_CD","CLM_LN_NO","DIAG_NO","ICD_VRSN_CD","DIAG_POA_IN"
)

# ----------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
#   Derive ordinal with row_number partitioned by CLM_ID (mimicking stage var logic).
# ----------------------------------------------------------------------------
w = Window.partitionBy("CLM_ID").orderBy("CLM_LN_NO","DIAG_NO")
df_BusinessRules_pre = df_SortDiagCds2.withColumn("svOrdnlNo", row_number().over(w))

df_BusinessRules = df_BusinessRules_pre.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),           # char(10)
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),               # char(1)
    rpad(lit("Y"), 1, " ").alias("PASS_THRU_IN"),             # char(1)
    lit(CurrentDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    expr("concat(SRC_SYS_CD, ';', CLM_ID, ';1)").alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_DIAG_SK"),
    col("CLM_ID").alias("ClmId"),
    rpad(col("svOrdnlNo").cast(StringType()), 2, " ").alias("CLM_DIAG_ORDNL_CD"),  # char(2)
    lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    Change(col("DIAG_CD"), '.', '').alias("DIAG_CD"),
    rpad(lit("NA"), 2, " ").alias("CLM_DIAG_POA_CD"),         # char(2)
    when(col("ICD_VRSN_CD") == '9', 'ICD9')
     .when(col("ICD_VRSN_CD") == '0', 'ICD10')
     .otherwise('UNK').alias("DIAG_CD_TYP_CD")
)

# ----------------------------------------------------------------------------
# Stage: Snapshot (CTransformerStage) 
#   It has 3 output links from the same input df_BusinessRules.
# ----------------------------------------------------------------------------

# Output link "AllCol" -> 18 columns
df_Snapshot_AllCol = df_BusinessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("ClmId").alias("CLM_ID"),
    col("CLM_DIAG_ORDNL_CD"),
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_DIAG_SK"),
    col("CRT_RUN_CYC_EXTCN_SK"),
    col("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    col("DIAG_CD"),
    col("CLM_DIAG_POA_CD"),
    col("DIAG_CD_TYP_CD")
)

# Output link "Snapshot" -> columns: CLM_ID, CLM_DIAG_ORDNL_CD
df_Snapshot_Snapshot = df_BusinessRules.select(
    col("ClmId").alias("CLM_ID"),
    col("CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD")
)

# Output link "Transform" -> columns: SRC_SYS_CD_SK, CLM_ID, CLM_DIAG_ORDNL_CD
df_Snapshot_Transform = df_BusinessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("ClmId").alias("CLM_ID"),
    col("CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD")
)

# ----------------------------------------------------------------------------
# Stage: ClmDiagPK (CContainerStage)
#   It has 2 inputs: df_Snapshot_AllCol and df_Snapshot_Transform
#   1 output -> "Key"
# ----------------------------------------------------------------------------
params_ClmDiagPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}

df_ClmDiagPK = ClmDiagPK(df_Snapshot_AllCol, df_Snapshot_Transform, params_ClmDiagPK)

# ----------------------------------------------------------------------------
# Stage: EyeMedClmDiag (CSeqFileStage)
#   Write file: key/EyeMedClmDiagExtr.EyeMedClmDiag.dat.#RunID#
# ----------------------------------------------------------------------------
# Final select with 18 columns from the container output
df_EyeMedClmDiag = df_ClmDiagPK.select(
    rpad(col("JOB_EXCTN_RCRD_ERR_SK").cast(StringType()), 0, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_DIAG_SK"),
    rpad(col("SRC_SYS_CD_SK").cast(StringType()), 0, " ").alias("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    rpad(col("CLM_DIAG_ORDNL_CD"), 2, " ").alias("CLM_DIAG_ORDNL_CD"),
    col("CRT_RUN_CYC_EXTCN_SK"),
    col("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    col("DIAG_CD"),
    rpad(col("CLM_DIAG_POA_CD"), 2, " ").alias("CLM_DIAG_POA_CD"),
    col("DIAG_CD_TYP_CD")
)

write_files(
    df_EyeMedClmDiag,
    f"{adls_path}/key/EyeMedClmDiagExtr.EyeMedClmDiag.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: Transformer (CTransformerStage)
#   Input: from Snapshot "df_Snapshot_Snapshot"
#   StageVariable: svDiagOrdlCd = GetFkeyCodes(SrcSysCd, 0, "DIAGNOSIS ORDINAL", Snapshot.CLM_DIAG_ORDNL_CD, 'X')
# ----------------------------------------------------------------------------
df_Transformer_pre = df_Snapshot_Snapshot.withColumn(
    "svDiagOrdlCd",
    GetFkeyCodes(
        SrcSysCd,
        lit(0),
        lit("DIAGNOSIS ORDINAL"),
        col("CLM_DIAG_ORDNL_CD"),
        lit("X")
    )
)

df_Transformer = df_Transformer_pre.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("svDiagOrdlCd").alias("CLM_DIAG_ORDNL_CD")
)

# ----------------------------------------------------------------------------
# Stage: B_CLM_DIAG (CSeqFileStage)
#   Write file: load/B_CLM_DIAG.EYEMED.dat.#RunID#
# ----------------------------------------------------------------------------
df_B_CLM_DIAG = df_Transformer.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_DIAG_ORDNL_CD"
)

write_files(
    df_B_CLM_DIAG,
    f"{adls_path}/load/B_CLM_DIAG.EYEMED.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)