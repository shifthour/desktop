# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EYEMEDClmLnDiagExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the EyeMedClm_ClmLnLanding.dat file and runs through primary key using shared container ClmLnDiagPK
# MAGIC     
# MAGIC 
# MAGIC  
# MAGIC MODIFICATIONS:
# MAGIC Developer                                        Date                  Project/Altiris #     Change Description                                               Development Project        Code Reviewer          Date Reviewed       
# MAGIC ------------------                                      --------------------      -----------------------      -----------------------------------------------------------------------       --------------------------------         -------------------------------   ----------------------------      
# MAGIC Sethuraman Rajendran                    2018-03-22        5744 EYEMED     Original Programming                                             IntegrateDev2                   Kalyan Neelam           2018-04-04

# MAGIC Process to extract the Diag Codes
# MAGIC Writing Sequential File to ../key
# MAGIC Assign primary surrogate key
# MAGIC This container is used in:
# MAGIC FctsClmLnDiagExtr
# MAGIC NascoClmLnDiagExtr
# MAGIC BCBSSCClmLnDiagExtr
# MAGIC EyeMedClmLnDiagExtr
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Hash file (hf_clm_diag_allcol) cleared in the calling program
# MAGIC Apply business logic
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType
from pyspark.sql.functions import col, lit, trim, length, regexp_replace, row_number, when, explode, arrays_zip, array, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnDiagPK
# COMMAND ----------

# Parameters
CurrRunCycle = get_widget_value('CurrRunCycle', '1')
RunID = get_widget_value('RunID', '100')
SrcSysCdSk = get_widget_value('SrcSysCdSk', '-2022470479')
SrcSysCd = get_widget_value('SrcSysCd', 'EyeMed')
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
facets_secret_name = get_widget_value('facets_secret_name', '')
CurrentDate = get_widget_value('CurrentDate', '2018-03-29')

# Schema for EyeMedClmLnLanding (CSeqFileStage)
schema_EyeMedClmLnLanding = StructType([
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_NO", DecimalType(38,10), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("DOB", StringType(), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("SUB_ID_IDS", StringType(), False),
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
    StructField("FLR", StringType(), True)
])

df_EyeMedClmLnLanding = (
    spark.read.format("csv")
    .schema(schema_EyeMedClmLnLanding)
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .load(f"{adls_path}/verified/EyeMedClm_ClmLnlanding.dat.{RunID}")
)

# Xfm_Diag_cd (CTransformerStage)
df_Xfm_Diag_cd = df_EyeMedClmLnLanding.select(
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
    lit("1").alias("DIAGNO1"),
    lit("2").alias("DIAGNO2"),
    lit("3").alias("DIAGNO3"),
    lit("4").alias("DIAGNO4"),
    lit("5").alias("DIAGNO5"),
    lit("6").alias("DIAGNO6"),
    lit("7").alias("DIAGNO7"),
    lit("8").alias("DIAGNO8")
)

# DiagCd_Pivot (Pivot)
df_pivot = (
    df_Xfm_Diag_cd.select(
        col("CLM_ID"),
        col("CLM_LN_NO"),
        explode(
            arrays_zip(
                array(
                    col("DIAG_CD_1"),
                    col("DIAG_CD_2"),
                    col("DIAG_CD_3"),
                    col("DIAG_CD_4"),
                    col("DIAG_CD_5"),
                    col("DIAG_CD_6"),
                    col("DIAG_CD_7"),
                    col("DIAG_CD_8")
                ),
                array(
                    col("DIAGNO1"),
                    col("DIAGNO2"),
                    col("DIAGNO3"),
                    col("DIAGNO4"),
                    col("DIAGNO5"),
                    col("DIAGNO6"),
                    col("DIAGNO7"),
                    col("DIAGNO8")
                )
            )
        ).alias("pivoted"),
        col("ICD_CD_SET").alias("ICD_VRSN_CD")
    )
    .withColumn("DIAG_CD", col("pivoted").getItem(0))
    .withColumn("DIAG_NO", col("pivoted").getItem(1))
    .drop("pivoted")
)

# DiagCds (CTransformerStage)
df_DiagCds = (
    df_pivot
    .filter(
        col("DIAG_CD").isNotNull() &
        (length(trim(col("DIAG_CD"))) > 0)
    )
    .select(
        col("CLM_ID"),
        col("CLM_LN_NO"),
        col("DIAG_CD"),
        col("DIAG_NO"),
        col("ICD_VRSN_CD")
    )
)

# SortDiagCds1 (sort)
df_SortDiagCds1 = df_DiagCds.sort(
    col("CLM_ID").asc(),
    col("CLM_LN_NO").asc(),
    col("DIAG_NO").desc()
)

# hf_eyemed_medclm_diagcd_dedupe (CHashedFileStage) => Scenario A
# Replace with dedup based on key columns (CLM_ID, CLM_LN_NO, DIAG_CD)
df_hashDedup = df_SortDiagCds1.dropDuplicates(["CLM_ID", "CLM_LN_NO", "DIAG_CD"])

# SortDiagCds2 (sort)
df_SortDiagCds2 = df_hashDedup.sort(
    col("CLM_ID").asc(),
    col("CLM_LN_NO").asc(),
    col("DIAG_NO").asc()
)

# BusinessRules (CTransformerStage)
# Use row_number to replicate svOrdnlCd logic
w = Window.partitionBy("CLM_ID","CLM_LN_NO").orderBy("DIAG_NO")
df_BusinessRules_pre = df_SortDiagCds2.withColumn("svOrdnlCd", row_number().over(w))

df_BusinessRules = df_BusinessRules_pre.select(
    col("CLM_ID"),
    col("CLM_LN_NO").alias("CLM_LN_SEQ_NO"),
    regexp_replace(col("DIAG_CD"), "\\.", "").alias("DIAG_CD"),
    col("ICD_VRSN_CD"),
    col("svOrdnlCd").alias("CLM_LN_DIAG_ORDNL_CD")
).withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0)) \
 .withColumn("INSRT_UPDT_CD", lit("I")) \
 .withColumn("DISCARD_IN", lit("N")) \
 .withColumn("PASS_THRU_IN", lit("Y")) \
 .withColumn("ERR_CT", lit(0)) \
 .withColumn("RECYCLE_CT", lit(0)) \
 .withColumn("SRC_SYS_CD", lit(SrcSysCd)) \
 .withColumn("PRI_KEY_STRING",
    lit(SrcSysCd)
    .concat(lit(";"))
    .concat(col("CLM_ID"))
    .concat(lit(";"))
    .concat(col("CLM_LN_SEQ_NO"))
    .concat(lit(";"))
    .concat(lit("1"))
 ) \
 .withColumn("CLM_LN_DIAG_SK", lit(0)) \
 .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle)) \
 .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle)) \
 .withColumn("DIAG_CD_TYP_CD",
    when(col("ICD_VRSN_CD") == '9', 'ICD9')
    .when(col("ICD_VRSN_CD") == '0', 'ICD10')
    .otherwise('UNK')
 )

# Snapshot (CTransformerStage) - three output links: AllCol, Snapshot, Transform
# 1) AllCol
df_AllCol = df_BusinessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_LN_DIAG_ORDNL_CD").alias("CLM_LN_DIAG_ORDNL_CD"),
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    lit(CurrentDate).alias("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_LN_DIAG_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("DIAG_CD"),
    col("DIAG_CD_TYP_CD")
)

# 2) Snapshot
df_Snapshot_Snapshot = df_BusinessRules.select(
    col("CLM_ID").alias("CLCL_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DIAG_ORDNL_CD")
)

# 3) Transform
df_Transform = df_BusinessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_LN_DIAG_ORDNL_CD").alias("CLM_LN_DIAG_ORDNL_CD")
)

# ClmLnDiagPK (CContainerStage)
params_ClmLnDiagPK = {
    "DriverTable": "",
    "RunID": RunID,
    "CurrRunCycle": CurrRunCycle,
    "CurrDateTime": CurrentDate,
    "FacetsDB": "",
    "FacetsOwner": "",
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk,
    "IDSOwner": IDSOwner
}
df_ClmLnDiagPK = ClmLnDiagPK(df_AllCol, df_Transform, params_ClmLnDiagPK)

# EyeMedClmLnDiagExtr (CSeqFileStage)
df_finalEyeMed = (
    df_ClmLnDiagPK
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_ID", rpad(col("CLM_ID"), 18, " "))
    .withColumn("DIAG_CD", rpad(col("DIAG_CD"), 8, " "))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_LN_DIAG_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_DIAG_ORDNL_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "DIAG_CD",
        "DIAG_CD_TYP_CD"
    )
)

write_files(
    df_finalEyeMed,
    f"{adls_path}/key/EyeMedClmLnDiagExtr.EyeMedClmLnDiag.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Transformer (CTransformerStage) after Snapshot (the "V200S3P1" path)
df_TransformerInput = df_Snapshot_Snapshot

df_Transformer = df_TransformerInput.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLCL_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    GetFkeyCodes(
        SrcSysCd,
        lit(0),
        lit("DIAGNOSIS ORDINAL"),
        col("CLM_LN_DIAG_ORDNL_CD"),
        lit("X")
    ).alias("CLM_LN_DIAG_ORDNL_CD_SK")
)

# B_CLM_LN_DIAG (CSeqFileStage)
df_final_B_CLM_LN_DIAG = df_Transformer.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_DIAG_ORDNL_CD_SK"
)

write_files(
    df_final_B_CLM_LN_DIAG,
    f"{adls_path}/load/B_CLM_LN_DIAG.EYEMED.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)