# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004, 2005, 2006, 2007, 2008  BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  FctsClmExtr1Seq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Pulls data from CMC_CLOV_OVERPAY to a landing file for the IDS
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               SAndrew   08/04/2004-   Originally Programmed
# MAGIC               Steph Goddard  02/16/2006   Added transform and primary key for sequencer processing
# MAGIC               BJ Luce            03/20/2006     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC              Steph Goddard   03/31/2006   populated ORIG_PCA_OVER_PAYMT_AMT with CLOV_ORIG_HSA_AMT
# MAGIC             Sanderw  12/08/2006   Project 1756  - Reversal logix added for new status codes 89 and  and 99
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Oliver Nielsen          08/15/2007       Balancing              Added Snapshot extract for balancing                                        devlIDS30                      Steph Goddard          8/30/07
# MAGIC 
# MAGIC Bhoomi Dasari         2008-07-07      3657(Primary Key)    Changed primary key process from hash file to DB2 table            devlIDS                        Brent Leland               07-11-2008
# MAGIC Prabhu ES               2022-02-28      S2S Remediation     MSSQL connection parameters added                                        IntegrateDev5               Kalyan Neelam           2022-06-10

# MAGIC Pulling Facets Claim Override Data
# MAGIC hf_clm_ovr_paymnt_allcol HASH file contained in Shared Container is cleared
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, when, length, upper, concat
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
FacetsOwner = get_widget_value('$FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmOverPaymtPK
# COMMAND ----------

# 1) Read from hashed file "hf_clm_fcts_reversals" (Scenario C => read parquet)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_hf_clm_fcts_reversals = df_hf_clm_fcts_reversals.select(
    "CLCL_ID",
    "CLCL_CUR_STS",
    "CLCL_PAID_DT",
    "CLCL_ID_ADJ_TO",
    "CLCL_ID_ADJ_FROM"
)

# 2) Read from hashed file "clm_nasco_dup_bypass" (Scenario C => read parquet)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
df_clm_nasco_dup_bypass = df_clm_nasco_dup_bypass.select("CLM_ID")

# 3) Read from ODBCConnector "CMC_CLOR_CL_OVERPAY"
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"""
SELECT
PAY.CLCL_ID,
PAY.CLOV_PAYEE_IND,
PAY.LOBD_ID,
PAY.PRPR_ID,
PAY.SBSB_CK,
PAY.MEME_CK,
PAY.CLOV_CREATE_DT,
PAY.CLOV_AMT,
PAY.CLOV_AUTO_REDUC,
PAY.ACPR_REF_ID,
PAY.CLCL_CL_TYPE,
PAY.CLOV_ORIG_HSA_AMT
FROM {FacetsOwner}.CMC_CLOV_OVERPAY PAY,
     tempdb..{DriverTable} TMP
WHERE  PAY.CLCL_ID = TMP.CLM_ID
"""
df_CMC_CLOR_CL_OVERPAY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# 4) Transformer "StripField"
df_Strip = df_CMC_CLOR_CL_OVERPAY
df_Strip = df_Strip.withColumn(
    "CLCL_ID",
    strip_field(col("CLCL_ID"))
).withColumn(
    "CLOV_PAYEE_IND",
    when(
        col("CLOV_PAYEE_IND").isNull() | (length(trim(col("CLOV_PAYEE_IND"))) == 0),
        lit("UNK")
    ).otherwise(upper(trim(col("CLOV_PAYEE_IND"))))
).withColumn(
    "LOBD_ID",
    strip_field(col("LOBD_ID"))
).withColumn(
    "PRPR_ID",
    strip_field(col("PRPR_ID"))
).withColumn(
    "SBSB_CK",
    col("SBSB_CK")
).withColumn(
    "MEME_CK",
    col("MEME_CK")
).withColumn(
    "CLOV_CREATE_DT",
    col("CLOV_CREATE_DT")
).withColumn(
    "CLOV_AMT",
    col("CLOV_AMT")
).withColumn(
    "CLOV_AUTO_REDUC",
    strip_field(col("CLOV_AUTO_REDUC"))
).withColumn(
    "ACPR_REF_ID",
    strip_field(col("ACPR_REF_ID"))
).withColumn(
    "CLCL_CL_TYPE",
    strip_field(col("CLCL_CL_TYPE"))
).withColumn(
    "CLOV_ORIG_HSA_AMT",
    col("CLOV_ORIG_HSA_AMT")
)
df_Strip = df_Strip.select(
    "CLCL_ID",
    "CLOV_PAYEE_IND",
    "LOBD_ID",
    "PRPR_ID",
    "SBSB_CK",
    "MEME_CK",
    "CLOV_CREATE_DT",
    "CLOV_AMT",
    "CLOV_AUTO_REDUC",
    "ACPR_REF_ID",
    "CLCL_CL_TYPE",
    "CLOV_ORIG_HSA_AMT"
)

# 5) Transformer "BusinessRules" with two lookup links: hf_clm_fcts_reversals (fcts_reversals) and clm_nasco_dup_bypass (nasco_dup_lkup)
df_BusinessRules_temp = df_Strip.alias("Strip").join(
    df_hf_clm_fcts_reversals.alias("fcts_reversals"),
    on=[col("Strip.CLCL_ID") == col("fcts_reversals.CLCL_ID")],
    how="left"
)
df_BusinessRules = df_BusinessRules_temp.alias("temp").join(
    df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
    on=[col("temp.CLCL_ID") == col("nasco_dup_lkup.CLM_ID")],
    how="left"
).withColumn(
    "ClmId",
    trim(col("Strip.CLCL_ID"))
)

# Output link "ClmOvrPay" => filter IsNull(nasco_dup_lkup.CLM_ID)=true
df_BusinessRules_ClmOvrPay = df_BusinessRules.filter(
    col("nasco_dup_lkup.CLM_ID").isNull()
)

df_ClmOvrPay = (
    df_BusinessRules_ClmOvrPay
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit("FACETS"))
    .withColumn("PRI_KEY_STRING", concat(lit("FACETS;"), col("ClmId"), lit(";"), col("Strip.CLOV_PAYEE_IND"), lit(";"), col("Strip.LOBD_ID")))
    .withColumn("CLM_OVER_PAYMT_SK", lit(0))
    .withColumn("CLM_ID", col("ClmId"))
    .withColumn("CLM_OVER_PAYMT_PAYE_IND", col("Strip.CLOV_PAYEE_IND"))
    .withColumn(
        "LOB_NO",
        when(
            col("Strip.LOBD_ID").isNull() | (length(trim(col("Strip.LOBD_ID"))) == 0),
            lit("NA")
        ).otherwise(upper(trim(col("Strip.LOBD_ID"))))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn(
        "PAYMT_RDUCTN_ID",
        when(
            col("Strip.ACPR_REF_ID").isNull() | (length(trim(col("Strip.ACPR_REF_ID"))) == 0),
            lit("UNK")
        ).otherwise(upper(trim(col("Strip.ACPR_REF_ID"))))
    )
    .withColumn(
        "ORIG_PAYE_PROV_ID",
        when(
            col("Strip.CLOV_PAYEE_IND") == lit("P"),
            when(
                col("Strip.PRPR_ID").isNull() | (length(trim(col("Strip.PRPR_ID"))) == 0),
                lit("UNK")
            ).otherwise(upper(trim(col("Strip.PRPR_ID"))))
        ).otherwise(
            when(
                col("Strip.PRPR_ID").isNull() | (length(trim(col("Strip.PRPR_ID"))) == 0),
                lit("NA")
            ).otherwise(upper(trim(col("Strip.PRPR_ID"))))
        )
    )
    .withColumn(
        "ORIG_PAYE_SUB_CK",
        when(
            col("Strip.CLOV_PAYEE_IND") == lit("S"),
            when(
                col("Strip.SBSB_CK").isNull() | (length(trim(col("Strip.SBSB_CK"))) == 0),
                lit("UNK")
            ).otherwise(upper(trim(col("Strip.SBSB_CK"))))
        ).otherwise(
            when(
                col("Strip.SBSB_CK").isNull() | (length(trim(col("Strip.SBSB_CK"))) == 0),
                lit("NA")
            ).otherwise(upper(trim(col("Strip.SBSB_CK"))))
        )
    )
    .withColumn(
        "ORIG_PAYMT_MBR_CK",
        when(
            col("Strip.MEME_CK").isNull() | (length(trim(col("Strip.MEME_CK"))) == 0),
            lit("UNK")
        ).otherwise(upper(trim(col("Strip.MEME_CK"))))
    )
    .withColumn(
        "BYPS_AUTO_OVER_PAYMT_RDUCTN_IN",
        when(
            col("Strip.CLOV_AUTO_REDUC").isNull() | (length(trim(col("Strip.CLOV_AUTO_REDUC"))) == 0),
            lit("X")
        ).otherwise(
            when(
                col("Strip.CLOV_AUTO_REDUC").isNull() | (length(trim(col("Strip.CLOV_AUTO_REDUC"))) == 0),
                lit("UNK")
            ).otherwise(upper(trim(col("Strip.CLOV_AUTO_REDUC"))))
        )
    )
    .withColumn("ORIG_PCA_OVER_PAYMT_AMT", col("Strip.CLOV_ORIG_HSA_AMT"))
    .withColumn("OVER_PAYMT_AMT", col("Strip.CLOV_AMT"))
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
        "CLM_OVER_PAYMT_SK",
        "CLM_ID",
        "CLM_OVER_PAYMT_PAYE_IND",
        "LOB_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PAYMT_RDUCTN_ID",
        "ORIG_PAYE_PROV_ID",
        "ORIG_PAYE_SUB_CK",
        "ORIG_PAYMT_MBR_CK",
        "BYPS_AUTO_OVER_PAYMT_RDUCTN_IN",
        "ORIG_PCA_OVER_PAYMT_AMT",
        "OVER_PAYMT_AMT"
    )
)

# Output link "reversals" => filter IsNull(fcts_reversals.CLCL_ID)=false and CLCL_CUR_STS in ("89","91","99")
df_BusinessRules_reversals = df_BusinessRules.filter(
    (col("fcts_reversals.CLCL_ID").isNotNull()) &
    (
        (col("fcts_reversals.CLCL_CUR_STS") == lit("89")) |
        (col("fcts_reversals.CLCL_CUR_STS") == lit("91")) |
        (col("fcts_reversals.CLCL_CUR_STS") == lit("99"))
    )
)

df_reversals = (
    df_BusinessRules_reversals
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit("FACETS"))
    .withColumn("PRI_KEY_STRING", concat(lit("FACETS;"), col("ClmId"), lit("R;"), col("Strip.CLOV_PAYEE_IND")))
    .withColumn("CLM_OVER_PAYMT_SK", lit(0))
    .withColumn("CLM_ID", concat(col("ClmId"), lit("R")))
    .withColumn("CLM_OVER_PAYMT_PAYE_IND", col("Strip.CLOV_PAYEE_IND"))
    .withColumn(
        "LOB_NO",
        when(
            col("Strip.LOBD_ID").isNull() | (length(trim(col("Strip.LOBD_ID"))) == 0),
            lit("NA")
        ).otherwise(upper(trim(col("Strip.LOBD_ID"))))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn(
        "PAYMT_RDUCTN_ID",
        when(
            col("Strip.ACPR_REF_ID").isNull() | (length(trim(col("Strip.ACPR_REF_ID"))) == 0),
            lit("UNK")
        ).otherwise(upper(trim(col("Strip.ACPR_REF_ID"))))
    )
    .withColumn(
        "ORIG_PAYE_PROV_ID",
        when(
            col("Strip.CLOV_PAYEE_IND") == lit("P"),
            when(
                col("Strip.PRPR_ID").isNull() | (length(trim(col("Strip.PRPR_ID"))) == 0),
                lit("UNK")
            ).otherwise(upper(trim(col("Strip.PRPR_ID"))))
        ).otherwise(
            when(
                col("Strip.PRPR_ID").isNull() | (length(trim(col("Strip.PRPR_ID"))) == 0),
                lit("NA")
            ).otherwise(upper(trim(col("Strip.PRPR_ID"))))
        )
    )
    .withColumn(
        "ORIG_PAYE_SUB_CK",
        when(
            col("Strip.CLOV_PAYEE_IND") == lit("S"),
            when(
                col("Strip.SBSB_CK").isNull() | (length(trim(col("Strip.SBSB_CK"))) == 0),
                lit("UNK")
            ).otherwise(upper(trim(col("Strip.SBSB_CK"))))
        ).otherwise(
            when(
                col("Strip.SBSB_CK").isNull() | (length(trim(col("Strip.SBSB_CK"))) == 0),
                lit("NA")
            ).otherwise(upper(trim(col("Strip.SBSB_CK"))))
        )
    )
    .withColumn(
        "ORIG_PAYMT_MBR_CK",
        when(
            col("Strip.MEME_CK").isNull() | (length(trim(col("Strip.MEME_CK"))) == 0),
            lit("UNK")
        ).otherwise(upper(trim(col("Strip.MEME_CK"))))
    )
    .withColumn(
        "BYPS_AUTO_OVER_PAYMT_RDUCTN_IN",
        when(
            col("Strip.CLOV_AUTO_REDUC").isNull() | (length(trim(col("Strip.CLOV_AUTO_REDUC"))) == 0),
            lit("X")
        ).otherwise(
            when(
                col("Strip.CLOV_AUTO_REDUC").isNull() | (length(trim(col("Strip.CLOV_AUTO_REDUC"))) == 0),
                lit("UNK")
            ).otherwise(upper(trim(col("Strip.CLOV_AUTO_REDUC"))))
        )
    )
    .withColumn(
        "ORIG_PCA_OVER_PAYMT_AMT",
        when(
            col("Strip.CLOV_ORIG_HSA_AMT") > lit(0),
            -col("Strip.CLOV_ORIG_HSA_AMT")
        ).otherwise(col("Strip.CLOV_ORIG_HSA_AMT"))
    )
    .withColumn(
        "OVER_PAYMT_AMT",
        when(
            col("Strip.CLOV_AMT") > lit(0),
            -col("Strip.CLOV_AMT")
        ).otherwise(col("Strip.CLOV_AMT"))
    )
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
        "CLM_OVER_PAYMT_SK",
        "CLM_ID",
        "CLM_OVER_PAYMT_PAYE_IND",
        "LOB_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PAYMT_RDUCTN_ID",
        "ORIG_PAYE_PROV_ID",
        "ORIG_PAYE_SUB_CK",
        "ORIG_PAYMT_MBR_CK",
        "BYPS_AUTO_OVER_PAYMT_RDUCTN_IN",
        "ORIG_PCA_OVER_PAYMT_AMT",
        "OVER_PAYMT_AMT"
    )
)

# 6) Collector => union the two outputs
df_Collector = df_reversals.unionByName(df_ClmOvrPay)

# 7) Now "Snapshot" stage => single input => multiple outputs

# The single input to Snapshot:
df_Snapshot = df_Collector

# Output pin "AllColl"
df_AllColl = df_Snapshot.withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSk)).select(
    lit(None).alias("IGNORE_THIS"),  # placeholder to keep indexing consistent if needed
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_OVER_PAYMT_PAYE_IND"),
    col("LOB_NO"),
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("PRI_KEY_STRING"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PAYMT_RDUCTN_ID"),
    col("ORIG_PAYE_PROV_ID"),
    col("ORIG_PAYE_SUB_CK"),
    col("ORIG_PAYMT_MBR_CK"),
    col("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
    col("ORIG_PCA_OVER_PAYMT_AMT"),
    col("OVER_PAYMT_AMT")
)

# Rename columns to match exactly the pin listing "AllColl"
df_AllColl = df_AllColl.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_OVER_PAYMT_SK"),  # will fill with lit(0) if needed
    col("CLM_ID"),
    col("CLM_OVER_PAYMT_PAYE_IND"),
    col("LOB_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PAYMT_RDUCTN_ID"),
    col("ORIG_PAYE_PROV_ID"),
    col("ORIG_PAYE_SUB_CK"),
    col("ORIG_PAYMT_MBR_CK"),
    col("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
    col("ORIG_PCA_OVER_PAYMT_AMT"),
    col("OVER_PAYMT_AMT")
).withColumn(
    "CLM_OVER_PAYMT_SK", lit(0)  # because original stages set it to 0
)

# Output pin "Load"
df_Load = df_Snapshot.select(
    col("CLM_ID"),
    col("CLM_OVER_PAYMT_PAYE_IND"),
    col("LOB_NO")
)

# Output pin "Transform"
df_Transform = df_Snapshot.withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSk)).select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_OVER_PAYMT_PAYE_IND").alias("CLM_OVER_PAYMT_PAYE_CD"),
    col("LOB_NO").alias("LOB_NO")
)

# 8) Next "Transformer" stage => input "Load"
df_Transformer_in = df_Load
df_Transformer_out = df_Transformer_in.withColumn(
    "SRC_SYS_CD_SK",
    lit(SrcSysCdSk)
).withColumn(
    "CLM_OVER_PAYMT_PAYE_CD_SK",
    # Stage variable svClmOvrPaySk => calls user function GetFkeyCodes() => assume it's already defined
    # We'll just replicate the expression:
    lit(None)  # placeholder, we then do the expression below
).withColumn(
    "CLM_OVER_PAYMT_PAYE_CD_SK",
    expr("GetFkeyCodes('FACETS', 0, 'CLAIM OVER PAYMENT PAYEE', CLM_OVER_PAYMT_PAYE_IND, 'X')")
).select(
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_OVER_PAYMT_PAYE_CD_SK").alias("CLM_OVER_PAYMT_PAYE_CD_SK"),
    col("LOB_NO")
)

# 9) "B_CLM_OVER_PAYMT" => CSeqFileStage => write to .dat
#    The job wants to write "B_CLM_OVER_PAYMT.FACETS.dat.#RunID#"
#    with no header, overwrite mode, delimiter = ',', quoteChar = '"'
#    We must rpad char columns. The stage has columns:
#    SRC_SYS_CD_SK (PK), CLM_ID (PK), CLM_OVER_PAYMT_PAYE_CD_SK, LOB_NO (char(4))

df_B_CLM_OVER_PAYMT = df_Transformer_out.select(
    rpad(col("SRC_SYS_CD_SK").cast(StringType()), 1, " ").alias("SRC_SYS_CD_SK"),
    rpad(col("CLM_ID").cast(StringType()), 1, " ").alias("CLM_ID"),
    rpad(col("CLM_OVER_PAYMT_PAYE_CD_SK").cast(StringType()), 1, " ").alias("CLM_OVER_PAYMT_PAYE_CD_SK"),
    rpad(col("LOB_NO").cast(StringType()), 4, " ").alias("LOB_NO")
)
write_files(
    df_B_CLM_OVER_PAYMT,
    f"{adls_path}/load/B_CLM_OVER_PAYMT.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 10) "ClmOverPaymtPK" => Container Stage with 2 inputs => "AllColl" and "Transform"
#     We call the shared container function with two dataframes and appropriate params
params_for_container = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrentDate,
    "$IDSOwner": IDSOwner
}
df_ClmOverPaymtPK_out = ClmOverPaymtPK(df_AllColl, df_Transform, params_for_container)
# The container outputs one link => "Key"

# 11) "FctsClmOverPaymtExtr" => CSeqFileStage => final write to .dat
df_FctsClmOverPaymtExtr = df_ClmOverPaymtPK_out.select(
    rpad(col("JOB_EXCTN_RCRD_ERR_SK").cast(StringType()), 1, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD").cast(StringType()), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN").cast(StringType()), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN").cast(StringType()), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD").cast(StringType()), 6, " ").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_OVER_PAYMT_SK"),
    rpad(col("CLM_ID").cast(StringType()), 1, " ").alias("CLM_ID"),
    rpad(col("CLM_OVER_PAYMT_PAYE_IND").cast(StringType()), 1, " ").alias("CLM_OVER_PAYMT_PAYE_IND"),
    rpad(col("LOB_NO").cast(StringType()), 4, " ").alias("LOB_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("PAYMT_RDUCTN_ID").cast(StringType()), 16, " ").alias("PAYMT_RDUCTN_ID"),
    rpad(col("ORIG_PAYE_PROV_ID").cast(StringType()), 10, " ").alias("ORIG_PAYE_PROV_ID"),
    col("ORIG_PAYE_SUB_CK"),
    col("ORIG_PAYMT_MBR_CK"),
    rpad(col("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN").cast(StringType()), 1, " ").alias("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
    col("ORIG_PCA_OVER_PAYMT_AMT"),
    col("OVER_PAYMT_AMT")
)
write_files(
    df_FctsClmOverPaymtExtr,
    f"{adls_path}/key/FctsClmOverPaymtExtr.FctsClmOverPaymt.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)