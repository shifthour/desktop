# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004, 2005, 2006, 2007, 2008, 2020, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmOvrPayExtr
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
# MAGIC 
# MAGIC Matt Newman          2020-08-10      MA                          Copied from original and changed to use LHO sources/names  IntegrateDev2
# MAGIC 
# MAGIC Harikanth Reddy    10/12/2020                                     Brought up to standards                                                              IntegrateDev2
# MAGIC Kotha Venkat                      
# MAGIC Prabhu ES               2022-03-29       S2S                        MSSQL ODBC conn params added                                            IntegrateDev5                        	Ken Bradmon	2022-06-11

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
from pyspark.sql.types import StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, when, length, upper, rpad, expr
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

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
FROM #$LhoFacetsStgOwner#.CMC_CLOV_OVERPAY PAY,
     tempdb..{DriverTable} TMP
WHERE PAY.CLCL_ID = TMP.CLM_ID
"""

df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Strip = (
    df_Extract
    .withColumn("CLCL_ID", strip_field(col("CLCL_ID")))
    .withColumn("CLOV_PAYEE_IND", when(col("CLOV_PAYEE_IND").isNull() | (length(trim(col("CLOV_PAYEE_IND"))) == 0), lit("UNK"))
                .otherwise(trim(col("CLOV_PAYEE_IND")).upper()))
    .withColumn("LOBD_ID", strip_field(col("LOBD_ID")))
    .withColumn("PRPR_ID", strip_field(col("PRPR_ID")))
    .withColumn("SBSB_CK", col("SBSB_CK"))
    .withColumn("MEME_CK", col("MEME_CK"))
    .withColumn("CLOV_CREATE_DT", col("CLOV_CREATE_DT"))
    .withColumn("CLOV_AMT", col("CLOV_AMT"))
    .withColumn("CLOV_AUTO_REDUC", strip_field(col("CLOV_AUTO_REDUC")))
    .withColumn("ACPR_REF_ID", strip_field(col("ACPR_REF_ID")))
    .withColumn("CLCL_CL_TYPE", strip_field(col("CLCL_CL_TYPE")))
    .withColumn("CLOV_ORIG_HSA_AMT", col("CLOV_ORIG_HSA_AMT"))
)

df_join = (
    df_Strip.alias("Strip")
    .withColumn("ClmId", trim(col("Strip.CLCL_ID")))
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        col("Strip.CLCL_ID") == col("fcts_reversals.CLCL_ID"),
        how="left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        col("Strip.CLCL_ID") == col("nasco_dup_lkup.CLM_ID"),
        how="left"
    )
)

df_ClmOvrPay = (
    df_join
    .filter(col("nasco_dup_lkup.CLM_ID").isNull())
    .select(
        lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        lit("I").alias("INSRT_UPDT_CD"),
        lit("N").alias("DISCARD_IN"),
        lit("Y").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        lit(0).alias("ERR_CT"),
        lit(0).alias("RECYCLE_CT"),
        lit(SrcSysCd).alias("SRC_SYS_CD"),
        expr("SrcSysCd || ';' || ClmId || ';' || Strip.CLOV_PAYEE_IND || ';' || Strip.LOBD_ID").alias("PRI_KEY_STRING"),
        lit(0).alias("CLM_OVER_PAYMT_SK"),
        col("ClmId").alias("CLM_ID"),
        col("Strip.CLOV_PAYEE_IND").alias("CLM_OVER_PAYMT_PAYE_IND"),
        when(col("Strip.LOBD_ID").isNull() | (length(trim(col("Strip.LOBD_ID"))) == 0), lit("NA"))
        .otherwise(trim(col("Strip.LOBD_ID")).upper()).alias("LOB_NO"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        when(col("Strip.ACPR_REF_ID").isNull() | (length(trim(col("Strip.ACPR_REF_ID"))) == 0), lit("UNK"))
        .otherwise(trim(col("Strip.ACPR_REF_ID")).upper()).alias("PAYMT_RDUCTN_ID"),
        when(col("Strip.CLOV_PAYEE_IND") == lit("P"),
             when(col("Strip.PRPR_ID").isNull() | (length(trim(col("Strip.PRPR_ID"))) == 0), lit("UNK"))
             .otherwise(trim(col("Strip.PRPR_ID")).upper()))
        .otherwise(
            when(col("Strip.PRPR_ID").isNull() | (length(trim(col("Strip.PRPR_ID"))) == 0), lit("NA"))
            .otherwise(trim(col("Strip.PRPR_ID")).upper()))
        ).alias("ORIG_PAYE_PROV_ID"),
        when(col("Strip.CLOV_PAYEE_IND") == lit("S"),
             when(col("Strip.SBSB_CK").isNull() | (length(trim(col("Strip.SBSB_CK"))) == 0), lit("UNK"))
             .otherwise(trim(col("Strip.SBSB_CK")).upper()))
        .otherwise(
            when(col("Strip.SBSB_CK").isNull() | (length(trim(col("Strip.SBSB_CK"))) == 0), lit("NA"))
            .otherwise(trim(col("Strip.SBSB_CK")).upper()))
        ).alias("ORIG_PAYE_SUB_CK"),
        when(col("Strip.MEME_CK").isNull() | (length(trim(col("Strip.MEME_CK"))) == 0), lit("UNK"))
        .otherwise(trim(col("Strip.MEME_CK")).upper()).alias("ORIG_PAYMT_MBR_CK"),
        when(
            col("Strip.CLOV_AUTO_REDUC").isNull() | (length(trim(col("Strip.CLOV_AUTO_REDUC"))) == 0),
            lit("X")
        ).otherwise(
            when(col("Strip.CLOV_AUTO_REDUC").isNull() | (length(trim(col("Strip.CLOV_AUTO_REDUC"))) == 0), lit("UNK"))
            .otherwise(trim(col("Strip.CLOV_AUTO_REDUC")).upper())
        ).alias("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
        col("Strip.CLOV_ORIG_HSA_AMT").alias("ORIG_PCA_OVER_PAYMT_AMT"),
        col("Strip.CLOV_AMT").alias("OVER_PAYMT_AMT")
    )
)

df_reversals = (
    df_join
    .filter(
        (col("fcts_reversals.CLCL_ID").isNotNull()) &
        ((col("fcts_reversals.CLCL_CUR_STS") == lit("89")) |
         (col("fcts_reversals.CLCL_CUR_STS") == lit("91")) |
         (col("fcts_reversals.CLCL_CUR_STS") == lit("99")))
    )
    .select(
        lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        lit("I").alias("INSRT_UPDT_CD"),
        lit("N").alias("DISCARD_IN"),
        lit("Y").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        lit(0).alias("ERR_CT"),
        lit(0).alias("RECYCLE_CT"),
        lit(SrcSysCd).alias("SRC_SYS_CD"),
        expr("SrcSysCd || ClmId || 'R;' || Strip.CLOV_PAYEE_IND").alias("PRI_KEY_STRING"),
        lit(0).alias("CLM_OVER_PAYMT_SK"),
        expr("ClmId || 'R'").alias("CLM_ID"),
        col("Strip.CLOV_PAYEE_IND").alias("CLM_OVER_PAYMT_PAYE_IND"),
        when(col("Strip.LOBD_ID").isNull() | (length(trim(col("Strip.LOBD_ID"))) == 0), lit("NA"))
        .otherwise(trim(col("Strip.LOBD_ID")).upper()).alias("LOB_NO"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        when(col("Strip.ACPR_REF_ID").isNull() | (length(trim(col("Strip.ACPR_REF_ID"))) == 0), lit("UNK"))
        .otherwise(trim(col("Strip.ACPR_REF_ID")).upper()).alias("PAYMT_RDUCTN_ID"),
        when(col("Strip.CLOV_PAYEE_IND") == lit("P"),
             when(col("Strip.PRPR_ID").isNull() | (length(trim(col("Strip.PRPR_ID"))) == 0), lit("UNK"))
             .otherwise(trim(col("Strip.PRPR_ID")).upper()))
        .otherwise(
            when(col("Strip.PRPR_ID").isNull() | (length(trim(col("Strip.PRPR_ID"))) == 0), lit("NA"))
            .otherwise(trim(col("Strip.PRPR_ID")).upper()))
        ).alias("ORIG_PAYE_PROV_ID"),
        when(col("Strip.CLOV_PAYEE_IND") == lit("S"),
             when(col("Strip.SBSB_CK").isNull() | (length(trim(col("Strip.SBSB_CK"))) == 0), lit("UNK"))
             .otherwise(trim(col("Strip.SBSB_CK")).upper()))
        .otherwise(
            when(col("Strip.SBSB_CK").isNull() | (length(trim(col("Strip.SBSB_CK"))) == 0), lit("NA"))
            .otherwise(trim(col("Strip.SBSB_CK")).upper()))
        ).alias("ORIG_PAYE_SUB_CK"),
        when(col("Strip.MEME_CK").isNull() | (length(trim(col("Strip.MEME_CK"))) == 0), lit("UNK"))
        .otherwise(trim(col("Strip.MEME_CK")).upper()).alias("ORIG_PAYMT_MBR_CK"),
        when(
            col("Strip.CLOV_AUTO_REDUC").isNull() | (length(trim(col("Strip.CLOV_AUTO_REDUC"))) == 0),
            lit("X")
        ).otherwise(
            when(col("Strip.CLOV_AUTO_REDUC").isNull() | (length(trim(col("Strip.CLOV_AUTO_REDUC"))) == 0), lit("UNK"))
            .otherwise(trim(col("Strip.CLOV_AUTO_REDUC")).upper())
        ).alias("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
        when(col("Strip.CLOV_ORIG_HSA_AMT") > lit(0), -col("Strip.CLOV_ORIG_HSA_AMT"))
        .otherwise(col("Strip.CLOV_ORIG_HSA_AMT")).alias("ORIG_PCA_OVER_PAYMT_AMT"),
        when(col("Strip.CLOV_AMT") > lit(0), -col("Strip.CLOV_AMT"))
        .otherwise(col("Strip.CLOV_AMT")).alias("OVER_PAYMT_AMT")
    )
)

df_ClmOvrPay_cols = [
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
]
df_ClmOvrPay_final = df_ClmOvrPay.select(df_ClmOvrPay_cols)
df_reversals_final = df_reversals.select(df_ClmOvrPay_cols)

df_collector = df_ClmOvrPay_final.unionByName(df_reversals_final)

df_Snapshot_input = df_collector

df_AllColl = (
    df_Snapshot_input
    .withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSk))
    .select(
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CLM_ID").alias("CLM_ID"),
        col("CLM_OVER_PAYMT_PAYE_IND").alias("CLM_OVER_PAYMT_PAYE_IND"),
        col("LOB_NO").alias("LOB_NO"),
        col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ERR_CT").alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("PAYMT_RDUCTN_ID").alias("PAYMT_RDUCTN_ID"),
        col("ORIG_PAYE_PROV_ID").alias("ORIG_PAYE_PROV_ID"),
        col("ORIG_PAYE_SUB_CK").alias("ORIG_PAYE_SUB_CK"),
        col("ORIG_PAYMT_MBR_CK").alias("ORIG_PAYMT_MBR_CK"),
        col("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN").alias("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
        col("ORIG_PCA_OVER_PAYMT_AMT").alias("ORIG_PCA_OVER_PAYMT_AMT"),
        col("OVER_PAYMT_AMT").alias("OVER_PAYMT_AMT")
    )
)

df_Load = (
    df_Snapshot_input
    .select(
        col("CLM_ID").alias("CLM_ID"),
        col("CLM_OVER_PAYMT_PAYE_IND").alias("CLM_OVER_PAYMT_PAYE_IND"),
        col("LOB_NO").alias("LOB_NO")
    )
)

df_Transform = (
    df_Snapshot_input
    .withColumn("CLM_OVER_PAYMT_PAYE_CD", col("CLM_OVER_PAYMT_PAYE_IND"))
    .select(
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CLM_ID").alias("CLM_ID"),
        col("CLM_OVER_PAYMT_PAYE_CD").alias("CLM_OVER_PAYMT_PAYE_CD"),
        col("LOB_NO").alias("LOB_NO")
    )
)

df_Transformer_input = df_Load

df_Transformer = (
    df_Transformer_input
    .withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSk))
    .withColumn("CLM_OVER_PAYMT_PAYE_CD_SK", GetFkeyCodes(SrcSysCd, lit(0), "CLAIM OVER PAYMENT PAYEE", col("CLM_OVER_PAYMT_PAYE_IND"), 'X'))
)

df_RowCount = df_Transformer.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_OVER_PAYMT_PAYE_CD_SK").alias("CLM_OVER_PAYMT_PAYE_CD_SK"),
    col("LOB_NO").alias("LOB_NO")
)

df_B_CLM_OVER_PAYMT = df_RowCount

df_B_CLM_OVER_PAYMT_for_write = df_B_CLM_OVER_PAYMT.withColumn(
    "LOB_NO",
    rpad(col("LOB_NO"), 4, " ")
).select("SRC_SYS_CD_SK","CLM_ID","CLM_OVER_PAYMT_PAYE_CD_SK","LOB_NO")

write_files(
    df_B_CLM_OVER_PAYMT_for_write,
    f"{adls_path}/load/B_CLM_OVER_PAYMT.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmOverPaymtPK
# COMMAND ----------

params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrentDate,
    "$IDSOwner": IDSOwner
}

df_ClmOverPaymtPK = ClmOverPaymtPK(df_AllColl, df_Transform, params)

df_FctsClmOverPaymtExtr = df_ClmOverPaymtPK.select(
    rpad(col("JOB_EXCTN_RCRD_ERR_SK"), 1, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), 0, " ").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("CLM_OVER_PAYMT_SK").alias("CLM_OVER_PAYMT_SK"),
    col("CLM_ID").alias("CLM_ID"),
    rpad(col("CLM_OVER_PAYMT_PAYE_IND"), 1, " ").alias("CLM_OVER_PAYMT_PAYE_IND"),
    rpad(col("LOB_NO"), 4, " ").alias("LOB_NO"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("PAYMT_RDUCTN_ID"), 16, " ").alias("PAYMT_RDUCTN_ID"),
    rpad(col("ORIG_PAYE_PROV_ID"), 10, " ").alias("ORIG_PAYE_PROV_ID"),
    col("ORIG_PAYE_SUB_CK").alias("ORIG_PAYE_SUB_CK"),
    col("ORIG_PAYMT_MBR_CK").alias("ORIG_PAYMT_MBR_CK"),
    rpad(col("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"), 1, " ").alias("BYPS_AUTO_OVER_PAYMT_RDUCTN_IN"),
    col("ORIG_PCA_OVER_PAYMT_AMT").alias("ORIG_PCA_OVER_PAYMT_AMT"),
    col("OVER_PAYMT_AMT").alias("OVER_PAYMT_AMT")
)

write_files(
    df_FctsClmOverPaymtExtr,
    f"{adls_path}/key/LhoFctsClmOverPaymtExtr.LhoFctsClmOverPaymt.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)