# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2021 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  MAInboundClmExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:Generates Claim Provider Extract File to be processed by IDS routines
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Date                 Developer                         Project/Altiris#                      Change Description                                         Development Project           Code Reviewer          Date Reviewed
# MAGIC ------------------      ----------------------------              -----------------------                      -----------------------------------------------                         ---------------------------------         -------------------------------   ----------------------------
# MAGIC 2021-12-20       Lokesh K                          US 404552                                 Initial programming                                           IntegrateDev2               Jeyaprasanna            2022-02-06   
# MAGIC 
# MAGIC 2023-12-21       Kshema H K                   US 599810                   Added  logic to remove Alpha characters in                   IntegrateDev1                 Jeyaprasanna            2023-12-21
# MAGIC                                                                                                      Transformer_80  for PROV_ID field. 
# MAGIC 2024-04-04      Ediga Maruthi                   US 614444                  Added Columns DOC_NO,USER_DEFN_TX_1,            IntegrateDev1                Jeyaprasanna            2024-04-19
# MAGIC                                                                                                     USER_DEFN_TX_2,USER_DEFN_DT_1,
# MAGIC                                                                                                     USER_DEFN_DT_2,USER_DEFN_AMT_1,
# MAGIC                                                                                                     USER_DEFN_AMT_2 in MAInboundClmLanding.

# MAGIC MA Inbound Claim Provider Extract
# MAGIC This container is used in:
# MAGIC ArgusClmProvExtr
# MAGIC PCSClmProvExtr
# MAGIC NascoClmProvExtr
# MAGIC FctsClmProvExtr
# MAGIC WellDyneClmProvExtr
# MAGIC MCSourceClmProvExtr
# MAGIC MedicaidClmProvExtr
# MAGIC MedtrakClmProvExtr
# MAGIC BCBSSCClmProvExtr
# MAGIC BCBSSCMedClmProvExtr
# MAGIC BCAClmProvExtr
# MAGIC MAInboundClmProvExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to /key
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
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmProvPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
InFile_F = get_widget_value('InFile_F','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_PROV = f"""
SELECT 
PRV.PROV_ID,
PRV.NTNL_PROV_ID,
PRV.TAX_ID
FROM {IDSOwner}.PROV PRV,
{IDSOwner}.CD_MPPNG CD
WHERE PRV.SRC_SYS_CD_SK=CD.CD_MPPNG_SK
AND CD.SRC_CD= '{SrcSysCd}'
AND CD.SRC_CLCTN_CD = 'IDS'
AND CD.SRC_SYS_CD = 'IDS'
AND CD.SRC_DOMAIN_NM = 'SOURCE SYSTEM'
AND CD.TRGT_CLCTN_CD = 'IDS'
AND CD.TRGT_DOMAIN_NM = 'SOURCE SYSTEM'
"""
df_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_PROV)
    .load()
)

df_hf_ma_inbound_provid = dedup_sort(df_PROV, ["PROV_ID"], [])

schema_MAInboundClmLanding = StructType([
    StructField("REC_TYPE", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLM_TYP_CD", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True),
    StructField("CLM_LN_SEQ_NO", StringType(), True),
    StructField("CLM_ADJ_IN", StringType(), True),
    StructField("CLM_ADJ_FROM_CLM_ID", StringType(), True),
    StructField("CLM_ADJ_TO_CLM_ID", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("MCARE_BNFCRY_ID", StringType(), True),
    StructField("MBR_FIRST_NM", StringType(), True),
    StructField("MBR_MIDINIT", StringType(), True),
    StructField("MBR_LAST_NM", StringType(), True),
    StructField("MBR_BRTH_DT", StringType(), True),
    StructField("MBR_MAIL_ADDR_LN_1", StringType(), True),
    StructField("MBR_MAIL_ADDR_LN_2", StringType(), True),
    StructField("MBR_MAIL_ADDR_CITY_NM", StringType(), True),
    StructField("MBR_MAIL_ADDR_ST_CD", StringType(), True),
    StructField("MBR_MAIL_ADDR_ZIP_CD_5", StringType(), True),
    StructField("PROV_ID", StringType(), True),
    StructField("PROV_NM", StringType(), True),
    StructField("PROV_NTNL_PROV_ID", StringType(), True),
    StructField("PROV_TAX_ID", StringType(), True),
    StructField("PROV_TXNMY_CD", StringType(), True),
    StructField("PROV_PRI_MAIL_ADDR_LN_1", StringType(), True),
    StructField("PROV_PRI_MAIL_ADDR_LN_2", StringType(), True),
    StructField("PROV_PRI_MAIL_ADDR_LN_3", StringType(), True),
    StructField("PROV_PRI_MAIL_ADDR_CITY_NM", StringType(), True),
    StructField("PROV_PRI_MAIL_ADDR_ST_CD", StringType(), True),
    StructField("PROV_PRI_MAIL_ADDR_ZIP_CD_5", StringType(), True),
    StructField("CLM_SVC_PROV_SPEC_CD", StringType(), True),
    StructField("SVC_PROV_ID", StringType(), True),
    StructField("SVC_PROV_NM", StringType(), True),
    StructField("SVC_PROV_NTNL_PROV_ID", StringType(), True),
    StructField("SVC_PROV_TAX_ID", StringType(), True),
    StructField("SVC_PROV_TXNMY_CD", StringType(), True),
    StructField("SVC_PROV_ADDR_LN1", StringType(), True),
    StructField("SVC_PROV_ADDR_LN2", StringType(), True),
    StructField("SVC_PROV_ADDR_LN3", StringType(), True),
    StructField("SVC_PROV_CITY_NM", StringType(), True),
    StructField("SVC_PROV_ST_CD", StringType(), True),
    StructField("SVC_PROV_ZIP_CD_5", StringType(), True),
    StructField("CLM_SVC_STRT_DT", StringType(), True),
    StructField("CLM_SVC_END_DT", StringType(), True),
    StructField("CLM_PD_DT", StringType(), True),
    StructField("CLM_RCVD_DT", StringType(), True),
    StructField("CLM_PAYE_CD", StringType(), True),
    StructField("CLM_NTWK_STTUS_CD", StringType(), True),
    StructField("CLM_LN_FINL_DISP_CD", StringType(), True),
    StructField("DIAG_CD_1", StringType(), True),
    StructField("DIAG_CD_2", StringType(), True),
    StructField("DIAG_CD_3", StringType(), True),
    StructField("CLM_LN_CAP_LN_IN", StringType(), True),
    StructField("Claim_Line_Denied_Indicator", StringType(), True),
    StructField("Explanation_Code", StringType(), True),
    StructField("Explanation_Code_Description", StringType(), True),
    StructField("DNTL_CLM_LN_TOOTH_NO", StringType(), True),
    StructField("DNTL_CLM_LN_TOOTH_SRFC_TX", StringType(), True),
    StructField("Adjustment_Reason_Code", StringType(), True),
    StructField("Remittance_Advice_Remark_Code_RARC", StringType(), True),
    StructField("DIAG_CD_TYP_CD", StringType(), True),
    StructField("PROC_CD", StringType(), True),
    StructField("PROC_CD_DESC", StringType(), True),
    StructField("Procedure_Code_Modifier1", StringType(), True),
    StructField("Procedure_Code_Modifier2", StringType(), True),
    StructField("Procedure_Code_Modifier3", StringType(), True),
    StructField("CLM_LN_SVC_STRT_DT", StringType(), True),
    StructField("CLM_LN_SVC_END_DT", StringType(), True),
    StructField("CLM_LN_CHRG_AMT", StringType(), True),
    StructField("CLM_LN_ALW_AMT", StringType(), True),
    StructField("CLM_LN_DSALW_AMT", StringType(), True),
    StructField("CLM_LN_COPAY_AMT", StringType(), True),
    StructField("CLM_LN_COINS_AMT", StringType(), True),
    StructField("CLM_LN_DEDCT_AMT", StringType(), True),
    StructField("Plan_Pay_Amount", StringType(), True),
    StructField("Patient_Responsibility_Amount", StringType(), True),
    StructField("CLM_LN_AGMNT_PRICE_AMT", StringType(), True),
    StructField("CLM_LN_RISK_WTHLD_AMT", StringType(), True),
    StructField("CLM_PROV_ROLE_TYPE_CD", StringType(), True),
    StructField("CLM_LN_PD_AMT", StringType(), True),
    StructField("CLM_COB_PD_AMT", StringType(), True),
    StructField("CLM_COB_ALW_AMT", StringType(), True),
    StructField("UNIT_CT", StringType(), True),
    StructField("CLAIM_STS_CD", StringType(), True),
    StructField("ADJ_FROM_CLM_ID", StringType(), True),
    StructField("ADJ_TO_CLM_ID", StringType(), True),
    StructField("DOC_NO", StringType(), True),
    StructField("USER_DEFN_TX_1", StringType(), True),
    StructField("USER_DEFN_TX_2", StringType(), True),
    StructField("USER_DEFN_DT_1", StringType(), True),
    StructField("USER_DEFN_DT_2", StringType(), True),
    StructField("USER_DEFN_AMT_1", DecimalType(38,10), True),
    StructField("USER_DEFN_AMT_2", DecimalType(38,10), True)
])

df_MAInboundClmLanding = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_MAInboundClmLanding)
    .load(f"{adls_path}/verified/{InFile_F}")
)

df_Bill = (
    df_MAInboundClmLanding
    .filter(F.col("REC_TYPE") == 'CLAIM HEADER')
    .select(
        F.col("CLM_ID").alias("CLAIM_ID"),
        F.when(F.col("PROV_ID").isNull(), 'NA').otherwise('BILL').alias("CLM_PROV_ROLE_TYP_CD"),
        F.when(
            F.col("SRC_SYS_CD") == 'DOMINION',
            F.when(
                F.substring(F.col("PROV_ID"), 1, 1) == 'F',
                Convert(F.lit('ABCDEFGHIJKLMNOPQRSTUVWXYZ'), F.lit(''), F.col("SVC_PROV_ID"))
            ).otherwise(
                Convert(F.lit('ABCDEFGHIJKLMNOPQRSTUVWXYZ'), F.lit(''), F.col("PROV_ID"))
            )
        ).otherwise(
            Convert(F.lit('ABCDEFGHIJKLMNOPQRSTUVWXYZ'), F.lit(''), F.col("PROV_ID"))
        ).alias("PROVIDER_ID")
    )
)

df_Service = (
    df_MAInboundClmLanding
    .filter(F.col("REC_TYPE") == 'CLAIM HEADER')
    .select(
        F.col("CLM_ID").alias("CLAIM_ID"),
        F.when(F.col("SVC_PROV_ID").isNull(), 'NA').otherwise('SVC').alias("CLM_PROV_ROLE_TYP_CD"),
        Convert(F.lit('ABCDEFGHIJKLMNOPQRSTUVWXYZ'), F.lit(''), F.col("SVC_PROV_ID")).alias("PROVIDER_ID")
    )
)

df_link_collector = df_Bill.unionByName(df_Service)

df_hf_ma_rmdprovid = dedup_sort(df_link_collector, ["CLAIM_ID","CLM_PROV_ROLE_TYP_CD"], [])

df_Snapshot = df_hf_ma_rmdprovid.alias("DQ").join(
    df_hf_ma_inbound_provid.alias("PROV_IDlkp"),
    (F.col("DQ.PROVIDER_ID") == F.col("PROV_IDlkp.PROV_ID")) & (F.col("DQ.PROVIDER_ID") == F.col("PROV_IDlkp.NTNL_PROV_ID")),
    "left"
)

df_Snapshot_AllCol = df_Snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("DQ.CLAIM_ID").alias("CLM_ID"),
    F.col("DQ.CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat_ws(";", F.lit(SrcSysCd), F.col("DQ.CLAIM_ID"), F.col("DQ.CLM_PROV_ROLE_TYP_CD")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROV_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        (F.col("DQ.PROVIDER_ID").isNull()) | (F.length(trim(F.col("DQ.PROVIDER_ID"))) == 0),
        'NA'
    ).otherwise(trim(F.col("DQ.PROVIDER_ID"))).alias("PROV_ID"),
    F.when(
        (F.col("PROV_IDlkp.TAX_ID").isNull()) | (F.length(trim(F.col("PROV_IDlkp.TAX_ID"))) == 0),
        'NA'
    ).otherwise(
        Ereplace(trim(F.col("PROV_IDlkp.TAX_ID")), F.lit('-'), F.lit(''))
    ).alias("TAX_ID"),
    F.lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    F.when(
        (F.col("PROV_IDlkp.NTNL_PROV_ID").isNull()) | (F.length(trim(F.col("PROV_IDlkp.NTNL_PROV_ID"))) == 0),
        'NA'
    ).otherwise(trim(F.col("PROV_IDlkp.NTNL_PROV_ID"))).alias("NTNL_PROV_ID")
)

df_Snapshot_Transform = df_Snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("DQ.CLAIM_ID").alias("CLM_ID"),
    F.col("DQ.CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD")
)

df_Snapshot_SnapShot = df_Snapshot.select(
    F.col("DQ.CLAIM_ID").alias("CLM_ID"),
    F.col("DQ.CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD"),
    F.when(
        (F.col("DQ.PROVIDER_ID").isNull()) | (F.length(trim(F.col("DQ.PROVIDER_ID"))) == 0),
        'NA'
    ).otherwise(trim(F.col("DQ.PROVIDER_ID"))).alias("PROV_ID")
)

params_ClmProvPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}

df_ClmProvPK_Key = ClmProvPK(df_Snapshot_AllCol, df_Snapshot_Transform, params_ClmProvPK)

df_ClmProvPK_Key_out = df_ClmProvPK_Key.select(
    F.rpad(F.col("JOB_EXCTN_RCRD_ERR_SK").cast(StringType()), 0, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_PROV_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_ID"),
    F.rpad(F.col("TAX_ID"), 9, " ").alias("TAX_ID"),
    F.col("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    F.col("NTNL_PROV_ID")
)

write_files(
    df_ClmProvPK_Key_out,
    f"{adls_path}/key/{SrcSysCd}ClmProvExtr.{SrcSysCd}ClmProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Transformer = df_Snapshot_SnapShot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    GetFkeyCodes(SrcSysCd, 0, "CLAIM PROVIDER ROLE TYPE", F.col("CLM_PROV_ROLE_TYP_CD"), 'X').alias("CLM_PROV_ROLE_TYP_CD_SK"),
    F.col("PROV_ID").alias("PROV_ID")
)

write_files(
    df_Transformer.select(
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_PROV_ROLE_TYP_CD_SK",
        "PROV_ID"
    ),
    f"{adls_path}/load/B_CLM_PROV.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)