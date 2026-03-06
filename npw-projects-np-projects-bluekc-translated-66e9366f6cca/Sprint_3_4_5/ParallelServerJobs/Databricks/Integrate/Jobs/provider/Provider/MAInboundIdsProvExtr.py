# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT2021 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  MAInboundIdsProvExtr
# MAGIC 
# MAGIC Calling Job: MAInboundProvExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION: Control job to process MA Inbound Encounter Claims into IDS
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer           	 Date                	User Story #        	Change Description                                               Project                    		Reviewed By                           Reviewed Date
# MAGIC -------------------------      ---------------------   	----------------   	--------------------------------------------------------             -----------------------------------------     	-------------------------  		-------------------
# MAGIC Lokesh K                 2021-11-29               US 404552                Initial programming                                     IntegrateDev2                                   Jeyaprasanna                           2022-02-05
# MAGIC 
# MAGIC Arpitha                    2024-02-02                US 599081            Added logic to BusinessRules transformer
# MAGIC                                                                                                constraint for Dominion                                  IntegrateDev1                                   Jeyaprasanna                           2024-02-0  
# MAGIC Ediga Maruthi         2024-04-04                 US 614444           Added DOC_NO,USER_DEFN_TX_1,                  IntegrateDev1                          Jeyaprasanna                            2024-04-19
# MAGIC                                                                                               USER_DEFN_TX_2,USER_DEFN_DT_1,
# MAGIC                                                                                               USER_DEFN_DT_2,USER_DEFN_AMT_1,
# MAGIC                                                                                               USER_DEFN_AMT_2. Columns in 
# MAGIC                                                                                               InboundProvFile.

# MAGIC Job Name: MAInboundIdsProvExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from pyspark.sql.functions import col, substring, when, lit, translate, rpad, isnan, expr, concat
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
InboundProvFile_MA = get_widget_value('InboundProvFile_MA','')

schema_inboundprovfile = StructType([
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
    StructField("DOC_NO", StringType(), True),
    StructField("USER_DEFN_TX_1", StringType(), True),
    StructField("USER_DEFN_TX_2", StringType(), True),
    StructField("USER_DEFN_DT_1", StringType(), True),
    StructField("USER_DEFN_DT_2", StringType(), True),
    StructField("USER_DEFN_AMT_1", DecimalType(38,10), True),
    StructField("USER_DEFN_AMT_2", DecimalType(38,10), True)
])

df_inbound = (
    spark.read
    .option("delimiter", "|")
    .option("header", False)
    .option("quote", None)
    .option("escape", None)
    .option("ignoreLeadingWhiteSpace", False)
    .option("ignoreTrailingWhiteSpace", False)
    .schema(schema_inboundprovfile)
    .csv(f"{adls_path_raw}/landing/{InboundProvFile_MA}")
)

df_SRVC_PROV = df_inbound.filter(
    (
        (trim(col("SRC_SYS_CD")) == 'DOMINION') &
        (substring(trim(col("PROV_ID")), 1, 1) == 'F') &
        (trim(col("SVC_PROV_ID")) != '') &
        (col("SVC_PROV_ID").isNotNull()) &
        (col("REC_TYPE") == 'CLAIM HEADER')
    )
    |
    (
        (trim(col("SRC_SYS_CD")) != 'DOMINION') &
        (trim(col("SVC_PROV_ID")) != '') &
        (col("SVC_PROV_ID").isNotNull()) &
        (col("REC_TYPE") == 'CLAIM HEADER')
    )
).select(
    when(
        trim(col("SRC_SYS_CD")) == 'DOMINION',
        translate(col("SVC_PROV_ID"), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', '')
    )
    .otherwise(col("SVC_PROV_ID"))
    .alias("PROVIDER_ID"),
    col("SVC_PROV_NM").alias("PROVIDER_NAME"),
    lit("").alias("TYPE_OF_PROV"),
    col("SVC_PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    col("SVC_PROV_TAX_ID").alias("PROV_TAX_ID"),
    col("SVC_PROV_TXNMY_CD").alias("PROV_TXNMY_CD")
)

df_BILL_PROV = df_inbound.filter(
    (
        (trim(col("SRC_SYS_CD")) == 'DOMINION') &
        (substring(trim(col("PROV_ID")), 1, 1) == 'P') &
        (trim(col("PROV_ID")) != '') &
        (col("PROV_ID").isNotNull()) &
        (col("REC_TYPE") == 'CLAIM HEADER')
    )
    |
    (
        (trim(col("SRC_SYS_CD")) != 'DOMINION') &
        (trim(col("PROV_ID")) != '') &
        (col("PROV_ID").isNotNull()) &
        (col("REC_TYPE") == 'CLAIM HEADER')
    )
).select(
    when(
        trim(col("SRC_SYS_CD")) == 'DOMINION',
        translate(col("PROV_ID"), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', '')
    )
    .otherwise(col("PROV_ID"))
    .alias("PROVIDER_ID"),
    col("PROV_NM").alias("PROVIDER_NAME"),
    lit("").alias("TYPE_OF_PROV"),
    col("PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    col("PROV_TAX_ID").alias("PROV_TAX_ID"),
    col("PROV_TXNMY_CD").alias("PROV_TXNMY_CD")
)

df_SRVC_ADDR = df_inbound.filter(
    (
        (trim(col("SRC_SYS_CD")) == 'DOMINION') &
        (substring(trim(col("PROV_ID")), 1, 1) == 'F') &
        (trim(col("SVC_PROV_ID")) != '') &
        (col("SVC_PROV_ID").isNotNull()) &
        (col("REC_TYPE") == 'CLAIM HEADER')
    )
    |
    (
        (trim(col("SRC_SYS_CD")) != 'DOMINION') &
        (trim(col("SVC_PROV_ID")) != '') &
        (col("SVC_PROV_ID").isNotNull()) &
        (col("REC_TYPE") == 'CLAIM HEADER')
    )
).select(
    when(
        trim(col("SRC_SYS_CD")) == 'DOMINION',
        translate(col("SVC_PROV_ID"), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', '')
    )
    .otherwise(col("SVC_PROV_ID"))
    .alias("PROVIDER_ID"),
    col("SVC_PROV_ADDR_LN1").alias("PROVIDER_ADDR_1"),
    col("SVC_PROV_ADDR_LN2").alias("PROVIDER_ADDR_2"),
    col("SVC_PROV_CITY_NM").alias("PROVIDER_CITY"),
    col("SVC_PROV_ST_CD").alias("PROVIDER_STATE_CD"),
    col("SVC_PROV_ZIP_CD_5").alias("PROVIDER_ZIP_CD_5"),
    lit("").alias("TYPE_OF_PROV")
)

df_BILL_ADDR = df_inbound.filter(
    (
        (trim(col("SRC_SYS_CD")) == 'DOMINION') &
        (substring(trim(col("PROV_ID")), 1, 1) == 'P') &
        (trim(col("PROV_ID")) != '') &
        (col("PROV_ID").isNotNull()) &
        (col("REC_TYPE") == 'CLAIM HEADER')
    )
    |
    (
        (trim(col("SRC_SYS_CD")) != 'DOMINION') &
        (trim(col("PROV_ID")) != '') &
        (col("PROV_ID").isNotNull()) &
        (col("REC_TYPE") == 'CLAIM HEADER')
    )
).select(
    when(
        trim(col("SRC_SYS_CD")) == 'DOMINION',
        translate(col("PROV_ID"), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', '')
    )
    .otherwise(col("PROV_ID"))
    .alias("PROVIDER_ID"),
    col("PROV_PRI_MAIL_ADDR_LN_1").alias("PROVIDER_ADDR_1"),
    col("PROV_PRI_MAIL_ADDR_LN_2").alias("PROVIDER_ADDR_2"),
    col("PROV_PRI_MAIL_ADDR_CITY_NM").alias("PROVIDER_CITY"),
    col("PROV_PRI_MAIL_ADDR_ST_CD").alias("PROVIDER_STATE_CD"),
    col("PROV_PRI_MAIL_ADDR_ZIP_CD_5").alias("PROVIDER_ZIP_CD_5"),
    lit("").alias("TYPE_OF_PROV")
)

df_fn_prov = df_SRVC_PROV.union(df_BILL_PROV)

df_rmdup_prov = dedup_sort(
    df_fn_prov,
    partition_cols=["PROVIDER_ID", "PROVIDER_NAME"],
    sort_cols=[("PROVIDER_ID", "A"), ("PROVIDER_NAME", "A")]
)

df_final_prov = df_rmdup_prov.select(
    col("PROVIDER_ID"),
    col("PROVIDER_NAME"),
    col("TYPE_OF_PROV"),
    col("PROV_NTNL_PROV_ID"),
    col("PROV_TAX_ID"),
    col("PROV_TXNMY_CD")
)

df_final_prov = df_final_prov.select(
    rpad(col("PROVIDER_ID"), 255, " ").alias("PROVIDER_ID"),
    rpad(col("PROVIDER_NAME"), 255, " ").alias("PROVIDER_NAME"),
    rpad(col("TYPE_OF_PROV"), 255, " ").alias("TYPE_OF_PROV"),
    rpad(col("PROV_NTNL_PROV_ID"), 255, " ").alias("PROV_NTNL_PROV_ID"),
    rpad(col("PROV_TAX_ID"), 255, " ").alias("PROV_TAX_ID"),
    rpad(col("PROV_TXNMY_CD"), 255, " ").alias("PROV_TXNMY_CD")
)

write_files(
    df_final_prov,
    f"{adls_path}/ds/PROV.{SrcSysCd}.extr.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_fn_addr = df_BILL_ADDR.union(df_SRVC_ADDR)

df_rmdup_addr = dedup_sort(
    df_fn_addr,
    partition_cols=["PROVIDER_ID", "TYPE_OF_PROV", "PROVIDER_ADDR_1", "PROVIDER_ADDR_2", "PROVIDER_CITY", "PROVIDER_STATE_CD", "PROVIDER_ZIP_CD_5"],
    sort_cols=[
        ("PROVIDER_ID", "A"),
        ("TYPE_OF_PROV", "A"),
        ("PROVIDER_ADDR_1", "A"),
        ("PROVIDER_ADDR_2", "A"),
        ("PROVIDER_CITY", "A"),
        ("PROVIDER_STATE_CD", "A"),
        ("PROVIDER_ZIP_CD_5", "A")
    ]
)

df_final_addr = df_rmdup_addr.select(
    col("PROVIDER_ID"),
    col("PROVIDER_ADDR_1"),
    col("PROVIDER_ADDR_2"),
    col("PROVIDER_CITY"),
    col("PROVIDER_STATE_CD"),
    col("PROVIDER_ZIP_CD_5"),
    col("TYPE_OF_PROV")
)

df_final_addr = df_final_addr.select(
    rpad(col("PROVIDER_ID"), 255, " ").alias("PROVIDER_ID"),
    rpad(col("PROVIDER_ADDR_1"), 255, " ").alias("PROVIDER_ADDR_1"),
    rpad(col("PROVIDER_ADDR_2"), 255, " ").alias("PROVIDER_ADDR_2"),
    rpad(col("PROVIDER_CITY"), 255, " ").alias("PROVIDER_CITY"),
    rpad(col("PROVIDER_STATE_CD"), 255, " ").alias("PROVIDER_STATE_CD"),
    rpad(col("PROVIDER_ZIP_CD_5"), 255, " ").alias("PROVIDER_ZIP_CD_5"),
    rpad(col("TYPE_OF_PROV"), 255, " ").alias("TYPE_OF_PROV")
)

write_files(
    df_final_addr,
    f"{adls_path}/ds/PROV_ADDR.{SrcSysCd}.extr.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)