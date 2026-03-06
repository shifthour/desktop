# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Job: I820CMSAcaPgmPaymtExtr
# MAGIC 
# MAGIC This job Extracts data from Gorman Program  file
# MAGIC 
# MAGIC CALLED BY: GORMANI820BCBSFinanceTablesLoadSeq
# MAGIC 
# MAGIC Developer                                            Date                          Project or Ticket                         Description                                    Environment                   Code Reviewer               Review Date
# MAGIC ------------------------                           -----------------------          ---------------------------------------            ---------------------------------------------------    -----------------------------          -----------------------------          ------------------------------
# MAGIC Raj Kommineni                               2021-12-15              US390105                                        Original programming                     IntegrateDev2           Jeyaprasanna                 2021-12-18

# MAGIC This job generates data load to CMS_ACA_PGM_PAYMT table form Gorman Program file
# MAGIC Sort on Alternate Keys to generate Sequence number
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
RunID = get_widget_value("RunID", "")
GormProgFile = get_widget_value("GormProgFile", "")

# ----------------------------------------------------
# Stage: DB2_CD_MPPNG (DB2ConnectorPX)
# ----------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    f"SELECT DISTINCT SRC_CD, TRGT_CD, SRC_CLCTN_CD, SRC_DOMAIN_NM, "
    f"SRC_SYS_CD, TRGT_CLCTN_CD, TRGT_DOMAIN_NM "
    f"FROM {IDSOwner}.CD_MPPNG "
    f"WHERE SRC_SYS_CD = 'CMS' "
    f"  AND SRC_CLCTN_CD = 'CMS' "
    f"  AND SRC_DOMAIN_NM = 'ENROLLMENT PAYMENT TYPE' "
    f"  AND TRGT_CLCTN_CD = 'IDS' "
    f"  AND TRGT_DOMAIN_NM = 'ENROLLMENT PAYMENT TYPE'"
)

df_DB2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# ----------------------------------------------------
# Stage: Valencia_Prog_File (PxSequentialFile)
# ----------------------------------------------------
schema_valencia_prog_file = StructType([
    StructField("HIX_820_CMS_ID", IntegerType(), True),
    StructField("RUN_DATE", StringType(), True),
    StructField("TRADING_PARTNER_ID", StringType(), True),
    StructField("EFT_EFFECTIVE_DATE", StringType(), True),
    StructField("EFT_TRACE_NUMBER", StringType(), True),
    StructField("EXCHANGE_TOTAL_EFT_PAYMENT_AMOUNT", DecimalType(38, 10), True),
    StructField("EXCHANGE_ASSIGNED_QHP_ID", StringType(), True),
    StructField("PAYMENT_TYPE", StringType(), True),
    StructField("PAYMENT_AMOUNT", DecimalType(38, 10), True),
    StructField("PAYMENT_START_DATE", StringType(), True),
    StructField("PAYMENT_END_DATE", StringType(), True),
    StructField("REPORT_TYPE_1", StringType(), True),
    StructField("REPORT_DCN_1", StringType(), True),
    StructField("REPORT_TYPE_2", StringType(), True),
    StructField("REPORT_DCN_2", StringType(), True),
    StructField("FILE_TYPE", StringType(), True),
    StructField("LOAD_FILE_NAME", StringType(), True),
    StructField("CREATED_BY", StringType(), True),
    StructField("CREATED_ON", StringType(), True),
    StructField("TOTAL_PROGRAM_AMOUNT", DecimalType(38, 10), True),
])

df_Valencia_Prog_File = (
    spark.read
    .option("header", True)
    .option("sep", ",")
    .option("quote", '"')
    .option("nullValue", None)
    .schema(schema_valencia_prog_file)
    .csv(f"{adls_path_raw}/landing/{GormProgFile}")
)

# ----------------------------------------------------
# Stage: TrnsFields (CTransformerStage)
# ----------------------------------------------------
df_TrnsFields = (
    df_Valencia_Prog_File
    .withColumn(
        "EFT_EFFECTIVE_DATE",
        F.when(F.col("EFT_EFFECTIVE_DATE").isNull(), F.lit("1753-01-01")).otherwise(F.col("EFT_EFFECTIVE_DATE"))
    )
    .withColumn(
        "PAYMENT_START_DATE",
        F.when(F.col("PAYMENT_START_DATE").isNull(), F.lit("1753-01-01")).otherwise(F.col("PAYMENT_START_DATE"))
    )
    .withColumn(
        "PAYMENT_TYPE",
        F.when(F.col("PAYMENT_TYPE").isNull(), F.lit("")).otherwise(F.col("PAYMENT_TYPE"))
    )
    .withColumn(
        "REPORT_TYPE_1",
        F.when(F.col("REPORT_TYPE_1").isNull(), F.lit("")).otherwise(F.col("REPORT_TYPE_1"))
    )
    .withColumn(
        "REPORT_DCN_1",
        F.when(F.col("REPORT_DCN_1").isNull(), F.lit("")).otherwise(F.col("REPORT_DCN_1"))
    )
    .withColumn(
        "REPORT_TYPE_2",
        F.when(F.col("REPORT_TYPE_2").isNull(), F.lit("")).otherwise(F.col("REPORT_TYPE_2"))
    )
    .withColumn(
        "REPORT_DCN_2",
        F.when(F.col("REPORT_DCN_2").isNull(), F.lit("")).otherwise(F.col("REPORT_DCN_2"))
    )
    .withColumn(
        "EXCHANGE_ASSIGNED_QHP_ID",
        F.when(F.col("EXCHANGE_ASSIGNED_QHP_ID").isNull(), F.lit("")).otherwise(F.col("EXCHANGE_ASSIGNED_QHP_ID"))
    )
    .withColumn(
        "PAYMENT_END_DATE",
        F.when(F.col("PAYMENT_END_DATE").isNull(), F.lit("1753-01-01")).otherwise(F.col("PAYMENT_END_DATE"))
    )
    .withColumn(
        "PAYMENT_AMOUNT",
        F.when(F.col("PAYMENT_AMOUNT").isNull(), F.lit(0)).otherwise(F.col("PAYMENT_AMOUNT"))
    )
    .withColumn(
        "HIX_820_CMS_ID",
        F.when(F.col("HIX_820_CMS_ID").isNull(), F.lit(0)).otherwise(F.col("HIX_820_CMS_ID"))
    )
    .withColumn(
        "EFT_TRACE_NUMBER",
        F.when(F.col("EFT_TRACE_NUMBER").isNull(), F.lit("")).otherwise(F.col("EFT_TRACE_NUMBER"))
    )
    .withColumn(
        "CREATED_ON",
        F.when(F.col("CREATED_ON").isNull(), F.lit("1753-01-01")).otherwise(F.col("CREATED_ON"))
    )
    .withColumn(
        "RUN_DATE",
        F.when(F.col("RUN_DATE").isNull(), F.lit("1753-01-01")).otherwise(F.col("RUN_DATE"))
    )
)

# ----------------------------------------------------
# Stage: Lkp_PaymentType (PxLookup)
#   - Left join for match
#   - Produce matched (primary) output
#   - Produce reject output
# ----------------------------------------------------
df_Lkp = (
    df_TrnsFields.alias("Lnk_PaymetType_In")
    .join(
        df_DB2_CD_MPPNG.alias("LkFrom_CD_MPPNG"),
        (
            (F.col("Lnk_PaymetType_In.PAYMENT_TYPE") == F.col("LkFrom_CD_MPPNG.SRC_CD"))
            & (F.col("Lnk_PaymetType_In.PAYMENT_TYPE") == F.col("LkFrom_CD_MPPNG.TRGT_CD"))
        ),
        how="left"
    )
)

# Matched rows
df_Lkp_PaymentType_out = (
    df_Lkp
    .filter(F.col("LkFrom_CD_MPPNG.SRC_CD").isNotNull())
    .select(
        F.col("Lnk_PaymetType_In.EFT_EFFECTIVE_DATE").alias("EFT_EFFECTIVE_DATE"),
        F.col("Lnk_PaymetType_In.PAYMENT_START_DATE").alias("PAYMENT_START_DATE"),
        F.col("Lnk_PaymetType_In.PAYMENT_TYPE").alias("PAYMENT_TYPE"),
        F.col("Lnk_PaymetType_In.REPORT_TYPE_1").alias("REPORT_TYPE_1"),
        F.col("Lnk_PaymetType_In.REPORT_DCN_1").alias("REPORT_DCN_1"),
        F.col("Lnk_PaymetType_In.REPORT_TYPE_2").alias("REPORT_TYPE_2"),
        F.col("Lnk_PaymetType_In.REPORT_DCN_2").alias("REPORT_DCN_2"),
        F.col("Lnk_PaymetType_In.EXCHANGE_ASSIGNED_QHP_ID").alias("EXCHANGE_ASSIGNED_QHP_ID"),
        F.col("Lnk_PaymetType_In.PAYMENT_END_DATE").alias("PAYMENT_END_DATE"),
        F.col("Lnk_PaymetType_In.PAYMENT_AMOUNT").alias("PAYMENT_AMOUNT"),
        F.col("Lnk_PaymetType_In.HIX_820_CMS_ID").alias("HIX_820_CMS_ID"),
        F.col("Lnk_PaymetType_In.EFT_TRACE_NUMBER").alias("EFT_TRACE_NUMBER"),
        F.col("Lnk_PaymetType_In.CREATED_ON").alias("CREATED_ON"),
        F.col("Lnk_PaymetType_In.RUN_DATE").alias("RUN_DATE"),
        F.col("LkFrom_CD_MPPNG.TRGT_CD").alias("TRGT_CD")
    )
)

# Reject rows
df_Lkp_Reject = (
    df_Lkp
    .filter(F.col("LkFrom_CD_MPPNG.SRC_CD").isNull())
)

# Build the reject dataframe from the original references (Valencia_Prog_File columns)
# Reapply the expressions shown under Rej_Valencia_Error using the original columns
df_Rej_Valencia_Error = df_Lkp_Reject.alias("rej").join(
    df_Valencia_Prog_File.alias("orig"),
    on=[
        df_Lkp_Reject["HIX_820_CMS_ID"] == df_Valencia_Prog_File["HIX_820_CMS_ID"],
        df_Lkp_Reject["RUN_DATE"] == df_Valencia_Prog_File["RUN_DATE"],
        df_Lkp_Reject["TRADING_PARTNER_ID"] == df_Valencia_Prog_File["TRADING_PARTNER_ID"],
        df_Lkp_Reject["EFT_EFFECTIVE_DATE"] == df_Valencia_Prog_File["EFT_EFFECTIVE_DATE"],
        df_Lkp_Reject["EFT_TRACE_NUMBER"] == df_Valencia_Prog_File["EFT_TRACE_NUMBER"],
        df_Lkp_Reject["EXCHANGE_TOTAL_EFT_PAYMENT_AMOUNT"] == df_Valencia_Prog_File["EXCHANGE_TOTAL_EFT_PAYMENT_AMOUNT"],
        df_Lkp_Reject["EXCHANGE_ASSIGNED_QHP_ID"] == df_Valencia_Prog_File["EXCHANGE_ASSIGNED_QHP_ID"],
        df_Lkp_Reject["PAYMENT_TYPE"] == df_Valencia_Prog_File["PAYMENT_TYPE"],
        df_Lkp_Reject["PAYMENT_AMOUNT"] == df_Valencia_Prog_File["PAYMENT_AMOUNT"],
        df_Lkp_Reject["PAYMENT_START_DATE"] == df_Valencia_Prog_File["PAYMENT_START_DATE"],
        df_Lkp_Reject["PAYMENT_END_DATE"] == df_Valencia_Prog_File["PAYMENT_END_DATE"],
        df_Lkp_Reject["REPORT_TYPE_1"] == df_Valencia_Prog_File["REPORT_TYPE_1"],
        df_Lkp_Reject["REPORT_DCN_1"] == df_Valencia_Prog_File["REPORT_DCN_1"],
        df_Lkp_Reject["REPORT_TYPE_2"] == df_Valencia_Prog_File["REPORT_TYPE_2"],
        df_Lkp_Reject["REPORT_DCN_2"] == df_Valencia_Prog_File["REPORT_DCN_2"],
        df_Lkp_Reject["FILE_TYPE"] == df_Valencia_Prog_File["FILE_TYPE"],
        df_Lkp_Reject["LOAD_FILE_NAME"] == df_Valencia_Prog_File["LOAD_FILE_NAME"],
        df_Lkp_Reject["CREATED_BY"] == df_Valencia_Prog_File["CREATED_BY"],
        df_Lkp_Reject["CREATED_ON"] == df_Valencia_Prog_File["CREATED_ON"],
        df_Lkp_Reject["TOTAL_PROGRAM_AMOUNT"] == df_Valencia_Prog_File["TOTAL_PROGRAM_AMOUNT"],
    ],
    how="left"
).select(
    F.when(F.col("orig.EFT_EFFECTIVE_DATE").isNull(), F.lit("1753-01-01")).otherwise(F.col("orig.EFT_EFFECTIVE_DATE")).alias("EFT_EFFECTIVE_DATE"),
    F.when(F.col("orig.PAYMENT_START_DATE").isNull(), F.lit("1753-01-01")).otherwise(F.col("orig.PAYMENT_START_DATE")).alias("PAYMENT_START_DATE"),
    F.when(F.col("orig.PAYMENT_TYPE").isNull(), F.lit(" ")).otherwise(F.col("orig.PAYMENT_TYPE")).alias("PAYMENT_TYPE"),
    F.when(F.col("orig.REPORT_TYPE_1").isNull(), F.lit(" ")).otherwise(F.col("orig.REPORT_TYPE_1")).alias("REPORT_TYPE_1"),
    F.when(F.col("orig.REPORT_DCN_1").isNull(), F.lit(" ")).otherwise(F.col("orig.REPORT_DCN_1")).alias("REPORT_DCN_1"),
    F.when(F.col("orig.REPORT_TYPE_2").isNull(), F.lit(" ")).otherwise(F.col("orig.REPORT_TYPE_2")).alias("REPORT_TYPE_2"),
    F.when(F.col("orig.REPORT_DCN_2").isNull(), F.lit(" ")).otherwise(F.col("orig.REPORT_DCN_2")).alias("REPORT_DCN_2"),
    F.when(F.col("orig.EXCHANGE_ASSIGNED_QHP_ID").isNull(), F.lit(" ")).otherwise(F.col("orig.EXCHANGE_ASSIGNED_QHP_ID")).alias("EXCHANGE_ASSIGNED_QHP_ID"),
    F.when(F.col("orig.PAYMENT_END_DATE").isNull(), F.lit("1753-01-01")).otherwise(F.col("orig.PAYMENT_END_DATE")).alias("PAYMENT_END_DATE"),
    F.when(F.col("orig.PAYMENT_AMOUNT").isNull(), F.lit(0)).otherwise(F.col("orig.PAYMENT_AMOUNT")).alias("PAYMENT_AMOUNT"),
    F.when(F.col("orig.HIX_820_CMS_ID").isNull(), F.lit("0")).otherwise(F.col("orig.HIX_820_CMS_ID")).alias("HIX_820_CMS_ID"),
    F.when(F.col("orig.EFT_TRACE_NUMBER").isNull(), F.lit(" ")).otherwise(F.col("orig.EFT_TRACE_NUMBER")).alias("EFT_TRACE_NUMBER"),
    F.when(F.col("orig.CREATED_ON").isNull(), F.lit("1753-01-01")).otherwise(F.col("orig.CREATED_ON")).alias("CREATED_ON"),
    F.when(F.col("orig.RUN_DATE").isNull(), F.lit("1753-01-01")).otherwise(F.col("orig.RUN_DATE")).alias("RUN_DATE")
)

# ----------------------------------------------------
# Stage: SF_Valencia_Error_Report (PxSequentialFile) - Write
# ----------------------------------------------------
# Apply rpad for columns declared as char(...). We see these columns with declared lengths: EFT_EFFECTIVE_DATE(char 10), PAYMENT_START_DATE(char 10),
# PAYMENT_END_DATE(char 10), RUN_DATE(char 10). Others are varchars or decimals with no declared length. 
df_Rej_Valencia_Error_rpad = (
    df_Rej_Valencia_Error
    .withColumn("EFT_EFFECTIVE_DATE", F.rpad(F.col("EFT_EFFECTIVE_DATE"), 10, " "))
    .withColumn("PAYMENT_START_DATE", F.rpad(F.col("PAYMENT_START_DATE"), 10, " "))
    .withColumn("PAYMENT_END_DATE", F.rpad(F.col("PAYMENT_END_DATE"), 10, " "))
    .withColumn("RUN_DATE", F.rpad(F.col("RUN_DATE"), 10, " "))
)

df_Rej_Valencia_Error_final = df_Rej_Valencia_Error_rpad.select(
    "EFT_EFFECTIVE_DATE",
    "PAYMENT_START_DATE",
    "PAYMENT_TYPE",
    "REPORT_TYPE_1",
    "REPORT_DCN_1",
    "REPORT_TYPE_2",
    "REPORT_DCN_2",
    "EXCHANGE_ASSIGNED_QHP_ID",
    "PAYMENT_END_DATE",
    "PAYMENT_AMOUNT",
    "HIX_820_CMS_ID",
    "EFT_TRACE_NUMBER",
    "CREATED_ON",
    "RUN_DATE"
)

write_files(
    df_Rej_Valencia_Error_final,
    f"{adls_path_raw}/landing/Cms_Error_Report_{RunID}.csv",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=" "
)

# ----------------------------------------------------
# Stage: Trns_PgmPaymt (CTransformerStage)
# ----------------------------------------------------
df_Trns_PgmPaymt = (
    df_Lkp_PaymentType_out
    .withColumn(
        "ACTVTY_YRMO",
        F.when(
            F.length(F.trim(F.col("EFT_EFFECTIVE_DATE"))) == 10,
            F.col("EFT_EFFECTIVE_DATE").substr(F.lit(7), F.lit(4)).concat(F.col("EFT_EFFECTIVE_DATE").substr(F.lit(1), F.lit(2)))
        ).when(
            F.length(F.trim(F.col("EFT_EFFECTIVE_DATE"))) == 9,
            F.col("EFT_EFFECTIVE_DATE").substr(F.lit(6), F.lit(4)).concat(F.col("EFT_EFFECTIVE_DATE").substr(F.lit(1), F.lit(2)))
        ).otherwise(F.lit("NA"))
    )
    .withColumn(
        "PAYMT_COV_YRMO",
        F.when(
            F.length(F.trim(F.col("PAYMENT_START_DATE"))) == 10,
            F.col("PAYMENT_START_DATE").substr(F.lit(7), F.lit(4)).concat(F.col("PAYMENT_START_DATE").substr(F.lit(1), F.lit(2)))
        ).when(
            F.length(F.trim(F.col("PAYMENT_START_DATE"))) == 9,
            F.col("PAYMENT_START_DATE").substr(F.lit(6), F.lit(4)).concat(F.col("EFT_EFFECTIVE_DATE").substr(F.lit(1), F.lit(2)))
        ).otherwise(F.lit("NA"))
    )
    .withColumn(
        "PAYMT_TYP_CD",
        F.col("TRGT_CD")
    )
    .withColumn(
        "STATE_CODE",
        F.when(
            (
                (F.trim(F.col("REPORT_TYPE_1")) == F.lit("ISSUERIDRPT"))
                & (F.trim(F.col("REPORT_DCN_1")) == F.lit("94248"))
            )
            | (
                (F.trim(F.col("REPORT_TYPE_2")) == F.lit("ISSUERIDRPT"))
                & (F.trim(F.col("REPORT_DCN_2")) == F.lit("94248"))
            ),
            F.lit("KS")
        ).when(
            (
                (F.trim(F.col("REPORT_TYPE_1")) == F.lit("ISSUERIDRPT"))
                & (F.trim(F.col("REPORT_DCN_1")) == F.lit("34762"))
            )
            | (
                (F.trim(F.col("REPORT_TYPE_2")) == F.lit("ISSUERIDRPT"))
                & (F.trim(F.col("REPORT_DCN_2")) == F.lit("34762"))
            ),
            F.lit("MO")
        ).otherwise(F.lit("NA"))
    )
    .withColumn(
        "QHP_ID",
        F.col("EXCHANGE_ASSIGNED_QHP_ID")
    )
    .withColumn(
        "COV_START_DT",
        F.to_timestamp(
            F.concat(
                F.col("PAYMENT_START_DATE").substr(F.lit(7), F.lit(4)),
                F.lit("-"),
                F.col("PAYMENT_START_DATE").substr(F.lit(1), F.lit(2)),
                F.lit("-"),
                F.col("PAYMENT_START_DATE").substr(F.lit(4), F.lit(2)),
                F.lit(" 00:00:00")
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .withColumn(
        "COV_END_DT",
        F.to_timestamp(
            F.concat(
                F.col("PAYMENT_END_DATE").substr(F.lit(7), F.lit(4)),
                F.lit("-"),
                F.col("PAYMENT_END_DATE").substr(F.lit(1), F.lit(2)),
                F.lit("-"),
                F.col("PAYMENT_END_DATE").substr(F.lit(4), F.lit(2)),
                F.lit(" 00:00:00")
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .withColumn(
        "TRANS_AMT",
        F.col("PAYMENT_AMOUNT")
    )
    .withColumn(
        "ACA_PGM_PAYMT_UNIQ_KEY",
        F.col("HIX_820_CMS_ID")
    )
    .withColumn(
        "EXCH_RPT_DOC_CTL_NO",
        F.when(
            (F.trim(F.col("REPORT_TYPE_1")) != F.lit("")) & (F.trim(F.col("REPORT_TYPE_1")) != F.lit("ISSUERIDRPT")),
            F.trim(F.col("REPORT_DCN_1"))
        ).when(
            (F.trim(F.col("REPORT_TYPE_2")) != F.lit("")) & (F.trim(F.col("REPORT_TYPE_2")) != F.lit("ISSUERIDRPT")),
            F.trim(F.col("REPORT_DCN_2"))
        ).otherwise(F.lit("NULL"))
    )
    .withColumn(
        "EXCH_RPT_NM",
        F.when(
            (F.trim(F.col("REPORT_TYPE_1")) != F.lit("")) & (F.trim(F.col("REPORT_TYPE_1")) != F.lit("ISSUERIDRPT")),
            F.trim(F.col("REPORT_TYPE_1"))
        ).when(
            (F.trim(F.col("REPORT_TYPE_2")) != F.lit("")) & (F.trim(F.col("REPORT_TYPE_2")) != F.lit("ISSUERIDRPT")),
            F.trim(F.col("REPORT_TYPE_2"))
        ).otherwise(F.lit("NULL"))
    )
    .withColumn("EFT_TRACE_ID", F.col("EFT_TRACE_NUMBER"))
    .withColumn("CRT_DT", current_timestamp())
    .withColumn("LAST_UPDT_DT", current_timestamp())
)

# ----------------------------------------------------
# Stage: Sort_SeqKeys (PxSort)
# ----------------------------------------------------
df_Sort_SeqKeys = df_Trns_PgmPaymt.orderBy(
    "ACTVTY_YRMO", "PAYMT_COV_YRMO", "STATE_CODE", "QHP_ID"
)

# ----------------------------------------------------
# Stage: Tran_PgmPaymtSeqNo (CTransformerStage)
#   Emulate the stage variables with a partition-based row_number
# ----------------------------------------------------
windowSpec = (
    Window.partitionBy("ACTVTY_YRMO","PAYMT_COV_YRMO","STATE_CODE","QHP_ID")
    .orderBy("ACTVTY_YRMO","PAYMT_COV_YRMO","STATE_CODE","QHP_ID")
)

df_Tran_PgmPaymtSeqNo = (
    df_Sort_SeqKeys
    .withColumn("CMS_ACA_PGM_PAYMT_SEQ_NO", F.row_number().over(windowSpec))
)

# Select final columns:
df_Tran_PgmPaymtSeqNo_selected = df_Tran_PgmPaymtSeqNo.select(
    F.col("ACTVTY_YRMO").alias("ACTVTY_YRMO"),
    F.col("PAYMT_COV_YRMO").alias("PAYMT_COV_YRMO"),
    F.col("PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
    F.col("STATE_CODE").alias("STATE_CODE"),
    F.col("QHP_ID").alias("QHP_ID"),
    F.col("CMS_ACA_PGM_PAYMT_SEQ_NO").alias("CMS_ACA_PGM_PAYMT_SEQ_NO"),
    F.col("COV_START_DT").alias("COV_START_DT"),
    F.col("COV_END_DT").alias("COV_END_DT"),
    F.col("TRANS_AMT").alias("TRANS_AMT"),
    F.col("ACA_PGM_PAYMT_UNIQ_KEY").alias("ACA_PGM_PAYMT_UNIQ_KEY"),
    F.col("EXCH_RPT_DOC_CTL_NO").alias("EXCH_RPT_DOC_CTL_NO"),
    F.col("EXCH_RPT_NM").alias("EXCH_RPT_NM"),
    F.col("EFT_TRACE_ID").alias("EFT_TRACE_ID"),
    F.col("CRT_DT").alias("CRT_DT"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

# ----------------------------------------------------
# Stage: SF_CMSAcaPgmPaymtExtr (PxSequentialFile) - Write
#   File path => "load" => we use adls_path + "/load"...
#   Output columns with char(...) -> apply rpad to char(6) columns
# ----------------------------------------------------
df_Final_rpad = (
    df_Tran_PgmPaymtSeqNo_selected
    .withColumn("ACTVTY_YRMO", F.rpad(F.col("ACTVTY_YRMO"), 6, " "))
    .withColumn("PAYMT_COV_YRMO", F.rpad(F.col("PAYMT_COV_YRMO"), 6, " "))
)

df_Final_out = df_Final_rpad.select(
    "ACTVTY_YRMO",
    "PAYMT_COV_YRMO",
    "PAYMT_TYP_CD",
    "STATE_CODE",
    "QHP_ID",
    "CMS_ACA_PGM_PAYMT_SEQ_NO",
    "COV_START_DT",
    "COV_END_DT",
    "TRANS_AMT",
    "ACA_PGM_PAYMT_UNIQ_KEY",
    "EXCH_RPT_DOC_CTL_NO",
    "EXCH_RPT_NM",
    "EFT_TRACE_ID",
    "CRT_DT",
    "LAST_UPDT_DT"
)

write_files(
    df_Final_out,
    f"{adls_path}/load/I820_CMS_ACA_PGM_PAYMT_VALENCIA.DAT",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)