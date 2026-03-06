# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Remark code record data from Facets and generates a data file for RedCard.
# MAGIC 
# MAGIC Job Name:  ClmRmarkCdDescExtr
# MAGIC Called By:RedCardEobExtrCntl
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                               Date                 Project/Altiris #      Change Description                                                                      Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                           --------------------     ------------------------      -----------------------------------------------------------------------                               --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi                     2020-12-05            RedCard                     Original Devlopment                                                                OutboundDev3             Jaideep Mankala       12/27/2020
# MAGIC Raja Gummadi                     2021-02-04            343913                 Added void logic                                                                            OutboundDev3                Jaideep Mankala        02/04/2021
# MAGIC Megan Conway                2022-03-09               	                    S2S Remediation - MSSQL connection parameters added	            OutboundDev5		Ken Bradmon	2022-05-19


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Outbound
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CLMPDDT = get_widget_value('CLMPDDT','')

# ---------------------------------------------------------
# Stage: CLM (ODBCConnectorPX)
# ---------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
df_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT EXCD.EXCD_ID, EXCD.EXCD_LONG_TEXT1, EXCD.EXCD_LONG_TEXT2 FROM {FacetsOwner}.CMC_EXCD_EXPL_CD EXCD")
    .load()
)

# ---------------------------------------------------------
# Stage: Remove_Duplicates_16 (PxRemDup)
# ---------------------------------------------------------
df_Remove_Duplicates_16_tmp = dedup_sort(
    df_CLM,
    partition_cols=["EXCD_ID"],
    sort_cols=[("EXCD_ID", "A")]
)
df_Remove_Duplicates_16 = df_Remove_Duplicates_16_tmp.select(
    "EXCD_ID",
    "EXCD_LONG_TEXT1",
    "EXCD_LONG_TEXT2"
)

# ---------------------------------------------------------
# Stage: SvcLnFile (PxSequentialFile) reading "Zelis_EOBClaimSvcLn02.dat"
# ---------------------------------------------------------
schema_SvcLnFile = StructType([
    StructField("CRCRDTYP", StringType(), False),
    StructField("CRCRDVRSN", StringType(), False),
    StructField("CDOCID", StringType(), False),
    StructField("CCLMEQUENCE", StringType(), False),
    StructField("CCLMNO", StringType(), False),
    StructField("CSVCLINEEQUENCE", StringType(), False),
    StructField("CLNNO", StringType(), False),
    StructField("CTYPOFSVC", StringType(), False),
    StructField("CPROCCD", StringType(), False),
    StructField("CPLCOFSVC", StringType(), False),
    StructField("CUNIT", StringType(), False),
    StructField("CPROC", StringType(), False),
    StructField("CSVCATESTRT", StringType(), False),
    StructField("CSVCATEEND", StringType(), False),
    StructField("CTOTCHRG", StringType(), False),
    StructField("CDSCNT", StringType(), False),
    StructField("CINELG", StringType(), False),
    StructField("CPEND", StringType(), False),
    StructField("CALLOWED", StringType(), False),
    StructField("CNOTESCD1", StringType(), False),
    StructField("CNOTESCD2", StringType(), False),
    StructField("CNOTESCD3", StringType(), False),
    StructField("CDEDCT", StringType(), False),
    StructField("CCOPAY", StringType(), False),
    StructField("CCOINS", StringType(), False),
    StructField("CBAL", StringType(), False),
    StructField("CPCT", StringType(), False),
    StructField("COTHRCAR", StringType(), False),
    StructField("CPAYMT", StringType(), False),
    StructField("CPATNRESP", StringType(), False),
    StructField("CDESC", StringType(), False),
    StructField("CTOOTHNO", StringType(), False),
    StructField("CTOOTHSRFC", StringType(), False),
    StructField("CPATNACCTNO", StringType(), False),
    StructField("CRXNO", StringType(), False),
    StructField("COTHRAMT1", StringType(), False),
    StructField("COTHRAMT2", StringType(), False),
    StructField("COTHRAMT3", StringType(), False),
    StructField("COTHRAMT4", StringType(), False),
    StructField("COTHRAMT5", StringType(), False),
    StructField("COPENFLD1", StringType(), False),
    StructField("COPENFLD2", StringType(), False),
    StructField("COPENFLD3", StringType(), False),
    StructField("COPENFLD4", StringType(), False),
    StructField("COPENFLD5", StringType(), False),
    StructField("CCLMRELSTRING", StringType(), False),
    StructField("CRSNBLANDCUST", StringType(), False),
    StructField("CCOBALW", StringType(), False),
    StructField("CPPOCNTRNO", StringType(), False),
    StructField("COVRSNBLANDCUST", StringType(), False),
    StructField("CNOTESCD4", StringType(), False),
    StructField("CNOTESCD5", StringType(), False),
    StructField("CNOTESCD1ISPATNREPONSIBILITY", StringType(), False),
    StructField("CNOTESCD2ISPATNREPONSIBILITY", StringType(), False),
    StructField("CNOTESCD3ISPATNREPONSIBILITY", StringType(), False),
    StructField("CNOTESCD4ISPATNREPONSIBILITY", StringType(), False),
    StructField("CNOTESCD5ISPATNREPONSIBILITY", StringType(), False),
    StructField("CMCAREASGMT", StringType(), False),
    StructField("CPROCMOD", StringType(), False),
    StructField("CPNLTY", StringType(), False),
    StructField("CNOTESCD1TYP", StringType(), False),
    StructField("CNOTESCD2TYP", StringType(), False),
    StructField("CNOTESCD3TYP", StringType(), False),
    StructField("CNOTESCD4TYP", StringType(), False),
    StructField("CNOTESCD5TYP", StringType(), False),
    StructField("CALTPROCCD", StringType(), False),
    StructField("CSVCQUALIFER", StringType(), False),
    StructField("CSVCADJAMT", StringType(), False),
    StructField("CSVCADJGRPCD", StringType(), False),
    StructField("CSVCADJRSNCD", StringType(), False),
    StructField("CSVCRARC", StringType(), False),
    StructField("CLNITEMCTLNO", StringType(), False),
    StructField("CORIGPROCCD", StringType(), False),
    StructField("COTHRAMT6", StringType(), False),
    StructField("COTHRAMT7", StringType(), False),
    StructField("COTHRAMT8", StringType(), False),
    StructField("COTHRAMT9", StringType(), False),
    StructField("COTHRAMT10", StringType(), False),
    StructField("COTHRAMT11", StringType(), False),
    StructField("COTHRAMT12", StringType(), False),
    StructField("COTHRAMT13", StringType(), False),
    StructField("CICDCD", StringType(), False),
    StructField("CPAYMTPROV", StringType(), False),
    StructField("CPAYMTENR", StringType(), False),
    StructField("CCOBDEDCT", StringType(), False),
    StructField("CCOBCR", StringType(), False),
    StructField("CDIAGCD1", StringType(), False),
    StructField("CDIAGCD2", StringType(), False),
    StructField("CDIAGCD3", StringType(), False),
    StructField("CDIAGCD4", StringType(), False),
    StructField("CDIAGCD5", StringType(), False),
    StructField("CPROVNM", StringType(), False),
    StructField("COPENFLD6", StringType(), False),
    StructField("COPENFLD7", StringType(), False),
    StructField("COPENFLD8", StringType(), False),
    StructField("COPENFLD9", StringType(), False),
    StructField("COPENFLD10", StringType(), False),
    StructField("COTHRAMT14", StringType(), False),
    StructField("COTHRAMT15", StringType(), False),
    StructField("COTHRAMT16", StringType(), False),
    StructField("COTHRAMT17", StringType(), False),
    StructField("COTHRAMT18", StringType(), False),
    StructField("CNETCOVDSCNTAMT", StringType(), False),
    StructField("CPPONAME", StringType(), False),
    StructField("CRVNUCD", StringType(), False),
    StructField("CNTNLDRUGCD", StringType(), False),
    StructField("CWTHLDAMT", StringType(), False),
])
df_SvcLnFile = (
    spark.read.format("csv")
    .option("delimiter", "\t")
    .option("header", False)
    .schema(schema_SvcLnFile)
    .load(f"{adls_path_publish}/external/Zelis_EOBClaimSvcLn02.dat")
)

# ---------------------------------------------------------
# Stage: Transformer_82 (CTransformerStage)
# ---------------------------------------------------------
# Create a column for the stage variable svRemarkCode
df_Transformer_82 = df_SvcLnFile.withColumn(
    "svRemarkCode",
    F.when(
        F.length(F.trim(F.col("CNOTESCD1"))) > 3,
        F.col("CNOTESCD1").substr(5, 3)
    ).otherwise(F.col("CNOTESCD1"))
)

# Output link1 (no constraint)
df_Transformer_82_link1 = df_Transformer_82.select(
    F.col("CCLMRELSTRING").alias("CCLMRELSTRING"),
    F.col("CSVCLINEEQUENCE").alias("CSVCLINEEQUENCE"),
    F.col("CDOCID").alias("CDOCID"),
    F.trim(F.col("CNOTESCD1")).substr(1, 3).alias("CNOTESCD1")
)

# Output DSLink22 (constraint: Trim(CNOTESCD3) <> '')
df_Transformer_82_DSLink22 = df_Transformer_82.filter(
    F.trim(F.col("CNOTESCD3")) != ""
).select(
    F.col("CCLMRELSTRING").alias("CCLMRELSTRING"),
    F.col("CSVCLINEEQUENCE").alias("CSVCLINEEQUENCE"),
    F.col("CDOCID").alias("CDOCID"),
    F.trim(F.col("CNOTESCD3")).substr(1, 3).alias("CNOTESCD1")
)

# Output DSLink24 (constraint: Trim(svRemarkCode) <> '')
df_Transformer_82_DSLink24 = df_Transformer_82.filter(
    F.trim(F.col("svRemarkCode")) != ""
).select(
    F.col("CCLMRELSTRING").alias("CCLMRELSTRING"),
    F.col("CSVCLINEEQUENCE").alias("CSVCLINEEQUENCE"),
    F.col("CDOCID").alias("CDOCID"),
    F.col("svRemarkCode").alias("CNOTESCD1")
)

# ---------------------------------------------------------
# Stage: SvcLnTot (PxSequentialFile) reading "Zelis_EOBClaimSvcLnTotal04.dat"
# ---------------------------------------------------------
schema_SvcLnTot = StructType([
    StructField("CRCRDTYP", StringType(), False),
    StructField("CRCRDVRSN", StringType(), False),
    StructField("CDOCID", StringType(), False),
    StructField("CCLMEQUENCE", StringType(), False),
    StructField("CCLMNO", StringType(), False),
    StructField("CTOTCHRG", StringType(), True),
    StructField("CDSCNT", StringType(), True),
    StructField("CINELG", StringType(), True),
    StructField("CPEND", StringType(), False),
    StructField("CALLOWED", StringType(), True),
    StructField("CDEDCT", StringType(), True),
    StructField("CCOPAY", StringType(), True),
    StructField("CCOINS", StringType(), True),
    StructField("CBAL", StringType(), False),
    StructField("COTHRCAR", StringType(), True),
    StructField("CPAYMT", StringType(), True),
    StructField("CPATNRESP", StringType(), True),
    StructField("COTHRAMT1", StringType(), False),
    StructField("COTHRAMT2", StringType(), False),
    StructField("COTHRAMT3", StringType(), False),
    StructField("COTHRAMT4", StringType(), False),
    StructField("COTHRAMT5", StringType(), False),
    StructField("COPENFLD1", StringType(), False),
    StructField("COPENFLD2", StringType(), False),
    StructField("COPENFLD3", StringType(), False),
    StructField("COPENFLD4", StringType(), False),
    StructField("COPENFLD5", StringType(), False),
    StructField("CCLMRELSTRING", StringType(), False),
    StructField("CCOBALW", StringType(), False),
    StructField("COVRSNBLANDCUST", StringType(), False),
    StructField("CPNLTY", StringType(), False),
    StructField("COTHRAMT6", StringType(), False),
    StructField("COTHRAMT7", StringType(), False),
    StructField("COTHRAMT8", StringType(), False),
    StructField("COTHRAMT9", StringType(), False),
    StructField("COTHRAMT10", StringType(), False),
    StructField("COTHRAMT11", StringType(), False),
    StructField("COTHRAMT12", StringType(), False),
    StructField("COTHRAMT13", StringType(), False),
    StructField("CPAYMTPROV", StringType(), False),
    StructField("CPAYMTENR", StringType(), False),
    StructField("CCOBDEDCT", StringType(), False),
    StructField("CCOBCR", StringType(), False),
    StructField("COTHRAMT14", StringType(), False),
    StructField("COTHRAMT15", StringType(), False),
    StructField("COTHRAMT16", StringType(), False),
    StructField("COTHRAMT17", StringType(), False),
    StructField("COTHRAMT18", StringType(), False),
    StructField("CNETCOVDSCNTAMT", StringType(), False),
    StructField("CRSNBLANDCUST", StringType(), False),
    StructField("CTOTOTHRCOBALW", StringType(), False),
    StructField("CTOTMCARECOB", StringType(), False),
    StructField("CORIGMCAREBILLAMT", StringType(), False),
    StructField("CWTHLDAMT", StringType(), False),
])
df_SvcLnTot = (
    spark.read.format("csv")
    .option("delimiter", "\t")
    .option("header", False)
    .schema(schema_SvcLnTot)
    .load(f"{adls_path_publish}/external/Zelis_EOBClaimSvcLnTotal04.dat")
)

# ---------------------------------------------------------
# Stage: Copy_of_Transformer_82 (CTransformerStage)
# ---------------------------------------------------------
df_Copy_of_Transformer_82 = df_SvcLnTot.withColumn(
    "svRemarkCode",
    F.when(
        F.length(F.trim(F.col("COPENFLD1"))) > 0,
        F.lit("Y")
    ).otherwise(F.lit("N"))
)
df_Copy_of_Transformer_82_svcLn = df_Copy_of_Transformer_82.filter(
    F.col("svRemarkCode") == "Y"
).select(
    F.col("CCLMNO").alias("CCLMRELSTRING"),
    F.col("CCLMEQUENCE").alias("CSVCLINEEQUENCE"),
    F.col("CDOCID").alias("CDOCID"),
    F.col("COPENFLD1").alias("CNOTESCD1")
)

# ---------------------------------------------------------
# Stage: Funnel_25 (PxFunnel)
# ---------------------------------------------------------
df_Funnel_25 = df_Transformer_82_link1.unionByName(df_Transformer_82_DSLink22, allowMissingColumns=True)
df_Funnel_25 = df_Funnel_25.unionByName(df_Transformer_82_DSLink24, allowMissingColumns=True)
df_Funnel_25 = df_Funnel_25.unionByName(df_Copy_of_Transformer_82_svcLn, allowMissingColumns=True)

# Make sure columns are in the correct order
df_Funnel_25 = df_Funnel_25.select(
    "CCLMRELSTRING",
    "CSVCLINEEQUENCE",
    "CDOCID",
    "CNOTESCD1"
)

# ---------------------------------------------------------
# Stage: Lookup_17 (PxLookup) - primary: DSLink84; lookup: lnk_meme_ck
# Join: DSLink84.CNOTESCD1 = lnk_meme_ck.EXCD_ID (inner)
# ---------------------------------------------------------
df_Lookup_17_joined = df_Funnel_25.alias("f").join(
    df_Remove_Duplicates_16.alias("r"),
    on=(F.col("f.CNOTESCD1") == F.col("r.EXCD_ID")),
    how="inner"
)
df_Lookup_17 = df_Lookup_17_joined.select(
    F.col("f.CCLMRELSTRING").alias("CLCL_ID"),
    F.col("f.CSVCLINEEQUENCE").alias("CDML_SEQ_NO"),
    F.col("r.EXCD_ID").alias("CDML_DISALL_EXCD"),
    F.col("r.EXCD_LONG_TEXT1").alias("EXCD_LONG_TEXT1"),
    F.col("r.EXCD_LONG_TEXT2").alias("EXCD_LONG_TEXT2"),
    F.col("f.CDOCID").alias("CDOCID")
)

# ---------------------------------------------------------
# Stage: Transformer_1 (CTransformerStage)
# Constraint: Trim(DSLink7.CDML_DISALL_EXCD) <> ''
# ---------------------------------------------------------
df_Transformer_1_filtered = df_Lookup_17.filter(F.trim(F.col("CDML_DISALL_EXCD")) != "")
df_Transformer_1_DSLink2 = df_Transformer_1_filtered.select(
    F.lit("21").alias("CRCRDTYP"),
    F.lit("06").alias("CRCRDVRSN"),
    F.col("CDOCID").alias("CDOCID"),
    F.col("CDML_DISALL_EXCD").alias("CREMARKCD"),
    (F.trim(F.col("EXCD_LONG_TEXT1")) + F.lit(" ") + F.trim(F.col("EXCD_LONG_TEXT2"))).alias("CREMARKDESC"),
    F.trim(F.col("CLCL_ID")).alias("CCLMNO"),
    F.trim(F.col("CDML_SEQ_NO")).alias("CLNNO"),
    F.lit("").alias("CAMT"),
    F.lit("").alias("CREMARKDESC2"),
    F.lit("").alias("CREMARKCDTYP")
)

# ---------------------------------------------------------
# Stage: Sequential_File_0 (PxSequentialFile) - Write "Zelis_EOBClaimRemarkCdDesc21.dat"
# ---------------------------------------------------------
# Apply rpad for all char columns, then write in the correct order with no header, tab-delimited
df_final = df_Transformer_1_DSLink2 \
    .withColumn("CRCRDTYP", F.rpad(F.col("CRCRDTYP"), 2, " ")) \
    .withColumn("CRCRDVRSN", F.rpad(F.col("CRCRDVRSN"), 2, " ")) \
    .withColumn("CDOCID", F.rpad(F.col("CDOCID"), 25, " ")) \
    .withColumn("CREMARKCD", F.rpad(F.col("CREMARKCD"), 10, " ")) \
    .withColumn("CREMARKDESC", F.rpad(F.col("CREMARKDESC"), 1200, " ")) \
    .withColumn("CCLMNO", F.rpad(F.col("CCLMNO"), 25, " ")) \
    .withColumn("CLNNO", F.rpad(F.col("CLNNO"), 4, " ")) \
    .withColumn("CAMT", F.rpad(F.col("CAMT"), 15, " ")) \
    .withColumn("CREMARKDESC2", F.rpad(F.col("CREMARKDESC2"), 100, " ")) \
    .withColumn("CREMARKCDTYP", F.rpad(F.col("CREMARKCDTYP"), 10, " ")) \
    .select(
        "CRCRDTYP",
        "CRCRDVRSN",
        "CDOCID",
        "CREMARKCD",
        "CREMARKDESC",
        "CCLMNO",
        "CLNNO",
        "CAMT",
        "CREMARKDESC2",
        "CREMARKCDTYP"
    )

write_files(
    df_final,
    f"{adls_path_publish}/external/Zelis_EOBClaimRemarkCdDesc21.dat",
    delimiter="\t",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)