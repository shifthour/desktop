# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_3 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 08/30/07 07:19:35 Batch  14487_26382 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 10:06:05 Batch  14484_36371 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 06/21/07 14:03:38 Batch  14417_50622 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 05/25/07 11:20:32 Batch  14390_40859 PROMOTE bckcetl ids20 dsadm rc for steph 
# MAGIC ^1_1 05/25/07 11:11:59 Batch  14390_40332 INIT bckcett testIDS30 dsadm rc for steph 
# MAGIC ^1_1 05/16/07 11:48:45 Batch  14381_42530 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 05/16/07 11:43:58 Batch  14381_42243 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC Â© Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  Fcts claim line savings sequencer
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   This program will determine which disallow type code to pick if there are more than one per claim line with a bypass code of 'Y', then update the driver table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Steph Goddard         4/25/2007       HDMS                   New Program                                                                              devlIDS30                        Brent Leland             05/15/2007

# MAGIC Set all records to the first non-null disallow source code - since the bypass indicator is 'Y' for these records.
# MAGIC This program will calculate override disallow source codes for rows with multiple responsibility source codes
# MAGIC Get the disallow source codes and put them all in one record so the highest pecking order can be determined
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, lit
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameters
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')

# Prepare JDBC connection
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Read "extract_data" link from IDS stage
extract_query_extract_data = """
SELECT DRVR.CLM_ID,
       DRVR.CLM_LN_SEQ_NO,
       DRVR.RESP_SRC_CD,
       DRVR.CLM_LN_DSALW_TYP_SRC_CD
FROM {0}.W_CLM_LN_SAV_DRVR2 SUB,
     {0}.W_CLM_LN_SAV_DRVR DRVR
WHERE SUB.CLM_ID = DRVR.CLM_ID
  AND SUB.CLM_LN_SEQ_NO = DRVR.CLM_LN_SEQ_NO
  AND SUB.RESP_SRC_CD = DRVR.RESP_SRC_CD
""".format(IDSOwner)

df_extract_data = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_extract_data)
    .load()
)

# Read "claims" link from IDS stage
extract_query_claims = """
SELECT CLM_ID,
       CLM_LN_SEQ_NO,
       RESP_SRC_CD
FROM {0}.W_CLM_LN_SAV_DRVR2
""".format(IDSOwner)

df_claims = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_claims)
    .load()
)

# Transformer "trim" (Stage: trim)
df_trim = df_extract_data.select(
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    trim(col("RESP_SRC_CD")).alias("RESP_SRC_CD"),
    trim(col("CLM_LN_DSALW_TYP_SRC_CD")).alias("CLM_LN_DSALW_TYP_SRC_CD")
)

# Hashed File "hf_clmlnsavdrvr2_input" (Scenario A: Intermediate hashed file).
# Deduplicate on key columns (CLM_ID, CLM_LN_SEQ_NO, RESP_SRC_CD, CLM_LN_DSALW_TYP_SRC_CD)
df_hf_clmlnsavdrvr2_input = df_trim.dropDuplicates(["CLM_ID","CLM_LN_SEQ_NO","RESP_SRC_CD","CLM_LN_DSALW_TYP_SRC_CD"])

# BusinessRules Transformer
# Primary link is df_claims. We must do 22 left joins on df_hf_clmlnsavdrvr2_input,
# each filtered by a specific value of CLM_LN_DSALW_TYP_SRC_CD.

df_XE = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("XE")).alias("XE")
df_LT = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("LT")).alias("LT")
df_DC = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("DC")).alias("DC")
df_DP = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("DP")).alias("DP")
df_CAP = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("CAP")).alias("CAP")
df_CE = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("CE")).alias("CE")
df_PN = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("PN")).alias("PN")
df_SD = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("SD")).alias("SD")
df_UM = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("UM")).alias("UM")
df_DS = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("DS")).alias("DS")
df_RW = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("RW")).alias("RW")
df_SE = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("SE")).alias("SE")
df_PI = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("PI")).alias("PI")
df_SP = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("SP")).alias("SP")
df_DX = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("DX")).alias("DX")
df_AX = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("AX")).alias("AX")
df_ATAU = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("ATAU")).alias("ATAU")
df_DA = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("DA")).alias("DA")
df_AA = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("AA")).alias("AA")
df_DTDU = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("DTDU")).alias("DTDU")
df_PC = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("PC")).alias("PC")
df_XX = df_hf_clmlnsavdrvr2_input.filter(col("CLM_LN_DSALW_TYP_SRC_CD") == lit("XX")).alias("XX")

# Chain the left joins
df_businessrules = (
    df_claims.alias("claims")
    .join(df_XE, [trim(col("claims.CLM_ID")) == col("XE.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("XE.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("XE.RESP_SRC_CD")], "left")
    .join(df_LT, [trim(col("claims.CLM_ID")) == col("LT.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("LT.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("LT.RESP_SRC_CD")], "left")
    .join(df_DC, [trim(col("claims.CLM_ID")) == col("DC.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("DC.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("DC.RESP_SRC_CD")], "left")
    .join(df_DP, [trim(col("claims.CLM_ID")) == col("DP.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("DP.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("DP.RESP_SRC_CD")], "left")
    .join(df_CAP, [trim(col("claims.CLM_ID")) == col("CAP.CLM_ID"),
                   trim(col("claims.CLM_LN_SEQ_NO")) == col("CAP.CLM_LN_SEQ_NO"),
                   trim(col("claims.RESP_SRC_CD")) == col("CAP.RESP_SRC_CD")], "left")
    .join(df_CE, [trim(col("claims.CLM_ID")) == col("CE.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("CE.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("CE.RESP_SRC_CD")], "left")
    .join(df_PN, [trim(col("claims.CLM_ID")) == col("PN.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("PN.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("PN.RESP_SRC_CD")], "left")
    .join(df_SD, [trim(col("claims.CLM_ID")) == col("SD.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("SD.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("SD.RESP_SRC_CD")], "left")
    .join(df_UM, [trim(col("claims.CLM_ID")) == col("UM.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("UM.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("UM.RESP_SRC_CD")], "left")
    .join(df_DS, [trim(col("claims.CLM_ID")) == col("DS.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("DS.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("DS.RESP_SRC_CD")], "left")
    .join(df_RW, [trim(col("claims.CLM_ID")) == col("RW.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("RW.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("RW.RESP_SRC_CD")], "left")
    .join(df_SE, [trim(col("claims.CLM_ID")) == col("SE.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("SE.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("SE.RESP_SRC_CD")], "left")
    .join(df_PI, [trim(col("claims.CLM_ID")) == col("PI.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("PI.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("PI.RESP_SRC_CD")], "left")
    .join(df_SP, [trim(col("claims.CLM_ID")) == col("SP.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("SP.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("SP.RESP_SRC_CD")], "left")
    .join(df_DX, [trim(col("claims.CLM_ID")) == col("DX.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("DX.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("DX.RESP_SRC_CD")], "left")
    .join(df_AX, [trim(col("claims.CLM_ID")) == col("AX.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("AX.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("AX.RESP_SRC_CD")], "left")
    .join(df_ATAU, [trim(col("claims.CLM_ID")) == col("ATAU.CLM_ID"),
                    trim(col("claims.CLM_LN_SEQ_NO")) == col("ATAU.CLM_LN_SEQ_NO"),
                    trim(col("claims.RESP_SRC_CD")) == col("ATAU.RESP_SRC_CD")], "left")
    .join(df_DA, [trim(col("claims.CLM_ID")) == col("DA.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("DA.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("DA.RESP_SRC_CD")], "left")
    .join(df_AA, [trim(col("claims.CLM_ID")) == col("AA.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("AA.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("AA.RESP_SRC_CD")], "left")
    .join(df_DTDU, [trim(col("claims.CLM_ID")) == col("DTDU.CLM_ID"),
                    trim(col("claims.CLM_LN_SEQ_NO")) == col("DTDU.CLM_LN_SEQ_NO"),
                    trim(col("claims.RESP_SRC_CD")) == col("DTDU.RESP_SRC_CD")], "left")
    .join(df_PC, [trim(col("claims.CLM_ID")) == col("PC.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("PC.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("PC.RESP_SRC_CD")], "left")
    .join(df_XX, [trim(col("claims.CLM_ID")) == col("XX.CLM_ID"),
                  trim(col("claims.CLM_LN_SEQ_NO")) == col("XX.CLM_LN_SEQ_NO"),
                  trim(col("claims.RESP_SRC_CD")) == col("XX.RESP_SRC_CD")], "left")
    .select(
        col("claims.CLM_ID").alias("CLM_ID"),
        col("claims.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("claims.RESP_SRC_CD").alias("RESP_SRC_CD"),
        when(col("XE.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("XE.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_XE"),
        when(col("LT.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("LT.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_LT"),
        when(col("DC.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("DC.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_DC"),
        when(col("DP.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("DP.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_DP"),
        when(col("CAP.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("CAP.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_CAP"),
        when(col("CE.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("CE.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_CE"),
        when(col("PN.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("PN.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_PN"),
        when(col("SD.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("SD.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_SD"),
        when(col("UM.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("UM.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_UM"),
        when(col("DS.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("DS.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_DS"),
        when(col("RW.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("RW.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_RW"),
        when(col("SE.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("SE.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_SE"),
        when(col("PI.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("PI.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_PI"),
        when(col("SP.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("SP.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_SP"),
        when(col("DX.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("DX.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_DX"),
        when(col("AX.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("AX.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_AX"),
        when(col("ATAU.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("ATAU.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_ATAU"),
        when(col("DA.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("DA.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_DA"),
        when(col("AA.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("AA.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_AA"),
        when(col("DTDU.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("DTDU.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_DTDU"),
        when(col("PC.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("PC.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_PC"),
        when(col("XX.CLM_LN_DSALW_TYP_SRC_CD").isNull(), None).otherwise(col("XX.CLM_LN_DSALW_TYP_SRC_CD")).alias("SRC_XX")
    )
)

# Next hashed file "hf_clmlnsavdrvr2_temp" also scenario A => deduplicate on key columns CLM_ID, CLM_LN_SEQ_NO
df_hf_clmlnsavdrvr2_temp = df_businessrules.dropDuplicates(["CLM_ID","CLM_LN_SEQ_NO"])

# Stage "CalcSource" => transform the data for output
df_calcsource = df_hf_clmlnsavdrvr2_temp.select(
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    when(
        col("SRC_XE").isNotNull(), col("SRC_XE")
    ).when(
        col("SRC_LT").isNotNull(), col("SRC_LT")
    ).when(
        col("SRC_DC").isNotNull(), col("SRC_DC")
    ).when(
        col("SRC_DP").isNotNull(), col("SRC_DP")
    ).when(
        col("SRC_CAP").isNotNull(), col("SRC_CAP")
    ).when(
        col("SRC_CE").isNotNull(), col("SRC_CE")
    ).when(
        col("SRC_PN").isNotNull(), col("SRC_PN")
    ).when(
        col("SRC_SD").isNotNull(), col("SRC_SD")
    ).when(
        col("SRC_UM").isNotNull(), col("SRC_UM")
    ).when(
        col("SRC_DS").isNotNull(), col("SRC_DS")
    ).when(
        col("SRC_RW").isNotNull(), col("SRC_RW")
    ).when(
        col("SRC_SE").isNotNull(), col("SRC_SE")
    ).when(
        col("SRC_PI").isNotNull(), col("SRC_PI")
    ).when(
        col("SRC_SP").isNotNull(), col("SRC_SP")
    ).when(
        col("SRC_DX").isNotNull(), col("SRC_DX")
    ).when(
        col("SRC_AX").isNotNull(), col("SRC_AX")
    ).when(
        col("SRC_ATAU").isNotNull(), col("SRC_ATAU")
    ).when(
        col("SRC_DA").isNotNull(), col("SRC_DA")
    ).when(
        col("SRC_AA").isNotNull(), col("SRC_AA")
    ).when(
        col("SRC_DTDU").isNotNull(), col("SRC_DTDU")
    ).when(
        col("SRC_PC").isNotNull(), col("SRC_PC")
    ).when(
        col("SRC_XX").isNotNull(), col("SRC_XX")
    ).otherwise(lit("XX")).alias("CLM_LN_DSALW_TYP_SRC_CD"),
    col("RESP_SRC_CD").alias("RESP_SRC_CD")
)

# Final stage "CLM_LN_SAV_DRVR" => merge into #$IDSOwner#.W_CLM_LN_SAV_DRVR
# Columns: CLM_ID, CLM_LN_SEQ_NO, CLM_LN_DSALW_TYP_SRC_CD, RESP_SRC_CD
# In the final output, apply rpad for char/varchar columns as required (lengths from the final table design).
df_enriched = df_calcsource.select(
    # CLM_ID is a varchar(20)
    rpad(col("CLM_ID"), 20, " ").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").cast(IntegerType()).alias("CLM_LN_SEQ_NO"),
    # CLM_LN_DSALW_TYP_SRC_CD is a varchar(20)
    rpad(col("CLM_LN_DSALW_TYP_SRC_CD"), 20, " ").alias("CLM_LN_DSALW_TYP_SRC_CD"),
    # RESP_SRC_CD is a varchar(20)
    rpad(col("RESP_SRC_CD"), 20, " ").alias("RESP_SRC_CD")
)

# Write df_enriched to a physical table in STAGING, then merge into {IDSOwner}.W_CLM_LN_SAV_DRVR
temp_table_name = "STAGING.IdsClmLnSavDrvr2Extr_CLM_LN_SAV_DRVR_temp"

execute_dml("DROP TABLE IF EXISTS {}".format(temp_table_name), jdbc_url_ids, jdbc_props_ids)

(
    df_enriched.write.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", temp_table_name)
    .mode("append")
    .save()
)

merge_sql = (
    f"MERGE {IDSOwner}.W_CLM_LN_SAV_DRVR AS T "
    f"USING {temp_table_name} AS S "
    f"ON (T.CLM_ID = S.CLM_ID AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO) "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.CLM_LN_DSALW_TYP_SRC_CD = S.CLM_LN_DSALW_TYP_SRC_CD, "
    f"T.RESP_SRC_CD = S.RESP_SRC_CD "
    f"WHEN NOT MATCHED THEN INSERT (CLM_ID, CLM_LN_SEQ_NO, CLM_LN_DSALW_TYP_SRC_CD, RESP_SRC_CD) "
    f"VALUES (S.CLM_ID, S.CLM_LN_SEQ_NO, S.CLM_LN_DSALW_TYP_SRC_CD, S.RESP_SRC_CD);"
)

execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)