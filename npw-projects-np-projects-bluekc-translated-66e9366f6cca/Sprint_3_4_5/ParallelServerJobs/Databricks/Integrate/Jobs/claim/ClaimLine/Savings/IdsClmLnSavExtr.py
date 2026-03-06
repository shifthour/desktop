# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC CALLED BY:  Fcts claim sequencer
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   This program will read the driver table, gather information from the claim, and create a file ready for the foreign key process.
# MAGIC 
# MAGIC 2;hf_clmlnsav_xref;hf_clm_ln_sav_allcol
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Steph Goddard         4/25/2007       HDMS                   New Program                                                                              devlIDS30                        Brent Leland             05/15/2007                                          
# MAGIC Ralph Tucker           2008-08-14      3657 Primary Key   Changed primary key from hash file to DB2 table                        devlIDS                            Steph Goddard         08/21/2008
# MAGIC 
# MAGIC Shanmugam A 	 2017-03-02         5321                     - Modify Data type on Claim id field from Char to 		 IntegrateDev2            Jag Yelavarthi          2017-03-07
# MAGIC 					          Varchar in ClmLnSavPK Shared Container

# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, first, sum as sum_, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnSavPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
RunID = get_widget_value('RunID','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from IDS (W_CLM_LN_SAV_DRVR and related tables)
extract_query_1 = f"""
SELECT DRVR.CLM_ID,
       DRVR.CLM_LN_SEQ_NO,
       DRVR.CLM_LN_DSALW_TYP_SRC_CD,
       DRVR.CLM_LN_DSALW_SK,
       DRVR.CLM_LN_DSALW_AMT,
       DRVR.RESP_SRC_CD,
       DRVR.CLM_NTWK_STTUS_SRC_CD,
       DRVR.CLM_LN_SK,
       CLM_COB_CD.TRGT_CD AS CLM_COB_CD,
       DRVR.CAP_LN_IN
  FROM {IDSOwner}.W_CLM_LN_SAV_DRVR DRVR,
       {IDSOwner}.CLM CLM,
       {IDSOwner}.CD_MPPNG CLM_COB_CD
 WHERE DRVR.CLM_SK = CLM.CLM_SK
   AND CLM.CLM_COB_CD_SK = CLM_COB_CD.CD_MPPNG_SK
"""
df_IDS_extract_data = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

# Read from IDS (P_CLM_LN_SAV_XREF)
extract_query_2 = f"""
SELECT CLM_LN_DSALW_TYP_CD,
       CLM_NTWK_STTUS_CD,
       EXCD_RESP_CD,
       CLM_LN_DSALW_TYP_NM,
       CLM_LN_DSALW_TYP_CAT_CD,
       CLM_LN_SAV_TYP_CD
  FROM {IDSOwner}.P_CLM_LN_SAV_XREF
"""
df_IDS_Xref = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

# Scenario A replacement for intermediate hashed file "hf_clmlnsav_xref"
# Deduplicate on key columns: CLM_LN_DSALW_TYP_CD, CLM_NTWK_STTUS_CD, EXCD_RESP_CD
df_hf_clmlnsav_xref = df_IDS_Xref.dropDuplicates(
    ["CLM_LN_DSALW_TYP_CD", "CLM_NTWK_STTUS_CD", "EXCD_RESP_CD"]
)

# Join in Transformer "Trans1" (left join)
df_Trans1_in = (
    df_IDS_extract_data.alias("extract_data")
    .join(
        df_hf_clmlnsav_xref.alias("xref_in"),
        (
            (col("extract_data.CLM_LN_DSALW_TYP_SRC_CD") == col("xref_in.CLM_LN_DSALW_TYP_CD"))
            & (col("extract_data.CLM_NTWK_STTUS_SRC_CD") == col("xref_in.CLM_NTWK_STTUS_CD"))
            & (col("extract_data.RESP_SRC_CD") == col("xref_in.EXCD_RESP_CD"))
        ),
        "left"
    )
)

# Define Transformer stage variables and final columns for link "Sum"
df_Trans1_vars = (
    df_Trans1_in
    .withColumn(
        "SavTypCd",
        when(col("xref_in.CLM_LN_SAV_TYP_CD").isNull(), lit("UNK"))
        .otherwise(col("xref_in.CLM_LN_SAV_TYP_CD"))
    )
    .withColumn(
        "CapIn",
        when(
            (col("extract_data.CAP_LN_IN") == lit("Y"))
            & (col("extract_data.CLM_LN_DSALW_TYP_SRC_CD") != lit("CAPFFSRDUCTN")),
            lit("CAP")
        ).otherwise(lit("N"))
    )
    .withColumn(
        "MCare",
        when(
            col("SavTypCd").isin("PROVNGTN", "RC", "MC", "OTHR")
            & col("extract_data.CLM_COB_CD").isin("MCARE", "MCAREAB", "MCAREB"),
            lit("EXTRNLMCARE")
        ).otherwise(lit("N"))
    )
    .withColumn(
        "Commed",
        when(
            col("SavTypCd").isin("PROVNGTN", "RC", "MC", "OTHR")
            & (col("extract_data.CLM_COB_CD") == lit("COMMED")),
            lit("EXTRNLBNF")
        ).otherwise(lit("N"))
    )
)

df_Trans1_Sum = df_Trans1_vars.select(
    col("extract_data.CLM_ID").alias("CLM_ID"),
    col("extract_data.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    when(col("CapIn") != lit("N"), col("CapIn"))
    .otherwise(
        when(col("MCare") != lit("N"), col("MCare"))
        .otherwise(
            when(col("Commed") != lit("N"), col("Commed"))
            .otherwise(col("SavTypCd"))
        )
    ).alias("CLM_LN_DSALW_TYP_SRC_CD"),
    col("extract_data.CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK"),
    col("extract_data.CLM_LN_DSALW_AMT").alias("CLM_LN_DSALW_AMT"),
    col("extract_data.RESP_SRC_CD").alias("RESP_SRC_CD"),
    col("extract_data.CLM_NTWK_STTUS_SRC_CD").alias("CLM_NTWK_STTUS_SRC_CD"),
    col("extract_data.CLM_LN_SK").alias("CLM_LN_SK"),
    col("extract_data.CLM_COB_CD").alias("CLM_COB_CD"),
    col("extract_data.CAP_LN_IN").alias("CAP_LN_IN")
)

# Aggregator_13
df_Aggregator_13 = (
    df_Trans1_Sum
    .groupBy("CLM_ID", "CLM_LN_SEQ_NO", "CLM_LN_DSALW_TYP_SRC_CD")
    .agg(
        first("CLM_LN_DSALW_SK").alias("CLM_LN_DSALW_SK"),
        sum_("CLM_LN_DSALW_AMT").alias("CLM_LN_DSALW_AMT"),
        first("RESP_SRC_CD").alias("RESP_SRC_CD"),
        first("CLM_NTWK_STTUS_SRC_CD").alias("CLM_NTWK_STTUS_SRC_CD"),
        first("CLM_LN_SK").alias("CLM_LN_SK"),
        first("CLM_COB_CD").alias("CLM_COB_CD"),
        first("CAP_LN_IN").alias("CAP_LN_IN")
    )
    .select(
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_DSALW_TYP_SRC_CD",
        "CLM_LN_DSALW_SK",
        "CLM_LN_DSALW_AMT",
        "RESP_SRC_CD",
        "CLM_NTWK_STTUS_SRC_CD",
        "CLM_LN_SK",
        "CLM_COB_CD",
        "CAP_LN_IN"
    )
)

# BusinessRules transformer outputs two links: "AllCol" and "Transform"

df_BusinessRules_AllCol = df_Aggregator_13.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_SRC_CD").alias("CLM_LN_SAV_TYP_CD"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(CurrentDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit("FACETS").alias("SRC_SYS_CD"),
    (
        lit("FACETS;")
        .concat(col("CLM_ID").cast("string"))
        .concat(lit(";"))
        .concat(col("CLM_LN_SEQ_NO").cast("string"))
        .concat(lit(";"))
        .concat(col("CLM_LN_DSALW_TYP_SRC_CD").cast("string"))
    ).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_LN_SAV_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_LN_SK").alias("CLM_LN_SK"),
    col("CLM_LN_DSALW_AMT").alias("SAV_AMT")
)

df_BusinessRules_Transform = df_Aggregator_13.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_LN_DSALW_TYP_SRC_CD").alias("CLM_LN_SAV_TYP_CD")
)

# Call the shared container "ClmLnSavPK"
params_ClmLnSavPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "CurrentDate": CurrentDate,
    "RunID": RunID,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd
}
df_ClmLnSavPK_Key = ClmLnSavPK(df_BusinessRules_Transform, df_BusinessRules_AllCol, params_ClmLnSavPK)

# Final output stage --> CSeqFileStage
df_IdsClmLnSavExtr = df_ClmLnSavPK_Key.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_LN_SAV_SK"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_SAV_TYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_LN_SK"),
    col("SAV_AMT")
)

write_files(
    df_IdsClmLnSavExtr,
    f"{adls_path}/key/IdsClmLnSavExtr.ClmLnSav.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)