# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME: LhoFctsClmLnShdwAdjExtr
# MAGIC 
# MAGIC PROCESSING:   Extracts data from Facets Shadow table and lookups CLM_LN_SK in CLM_LN table. 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi         2015-09-04       5382                       Original Programming                                                                                                                                                           Kalyan Neelam          2015-09-22
# MAGIC Raja Gummadi         2015-11-18       5382                       Added NULL handling to all fields, added driver table                                                                 IntegrateDev2                  Kalyan Neelam          2015-11-19
# MAGIC Christen Marshall     2020-08-12       262830                   Cloned for historical one time load for LHO / MA insourcing                                                        IntegrateDev2
# MAGIC Prabhu ES               2022-03-29       S2S                        MSSQL ODBC conn params added                                                                                            IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC Primary key is from CLM_LN table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, length, rpad, trim, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameters
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunID = get_widget_value('RunID','')
Logging = get_widget_value('Logging','N')
RunCycle = get_widget_value('RunCycle','')
CurrDate = get_widget_value('CurrDate','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
DriverTable = get_widget_value('DriverTable','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')
# Because the ODBC stage references #$LhoFacetsOwner#, define it as well:
LhoFacetsOwner = get_widget_value('LhoFacetsOwner','')
# Also define the secret name for LhoFacetsOwner
lhofacets_secret_name = get_widget_value('lhofacets_secret_name','')

# --------------------------------------------------------------------------------
# Stage: K_CLM_LN (DB2Connector) - read from IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_ids = f"SELECT SRC_SYS_CD_SK, CLM_ID, CLM_LN_SEQ_NO, CLM_LN_SK FROM {IDSOwner}.K_CLM_LN"
df_K_CLM_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_ids)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: CMC_CDSC_MED (ODBCConnector)
jdbc_url2, jdbc_props2 = get_db_config(lhofacets_secret_name)
extract_query_odbc = (
    f"SELECT "
    f"CMC_CDSC_SHAD_MED.CLCL_ID,"
    f"CMC_CDSC_SHAD_MED.CDML_SEQ_NO,"
    f"CMC_CDSC_SHAD_MED.MEME_CK,"
    f"CMC_CDSC_SHAD_MED.CLCS_STS,"
    f"CMC_CDSC_SHAD_MED.SEPY_SHDW_PFX_NVL,"
    f"CMC_CDSC_SHAD_MED.SESE_RULE,"
    f"CMC_CDSC_SHAD_MED.CDSC_ALT_RULE_IND,"
    f"CMC_CDSC_SHAD_MED.DEDE_SHDW_PFX_NVL,"
    f"CMC_CDSC_SHAD_MED.LTLT_SHDW_PFX_NVL,"
    f"CMC_CDSC_SHAD_MED.CDML_CONSIDER_CHG,"
    f"CMC_CDSC_SHAD_MED.CDSC_PRICE_ALLOW,"
    f"CMC_CDSC_SHAD_MED.CDSC_PRICE_UNITS,"
    f"CMC_CDSC_SHAD_MED.CDSC_ALLOW,"
    f"CMC_CDSC_SHAD_MED.CDSC_UNITS_ALLOW,"
    f"CMC_CDSC_SHAD_MED.CDSC_DED_AMT,"
    f"CMC_CDSC_SHAD_MED.CDSC_DED_ACC_NO,"
    f"CMC_CDSC_SHAD_MED.CDSC_COPAY_AMT,"
    f"CMC_CDSC_SHAD_MED.CDSC_COINS_AMT,"
    f"CMC_CDSC_SHAD_MED.CDSC_PAID_AMT,"
    f"CMC_CDSC_SHAD_MED.CDSC_DISALL_AMT,"
    f"CMC_CDSC_SHAD_MED.CDSC_DISALL_EXCD,"
    f"CMC_CDSC_SHAD_MED.CDSC_LOCK_TOKEN,"
    f"CMC_CDSC_SHAD_MED.ATXR_SOURCE_ID,"
    f"CMC_CDSC_SHAD_MED.SYS_LAST_UPD_DTM,"
    f"CMC_CDSC_SHAD_MED.SYS_USUS_ID,"
    f"CMC_CDSC_SHAD_MED.SYS_DBUSER_ID "
    f"FROM {LhoFacetsOwner}.CMC_CDSC_SHAD_MED CMC_CDSC_SHAD_MED, tempdb..{DriverTable} TMP "
    f"WHERE TMP.CLM_ID = CMC_CDSC_SHAD_MED.CLCL_ID"
)
df_CMC_CDSC_MED = (
    spark.read.format("jdbc")
    .option("url", jdbc_url2)
    .options(**jdbc_props2)
    .option("query", extract_query_odbc)
    .load()
)

# Add job parameters as columns in the primary link DataFrame
df_in = (
    df_CMC_CDSC_MED
    .withColumn("SrcSysCdSk", lit(SrcSysCdSk))
    .withColumn("SrcSysCd", lit(SrcSysCd))
    .withColumn("CurrDate", lit(CurrDate))
    .withColumn("RunCycle", lit(RunCycle))
)

# --------------------------------------------------------------------------------
# Stage: Transformer_0
# Join: (Left join "df_in" with "df_K_CLM_LN" on the given condition)
df_joined = df_in.alias("in").join(
    df_K_CLM_LN.alias("ClmLn"),
    (
        (col("in.SrcSysCdSk") == col("ClmLn.SRC_SYS_CD_SK")) &
        (trim(col("in.CLCL_ID")) == col("ClmLn.CLM_ID")) &
        (trim(col("in.CDML_SEQ_NO")) == col("ClmLn.CLM_LN_SEQ_NO"))
    ),
    how="left"
)

# Filter for output link "Key" using constraint: svClmLnSk <> 0 AND IsNull(svClmLnSk) = @FALSE
df_Key = df_joined.filter((col("ClmLn.CLM_LN_SK") != 0) & col("ClmLn.CLM_LN_SK").isNotNull())

# Build columns for "KeyFile" output
df_Key = (
    df_Key
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", rpad(lit("I"), 10, " "))
    .withColumn("DISCARD_IN", rpad(lit("N"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(lit("Y"), 1, " "))
    .withColumn("FIRST_RECYC_DT", col("CurrDate"))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", col("SrcSysCd"))
    .withColumn(
        "PRI_KEY_STRING",
        concat(trim(col("CLCL_ID")), trim(col("CDML_SEQ_NO")), col("SrcSysCd"))
    )
    .withColumn("CLM_LN_SK", col("ClmLn.CLM_LN_SK"))
    .withColumn("CLM_ID", trim(col("CLCL_ID")))
    .withColumn("CLM_LN_SEQ_NO", col("CDML_SEQ_NO"))
    .withColumn("SRC_SYS_CD_SK", col("SrcSysCdSk"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", col("RunCycle"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", col("RunCycle"))
    .withColumn(
        "CLM_LN_SHADOW_ADJDCT_DSALW_EXCD",
        when(
            (col("CDSC_DISALL_EXCD").isNull()) | (length(trim(col("CDSC_DISALL_EXCD"))) == 0),
            lit("NA")
        ).otherwise(trim(col("CDSC_DISALL_EXCD")))
    )
    .withColumn(
        "CLM_LN_SHADOW_ADJDCT_STTUS_CD",
        when(
            (col("CLCS_STS").isNull()) | (length(trim(col("CLCS_STS"))) == 0),
            lit("NA")
        ).otherwise(trim(col("CLCS_STS")))
    )
    .withColumn(
        "SHADOW_MED_UTIL_EDIT_IN",
        rpad(
            when(
                (col("CDSC_ALT_RULE_IND").isNull()) | (length(trim(col("CDSC_ALT_RULE_IND"))) == 0),
                lit(" ")
            ).otherwise(trim(col("CDSC_ALT_RULE_IND"))),
            1, " "
        )
    )
    .withColumn(
        "CNSD_CHRG_AMT",
        when(
            (col("CDML_CONSIDER_CHG").isNull()) | (length(trim(col("CDML_CONSIDER_CHG"))) == 0),
            lit(0)
        ).otherwise(trim(col("CDML_CONSIDER_CHG")))
    )
    .withColumn(
        "INIT_ADJDCT_ALW_AMT",
        when(
            (col("CDSC_PRICE_ALLOW").isNull()) | (length(trim(col("CDSC_PRICE_ALLOW"))) == 0),
            lit(0)
        ).otherwise(trim(col("CDSC_PRICE_ALLOW")))
    )
    .withColumn(
        "SHADOW_ADJDCT_ALW_AMT",
        when(
            (col("CDSC_ALLOW").isNull()) | (length(trim(col("CDSC_ALLOW"))) == 0),
            lit(0)
        ).otherwise(trim(col("CDSC_ALLOW")))
    )
    .withColumn(
        "SHADOW_ADJDCT_COINS_AMT",
        when(
            (col("CDSC_COINS_AMT").isNull()) | (length(trim(col("CDSC_COINS_AMT"))) == 0),
            lit(0)
        ).otherwise(trim(col("CDSC_COINS_AMT")))
    )
    .withColumn(
        "SHADOW_ADJDCT_COPAY_AMT",
        when(
            (col("CDSC_COPAY_AMT").isNull()) | (length(trim(col("CDSC_COPAY_AMT"))) == 0),
            lit(0)
        ).otherwise(trim(col("CDSC_COPAY_AMT")))
    )
    .withColumn(
        "SHADOW_ADJDCT_DEDCT_AMT",
        when(
            (col("CDSC_DED_AMT").isNull()) | (length(trim(col("CDSC_DED_AMT"))) == 0),
            lit(0)
        ).otherwise(trim(col("CDSC_DED_AMT")))
    )
    .withColumn(
        "SHADOW_ADJDCT_DSALW_AMT",
        when(
            (col("CDSC_DISALL_AMT").isNull()) | (length(trim(col("CDSC_DISALL_AMT"))) == 0),
            lit(0)
        ).otherwise(trim(col("CDSC_DISALL_AMT")))
    )
    .withColumn(
        "SHADOW_ADJDCT_PAYBL_AMT",
        when(
            (col("CDSC_PAID_AMT").isNull()) | (length(trim(col("CDSC_PAID_AMT"))) == 0),
            lit(0)
        ).otherwise(trim(col("CDSC_PAID_AMT")))
    )
    .withColumn(
        "INIT_ADJDCT_ALW_PRICE_UNIT_CT",
        when(
            (col("CDSC_PRICE_UNITS").isNull()) | (length(trim(col("CDSC_PRICE_UNITS"))) == 0),
            lit(1)
        ).otherwise(trim(col("CDSC_PRICE_UNITS")))
    )
    .withColumn(
        "SHADOW_ADJDCT_ALW_PRICE_UNIT_CT",
        when(
            (col("CDSC_UNITS_ALLOW").isNull()) | (length(trim(col("CDSC_UNITS_ALLOW"))) == 1),
            lit(0)
        ).otherwise(trim(col("CDSC_UNITS_ALLOW")))
    )
    .withColumn(
        "SHADOW_DEDCT_AMT_ACCUM_ID",
        when(
            (col("CDSC_DED_ACC_NO").isNull()) | (length(trim(col("CDSC_DED_ACC_NO"))) == 0),
            lit("NA")
        ).otherwise(trim(col("CDSC_DED_ACC_NO")))
    )
    .withColumn(
        "SHADOW_LMT_PFX_ID",
        when(
            (col("LTLT_SHDW_PFX_NVL").isNull()) | (length(trim(col("LTLT_SHDW_PFX_NVL"))) == 0),
            lit("NA")
        ).otherwise(trim(col("LTLT_SHDW_PFX_NVL")))
    )
    .withColumn(
        "SHADOW_PROD_CMPNT_DEDCT_PFX_ID",
        when(
            (col("DEDE_SHDW_PFX_NVL").isNull()) | (length(trim(col("DEDE_SHDW_PFX_NVL"))) == 0),
            lit("NA")
        ).otherwise(trim(col("DEDE_SHDW_PFX_NVL")))
    )
    .withColumn(
        "SHADOW_PROD_CMPNT_SVC_PAYMT_ID",
        when(
            (col("SEPY_SHDW_PFX_NVL").isNull()) | (length(trim(col("SEPY_SHDW_PFX_NVL"))) == 0),
            lit("NA")
        ).otherwise(trim(col("SEPY_SHDW_PFX_NVL")))
    )
    .withColumn(
        "SHADOW_SVC_RULE_TYP_TX",
        when(
            (col("SESE_RULE").isNull()) | (length(trim(col("SESE_RULE"))) == 0),
            lit("NA")
        ).otherwise(trim(col("SESE_RULE")))
    )
)

# Select columns in the exact order defined in DataStage
df_Key = df_Key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LN_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_SHADOW_ADJDCT_DSALW_EXCD",
    "CLM_LN_SHADOW_ADJDCT_STTUS_CD",
    "SHADOW_MED_UTIL_EDIT_IN",
    "CNSD_CHRG_AMT",
    "INIT_ADJDCT_ALW_AMT",
    "SHADOW_ADJDCT_ALW_AMT",
    "SHADOW_ADJDCT_COINS_AMT",
    "SHADOW_ADJDCT_COPAY_AMT",
    "SHADOW_ADJDCT_DEDCT_AMT",
    "SHADOW_ADJDCT_DSALW_AMT",
    "SHADOW_ADJDCT_PAYBL_AMT",
    "INIT_ADJDCT_ALW_PRICE_UNIT_CT",
    "SHADOW_ADJDCT_ALW_PRICE_UNIT_CT",
    "SHADOW_DEDCT_AMT_ACCUM_ID",
    "SHADOW_LMT_PFX_ID",
    "SHADOW_PROD_CMPNT_DEDCT_PFX_ID",
    "SHADOW_PROD_CMPNT_SVC_PAYMT_ID",
    "SHADOW_SVC_RULE_TYP_TX"
)

# --------------------------------------------------------------------------------
# Stage: KeyFile (CSeqFileStage) - write out
out_file_path = f"{adls_path}/key/LhoFctsClmLnShdwAdjExtr.LhoFctsClmLnShdwAdj.dat.{RunID}"
write_files(
    df_Key,
    out_file_path,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)