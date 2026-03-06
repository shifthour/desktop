# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ******************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: IdsAplCntl
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING: This job extracts data from IDS APL_LVL_STTUS and creats Load file for APL_LVL_STTUS_D table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                  Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                           ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty                08/02/2007                                              Originally Programmed                                                               devlEDW10              
# MAGIC 
# MAGIC Pooja Sunkara                  11/05/2013      5114                                Rewrite in Parallel                                                                      EnterpriseWrhsDevl  Peter Marshall             1/14/2014        
# MAGIC 
# MAGIC Ravi Singh                       12/07/2018      MTM- 5841                      Added in parameter and soruce query for extract NDBH,            EnterpriseDev2          Kalyan Neelam            2018-12-10       
# MAGIC                                                                                                          Evicore(MEDSLTNS) and Telligen Appeal process data

# MAGIC AppUser Lookup
# MAGIC CD_MPPNG Lookup
# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK and LAST_UPDT_RUN_CYC_EXCTN_DT_SK
# MAGIC Job name:
# MAGIC IdsEdwAplLvlSttusDExtr
# MAGIC EDW APL_LVL_STTUS_D Extract
# MAGIC Write APL_LVL_STTUS_D Data into a Sequential file for Load Ready Job.
# MAGIC Read data from source table APL_LVL_STTUS
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameter retrieval
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
MedSltnsExtractRunCycle = get_widget_value('MedSltnsExtractRunCycle','')
NdbhExtractRunCycle = get_widget_value('NdbhExtractRunCycle','')
TelligenExtractRunCycle = get_widget_value('TelligenExtractRunCycle','')

# Stage: db2_APL_LVL_STTUS_in (DB2ConnectorPX)
jdbc_url_db2_APL_LVL_STTUS_in, jdbc_props_db2_APL_LVL_STTUS_in = get_db_config(ids_secret_name)
query_db2_APL_LVL_STTUS_in = (
    f"SELECT APL_LVL_STTUS_SK, SRC_SYS_CD_SK, APL_ID, APL_LVL_SEQ_NO, SEQ_NO, APL_LVL_SK, "
    f"RTE_USER_SK, STTUS_USER_SK, APL_LVL_STTUS_CD_SK, APL_LVL_STTUS_RSN_CD_SK, STTUS_DTM "
    f"FROM {IDSOwner}.APL_LVL_STTUS STTUS "
    f"LEFT JOIN {IDSOwner}.CD_MPPNG CD "
    f"ON STTUS.SRC_SYS_CD_SK = CD.CD_MPPNG_SK "
    f"WHERE (CD.TRGT_CD = 'FACETS' AND STTUS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}) "
    f"OR (CD.TRGT_CD = 'MEDSLTNS' AND STTUS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MedSltnsExtractRunCycle}) "
    f"OR (CD.TRGT_CD = 'NDBH' AND STTUS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NdbhExtractRunCycle}) "
    f"OR (CD.TRGT_CD = 'TELLIGEN' AND STTUS.LAST_UPDT_RUN_CYC_EXCTN_SK >= {TelligenExtractRunCycle})"
)
df_db2_APL_LVL_STTUS_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_APL_LVL_STTUS_in)
    .options(**jdbc_props_db2_APL_LVL_STTUS_in)
    .option("query", query_db2_APL_LVL_STTUS_in)
    .load()
)

# Stage: db2_CD_MPPNG_in (DB2ConnectorPX)
jdbc_url_db2_CD_MPPNG_in, jdbc_props_db2_CD_MPPNG_in = get_db_config(ids_secret_name)
query_db2_CD_MPPNG_in = f"SELECT CD_MPPNG_SK, TRGT_CD, TRGT_CD_NM FROM {IDSOwner}.CD_MPPNG"
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG_in)
    .options(**jdbc_props_db2_CD_MPPNG_in)
    .option("query", query_db2_CD_MPPNG_in)
    .load()
)

# Stage: Cpy_CdMppng (PxCopy)
df_ref_AplLvlSttusCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_ref_SrcSysCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_ref_AplLvlSttusRsnCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# Stage: db2_APP_USER_in (DB2ConnectorPX)
jdbc_url_db2_APP_USER_in, jdbc_props_db2_APP_USER_in = get_db_config(ids_secret_name)
query_db2_APP_USER_in = f"SELECT USER_SK, USER_ID FROM {IDSOwner}.APP_USER APP"
df_db2_APP_USER_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_APP_USER_in)
    .options(**jdbc_props_db2_APP_USER_in)
    .option("query", query_db2_APP_USER_in)
    .load()
)

# Stage: Cpy_AppUser (PxCopy)
df_ref_RteUserId = df_db2_APP_USER_in.select(
    F.col("USER_SK").alias("USER_SK"),
    F.col("USER_ID").alias("USER_ID")
)
df_ref_SttusUserId = df_db2_APP_USER_in.select(
    F.col("USER_SK").alias("USER_SK"),
    F.col("USER_ID").alias("USER_ID")
)

# Stage: Lkp_Codes (PxLookup)
df_Lkp_joined = (
    df_db2_APL_LVL_STTUS_in.alias("Ink_IdsEdwAplLvlSttusDExtr_inABC")
    .join(
        df_ref_SrcSysCd.alias("ref_SrcSysCd"),
        F.col("Ink_IdsEdwAplLvlSttusDExtr_inABC.SRC_SYS_CD_SK") == F.col("ref_SrcSysCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_ref_AplLvlSttusCd.alias("ref_AplLvlSttusCd"),
        F.col("Ink_IdsEdwAplLvlSttusDExtr_inABC.APL_LVL_STTUS_CD_SK") == F.col("ref_AplLvlSttusCd.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_ref_RteUserId.alias("ref_RteUserId"),
        F.col("Ink_IdsEdwAplLvlSttusDExtr_inABC.RTE_USER_SK") == F.col("ref_RteUserId.USER_SK"),
        "left"
    )
    .join(
        df_ref_SttusUserId.alias("ref_SttusUserId"),
        F.col("Ink_IdsEdwAplLvlSttusDExtr_inABC.STTUS_USER_SK") == F.col("ref_SttusUserId.USER_SK"),
        "left"
    )
    .join(
        df_ref_AplLvlSttusRsnCd.alias("ref_AplLvlSttusRsnCd"),
        F.col("Ink_IdsEdwAplLvlSttusDExtr_inABC.APL_LVL_STTUS_RSN_CD_SK") == F.col("ref_AplLvlSttusRsnCd.CD_MPPNG_SK"),
        "left"
    )
)
df_lnk_IdsEdwAplLvlSttusDExtr_lkpout = df_Lkp_joined.select(
    F.col("Ink_IdsEdwAplLvlSttusDExtr_inABC.APL_LVL_STTUS_SK").alias("APL_LVL_STTUS_SK"),
    F.col("Ink_IdsEdwAplLvlSttusDExtr_inABC.APL_ID").alias("APL_ID"),
    F.col("Ink_IdsEdwAplLvlSttusDExtr_inABC.APL_LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
    F.col("Ink_IdsEdwAplLvlSttusDExtr_inABC.SEQ_NO").alias("SEQ_NO"),
    F.col("Ink_IdsEdwAplLvlSttusDExtr_inABC.APL_LVL_SK").alias("APL_LVL_SK"),
    F.col("Ink_IdsEdwAplLvlSttusDExtr_inABC.RTE_USER_SK").alias("RTE_USER_SK"),
    F.col("Ink_IdsEdwAplLvlSttusDExtr_inABC.STTUS_USER_SK").alias("STTUS_USER_SK"),
    F.col("Ink_IdsEdwAplLvlSttusDExtr_inABC.APL_LVL_STTUS_CD_SK").alias("APL_LVL_STTUS_CD_SK"),
    F.col("Ink_IdsEdwAplLvlSttusDExtr_inABC.APL_LVL_STTUS_RSN_CD_SK").alias("APL_LVL_STTUS_RSN_CD_SK"),
    F.col("Ink_IdsEdwAplLvlSttusDExtr_inABC.STTUS_DTM").alias("STTUS_DTM"),
    F.col("ref_SrcSysCd.TRGT_CD").alias("SRC_SYS_CD"),
    F.col("ref_AplLvlSttusCd.TRGT_CD").alias("APL_LVL_STTUS_CD"),
    F.col("ref_AplLvlSttusCd.TRGT_CD_NM").alias("APL_LVL_STTUS_NM"),
    F.col("ref_RteUserId.USER_ID").alias("RTE_USER_ID"),
    F.col("ref_SttusUserId.USER_ID").alias("STTUS_USER_ID"),
    F.col("ref_AplLvlSttusRsnCd.TRGT_CD").alias("APL_LVL_STTUS_RSN_CD"),
    F.col("ref_AplLvlSttusRsnCd.TRGT_CD_NM").alias("APL_LVL_STTUS_RSN_NM")
)

# Stage: xfrm_BusinessLogic (CTransformerStage) - Output link "Ink_IdsEdwAplLvlSttusDExtr_outMain"
df_xfrm_outMain_filtered = df_lnk_IdsEdwAplLvlSttusDExtr_lkpout.filter(
    (F.col("APL_LVL_STTUS_CD_SK") != 0) & (F.col("APL_LVL_STTUS_CD_SK") != 1)
)
df_xfrm_outMain = (
    df_xfrm_outMain_filtered
    .withColumn("APL_LVL_STTUS_CD", F.when(F.col("APL_LVL_STTUS_CD").isNotNull(), F.col("APL_LVL_STTUS_CD")).otherwise(F.lit("NA")))
    .withColumn("APL_LVL_STTUS_NM", F.when(F.col("APL_LVL_STTUS_NM").isNotNull(), F.col("APL_LVL_STTUS_NM")).otherwise(F.lit("NA")))
    .withColumn("APL_LVL_STTUS_RSN_CD", F.when(F.col("APL_LVL_STTUS_RSN_CD").isNotNull(), F.col("APL_LVL_STTUS_RSN_CD")).otherwise(F.lit("NA")))
    .withColumn("APL_LVL_STTUS_RSN_NM", F.when(F.col("APL_LVL_STTUS_RSN_NM").isNotNull(), F.col("APL_LVL_STTUS_RSN_NM")).otherwise(F.lit("NA")))
    .withColumn("APL_LVL_STTUS_DT_SK", F.col("STTUS_DTM").substr(1, 10))
    .withColumn("RTE_USER_ID", F.when(F.col("RTE_USER_ID").isNotNull(), F.col("RTE_USER_ID")).otherwise(F.lit("NA")))
    .withColumn("STTUS_USER_ID", F.when(F.col("STTUS_USER_ID").isNotNull(), F.col("STTUS_USER_ID")).otherwise(F.lit("NA")))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
)

# Stage: xfrm_BusinessLogic (CTransformerStage) - Output link "NALink"
df_xfrm_NALink = spark.createDataFrame(
    [
        (
            1,        # APL_LVL_STTUS_SK
            'NA',     # SRC_SYS_CD
            'NA',     # APL_ID
            0,        # APL_LVL_SEQ_NO
            0,        # APL_LVL_STTUS_SEQ_NO
            '1753-01-01',  # CRT_RUN_CYC_EXCTN_DT_SK
            '1753-01-01',  # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
            1,        # APL_LVL_SK
            1,        # RTE_USER_SK
            1,        # STTUS_USER_SK
            'NA',     # APL_LVL_STTUS_CD
            'NA',     # APL_LVL_STTUS_NM
            'NA',     # APL_LVL_STTUS_RSN_CD
            'NA',     # APL_LVL_STTUS_RSN_NM
            '1753-01-01',  # APL_LVL_STTUS_DT_SK
            'NA',     # RTE_USER_ID
            'NA',     # STTUS_USER_ID
            100,      # CRT_RUN_CYC_EXCTN_SK
            100,      # LAST_UPDT_RUN_CYC_EXCTN_SK
            1,        # APL_LVL_STTUS_CD_SK
            1         # APL_LVL_STTUS_RSN_CD_SK
        )
    ],
    [
        "APL_LVL_STTUS_SK", "SRC_SYS_CD", "APL_ID", "APL_LVL_SEQ_NO", "APL_LVL_STTUS_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_DT_SK", "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", "APL_LVL_SK", "RTE_USER_SK",
        "STTUS_USER_SK", "APL_LVL_STTUS_CD", "APL_LVL_STTUS_NM", "APL_LVL_STTUS_RSN_CD",
        "APL_LVL_STTUS_RSN_NM", "APL_LVL_STTUS_DT_SK", "RTE_USER_ID", "STTUS_USER_ID",
        "CRT_RUN_CYC_EXCTN_SK", "LAST_UPDT_RUN_CYC_EXCTN_SK", "APL_LVL_STTUS_CD_SK", "APL_LVL_STTUS_RSN_CD_SK"
    ]
)

# Stage: xfrm_BusinessLogic (CTransformerStage) - Output link "UNKLink"
df_xfrm_UNKLink = spark.createDataFrame(
    [
        (
            0,         # APL_LVL_STTUS_SK
            'UNK',     # SRC_SYS_CD
            'UNK',     # APL_ID
            0,         # APL_LVL_SEQ_NO
            0,         # APL_LVL_STTUS_SEQ_NO
            '1753-01-01',  # CRT_RUN_CYC_EXCTN_DT_SK
            '1753-01-01',  # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
            0,         # APL_LVL_SK
            0,         # RTE_USER_SK
            0,         # STTUS_USER_SK
            'UNK',     # APL_LVL_STTUS_CD
            'UNK',     # APL_LVL_STTUS_NM
            'UNK',     # APL_LVL_STTUS_RSN_CD
            'UNK',     # APL_LVL_STTUS_RSN_NM
            '1753-01-01',  # APL_LVL_STTUS_DT_SK
            'UNK',     # RTE_USER_ID
            'UNK',     # STTUS_USER_ID
            100,       # CRT_RUN_CYC_EXCTN_SK
            100,       # LAST_UPDT_RUN_CYC_EXCTN_SK
            0,         # APL_LVL_STTUS_CD_SK
            0          # APL_LVL_STTUS_RSN_CD_SK
        )
    ],
    [
        "APL_LVL_STTUS_SK", "SRC_SYS_CD", "APL_ID", "APL_LVL_SEQ_NO", "APL_LVL_STTUS_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_DT_SK", "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", "APL_LVL_SK", "RTE_USER_SK",
        "STTUS_USER_SK", "APL_LVL_STTUS_CD", "APL_LVL_STTUS_NM", "APL_LVL_STTUS_RSN_CD",
        "APL_LVL_STTUS_RSN_NM", "APL_LVL_STTUS_DT_SK", "RTE_USER_ID", "STTUS_USER_ID",
        "CRT_RUN_CYC_EXCTN_SK", "LAST_UPDT_RUN_CYC_EXCTN_SK", "APL_LVL_STTUS_CD_SK", "APL_LVL_STTUS_RSN_CD_SK"
    ]
)

# Stage: FnlData (PxFunnel)
df_FnlData = (
    df_xfrm_outMain.unionByName(df_xfrm_NALink)
    .unionByName(df_xfrm_UNKLink)
)

# Apply column order and rpad for char columns before writing
df_FnlData_final = df_FnlData.select(
    F.col("APL_LVL_STTUS_SK"),
    F.col("SRC_SYS_CD"),
    F.col("APL_ID"),
    F.col("APL_LVL_SEQ_NO"),
    F.col("APL_LVL_STTUS_SEQ_NO"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("APL_LVL_SK"),
    F.col("RTE_USER_SK"),
    F.col("STTUS_USER_SK"),
    F.col("APL_LVL_STTUS_CD"),
    F.col("APL_LVL_STTUS_NM"),
    F.col("APL_LVL_STTUS_RSN_CD"),
    F.col("APL_LVL_STTUS_RSN_NM"),
    F.rpad(F.col("APL_LVL_STTUS_DT_SK"), 10, " ").alias("APL_LVL_STTUS_DT_SK"),
    F.rpad(F.col("RTE_USER_ID"), 10, " ").alias("RTE_USER_ID"),
    F.rpad(F.col("STTUS_USER_ID"), 10, " ").alias("STTUS_USER_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_LVL_STTUS_CD_SK"),
    F.col("APL_LVL_STTUS_RSN_CD_SK")
)

# Stage: seq_APL_LVL_STTUS_D_csv_load (PxSequentialFile) - write output
write_files(
    df_FnlData_final,
    f"{adls_path}/load/APL_LVL_STTUS_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)