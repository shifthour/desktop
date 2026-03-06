# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                           Change Description                                             Development Project      Code Reviewer              Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------        ---------------------------------------------------------              ----------------------------------   ---------------------------------    -------------------------  
# MAGIC Sravya Gorla                   2019-04-17           Spira Reporting                   Originally Programmed                                                     IntegrateDev2            Kalyan Neelam             2019-04-18
# MAGIC Sunny Mandadi              2019-06-14           Spira Reporting                  Changed the First Line is Column Name to                        IntegrateDev1            Kalyan Neelam             2019-06-18
# MAGIC                                                                                                               True in the source sequential file stage. 
# MAGIC Ashok kumar Baskaran  2021-01-20           331967                               Hash partition added in Remove duplicate stage               Integrate Dev2          Jeyaprasanna              2021-01-28
# MAGIC 
# MAGIC Ashok kumar Baskaran  2021-04-01           354692                               Hash partition added in Remove duplicate and join stage    Integrate Dev2          Jeyaprasanna              2021-04-01

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_MBR_VST.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Land into Seq File for the FKEY job
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

# --------------------------------------------------------------------------------
# db2_K_MBR_VST_In (DB2ConnectorPX) - Read from IDS Database
# --------------------------------------------------------------------------------
jdbc_url_db2_K_MBR_VST_In, jdbc_props_db2_K_MBR_VST_In = get_db_config(ids_secret_name)
extract_query_db2_K_MBR_VST_In = f"SELECT MBR_UNIQ_KEY, VST_ID, SRC_SYS_CD_SK, MBR_VST_SK, CRT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.K_MBR_VST"
df_db2_K_MBR_VST_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_K_MBR_VST_In)
    .options(**jdbc_props_db2_K_MBR_VST_In)
    .option("query", extract_query_db2_K_MBR_VST_In)
    .load()
)

df_db2_K_MBR_VST_In = df_db2_K_MBR_VST_In.alias("Extr")

# --------------------------------------------------------------------------------
# Sequential_File_129 (PxSequentialFile) - Read file into DataFrame
# --------------------------------------------------------------------------------
SeqFile129_schema = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), False),
    StructField("FIRST_RECYC_TS", TimestampType(), False),
    StructField("MBR_VST_SK", IntegerType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("VST_ID", StringType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_EXCNT_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCNT_SK", IntegerType(), False),
    StructField("PRI_POL_ID", StringType(), True),
    StructField("SEC_POL_ID", StringType(), True),
    StructField("PATN_EXTRNL_VNDR_ID", StringType(), False),
    StructField("VST_DT_SK", StringType(), False),
    StructField("VST_STTUS_CD", StringType(), True),
    StructField("VST_CLSD_DT_SK", StringType(), True),
    StructField("BILL_RVWED_DT_SK", StringType(), True),
    StructField("BILL_RVWED_BY_NM", StringType(), True),
    StructField("PROV_EXTRNL_VNDR_ID", StringType(), True),
    StructField("PROV_NTNL_PROV_ID", StringType(), False),
    StructField("prov_id_carectr", StringType(), True),
    StructField("PROV_ID_Practioner_lkp", StringType(), True),
    StructField("src_sys_cd_sk_prov_match", StringType(), True),
    StructField("PROV_ID_ipa_lkp", StringType(), True)
])

file_path_Sequential_File_129 = f"{adls_path}/verified/MBR_VST.{SrcSysCd}.Xfrm.{RunID}.dat"
df_Sequential_File_129 = (
    spark.read.format("csv")
    .schema(SeqFile129_schema)
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", True)
    .option("nullValue", None)
    .load(file_path_Sequential_File_129)
)

df_Sequential_File_129 = df_Sequential_File_129.alias("LoadFile")

# --------------------------------------------------------------------------------
# cpy_MultiStreams (PxCopy)
# --------------------------------------------------------------------------------
df_lnkRemDupDataIn = df_Sequential_File_129.select(
    F.col("LoadFile.VST_ID").alias("VST_ID"),
    F.col("LoadFile.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("LoadFile.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

df_lnkFullDataJnIn = df_Sequential_File_129.select(
    F.col("LoadFile.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("LoadFile.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LoadFile.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("LoadFile.VST_ID").alias("VST_ID"),
    F.col("LoadFile.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("LoadFile.CRT_RUN_EXCNT_SK").alias("CRT_RUN_EXCNT_SK"),
    F.col("LoadFile.LAST_UPDT_RUN_CYC_EXCNT_SK").alias("LAST_UPDT_RUN_CYC_EXCNT_SK"),
    F.col("LoadFile.PRI_POL_ID").alias("PRI_POL_ID"),
    F.col("LoadFile.SEC_POL_ID").alias("SEC_POL_ID"),
    F.col("LoadFile.PATN_EXTRNL_VNDR_ID").alias("PATN_EXTRNL_VNDR_ID"),
    F.col("LoadFile.VST_DT_SK").alias("VST_DT_SK"),
    F.col("LoadFile.VST_STTUS_CD").alias("VST_STTUS_CD"),
    F.col("LoadFile.VST_CLSD_DT_SK").alias("VST_CLSD_DT_SK"),
    F.col("LoadFile.BILL_RVWED_DT_SK").alias("BILL_RVWED_DT_SK"),
    F.col("LoadFile.BILL_RVWED_BY_NM").alias("BILL_RVWED_BY_NM"),
    F.col("LoadFile.PROV_EXTRNL_VNDR_ID").alias("PROV_EXTRNL_VNDR_ID"),
    F.col("LoadFile.PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    F.col("LoadFile.prov_id_carectr").alias("prov_id_carectr"),
    F.col("LoadFile.PROV_ID_Practioner_lkp").alias("PROV_ID_Practioner_lkp"),
    F.col("LoadFile.src_sys_cd_sk_prov_match").alias("src_sys_cd_sk_prov_match"),
    F.col("LoadFile.PROV_ID_ipa_lkp").alias("PROV_ID_ipa_lkp")
)

df_lnkRemDupDataIn = df_lnkRemDupDataIn.alias("lnkRemDupDataIn")
df_lnkFullDataJnIn = df_lnkFullDataJnIn.alias("lnkFullDataJnIn")

# --------------------------------------------------------------------------------
# rdp_NaturalKeys (PxRemDup) - Deduplicate on (VST_ID, MBR_UNIQ_KEY, SRC_SYS_CD_SK)
# --------------------------------------------------------------------------------
df_rdp_NaturalKeys = dedup_sort(
    df_lnkRemDupDataIn,
    ["VST_ID", "MBR_UNIQ_KEY", "SRC_SYS_CD_SK"],
    [("VST_ID", "A"), ("MBR_UNIQ_KEY", "A"), ("SRC_SYS_CD_SK", "A")]
).alias("lnkRemDupDataOut")

# --------------------------------------------------------------------------------
# jn_MrLn (PxJoin) - Left Outer Join
# --------------------------------------------------------------------------------
df_jn_MrLn = (
    df_rdp_NaturalKeys.join(
        df_db2_K_MBR_VST_In,
        on=[
            F.col("lnkRemDupDataOut.VST_ID") == F.col("Extr.VST_ID"),
            F.col("lnkRemDupDataOut.MBR_UNIQ_KEY") == F.col("Extr.MBR_UNIQ_KEY"),
            F.col("lnkRemDupDataOut.SRC_SYS_CD_SK") == F.col("Extr.SRC_SYS_CD_SK")
        ],
        how="left"
    )
    .select(
        F.col("lnkRemDupDataOut.VST_ID").alias("VST_ID"),
        F.col("lnkRemDupDataOut.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("lnkRemDupDataOut.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Extr.MBR_VST_SK").alias("MBR_VST_SK"),
        F.col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
    )
    .alias("JoinOut")
)

# --------------------------------------------------------------------------------
# xfm_PKEYgen (CTransformerStage)
#   Replaces NextSurrogateKey() with SurrogateKeyGen, 
#   Also sets CRT_RUN_CYC_EXCTN_SK = IDSRunCycle if MBR_VST_SK was originally null
# --------------------------------------------------------------------------------

# Mark which rows had original MBR_VST_SK as null
df_enriched = df_jn_MrLn.withColumn("wasMBR_VST_SK_Null", F.col("MBR_VST_SK").isNull()).alias("JoinOut")

# Apply SurrogateKeyGen
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MBR_VST_SK",<schema>,<secret_name>)

# Overwrite CRT_RUN_CYC_EXCTN_SK if originally null
df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.when(F.col("wasMBR_VST_SK_Null"), IDSRunCycle).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
)

# "New" link constraint: IsNull(JoinOut.MBR_VST_SK) == True BEFORE SurrogateKeyGen
df_xfm_PKEYgen_New = df_enriched.filter("wasMBR_VST_SK_Null = true").select(
    F.col("MBR_VST_SK").alias("MBR_VST_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("VST_ID").alias("VST_ID")
).alias("New")

# lnkPKEYxfmOut link: all rows pass
df_xfm_PKEYgen_lnkPKEYxfmOut = df_enriched.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("VST_ID").alias("VST_ID"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("MBR_VST_SK").alias("MBR_VST_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
).alias("lnkPKEYxfmOut")

# --------------------------------------------------------------------------------
# db2_K_MBR_VST_Load (DB2ConnectorPX) - Insert (Append) logic via Merge
# --------------------------------------------------------------------------------
jdbc_url_db2_K_MBR_VST_Load, jdbc_props_db2_K_MBR_VST_Load = get_db_config(ids_secret_name)

temp_table_db2_K_MBR_VST_Load = "STAGING.IdsMbrVstPkey_db2_K_MBR_VST_Load_temp"

# Drop temp table if exists
drop_sql_db2_K_MBR_VST_Load = f"DROP TABLE IF EXISTS {temp_table_db2_K_MBR_VST_Load}"
execute_dml(drop_sql_db2_K_MBR_VST_Load, jdbc_url_db2_K_MBR_VST_Load, jdbc_props_db2_K_MBR_VST_Load)

# Write df_xfm_PKEYgen_New to temp table
(
    df_xfm_PKEYgen_New
    .write
    .format("jdbc")
    .option("url", jdbc_url_db2_K_MBR_VST_Load)
    .options(**jdbc_props_db2_K_MBR_VST_Load)
    .option("dbtable", temp_table_db2_K_MBR_VST_Load)
    .mode("overwrite")
    .save()
)

# Merge into target table #$IDSOwner#.K_MBR_VST
merge_sql_db2_K_MBR_VST_Load = f"""
MERGE INTO {IDSOwner}.K_MBR_VST AS T
USING {temp_table_db2_K_MBR_VST_Load} AS S
ON
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
    AND T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
    AND T.VST_ID = S.VST_ID
WHEN NOT MATCHED THEN
    INSERT (MBR_VST_SK,CRT_RUN_CYC_EXCTN_SK,MBR_UNIQ_KEY,SRC_SYS_CD_SK,VST_ID)
    VALUES (S.MBR_VST_SK,S.CRT_RUN_CYC_EXCTN_SK,S.MBR_UNIQ_KEY,S.SRC_SYS_CD_SK,S.VST_ID);
"""
execute_dml(merge_sql_db2_K_MBR_VST_Load, jdbc_url_db2_K_MBR_VST_Load, jdbc_props_db2_K_MBR_VST_Load)

# --------------------------------------------------------------------------------
# jn_PKEYs (PxJoin) - Inner Join on (MBR_UNIQ_KEY, VST_ID, SRC_SYS_CD_SK)
# --------------------------------------------------------------------------------
df_jn_PKEYs = (
    df_lnkFullDataJnIn.join(
        df_xfm_PKEYgen_lnkPKEYxfmOut,
        on=[
            F.col("lnkFullDataJnIn.MBR_UNIQ_KEY") == F.col("lnkPKEYxfmOut.MBR_UNIQ_KEY"),
            F.col("lnkFullDataJnIn.VST_ID") == F.col("lnkPKEYxfmOut.VST_ID"),
            F.col("lnkFullDataJnIn.SRC_SYS_CD_SK") == F.col("lnkPKEYxfmOut.SRC_SYS_CD_SK")
        ],
        how="inner"
    )
    .select(
        F.col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("lnkPKEYxfmOut.MBR_VST_SK").alias("MBR_VST_SK"),
        F.col("lnkFullDataJnIn.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("lnkFullDataJnIn.VST_ID").alias("VST_ID"),
        F.col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnkFullDataJnIn.CRT_RUN_EXCNT_SK").alias("CRT_RUN_EXCNT_SK"),
        F.col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCNT_SK").alias("LAST_UPDT_RUN_CYC_EXCNT_SK"),
        F.col("lnkFullDataJnIn.PRI_POL_ID").alias("PRI_POL_ID"),
        F.col("lnkFullDataJnIn.SEC_POL_ID").alias("SEC_POL_ID"),
        F.col("lnkFullDataJnIn.PATN_EXTRNL_VNDR_ID").alias("PATN_EXTRNL_VNDR_ID"),
        F.col("lnkFullDataJnIn.VST_DT_SK").alias("VST_DT_SK"),
        F.col("lnkFullDataJnIn.VST_STTUS_CD").alias("VST_STTUS_CD"),
        F.col("lnkFullDataJnIn.VST_CLSD_DT_SK").alias("VST_CLSD_DT_SK"),
        F.col("lnkFullDataJnIn.BILL_RVWED_DT_SK").alias("BILL_RVWED_DT_SK"),
        F.col("lnkFullDataJnIn.BILL_RVWED_BY_NM").alias("BILL_RVWED_BY_NM"),
        F.col("lnkFullDataJnIn.PROV_EXTRNL_VNDR_ID").alias("PROV_EXTRNL_VNDR_ID"),
        F.col("lnkFullDataJnIn.PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
        F.col("lnkFullDataJnIn.prov_id_carectr").alias("prov_id_carectr"),
        F.col("lnkFullDataJnIn.PROV_ID_Practioner_lkp").alias("PROV_ID_Practioner_lkp"),
        F.col("lnkFullDataJnIn.src_sys_cd_sk_prov_match").alias("src_sys_cd_sk_prov_match"),
        F.col("lnkFullDataJnIn.PROV_ID_ipa_lkp").alias("PROV_ID_ipa_lkp"),
        F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
    )
    .alias("Pkey_Out")
)

# --------------------------------------------------------------------------------
# seq_MBR_VST_Pkey (PxSequentialFile) - Write final file
# --------------------------------------------------------------------------------
df_seq_MBR_VST_Pkey = df_jn_PKEYs

# Apply rpad to char/varchar columns in final output (23 columns total, in order):
df_seq_MBR_VST_Pkey = df_seq_MBR_VST_Pkey \
.withColumn("PRI_NAT_KEY_STRING", rpad(F.col("PRI_NAT_KEY_STRING"), <...>, " ")) \
.withColumn("VST_ID", rpad(F.col("VST_ID"), <...>, " ")) \
.withColumn("PRI_POL_ID", rpad(F.col("PRI_POL_ID"), <...>, " ")) \
.withColumn("SEC_POL_ID", rpad(F.col("SEC_POL_ID"), <...>, " ")) \
.withColumn("PATN_EXTRNL_VNDR_ID", rpad(F.col("PATN_EXTRNL_VNDR_ID"), <...>, " ")) \
.withColumn("VST_DT_SK", rpad(F.col("VST_DT_SK"), 10, " ")) \
.withColumn("VST_STTUS_CD", rpad(F.col("VST_STTUS_CD"), <...>, " ")) \
.withColumn("VST_CLSD_DT_SK", rpad(F.col("VST_CLSD_DT_SK"), 10, " ")) \
.withColumn("BILL_RVWED_DT_SK", rpad(F.col("BILL_RVWED_DT_SK"), 10, " ")) \
.withColumn("BILL_RVWED_BY_NM", rpad(F.col("BILL_RVWED_BY_NM"), <...>, " ")) \
.withColumn("PROV_EXTRNL_VNDR_ID", rpad(F.col("PROV_EXTRNL_VNDR_ID"), <...>, " ")) \
.withColumn("PROV_NTNL_PROV_ID", rpad(F.col("PROV_NTNL_PROV_ID"), <...>, " ")) \
.withColumn("prov_id_carectr", rpad(F.col("prov_id_carectr"), <...>, " ")) \
.withColumn("PROV_ID_Practioner_lkp", rpad(F.col("PROV_ID_Practioner_lkp"), <...>, " ")) \
.withColumn("src_sys_cd_sk_prov_match", rpad(F.col("src_sys_cd_sk_prov_match"), <...>, " ")) \
.withColumn("PROV_ID_ipa_lkp", rpad(F.col("PROV_ID_ipa_lkp"), <...>, " "))

file_path_seq_MBR_VST_Pkey = f"{adls_path}/key/MBR_VST.pkey.{RunID}.dat"

write_files(
    df_seq_MBR_VST_Pkey,
    file_path_seq_MBR_VST_Pkey,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)