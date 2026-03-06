# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   CrmGrpAhyPgmExtrSeq
# MAGIC 
# MAGIC PROCESSING:   Extract job for IDS table GRP_AHY_PGM. generates the Primary Key and creates a key file for FKey
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                       Date                 \(9)Project/Altiris #\(9)Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                 --------------------\(9)               ------------------------\(9)-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------   
# MAGIC Raja Gummadi             2016-06-21                              5414                       Original Programming                                                                      IntegrateDev1              Jag Yelavarthi              2016-06-24
# MAGIC 
# MAGIC Akhila Manickavelu     2017-04-10                             5671                        Updated the CRM table to new server and                                                                       Kalyan Neelam           2017-04-10
# MAGIC                                                                                                                     updated fields from table GRP_AHY_PGM
# MAGIC                                                                                                                      table(new table)
# MAGIC Raja Gummadi             2018-02-07                        TFS-20831                  Removed less than current date logic in source stage                    IntegrateDev2             Kalyan Neelam           2018-02-08

# MAGIC GRP_AHY_PGM Common Extract from CRM
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameter definitions (Preserving original names and adding corresponding secret names as per instructions)
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CRMAhyOwner = get_widget_value('CRMAhyOwner','')
crmahy_secret_name = get_widget_value('crmahy_secret_name','')
hf_grp_ahy_pgm_secret_name = get_widget_value('hf_grp_ahy_pgm_secret_name','')
CurrDateTime = get_widget_value('CurrDateTime','')
SourceSK = get_widget_value('SourceSK','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
LastUpdtRunDate = get_widget_value('LastUpdtRunDate','')

# Read from ODBCConnector Stage: GRP_AHY_PGM
jdbc_url_crmahy, jdbc_props_crmahy = get_db_config(crmahy_secret_name)
extract_query_GRP_AHY_PGM = f"SELECT GRP_ID, GRP_AHY_PGM_STRT_DT, AHY_OPT_OUT_FLAG, AHY_BUY_UP_FLAG, STTUS_NO, STTUS_NM, LAST_UPDT_DT from {CRMAhyOwner}.GRP_AHY_PGM WHERE LAST_UPDT_DT >= '{LastUpdtRunDate}'"
df_GRP_AHY_PGM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_crmahy)
    .options(**jdbc_props_crmahy)
    .option("query", extract_query_GRP_AHY_PGM)
    .load()
)

# Transformer Stage: Trim
# svGrpAhyPgmStrtDt = FORMAT.DATE(Trim.GRP_AHY_PGM_STRT_DT, 'SYBASE', 'TIMESTAMP', 'CCYY-MM-DD')
df_Trim_intermediate = df_GRP_AHY_PGM.withColumn(
    "svGrpAhyPgmStrtDt",
    FORMAT.DATE(F.col("GRP_AHY_PGM_STRT_DT"), "SYBASE", "TIMESTAMP", "CCYY-MM-DD")
)

df_Trim = df_Trim_intermediate.select(
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("svGrpAhyPgmStrtDt").alias("GRP_AHY_PGM_STRT_DT"),
    F.col("AHY_OPT_OUT_FLAG").alias("AHY_OPT_OUT_FLAG"),
    F.col("AHY_BUY_UP_FLAG").alias("AHY_BUY_UP_FLAG"),
    F.col("STTUS_NO").alias("STTUS_NO"),
    F.col("STTUS_NM").alias("STTUS_NM"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

# DB2Connector Stage: GRP
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_grp = "SELECT GRP.GRP_ID as GRP_ID,GRP.GRP_SK as GRP_SK FROM #$IDSOwner#.GRP GRP\n\n#$IDSOwner#.GRP GRP"
df_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_grp)
    .load()
)

# HashedFileStage (hf_grplkup_grpahypgm) - Scenario A (Intermediate hashed file)
# Key columns: GRP_ID (primaryKey)
# We deduplicate on GRP_ID before usage as a lookup
df_hf_grplkup_grpahypgm = dedup_sort(df_GRP, partition_cols=["GRP_ID"], sort_cols=[])

# DB2Connector Stage: Copy_of_GRP
extract_query_copygrp = "SELECT GRP_AHY_PGM.GRP_ID,GRP_AHY_PGM.GRP_AHY_PGM_STRT_DT_SK,GRP_AHY_PGM.GRP_AHY_PGM_END_DT_SK FROM #$IDSOwner#.GRP_AHY_PGM GRP_AHY_PGM;\n\n#$IDSOwner#.GRP GRP"
df_Copy_of_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_copygrp)
    .load()
)

# HashedFileStage (hf_grpahypgm) - Scenario A (Intermediate hashed file)
# Key columns: GRP_ID, GRP_AHY_PGM_STRT_DT_SK
df_hf_grpahypgm = dedup_sort(
    df_Copy_of_GRP,
    partition_cols=["GRP_ID", "GRP_AHY_PGM_STRT_DT_SK"],
    sort_cols=[]
)

# CHashedFileStage (hf_grp_ahy_pgm) - Scenario B (read-modify-write of the same hashed file)
# We treat it as a dummy table in IDS: dummy_hf_grp_ahy_pgm
jdbc_url_hf_grp_ahy_pgm, jdbc_props_hf_grp_ahy_pgm = get_db_config(hf_grp_ahy_pgm_secret_name)
df_hf_grp_ahy_pgm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_hf_grp_ahy_pgm)
    .options(**jdbc_props_hf_grp_ahy_pgm)
    .option(
        "query",
        "SELECT GRP_ID, GRP_AHY_PGM_STRT_DT_SK, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, GRP_AHY_PGM_SK FROM IDS.dummy_hf_grp_ahy_pgm"
    )
    .load()
)

# Transformer Stage: BusinessRules
# Join: primary link df_Trim as "Common"
# Lookup link df_hf_grplkup_grpahypgm as "grplkup" on Common.GRP_ID = grplkup.GRP_ID
# Lookup link df_hf_grpahypgm as "grpahylkup" on (Common.GRP_ID = grpahylkup.GRP_ID) and (Common.GRP_AHY_PGM_STRT_DT = grpahylkup.GRP_AHY_PGM_STRT_DT_SK)
df_businessrules = (
    df_Trim.alias("Common")
    .join(
        df_hf_grplkup_grpahypgm.alias("grplkup"),
        F.col("Common.GRP_ID") == F.col("grplkup.GRP_ID"),
        "left"
    )
    .join(
        df_hf_grpahypgm.alias("grpahylkup"),
        (
            (F.col("Common.GRP_ID") == F.col("grpahylkup.GRP_ID")) &
            (F.col("Common.GRP_AHY_PGM_STRT_DT") == F.col("grpahylkup.GRP_AHY_PGM_STRT_DT_SK"))
        ),
        "left"
    )
)

# Filter for the link "Transform" and "Snapshot" both use the same condition: Len(Trim(grplkup.GRP_SK)) > 0
df_transform_condition = df_businessrules.filter(
    F.length(trim(F.col("grplkup.GRP_SK"))) > 0
)

# Columns for the "Transform" link
# Many columns are constants or expressions.
df_transform = df_transform_condition.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),  # WhereExpression: "0"
    F.rpad(F.lit("I"),10," ").alias("INSRT_UPDT_CD"),  # char(10)
    F.rpad(F.lit("N"),1," ").alias("DISCARD_IN"),     # char(1)
    F.rpad(F.lit("Y"),1," ").alias("PASS_THRU_IN"),   # char(1)
    F.lit(CurrDateTime).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),  # WhereExpression: "0"
    F.lit(0).alias("RECYCLE_CT"),  # WhereExpression: "0"
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat_ws(";", F.lit(SrcSysCd), F.col("Common.GRP_ID"), F.col("Common.GRP_AHY_PGM_STRT_DT")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("GRP_AHY_PGM_SK"),  # PK, WhereExpression: "0"
    F.col("Common.GRP_ID").alias("GRP_ID"),
    F.rpad(F.col("Common.GRP_AHY_PGM_STRT_DT"),10," ").alias("GRP_AHY_PGM_STRT_DT_SK"),  # char(10)
    F.lit(SourceSK).alias("SRC_SYS_CD_SK"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        (
            (F.col("Common.STTUS_NO") == 1) &
            (F.col("grpahylkup.GRP_AHY_PGM_END_DT_SK").isNotNull()) &
            (F.col("grpahylkup.GRP_AHY_PGM_END_DT_SK") > F.lit(CurrDateTime))
        ),
        F.lit(CurrDateTime)
    ).otherwise("2199-12-31").alias("GRP_AHY_PGM_END_DT_SK"),  # char(10)
    F.rpad(F.lit("1753-01-01"),10," ").alias("GRP_AHY_PGM_INCNTV_STRT_DT_SK"),  # char(10)
    F.rpad(F.lit("2199-12-31"),10," ").alias("GRP_AHY_PGM_INCNTV_END_DT_SK"),  # char(10)
    F.lit("NA").alias("GRP_SEL_SPOUSE_AHY_PGM_ID"),
    F.lit("NA").alias("GRP_SEL_SUB_AHY_PGM_ID"),
    F.when(F.col("Common.AHY_BUY_UP_FLAG") == 1, F.lit("YES")).otherwise("NO").alias("GRP_SEL_SUB_AHY_ONLY_PGM_ID"),
    F.lit("NA").alias("GRP_AHY_PGM_AHY_BUYUP_CD"),
    F.lit("NA").alias("GRP_AHY_PGM_SPOUSE_BUYUP_CD"),
    F.lit("NA").alias("GRP_AHY_PGM_SUB_BUYUP_CD"),
    F.rpad(F.col("Common.AHY_OPT_OUT_FLAG"),1," ").alias("GRP_AHY_PGM_OPT_OUT_IN"),  # char(1)
    F.col("Common.STTUS_NO").alias("STTUS_NO")
)

# Columns for the "Snapshot" link
df_snapshot = df_transform_condition.select(
    F.col("Common.GRP_ID").alias("GRP_ID"),
    F.rpad(F.col("Common.GRP_AHY_PGM_STRT_DT"),10," ").alias("GRP_AHY_PGM_STRT_DT_SK"),  # char(10)
    F.lit(SourceSK).alias("SRC_SYS_CD_SK")
)

# Write to CSeqFileStage: B_GRP_AHY_PGM
# File name: "B_GRP_AHY_PGM.#SrcSysCd#.dat" in directory "load"
# Delimiter=",", quoteChar='"', overwrite, no header
write_files(
    df_snapshot,
    f"{adls_path}/load/B_GRP_AHY_PGM.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Next Transformer Stage: Out
# Primary link from df_transform => alias "Transform"
# Lookup link from df_hf_grp_ahy_pgm => alias "lkup" on (Transform.GRP_ID=lkup.GRP_ID, Transform.GRP_AHY_PGM_STRT_DT_SK=lkup.GRP_AHY_PGM_STRT_DT_SK, Transform.SRC_SYS_CD=lkup.SRC_SYS_CD)

df_out_join = (
    df_transform.alias("Transform")
    .join(
        df_hf_grp_ahy_pgm.alias("lkup"),
        (
            (F.col("Transform.GRP_ID") == F.col("lkup.GRP_ID")) &
            (F.col("Transform.GRP_AHY_PGM_STRT_DT_SK") == F.col("lkup.GRP_AHY_PGM_STRT_DT_SK")) &
            (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"))
        ),
        "left"
    )
)

# According to instructions, we must replace the stage variable usage with SurrogateKeyGen after we form df_enriched
df_enriched = df_out_join
# SurrogateKeyGen for the "SK" column based on usage:
# if IsNull(lkup.GRP_AHY_PGM_SK) => KeyMgtGetNextValueConcurrent => SurrogateKeyGen
# We place the SurrogateKeyGen call EXACTLY as required, with <DB sequence name>, <schema>, <secret_name> placeholders:
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

# Derive "NewCrtRunCycExtcnSk"
df_enriched = df_enriched.withColumn(
    "NewCrtRunCycExtcnSk",
    F.when(F.col("lkup.GRP_AHY_PGM_SK").isNull(), F.col("Transform.CRT_RUN_CYC_EXCTN_SK"))
     .otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

# Create df_out_key (Constraint: (Transform.STTUS_NO=1 And lkup.GRP_AHY_PGM_STRT_DT_SK not null) Or Transform.STTUS_NO=0)
df_out_key = df_enriched.filter(
    (
        ((F.col("Transform.STTUS_NO") == 1) & (F.col("lkup.GRP_AHY_PGM_STRT_DT_SK").isNotNull()))
    ) | (F.col("Transform.STTUS_NO") == 0)
)

df_out_key_final = df_out_key.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SK").alias("GRP_AHY_PGM_SK"),  # Stage variable "SK"
    F.col("Transform.GRP_ID").alias("GRP_ID"),
    F.col("Transform.GRP_AHY_PGM_STRT_DT_SK").alias("GRP_AHY_PGM_STRT_DT_SK"),
    F.col("Transform.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.GRP_AHY_PGM_END_DT_SK").alias("GRP_AHY_PGM_END_DT_SK"),
    F.col("Transform.GRP_AHY_PGM_INCNTV_STRT_DT_SK").alias("GRP_AHY_PGM_INCNTV_STRT_DT_SK"),
    F.col("Transform.GRP_AHY_PGM_INCNTV_END_DT_SK").alias("GRP_AHY_PGM_INCNTV_END_DT_SK"),
    F.col("Transform.GRP_SEL_SPOUSE_AHY_PGM_ID").alias("GRP_SEL_SPOUSE_AHY_PGM_ID"),
    F.col("Transform.GRP_SEL_SUB_AHY_PGM_ID").alias("GRP_SEL_SUB_AHY_PGM_ID"),
    F.col("Transform.GRP_SEL_SUB_AHY_ONLY_PGM_ID").alias("GRP_SEL_SUB_AHY_ONLY_PGM_ID"),
    F.col("Transform.GRP_AHY_PGM_AHY_BUYUP_CD").alias("GRP_AHY_PGM_AHY_BUYUP_CD"),
    F.col("Transform.GRP_AHY_PGM_SPOUSE_BUYUP_CD").alias("GRP_AHY_PGM_SPOUSE_BUYUP_CD"),
    F.col("Transform.GRP_AHY_PGM_SUB_BUYUP_CD").alias("GRP_AHY_PGM_SUB_BUYUP_CD"),
    F.col("Transform.GRP_AHY_PGM_OPT_OUT_IN").alias("GRP_AHY_PGM_OPT_OUT_IN")
)

# Write to CSeqFileStage: GrpAhyPgm_File
# file path: f"{adls_path}/key/CrmGrpAhyPgmExtr.GrpAhyPgm.dat.{RunID}"
# Delimiter=",", quoteChar='"', overwrite, no header
write_files(
    df_out_key_final,
    f"{adls_path}/key/CrmGrpAhyPgmExtr.GrpAhyPgm.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Create df_out_updt (Constraint: Transform.STTUS_NO=0 And IsNull(lkup.GRP_AHY_PGM_SK)=true)
df_out_updt = df_enriched.filter(
    (F.col("Transform.STTUS_NO") == 0) & (F.col("lkup.GRP_AHY_PGM_SK").isNull())
)

df_out_updt_final = df_out_updt.select(
    F.col("Transform.GRP_ID").alias("GRP_ID"),
    F.col("Transform.GRP_AHY_PGM_STRT_DT_SK").alias("GRP_AHY_PGM_STRT_DT_SK"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("SK").alias("GRP_AHY_PGM_SK")
)

# Write to the same hashed file (hf_grp_ahy_pgm) scenario B => Upsert into dummy_hf_grp_ahy_pgm
# 1) Write df_out_updt_final to a physical temp table "STAGING.CrmGrpAhyPgmExtr_hf_grp_ahy_pgmupdt_temp"
# 2) Merge into IDS.dummy_hf_grp_ahy_pgm

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.CrmGrpAhyPgmExtr_hf_grp_ahy_pgmupdt_temp",
    jdbc_url_hf_grp_ahy_pgm,
    jdbc_props_hf_grp_ahy_pgm
)

df_out_updt_final.write \
    .format("jdbc") \
    .option("url", jdbc_url_hf_grp_ahy_pgm) \
    .options(**jdbc_props_hf_grp_ahy_pgm) \
    .option("dbtable", "STAGING.CrmGrpAhyPgmExtr_hf_grp_ahy_pgmupdt_temp") \
    .mode("overwrite") \
    .save()

merge_sql_hf_grp_ahy_pgm = """
MERGE IDS.dummy_hf_grp_ahy_pgm AS T
USING STAGING.CrmGrpAhyPgmExtr_hf_grp_ahy_pgmupdt_temp AS S
ON
  T.GRP_ID = S.GRP_ID
  AND T.GRP_AHY_PGM_STRT_DT_SK = S.GRP_AHY_PGM_STRT_DT_SK
  AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.GRP_AHY_PGM_SK = S.GRP_AHY_PGM_SK
WHEN NOT MATCHED THEN
  INSERT (GRP_ID, GRP_AHY_PGM_STRT_DT_SK, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, GRP_AHY_PGM_SK)
  VALUES (S.GRP_ID, S.GRP_AHY_PGM_STRT_DT_SK, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.GRP_AHY_PGM_SK);
"""

execute_dml(merge_sql_hf_grp_ahy_pgm, jdbc_url_hf_grp_ahy_pgm, jdbc_props_hf_grp_ahy_pgm)

# Finally, CSeqFileStage: B_GRP_AHY_PGM and GrpAhyPgm_File have been handled above.
# All stages processed.