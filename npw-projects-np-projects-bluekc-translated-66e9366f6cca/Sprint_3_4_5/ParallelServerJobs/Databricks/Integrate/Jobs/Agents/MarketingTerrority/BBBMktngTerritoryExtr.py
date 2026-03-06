# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 12/06/07 10:30:05 Batch  14585_37808 INIT bckcetl ids20 dsadm dadm
# MAGIC ^1_1 10/03/07 10:51:00 Batch  14521_39063 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/09/06 14:25:59 Batch  14162_51974 PROMOTE bckcetl ids20 dsadm Keith for Sharon
# MAGIC ^1_1 10/09/06 14:14:37 Batch  14162_51294 INIT bckcett testIDS30 dsadm Keith for Sharon
# MAGIC ^1_7 10/05/06 14:35:41 Batch  14158_52545 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_7 10/05/06 14:34:04 Batch  14158_52446 INIT bckcett devlIDS30 u10157 SA
# MAGIC ^1_6 10/05/06 14:31:17 Batch  14158_52278 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 09/28/06 18:10:23 Batch  14151_65432 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 09/28/06 18:00:51 Batch  14151_64855 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 09/22/06 12:01:15 Batch  14145_43279 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 09/21/06 16:08:28 Batch  14144_58111 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 09/20/06 23:19:38 Batch  14143_83979 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 09/20/06 22:15:19 Batch  14143_80124 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 09/19/06 09:53:29 Batch  14142_35614 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     BBBAgntTerritoryExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data the BlueBasicBusiness BBB for loading to the IDS AGNT_TERR table
# MAGIC                             Lookups are by the IDS Member Group and Subscriber table for the Marketing Terriroty information
# MAGIC       
# MAGIC 
# MAGIC INPUTS:       BBB Agent Profile system
# MAGIC                       tables:  PARTY, TERRITORY, P
# MAGIC 
# MAGIC 
# MAGIC HASH FILES:   hf_agnt_terr
# MAGIC                       
# MAGIC 
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:  Extract, transform, and primary keying for Agent subject area.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Sharon Andrew   08/02/2006  -  Originally Programmed

# MAGIC Extracts from the following system:
# MAGIC SQLServer: SQL10\\SQL10
# MAGIC Database: BlueQ_prod 
# MAGIC Tables:  PARTY, PARTY_TERRITORY, and TERRITORY.
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic.
# MAGIC End of IDS Agent Addr extract from BlueQ sql server database BBB.
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunCycle = get_widget_value("RunCycle", "100")
RunID = get_widget_value("RunID", "2006070123456")
BBBOwner = get_widget_value("BBBOwner", "")
bbb_secret_name = get_widget_value("bbb_secret_name", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_mktng_terr_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT SRC_SYS_CD, TERR_CD, MKT_SEG, CRT_RUN_CYC_EXCTN_SK, TERR_SK FROM dummy_hf_mktng_terr"
    )
    .load()
)

jdbc_url_bbb, jdbc_props_bbb = get_db_config(bbb_secret_name)
extract_query_bbb = (
    "SELECT \n\nTERRITORY.TERR_CK, \nTERRITORY.TERR_CD, \nTERRITORY.MKT_SEG, "
    "TERRITORY.TERR_DESC, \nTERRITORY.SLS_REP_NM, \nTERRITORY.ACCT_COORDINATOR_NM, "
    "TERRITORY.ACCT_EXEC_NM, \nTERRITORY.EFF_DT, \nTERRITORY.TERM_DT, \nTERRITORY.CRT_DT, \n\n"
    "TERRITORY.LAST_UPDT_DT \nFROM "
    + BBBOwner
    + ".TERRITORY"
)
df_BlueBusiness = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bbb)
    .options(**jdbc_props_bbb)
    .option("query", extract_query_bbb)
    .load()
)

df_StripField = df_BlueBusiness.select(
    F.col("TERR_CD").alias("TERR_CD"),
    F.col("MKT_SEG").alias("MKT_SEG"),
    F.col("CRT_DT").alias("CRT_DT"),
    F.col("EFF_DT").alias("EFF_DT"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.col("TERM_DT").alias("TERM_DT"),
    F.col("TERR_CK").alias("TERR_CK"),
    F.col("ACCT_COORDINATOR_NM").alias("ACCT_COORDINATOR_NM"),
    F.col("ACCT_EXEC_NM").alias("ACCT_EXEC_NM"),
    F.col("SLS_REP_NM").alias("SLS_REP_NM"),
    F.col("TERR_DESC").alias("TERR_DESC")
)

df_BusinessRules = (
    df_StripField
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn("PASS_THRU_IN", F.col("RowPassThru"))
    .withColumn("FIRST_RECYC_DT", current_timestamp())
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("BBBAGNT"))
    .withColumn("PRI_KEY_STRING",
        F.concat_ws(
            ";",
            F.lit("BBBAGNT"),
            trim(F.col("TERR_CD")),
            F.upper(trim(F.col("MKT_SEG")))
        )
    )
    .withColumn(
        "TERR_CD",
        F.when(
            F.length(F.trim(F.substring("TERR_CD", 1, 10))) == 0,
            F.lit("UNK")
        ).otherwise(F.substring("TERR_CD", 1, 10))
    )
    .withColumn("MKT_SEG", F.upper(trim(F.col("MKT_SEG"))))
    .withColumn(
        "CRT_DT",
        F.when(
            F.length(F.trim(F.substring("CRT_DT", 1, 10))) == 0,
            F.lit("UNK")
        ).otherwise(F.substring("CRT_DT", 1, 10))
    )
    .withColumn(
        "EFF_DT",
        F.when(
            F.length(F.trim(F.substring("EFF_DT", 1, 10))) == 0,
            F.lit("UNK")
        ).otherwise(F.substring("EFF_DT", 1, 10))
    )
    .withColumn(
        "LAST_UPDT_DT",
        F.when(
            F.length(F.trim(F.substring("LAST_UPDT_DT", 1, 10))) == 0,
            F.lit("UNK")
        ).otherwise(F.substring("LAST_UPDT_DT", 1, 10))
    )
    .withColumn(
        "TERM_DT",
        F.when(
            F.length(F.trim(F.substring("TERM_DT", 1, 10))) == 0,
            F.lit("UNK")
        ).otherwise(F.substring("TERM_DT", 1, 10))
    )
    .withColumn(
        "ACCT_COORDINATOR_NM",
        F.when(
            F.length(trim(F.col("ACCT_COORDINATOR_NM"))) == 0,
            F.lit("UNK")
        ).otherwise(F.col("ACCT_COORDINATOR_NM"))
    )
    .withColumn(
        "ACCT_EXEC_NM",
        F.when(
            F.length(trim(F.col("ACCT_EXEC_NM"))) == 0,
            F.lit("UNK")
        ).otherwise(F.col("ACCT_EXEC_NM"))
    )
    .withColumn(
        "SLS_REP_NM",
        F.when(
            F.length(trim(F.col("SLS_REP_NM"))) == 0,
            F.lit("UNK")
        ).otherwise(F.col("SLS_REP_NM"))
    )
    .withColumn(
        "TERR_DESC",
        F.when(
            F.length(trim(F.col("TERR_DESC"))) == 0,
            F.lit("NA")
        ).otherwise(F.col("TERR_DESC"))
    )
)

df_PrimaryKey = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_mktng_terr_lookup.alias("lkup"),
        (
            (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD")) &
            (F.col("Transform.TERR_CD") == F.col("lkup.TERR_CD")) &
            (F.col("Transform.MKT_SEG") == F.col("lkup.MKT_SEG"))
        ),
        "left"
    )
    .select(
        F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
        F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("Transform.ERR_CT").alias("ERR_CT"),
        F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.when(
            F.col("lkup.TERR_SK").isNull(),
            F.lit(None)
        ).otherwise(F.col("lkup.TERR_SK")).alias("TERR_SK"),
        F.col("Transform.TERR_CD").alias("TERR_CD"),
        F.col("Transform.MKT_SEG").alias("MKT_SEG"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.when(
            F.col("lkup.TERR_SK").isNull(),
            F.lit(RunCycle)
        ).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Transform.CRT_DT").alias("CRT_DT"),
        F.col("Transform.EFF_DT").alias("EFF_DT"),
        F.col("Transform.LAST_UPDT_DT").alias("LAST_UPDT_DT"),
        F.col("Transform.TERM_DT").alias("TERM_DT"),
        F.col("Transform.TERR_CK").alias("TERR_CK"),
        F.col("Transform.ACCT_COORDINATOR_NM").alias("ACCT_COORDINATOR_NM"),
        F.col("Transform.ACCT_EXEC_NM").alias("ACCT_EXEC_NM"),
        F.col("Transform.SLS_REP_NM").alias("SLS_REP_NM"),
        F.col("Transform.TERR_DESC").alias("TERR_DESC"),
        F.col("lkup.TERR_CD").alias("lkup_TERR_CD")  # to filter later
    )
)

df_enriched = SurrogateKeyGen(df_PrimaryKey, <DB sequence name>, 'TERR_SK', <schema>, <secret_name>)

# Final output to IdsMktngTerrExtr
# Select columns in the exact order of the "Key" link
df_IdsMktngTerrExtr = df_enriched.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "TERR_SK",
    "TERR_CD",
    "MKT_SEG",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CRT_DT",
    "EFF_DT",
    "LAST_UPDT_DT",
    "TERM_DT",
    "TERR_CK",
    "ACCT_COORDINATOR_NM",
    "ACCT_EXEC_NM",
    "SLS_REP_NM",
    "TERR_DESC"
)

# Apply rpad for known char/varchar columns
df_IdsMktngTerrExtr = df_IdsMktngTerrExtr.withColumn(
    "INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " ")
).withColumn(
    "DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " ")
).withColumn(
    "PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " ")
).withColumn(
    "TERR_CD", F.rpad(F.col("TERR_CD"), 3, " ")
)

# For columns of type varchar with unknown length, use <...> to flag
df_IdsMktngTerrExtr = df_IdsMktngTerrExtr.withColumn(
    "SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), F.lit(<...>), " ")
).withColumn(
    "PRI_KEY_STRING", F.rpad(F.col("PRI_KEY_STRING"), F.lit(<...>), " ")
).withColumn(
    "MKT_SEG", F.rpad(F.col("MKT_SEG"), F.lit(<...>), " ")
).withColumn(
    "CRT_DT", F.rpad(F.col("CRT_DT"), F.lit(<...>), " ")
).withColumn(
    "EFF_DT", F.rpad(F.col("EFF_DT"), F.lit(<...>), " ")
).withColumn(
    "LAST_UPDT_DT", F.rpad(F.col("LAST_UPDT_DT"), F.lit(<...>), " ")
).withColumn(
    "TERM_DT", F.rpad(F.col("TERM_DT"), F.lit(<...>), " ")
).withColumn(
    "ACCT_COORDINATOR_NM", F.rpad(F.col("ACCT_COORDINATOR_NM"), F.lit(<...>), " ")
).withColumn(
    "ACCT_EXEC_NM", F.rpad(F.col("ACCT_EXEC_NM"), F.lit(<...>), " ")
).withColumn(
    "SLS_REP_NM", F.rpad(F.col("SLS_REP_NM"), F.lit(<...>), " ")
).withColumn(
    "TERR_DESC", F.rpad(F.col("TERR_DESC"), F.lit(<...>), " ")
)

write_files(
    df_IdsMktngTerrExtr,
    f"{adls_path}/key/BBBMktngTerritoryExtr.MktngTerr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Now handle the "hf_mktng_terr" write (Scenario B merge into dummy_hf_mktng_terr).
df_hf_mktng_terr_inserts = df_enriched.filter(F.col("lkup_TERR_CD").isNull()).select(
    "SRC_SYS_CD",
    "TERR_CD",
    "MKT_SEG",
    "CRT_RUN_CYC_EXCTN_SK",
    "TERR_SK"
)

temp_table_hf_mktng_terr = "STAGING.BBBMktngTerritoryExtr_hf_mktng_terr_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_hf_mktng_terr}", jdbc_url_ids, jdbc_props_ids)

(
    df_hf_mktng_terr_inserts.write.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", temp_table_hf_mktng_terr)
    .mode("overwrite")
    .save()
)

merge_sql_hf_mktng_terr = (
    f"MERGE dummy_hf_mktng_terr AS T "
    f"USING {temp_table_hf_mktng_terr} AS S "
    f"ON T.SRC_SYS_CD = S.SRC_SYS_CD "
    f"AND T.TERR_CD = S.TERR_CD "
    f"AND T.MKT_SEG = S.MKT_SEG "
    f"WHEN MATCHED THEN "
    f"  UPDATE SET "
    f"    T.SRC_SYS_CD = T.SRC_SYS_CD "
    f"    /* No actual update for matched rows, do nothing effectively */ "
    f"WHEN NOT MATCHED THEN "
    f"  INSERT (SRC_SYS_CD, TERR_CD, MKT_SEG, CRT_RUN_CYC_EXCTN_SK, TERR_SK) "
    f"  VALUES (S.SRC_SYS_CD, S.TERR_CD, S.MKT_SEG, S.CRT_RUN_CYC_EXCTN_SK, S.TERR_SK);"
)

execute_dml(merge_sql_hf_mktng_terr, jdbc_url_ids, jdbc_props_ids)