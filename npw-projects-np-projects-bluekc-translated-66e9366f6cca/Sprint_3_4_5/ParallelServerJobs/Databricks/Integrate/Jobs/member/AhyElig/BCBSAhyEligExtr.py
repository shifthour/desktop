# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  BCBSAhyEligExtr
# MAGIC CALLED BY:  ECommerceExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from AHY_ELIG and creates primary keying process   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari       2009-01-12                Initial programming                                                                               3863                devlIDScur                         Steph Goddard          02/12/2009
# MAGIC 
# MAGIC Bhoomi Dasari        2009-04-16               added Ebcbs parameters                                                                     3863                devlIDScur                         Steph Goddard          04/17/2009
# MAGIC 
# MAGIC Tim Sieg                2022-04-14                Adding ODBC connection                                                       S2S Remediation        IntegrateDev5	Ken Bradmon	2022-06-01

# MAGIC IDS extractions from different IDS tables for the lookup's
# MAGIC Added balancing snapshot
# MAGIC Writing Sequential File to /key
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Extract  AHY_ELIG_XREF Data
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    when,
    regexp_replace,
    rpad
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# PARAMETERS
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunDate = get_widget_value('RunDate','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SourceSk = get_widget_value('SourceSk','')
EBbcbsOwner = get_widget_value('EBbcbsOwner','')
ebbcbs_secret_name = get_widget_value('ebbcbs_secret_name','')

# READ FROM DUMMY TABLE IN SCENARIO B (hf_ahy_elig)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_hf_ahy_elig = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD, CLNT_SRC_ID, CLNT_GRP_NO, CLNT_SUBGRP_NO, EFF_DT_SK, CLS_ID, CRT_RUN_CYC_EXCTN_SK, AHY_ELIG_SK FROM dummy_hf_ahy_elig")
    .load()
)

# IDS_Tables (DB2Connector, Database=IDS)
df_SGrp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT SUBGRP.SUBGRP_ID as SUBGRP_ID,SUBGRP.GRP_ID as GRP_ID,SUBGRP.SUBGRP_SK as SUBGRP_SK FROM {IDSOwner}.SUBGRP SUBGRP")
    .load()
)

df_Cls = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT CLS.GRP_ID as GRP_ID,CLS.CLS_ID as CLS_ID,CLS.CLS_SK as CLS_SK FROM {IDSOwner}.CLS CLS")
    .load()
)

df_ClsPln = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT CLS_PLN.CLS_PLN_ID as CLS_PLN_ID,CLS_PLN.CLS_PLN_SK as CLS_PLN_SK FROM {IDSOwner}.CLS_PLN CLS_PLN")
    .load()
)

df_AppUsr1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT APP_USER.USER_ID as USER_ID,APP_USER.USER_SK as USER_SK FROM {IDSOwner}.APP_USER APP_USER")
    .load()
)

df_ClsGrp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT CLS.GRP_ID as GRP_ID,CLS.GRP_SK as GRP_SK FROM {IDSOwner}.CLS CLS")
    .load()
)

# Logic (Transformer) on AppUsr1 => AppUsr
df_AppUsr = (
    df_AppUsr1
    .withColumn(
        "USER_ID",
        trim(
            regexp_replace(
                col("USER_ID"), "[\\r\\n\\t]", ""
            )
        )
    )
    .withColumn("USER_SK", col("USER_SK"))
    .select("USER_ID", "USER_SK")
)

# hf_all_files is SCENARIO A for each link: deduplicate by key columns, then directly used as lookups

# SGrp => grp_lkup
df_grp_lkup = dedup_sort(df_SGrp, ["SUBGRP_ID","GRP_ID"], [])

# Cls => cls_lkup
df_cls_lkup = dedup_sort(df_Cls, ["GRP_ID","CLS_ID"], [])

# ClsPln => pln_lkup
df_pln_lkup = dedup_sort(df_ClsPln, ["CLS_PLN_ID"], [])

# AppUsr => usr_lkup
df_usr_lkup = dedup_sort(df_AppUsr, ["USER_ID"], [])

# ClsGrp => cgrp_lkup
df_cgrp_lkup = dedup_sort(df_ClsGrp, ["GRP_ID"], [])

# AHY_ELIG (ODBCConnector, reading from #$EBbcbsOwner#)
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(ebbcbs_secret_name)
df_AHY_ELIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", f"SELECT CLNT_SRC_CD,CLNT_GRP_NO,CLNT_SUBGRP_NO,EFF_DT,CLS_ID,GRP_ID,SUBGRP_ID,PLN_ID,ELIG_TYP_CD,INCLD_NON_MBR_IN,TERM_DT,USER_ID,AUDIT_TS FROM {EBbcbsOwner}.AHY_ELIG_XREF")
    .load()
)

# Strip (Transformer)
df_Extract = (
    df_AHY_ELIG
    .withColumn("CLNT_SRC_CD", regexp_replace(col("CLNT_SRC_CD"), "[\\r\\n\\t]", ""))
    .withColumn("CLNT_SRC_CD", trim(col("CLNT_SRC_CD")))
    .withColumn("CLNT_GRP_NO", trim(col("CLNT_GRP_NO")))
    .withColumn("CLNT_SUBGRP_NO", trim(col("CLNT_SUBGRP_NO")))
    # EFF_DT => assume we keep as string or date; job transforms to CCYY-MM-DD, treat as string for final
    .withColumn("EFF_DT", col("EFF_DT"))
    .withColumn("CLS_ID", trim(regexp_replace(col("CLS_ID"), "[\\r\\n\\t]", "")))
    .withColumn("GRP_ID", trim(regexp_replace(col("GRP_ID"), "[\\r\\n\\t]", "")))
    .withColumn("SUBGRP_ID", trim(regexp_replace(col("SUBGRP_ID"), "[\\r\\n\\t]", "")))
    .withColumn("PLN_ID", trim(regexp_replace(col("PLN_ID"), "[\\r\\n\\t]", "")))
    .withColumn("ELIG_TYP_CD", regexp_replace(col("ELIG_TYP_CD"), "[\\r\\n\\t]", ""))
    .withColumn("INCLD_NON_MBR_IN", col("INCLD_NON_MBR_IN"))
    # TERM_DT => same approach
    .withColumn("TERM_DT", col("TERM_DT"))
    .withColumn("USER_ID", trim(regexp_replace(col("USER_ID"), "[\\r\\n\\t]", "")))
    # AUDIT_TS => same approach
    .withColumn("AUDIT_TS", col("AUDIT_TS"))
    .select(
        "CLNT_SRC_CD",
        "CLNT_GRP_NO",
        "CLNT_SUBGRP_NO",
        "EFF_DT",
        "CLS_ID",
        "GRP_ID",
        "SUBGRP_ID",
        "PLN_ID",
        "ELIG_TYP_CD",
        "INCLD_NON_MBR_IN",
        "TERM_DT",
        "USER_ID",
        "AUDIT_TS"
    )
)

# Business_Logic (Transformer) - multiple lookup joins
df_Business_Logic = (
    df_Extract.alias("Extract")
    .join(df_grp_lkup.alias("grp_lkup"),
          [col("Extract.SUBGRP_ID") == col("grp_lkup.SUBGRP_ID"),
           col("Extract.GRP_ID") == col("grp_lkup.GRP_ID")],
          "left")
    .join(df_cls_lkup.alias("cls_lkup"),
          [col("Extract.GRP_ID") == col("cls_lkup.GRP_ID"),
           col("Extract.CLS_ID") == col("cls_lkup.CLS_ID")],
          "left")
    .join(df_pln_lkup.alias("pln_lkup"),
          col("Extract.PLN_ID") == col("pln_lkup.CLS_PLN_ID"),
          "left")
    .join(df_usr_lkup.alias("usr_lkup"),
          col("Extract.USER_ID") == col("usr_lkup.USER_ID"),
          "left")
    .join(df_cgrp_lkup.alias("cgrp_lkup"),
          col("Extract.GRP_ID") == col("cgrp_lkup.GRP_ID"),
          "left")
)

# Stage Variables (not directly used by final columns except embedded conditions):
# RowPassThru = 'Y'
# svEligTypCd = if isnull(Extract.ELIG_TYP_CD) = True or len(Extract.ELIG_TYP_CD)=0 => '0' else Extract.ELIG_TYP_CD
# svEffDtSk = GetFkeyAhyEligTyp("ECOM", 101, svEligTypCd, "X")  # not consumed in final outputs

# OutputPin "Transform" => we derive these columns
df_Transform = (
    df_Business_Logic
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", rpad(lit("I"), 10, " "))
    .withColumn("DISCARD_IN", rpad(lit("N"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(lit("Y"), 1, " "))
    .withColumn("FIRST_RECYC_DT", lit(RunDate))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit(SrcSysCd))
    .withColumn(
        "PRI_KEY_STRING",
        col("SRC_SYS_CD")
        .cast("string")
        .concat(lit(";"))
        .concat(
            regexp_replace(
                col("Extract.CLNT_SRC_CD"),
                "[\\r\\n\\t]", ""
            )
        )
        .concat(lit(";"))
        .concat(trim(col("Extract.CLNT_GRP_NO")))
        .concat(lit(";"))
        .concat(trim(col("Extract.CLNT_SUBGRP_NO")))
        .concat(lit(";"))
        .concat(col("Extract.EFF_DT"))
        .concat(lit(";"))
        .concat(
            regexp_replace(
                col("Extract.CLS_ID"),
                "[\\r\\n\\t]",
                ""
            )
        )
    )
    .withColumn("AHY_ELIG_SK", lit(0))
    .withColumn("CLNT_SRC_ID", col("Extract.CLNT_SRC_CD"))
    .withColumn("CLNT_GRP_NO", col("Extract.CLNT_GRP_NO"))
    .withColumn("CLNT_SUBGRP_NO", col("Extract.CLNT_SUBGRP_NO"))
    .withColumn("EFF_DT_SK", rpad(col("Extract.EFF_DT"), 10, " "))
    .withColumn("CLS_ID", col("Extract.CLS_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn(
        "ELIG_TYP_CD",
        rpad(
            when(
                col("Extract.ELIG_TYP_CD").isNull() | (trim(col("Extract.ELIG_TYP_CD")) == ""),
                lit("0")
            ).otherwise(col("Extract.ELIG_TYP_CD")),
            1,
            " "
        )
    )
    .withColumn(
        "CLS_SK",
        when(
            col("cls_lkup.CLS_SK").isNull() | (trim(col("cls_lkup.CLS_SK").cast("string")) == ""),
            lit(0)
        ).otherwise(col("cls_lkup.CLS_SK"))
    )
    .withColumn(
        "CLS_PLN_SK",
        when(
            col("pln_lkup.CLS_PLN_SK").isNull() | (trim(col("pln_lkup.CLS_PLN_SK").cast("string")) == ""),
            lit(0)
        ).otherwise(col("pln_lkup.CLS_PLN_SK"))
    )
    .withColumn(
        "GRP_SK",
        when(
            col("cgrp_lkup.GRP_SK").isNull() | (trim(col("cgrp_lkup.GRP_SK").cast("string")) == ""),
            lit(0)
        ).otherwise(col("cgrp_lkup.GRP_SK"))
    )
    .withColumn(
        "SUBGRP_SK",
        when(
            col("grp_lkup.SUBGRP_SK").isNull() | (trim(col("grp_lkup.SUBGRP_SK").cast("string")) == ""),
            lit(0)
        ).otherwise(col("grp_lkup.SUBGRP_SK"))
    )
    .withColumn(
        "INCLD_NON_MBR_IN",
        rpad(
            when(
                col("Extract.INCLD_NON_MBR_IN").isNull() | (trim(col("Extract.INCLD_NON_MBR_IN")) == ""),
                lit("N")
            ).otherwise(col("Extract.INCLD_NON_MBR_IN")),
            1,
            " "
        )
    )
    .withColumn("TERM_DT_SK", rpad(col("Extract.TERM_DT"), 10, " "))
    .withColumn("AUDIT_TS", rpad(col("Extract.AUDIT_TS"), 10, " "))
    .withColumn(
        "SRC_SYS_LAST_UPDT_USER_SK",
        when(
            col("usr_lkup.USER_SK").isNull() | (trim(col("usr_lkup.USER_SK").cast("string")) == ""),
            lit(0)
        ).otherwise(col("usr_lkup.USER_SK"))
    )
)

# Also "SnapShot" => 6 columns
df_SnapShot = (
    df_Business_Logic
    .select(
        rpad(lit(SourceSk), 1, " ").alias("SRC_SYS_CD_SK"),
        col("Extract.CLNT_SRC_CD").alias("CLNT_SRC_ID"),
        col("Extract.CLNT_GRP_NO").alias("CLNT_GRP_NO"),
        col("Extract.CLNT_SUBGRP_NO").alias("CLNT_SUBGRP_NO"),
        rpad(col("Extract.EFF_DT"), 10, " ").alias("EFF_DT_SK"),
        col("Extract.CLS_ID").alias("CLS_ID")
    )
)

# B_AHY_ELIG => Write to f"{adls_path}/load/B_AHY_ELIG.ECOM.dat"
df_B_AHY_ELIG = df_SnapShot.select(
    "SRC_SYS_CD_SK",
    "CLNT_SRC_ID",
    "CLNT_GRP_NO",
    "CLNT_SUBGRP_NO",
    "EFF_DT_SK",
    "CLS_ID"
)
write_files(
    df_B_AHY_ELIG,
    f"{adls_path}/load/B_AHY_ELIG.ECOM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Primary_Key (Transformer) - read (lkup) from df_hf_ahy_elig, write with stage variables
df_PK = (
    df_Transform.alias("Transform")
    .join(
        df_hf_ahy_elig.alias("lkup"),
        [
            col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD"),
            col("Transform.CLNT_SRC_ID") == col("lkup.CLNT_SRC_ID"),
            col("Transform.CLNT_GRP_NO") == col("lkup.CLNT_GRP_NO"),
            col("Transform.CLNT_SUBGRP_NO") == col("lkup.CLNT_SUBGRP_NO"),
            col("Transform.EFF_DT_SK") == col("lkup.EFF_DT_SK"),
            col("Transform.CLS_ID") == col("lkup.CLS_ID")
        ],
        "left"
    )
    .withColumn("lkup_AHY_ELIG_SK", col("lkup.AHY_ELIG_SK"))
    .withColumn("lkup_CRT_RUN_CYC_EXCTN_SK", col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    .withColumn(
        "SK",
        when(
            col("lkup.AHY_ELIG_SK").isNull(),
            None
        ).otherwise(col("lkup.AHY_ELIG_SK"))
    )
    .withColumn(
        "NewCrtRunCycExtcnSk",
        when(col("lkup.AHY_ELIG_SK").isNull(), lit(CurrRunCycle)).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
)

# SurrogateKeyGen to fill SK if null
df_enriched = SurrogateKeyGen(df_PK,<DB sequence name>,'SK',<schema>,<secret_name>)

# Build the final df_Key output
df_Key = (
    df_enriched
    .withColumn("AHY_ELIG_SK", col("SK"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", col("NewCrtRunCycExtcnSk"))
    # The job columns must match exactly in order
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "AHY_ELIG_SK",
        "CLNT_SRC_ID",
        "CLNT_GRP_NO",
        "CLNT_SUBGRP_NO",
        "EFF_DT_SK",
        "CLS_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ELIG_TYP_CD",
        "CLS_SK",
        "CLS_PLN_SK",
        "GRP_SK",
        "SUBGRP_SK",
        "INCLD_NON_MBR_IN",
        "TERM_DT_SK",
        "AUDIT_TS",
        "SRC_SYS_LAST_UPDT_USER_SK"
    )
)

# Write df_Key => IdsAhyEligExtr => Seq file => directory=key => filename=IdsAhyEligExtr.AhyElig.dat.#RunID#
df_IdsAhyEligExtr = df_Key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "AHY_ELIG_SK",
    "CLNT_SRC_ID",
    "CLNT_GRP_NO",
    "CLNT_SUBGRP_NO",
    "EFF_DT_SK",
    "CLS_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ELIG_TYP_CD",
    "CLS_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "SUBGRP_SK",
    "INCLD_NON_MBR_IN",
    "TERM_DT_SK",
    "AUDIT_TS",
    "SRC_SYS_LAST_UPDT_USER_SK"
)
write_files(
    df_IdsAhyEligExtr,
    f"{adls_path}/key/IdsAhyEligExtr.AhyElig.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# hf_ahy_elig_updt => scenario B => we do a merge for rows where lkup.AHY_ELIG_SK isNull
df_updt = df_enriched.filter(col("lkup_AHY_ELIG_SK").isNull()).select(
    "SRC_SYS_CD",
    "CLNT_SRC_ID",
    "CLNT_GRP_NO",
    "CLNT_SUBGRP_NO",
    "EFF_DT_SK",
    "CLS_ID",
    col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("SK").alias("AHY_ELIG_SK")
)

# Create temp table in STAGING and MERGE to dummy_hf_ahy_elig
temp_table_name = "STAGING.BCBSAhyEligExtr_hf_ahy_elig_updt_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url_ids, jdbc_props_ids)

df_updt.write \
    .format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO dummy_hf_ahy_elig AS T
USING {temp_table_name} AS S
ON (
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.CLNT_SRC_ID = S.CLNT_SRC_ID
    AND T.CLNT_GRP_NO = S.CLNT_GRP_NO
    AND T.CLNT_SUBGRP_NO = S.CLNT_SUBGRP_NO
    AND T.EFF_DT_SK = S.EFF_DT_SK
    AND T.CLS_ID = S.CLS_ID
)
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    CLNT_SRC_ID,
    CLNT_GRP_NO,
    CLNT_SUBGRP_NO,
    EFF_DT_SK,
    CLS_ID,
    CRT_RUN_CYC_EXCTN_SK,
    AHY_ELIG_SK
  )
  VALUES (
    S.SRC_SYS_CD,
    S.CLNT_SRC_ID,
    S.CLNT_GRP_NO,
    S.CLNT_SUBGRP_NO,
    S.EFF_DT_SK,
    S.CLS_ID,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.AHY_ELIG_SK
  );
"""
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)