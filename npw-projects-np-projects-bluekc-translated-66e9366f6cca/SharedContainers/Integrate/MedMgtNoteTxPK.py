# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

"""
MedMgtNoteTxPK  –  Shared Container converted from IBM DataStage

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Claim Cob

PROCESSING:    

MODIFICATIONS:
Developer                     Date                                                                 Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------             ----------------------------                                         ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari                2008-07-21                                                              Initial program                                                      3567 Primary Key            devlIDS 
Sravya Gorla                 2019-09-03                                                              Update NOTE_TX_SEQ_NO datatype to Smallint                           US140167                  IntegrateDev2              Jaideep Mankala          09/24/2019

Annotations:
IDS Primary Key Container for Medical Management
Used by FctsMedMgtNoteTxExtr
update primary key table (K_MED_MGT_NOTE_TX) with new keys created today
primary key hash file only contains current run keys
Assign primary surrogate key
SQL joins temp table with key table to assign known keys
Temp table is tuncated before load and runstat done after load
Load IDS temp. table
join primary key info with table info
Hashfile cleared everytime the job runs
"""

def run_MedMgtNoteTxPK(
    df_AllColl: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ---------------------------------------------------------------------
    #  Parameter unpacking
    # ---------------------------------------------------------------------
    CurrRunCycle         = params["CurrRunCycle"]
    CurrDate             = params["CurrDate"]
    SrcSysCdSk           = params["SrcSysCdSk"]
    SrcSysCd             = params["SrcSysCd"]
    RunID                = params["RunID"]
    IDSOwner             = params["IDSOwner"]
    ids_secret_name      = params["ids_secret_name"]
    ids_jdbc_url         = params["ids_jdbc_url"]
    ids_jdbc_props       = params["ids_jdbc_props"]
    adls_path            = params["adls_path"]
    adls_path_raw        = params["adls_path_raw"]
    adls_path_publish    = params["adls_path_publish"]

    # ---------------------------------------------------------------------
    #  hf_med_mgt_note_tx_allcol  (Scenario-a intermediate hash-file)
    # ---------------------------------------------------------------------
    df_allcoll_dedup = df_AllColl.dropDuplicates([
        "SRC_SYS_CD_SK",
        "MED_MGT_NOTE_DTM",
        "MED_MGT_NOTE_INPT_DTM",
        "NOTE_TX_SEQ_NO"
    ])

    # ---------------------------------------------------------------------
    #  K_MED_MGT_NOTE_TX_TEMP  –  load temp table then extract
    # ---------------------------------------------------------------------
    temp_table = f"{IDSOwner}.K_MED_MGT_NOTE_TX_TEMP"
    execute_dml(f"TRUNCATE TABLE {temp_table}", ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", temp_table)
        .mode("append")
        .save()
    )

    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_MED_MGT_NOTE_TX_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    extract_query = f"""
    SELECT k.MED_MGT_NOTE_TX_SK,
           w.SRC_SYS_CD_SK,
           w.MED_MGT_NOTE_DTM,
           w.MED_MGT_NOTE_INPT_DTM,
           w.NOTE_TX_SEQ_NO,
           k.CRT_RUN_CYC_EXCTN_SK
      FROM {IDSOwner}.K_MED_MGT_NOTE_TX_TEMP w,
           {IDSOwner}.K_MED_MGT_NOTE_TX      k
     WHERE w.SRC_SYS_CD_SK        = k.SRC_SYS_CD_SK
       AND w.MED_MGT_NOTE_DTM     = k.MED_MGT_NOTE_DTM
       AND w.MED_MGT_NOTE_INPT_DTM= k.MED_MGT_NOTE_INPT_DTM
       AND w.NOTE_TX_SEQ_NO       = k.NOTE_TX_SEQ_NO

    UNION

    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.MED_MGT_NOTE_DTM,
           w2.MED_MGT_NOTE_INPT_DTM,
           w2.NOTE_TX_SEQ_NO,
           {CurrRunCycle}
      FROM {IDSOwner}.K_MED_MGT_NOTE_TX_TEMP w2
     WHERE NOT EXISTS (
            SELECT k2.MED_MGT_NOTE_TX_SK
              FROM {IDSOwner}.K_MED_MGT_NOTE_TX k2
             WHERE w2.SRC_SYS_CD_SK         = k2.SRC_SYS_CD_SK
               AND w2.MED_MGT_NOTE_DTM      = k2.MED_MGT_NOTE_DTM
               AND w2.MED_MGT_NOTE_INPT_DTM = k2.MED_MGT_NOTE_INPT_DTM
               AND w2.NOTE_TX_SEQ_NO        = k2.NOTE_TX_SEQ_NO )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ---------------------------------------------------------------------
    #  PrimaryKey Transformer logic
    # ---------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("MED_MGT_NOTE_TX_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MED_MGT_NOTE_TX_SK",<schema>,<secret_name>)

    # ---------------------------------------------------------------------
    #  updt  –  parquet write (hf_med_mgt_note_tx)
    # ---------------------------------------------------------------------
    df_updt = (
        df_enriched.select(
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            "MED_MGT_NOTE_DTM",
            "MED_MGT_NOTE_INPT_DTM",
            "NOTE_TX_SEQ_NO",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "MED_MGT_NOTE_TX_SK"
        )
    )

    file_path_updt = f"{adls_path}/MedMgtNoteTxPK_updt.parquet"
    write_files(
        df_updt,
        file_path_updt,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ---------------------------------------------------------------------
    #  NewKeys  –  sequential file write (K_MED_MGT_NOTE_TX.dat)
    # ---------------------------------------------------------------------
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "MED_MGT_NOTE_DTM",
            "MED_MGT_NOTE_INPT_DTM",
            "NOTE_TX_SEQ_NO",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "MED_MGT_NOTE_TX_SK"
        )
    )

    file_path_seq = f"{adls_path}/load/K_MED_MGT_NOTE_TX.dat"
    write_files(
        df_NewKeys,
        file_path_seq,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ---------------------------------------------------------------------
    #  Keys  –  DataFrame for merge
    # ---------------------------------------------------------------------
    df_Keys = (
        df_enriched.select(
            "MED_MGT_NOTE_TX_SK",
            "SRC_SYS_CD_SK",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            "MED_MGT_NOTE_DTM",
            "MED_MGT_NOTE_INPT_DTM",
            "NOTE_TX_SEQ_NO",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ---------------------------------------------------------------------
    #  Merge Transformer 
    # ---------------------------------------------------------------------
    a = df_allcoll_dedup.alias("a")
    k = df_Keys.alias("k")
    join_cond = (
        (a["SRC_SYS_CD_SK"]        == k["SRC_SYS_CD_SK"]) &
        (a["MED_MGT_NOTE_DTM"]     == k["MED_MGT_NOTE_DTM"]) &
        (a["MED_MGT_NOTE_INPT_DTM"]== k["MED_MGT_NOTE_INPT_DTM"]) &
        (a["NOTE_TX_SEQ_NO"]       == k["NOTE_TX_SEQ_NO"])
    )

    df_Key = (
        a.join(k, join_cond, "left")
         .select(
             "a.JOB_EXCTN_RCRD_ERR_SK",
             "k.INSRT_UPDT_CD",
             "a.DISCARD_IN",
             "a.PASS_THRU_IN",
             "a.FIRST_RECYC_DT",
             "a.ERR_CT",
             "a.RECYCLE_CT",
             "k.SRC_SYS_CD",
             "a.PRI_KEY_STRING",
             "k.MED_MGT_NOTE_TX_SK",
             "a.MED_MGT_NOTE_DTM",
             "a.MED_MGT_NOTE_INPT_DTM",
             "a.NOTE_TX_SEQ_NO",
             "k.CRT_RUN_CYC_EXCTN_SK",
             F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
             "a.MED_MGT_NOTE_SK",
             "a.NOTE_TX"
         )
    )

    # ---------------------------------------------------------------------
    #  Return to caller
    # ---------------------------------------------------------------------
    return df_Key