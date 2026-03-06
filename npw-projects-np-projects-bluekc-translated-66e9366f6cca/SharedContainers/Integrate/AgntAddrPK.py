# Databricks notebook source
# MAGIC %run ./Utility

# COMMAND ----------
# MAGIC %run ./Routine_Functions

# COMMAND ----------
"""
Shared Container: AgntAddrPK
Description:
* VC LOGS *
^1_1 02/20/09 11:01:04 Batch  15027_39667 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 02/20/09 10:38:04 Batch  15027_38287 INIT bckcett testIDS dsadm bls for sa
^1_2 02/19/09 15:36:15 Batch  15026_56186 PROMOTE bckcett testIDS u03651 steph for Sharon - primary key containers
^1_2 02/19/09 15:33:19 Batch  15026_56002 INIT bckcett devlIDS u03651 steffy
^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   Shared container used for Primary Keying of Comm Agnt Addr job

CALLED BY : FctsAgntAddrExtr

PROCESSING:    



MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-09-24               Initial program                                                               3567 Primary Key    devlIDS                                 Steph Goddard          10/03/2008   

Jag Yelavarthi     2015-11-16              Truncate call for K_AGNT_ADDR_TEMP is added      TFS#10776             IntegrateDev1                    Kalyan Neelam           2015-11-17
"""
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F, types as T
from typing import Tuple

# COMMAND ----------
def run_AgntAddrPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the AgntAddrPK shared container.

    Parameters
    ----------
    df_AllCol : DataFrame
        DataFrame for link 'AllCol'.
    df_Transform : DataFrame
        DataFrame for link 'Transform'.
    params : dict
        Runtime parameters dictionary.

    Returns
    -------
    DataFrame
        DataFrame for link 'Key'.
    """

    # ------------------------------------------------------------------
    # Unpack required parameters
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # Stage: hf_agnt_addr_allcol  (Scenario a – intermediate hash file)
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "AGNT_ID", "ADDR_TYP_CD"],
        []
    )

    # ------------------------------------------------------------------
    # Stage: K_AGNT_ADDR_TEMP – load temp table
    before_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_AGNT_ADDR_TEMP')"
    execute_dml(before_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_AGNT_ADDR_TEMP")
        .mode("append")
        .save()
    )

    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_AGNT_ADDR_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    extract_query = f"""
    SELECT  k.AGNT_ADDR_SK,
            w.SRC_SYS_CD_SK,
            w.AGNT_ID,
            w.ADDR_TYP_CD,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_AGNT_ADDR_TEMP w,
         {IDSOwner}.K_AGNT_ADDR k
    WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
      AND w.AGNT_ID       = k.AGNT_ID
      AND w.ADDR_TYP_CD   = k.ADDR_TYP_CD
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.AGNT_ID,
           w2.ADDR_TYP_CD,
           {CurrRunCycle}
    FROM {IDSOwner}.K_AGNT_ADDR_TEMP w2
    WHERE NOT EXISTS (
        SELECT k2.AGNT_ADDR_SK
        FROM {IDSOwner}.K_AGNT_ADDR k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.AGNT_ID       = k2.AGNT_ID
          AND w2.ADDR_TYP_CD   = k2.ADDR_TYP_CD
    )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------------------
    # Stage: PrimaryKey transformer
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("AGNT_ADDR_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svAgntId", F.trim(F.col("AGNT_ID")))
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == "I", F.lit(None).cast(T.LongType()))
            .otherwise(F.col("AGNT_ADDR_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(
                F.col("svInstUpdt") == "I",
                F.lit(int(CurrRunCycle))
            ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # Build derivative DataFrames
    # updt link
    df_updt = df_enriched.select(
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.col("svAgntId").alias("AGNT_ID"),
        F.col("ADDR_TYP_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("AGNT_ADDR_SK")
    )

    # NewKeys link (insert only)
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I")
        .select(
            "SRC_SYS_CD_SK",
            F.col("svAgntId").alias("AGNT_ID"),
            "ADDR_TYP_CD",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("AGNT_ADDR_SK")
        )
    )

    # Keys link (all records)
    df_Keys = df_enriched.select(
        F.col("svSK").alias("AGNT_ADDR_SK"),
        "SRC_SYS_CD_SK",
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.col("svAgntId").alias("AGNT_ID"),
        "ADDR_TYP_CD",
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # ------------------------------------------------------------------
    # Stage: hf_agnt_addr  (Scenario c – write to parquet)
    parquet_path_updt = f"{adls_path}/AgntAddrPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path_updt,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: K_AGNT_ADDR (sequential file)
    seq_file_path = f"{adls_path}/load/K_AGNT_ADDR.dat"
    write_files(
        df_NewKeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: Merge transformer
    join_condition = (
        (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("all.AGNT_ID") == F.col("k.AGNT_ID")) &
        (F.col("all.ADDR_TYP_CD") == F.col("k.ADDR_TYP_CD"))
    )

    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(df_Keys.alias("k"), join_condition, "left")
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN").alias("DISCARD_IN"),
            F.col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("all.ERR_CT").alias("ERR_CT"),
            F.col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("k.AGNT_ADDR_SK").alias("AGNT_ADDR_SK"),
            F.col("all.AGNT_ID").alias("AGNT_ID"),
            F.col("all.ADDR_TYP_CD").alias("ADDR_TYP_CD"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(int(CurrRunCycle)).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.AGNT").alias("AGNT"),
            F.col("all.ADDR_LN_1").alias("ADDR_LN_1"),
            F.col("all.ADDR_LN_2").alias("ADDR_LN_2"),
            F.col("all.ADDR_LN_3").alias("ADDR_LN_3"),
            F.col("all.CITY_NM").alias("CITY_NM"),
            F.col("all.AGNT_ADDR_ST_CD").alias("AGNT_ADDR_ST_CD"),
            F.col("all.POSTAL_CD").alias("POSTAL_CD"),
            F.col("all.CNTY_NM").alias("CNTY_NM"),
            F.col("all.AGNT_ADDR_CTRY_CD").alias("AGNT_ADDR_CTRY_CD"),
            F.col("all.PHN_NO").alias("PHN_NO"),
            F.col("all.PHN_NO_EXT").alias("PHN_NO_EXT"),
            F.col("all.FAX_NO").alias("FAX_NO"),
            F.col("all.FAX_NO_EXT").alias("FAX_NO_EXT"),
            F.col("all.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX")
        )
    )

    # ------------------------------------------------------------------
    return df_Key