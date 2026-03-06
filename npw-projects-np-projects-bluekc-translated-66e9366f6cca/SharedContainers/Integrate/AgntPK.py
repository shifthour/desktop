# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Comm Agnt job

CALLED BY : FctsAgntExtr

PROCESSING:                                                                                                                             

MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-09-16               Initial program                                                               3567 Primary Key    devlIDS                                 Steph Goddard          09/22/2008

Dan Long           2014-06-17               Add field COCE_MCTR_VIP to the process.                TFS-1079                                                              Kalyan Neelam           2014-07-08
"""

# IDS Primary Key Container for Commission Agent
# Used by
#
# FctsAgntExtr
# primary key hash file only contains current run keys and is cleared before writing
# update primary key table (K_AGNT) with new keys created today
# Assign primary surrogate key
# Hash file (hf_agnt_allcol) cleared in calling job
# join primary key info with table info
# Load IDS temp. table
# Temp table is tuncated before load and runstat done after load
# SQL joins temp table with key table to assign known keys

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, trim
from typing import Tuple

def run_AgntPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the AgntPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input DataFrame corresponding to link 'AllCol'.
    df_Transform : DataFrame
        Input DataFrame corresponding to link 'Transform'.
    params : dict
        Runtime parameters and JDBC configurations.

    Returns
    -------
    DataFrame
        Output DataFrame corresponding to link 'Key'.
    """

    # --------------------------------------------------
    # Unpack parameters (do this exactly once)
    # --------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    # --------------------------------------------------
    # Handle the hashed-file stage 'hf_agnt_allcol' (scenario a)
    # --------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "AGNT_ID"],
        []
    )

    # --------------------------------------------------
    # Populate IDS.K_AGNT_TEMP
    # --------------------------------------------------
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_AGNT_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )

    (
        df_Transform
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_AGNT_TEMP")
        .mode("append")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_AGNT_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # --------------------------------------------------
    # Read W_Extract (K_AGNT_TEMP joined to K_AGNT)
    # --------------------------------------------------
    extract_query = f"""
    SELECT  k.AGNT_SK,
            w.SRC_SYS_CD_SK,
            w.AGNT_ID,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_AGNT_TEMP w,
         {IDSOwner}.K_AGNT k
    WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
      AND w.AGNT_ID       = k.AGNT_ID
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.AGNT_ID,
           {CurrRunCycle}
    FROM {IDSOwner}.K_AGNT_TEMP w2
    WHERE NOT EXISTS (
        SELECT k2.AGNT_SK
        FROM {IDSOwner}.K_AGNT k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.AGNT_ID       = k2.AGNT_ID
    )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # --------------------------------------------------
    # PrimaryKey Transformer logic
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", when(col("AGNT_SK") == lit(-1), lit("I")).otherwise(lit("U")))
        .withColumn("SrcSysCd",   lit(SrcSysCd))
        .withColumn("svAgntId",   trim(col("AGNT_ID")))
        .withColumn("svCrtRunCycExctnSk",
                    when(col("AGNT_SK") == lit(-1), lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK")))
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'AGNT_SK',<schema>,<secret_name>)

    # --------------------------------------------------
    # Build output streams from PrimaryKey Transformer
    # --------------------------------------------------
    df_updt = (
        df_enriched
        .select(
            col("SrcSysCd").alias("SRC_SYS_CD"),
            col("svAgntId").alias("AGNT_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("AGNT_SK")
        )
    )

    df_newkeys = (
        df_enriched
        .filter(col("svInstUpdt") == lit("I"))
        .select(
            col("SRC_SYS_CD_SK"),
            col("svAgntId").alias("AGNT_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("AGNT_SK")
        )
    )

    df_keys = (
        df_enriched
        .select(
            col("AGNT_SK"),
            col("SRC_SYS_CD_SK"),
            col("SrcSysCd").alias("SRC_SYS_CD"),
            col("svAgntId").alias("AGNT_ID"),
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # --------------------------------------------------
    # Write hash-file (as Parquet) and sequential file
    # --------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/AgntPK_updt.parquet",
        ",",
        "overwrite",
        True,
        True,
        "\"",
        None
    )

    write_files(
        df_newkeys,
        f"{adls_path}/load/K_AGNT.dat",
        ",",
        "overwrite",
        False,
        False,
        "\"",
        None
    )

    # --------------------------------------------------
    # Merge Transformer logic
    # --------------------------------------------------
    df_merge = (
        df_AllCol_dedup.alias("all")
        .join(
            df_keys.alias("k"),
            (col("all.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK")) &
            (col("all.AGNT_ID")       == col("k.AGNT_ID")),
            "left"
        )
        .select(
            col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("all.DISCARD_IN").alias("DISCARD_IN"),
            col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("all.ERR_CT").alias("ERR_CT"),
            col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("k.AGNT_SK").alias("AGNT_SK"),
            col("all.AGNT_ID").alias("AGNT_ID"),
            col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
            col("all.AGNT_INDV").alias("AGNT_INDV"),
            col("all.TAX_DMGRPHC").alias("TAX_DMGRPHC"),
            col("all.AGNT_ADDR_MAIL_TYP_CD").alias("AGNT_ADDR_MAIL_TYP_CD"),
            col("all.AGNT_ADDR_REMIT_TYP_CD").alias("AGNT_ADDR_REMIT_TYP_CD"),
            col("all.AGNT_PD_AGNT_TYP_CD").alias("AGNT_PD_AGNT_TYP_CD"),
            col("all.AGNT_PAYMT_METH_CD").alias("AGNT_PAYMT_METH_CD"),
            col("all.AGNT_TERM_RSN_CD").alias("AGNT_TERM_RSN_CD"),
            col("all.AGNT_TYP_CD").alias("AGNT_TYP_CD"),
            col("all.LAST_STMNT_FROM_DT").alias("LAST_STMNT_FROM_DT"),
            col("all.LAST_STMNT_THRU_DT").alias("LAST_STMNT_THRU_DT"),
            col("all.TERM_DT").alias("TERM_DT"),
            col("all.AGNT_UNIQ_KEY").alias("AGNT_UNIQ_KEY"),
            col("all.AGNT_INDV_ID").alias("AGNT_INDV_ID"),
            col("all.AGNT_NM").alias("AGNT_NM"),
            col("all.COCE_MCTR_VIP").alias("COCE_MCTR_VIP")
        )
    )

    # --------------------------------------------------
    # Return the final output DataFrame
    # --------------------------------------------------
    return df_merge