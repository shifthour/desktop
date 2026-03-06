# Databricks notebook source
# MAGIC %run ./Utility

# COMMAND ----------
# MAGIC %run ./Routine_Functions

# COMMAND ----------
"""
IncmInvcFeeDscntPK – Shared container used for Primary Keying of Income Invoice Fee Discount job.

* VC LOGS *
^1_1 01/14/09 10:16:22 Batch  14990_36985 PROMOTE bckcetl ids20 dsadm bls for sg
^1_1 01/14/09 10:05:39 Batch  14990_36343 INIT bckcett testIDS dsadm BLS FOR SG
^1_1 12/16/08 22:11:05 Batch  14961_79874 PROMOTE bckcett testIDS u03651 steph - income primary key conversion
^1_1 12/16/08 22:03:32 Batch  14961_79415 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Income Invoice Fee Discount job
CALLED BY : FctsIncomeInvcFeeDscntExtr

MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-09-28               Initial program                                                               3567 Primary Key    devlIDS                                Steph Goddard          10/03/2008
Goutham Kalidindi             2021-03-24      358186                            Changed Datatype length for field                      IntegrateDev1                  Jeyaprasanna             2021-03-31
                                                                                             BLIV_ID char(12) to Varchar(15)

IDS Primary Key Container for Income Invoice Fee Discount
Used by  FctsIncomeInvcFeeDscntExtr
Hash file (hf_invc_fee_dscnt_allcol) cleared in calling job
SQL joins temp table with key table to assign known keys
Temp table is truncated before load and runstat done after load
Load IDS temp. table then join primary key info with table info
Update primary key table (K_INVC_FEE_DSCNT) with new keys created today
Primary key hash file only contains current run keys and is cleared before writing
Assign primary surrogate key
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, lit

# COMMAND ----------
def run_IncmInvcFeeDscntPK(
    df_AllCol: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the IncmInvcFeeDscntPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input stream corresponding to link `AllCol`.
    params : dict
        Dictionary of runtime parameters.

    Returns
    -------
    DataFrame
        Output stream corresponding to link `Key`.
    """
    # --------------------------------------------------
    # Unpack parameters – each exactly once
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
    # Stage: hf_invc_fee_dscnt_allcol  (scenario a – intermediate hash file)
    partition_cols_hf = [
        "SRC_SYS_CD_SK",
        "BLIV_ID",
        "PMFA_ID",
        "BLFD_SOURCE",
        "BLFD_DISP_CD"
    ]
    df_AllCol_clean = dedup_sort(
        df_AllCol,
        partition_cols_hf,
        []  # no specific ordering
    )
    # --------------------------------------------------
    # Stage: K_INVC_FEE_DSCNT_TEMP  – database extract
    extract_query = f"""
    SELECT  k.INVC_FEE_DSCNT_SK,
            w.SRC_SYS_CD_SK,
            w.BILL_INVC_ID,
            w.FEE_DSCNT_ID,
            w.INVC_FEE_DSCNT_SRC_CD,
            w.INVC_FEE_DSCNT_BILL_DISP_CD,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_INVC_FEE_DSCNT_TEMP w,
         {IDSOwner}.K_INVC_FEE_DSCNT k
    WHERE w.SRC_SYS_CD_SK                 = k.SRC_SYS_CD_SK
      AND w.BILL_INVC_ID                  = k.BILL_INVC_ID
      AND w.FEE_DSCNT_ID                  = k.FEE_DSCNT_ID
      AND w.INVC_FEE_DSCNT_SRC_CD         = k.INVC_FEE_DSCNT_SRC_CD
      AND w.INVC_FEE_DSCNT_BILL_DISP_CD   = k.INVC_FEE_DSCNT_BILL_DISP_CD
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.BILL_INVC_ID,
           w2.FEE_DSCNT_ID,
           w2.INVC_FEE_DSCNT_SRC_CD,
           w2.INVC_FEE_DSCNT_BILL_DISP_CD,
           {CurrRunCycle}
    FROM {IDSOwner}.K_INVC_FEE_DSCNT_TEMP w2
    WHERE NOT EXISTS (
        SELECT k2.INVC_FEE_DSCNT_SK
        FROM {IDSOwner}.K_INVC_FEE_DSCNT k2
        WHERE w2.SRC_SYS_CD_SK               = k2.SRC_SYS_CD_SK
          AND w2.BILL_INVC_ID                = k2.BILL_INVC_ID
          AND w2.FEE_DSCNT_ID                = k2.FEE_DSCNT_ID
          AND w2.INVC_FEE_DSCNT_SRC_CD       = k2.INVC_FEE_DSCNT_SRC_CD
          AND w2.INVC_FEE_DSCNT_BILL_DISP_CD = k2.INVC_FEE_DSCNT_BILL_DISP_CD
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
    # Stage: PrimaryKey (transformer)
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt",
                    when(col("INVC_FEE_DSCNT_SK") == -1, lit("I"))
                    .otherwise(lit("U")))
        .withColumn("svBillInvcId", trim(col("BILL_INVC_ID")))
        .withColumn("svFeeDscntId", trim(col("FEE_DSCNT_ID")))
        .withColumn("svCrtRunCycExctnSk",
                    when(col("svInstUpdt") == "I", lit(CurrRunCycle))
                    .otherwise(col("CRT_RUN_CYC_EXCTN_SK")))
        .withColumn(
            "INVC_FEE_DSCNT_SK",
            when(col("svInstUpdt") == "I", lit(None)).otherwise(col("INVC_FEE_DSCNT_SK"))
        )
        .withColumn("SRC_SYS_CD", lit(SrcSysCd))
        .withColumnRenamed("svInstUpdt", "INSRT_UPDT_CD")
        .withColumnRenamed("svBillInvcId", "BILL_INVC_ID")
        .withColumnRenamed("svFeeDscntId", "FEE_DSCNT_ID")
        .withColumnRenamed("svCrtRunCycExctnSk", "CRT_RUN_CYC_EXCTN_SK")
    )
    # Surrogate-key generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        'INVC_FEE_DSCNT_SK',
        <schema>,
        <secret_name>
    )
    # --------------------------------------------------
    # Build output links from PrimaryKey
    df_updt = df_enriched.select(
        "SRC_SYS_CD",
        "BILL_INVC_ID",
        "FEE_DSCNT_ID",
        "INVC_FEE_DSCNT_SRC_CD",
        "INVC_FEE_DSCNT_BILL_DISP_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "INVC_FEE_DSCNT_SK"
    )
    df_NewKeys = (
        df_enriched
        .filter(col("INSRT_UPDT_CD") == lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "BILL_INVC_ID",
            "FEE_DSCNT_ID",
            "INVC_FEE_DSCNT_SRC_CD",
            "INVC_FEE_DSCNT_BILL_DISP_CD",
            "CRT_RUN_CYC_EXCTN_SK",
            "INVC_FEE_DSCNT_SK"
        )
    )
    df_Keys = df_enriched.select(
        "INVC_FEE_DSCNT_SK",
        "SRC_SYS_CD_SK",
        "BILL_INVC_ID",
        "FEE_DSCNT_ID",
        "INVC_FEE_DSCNT_SRC_CD",
        "INVC_FEE_DSCNT_BILL_DISP_CD",
        "SRC_SYS_CD",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK"
    )
    # --------------------------------------------------
    # Target: hf_invc_fee_dscnt  (scenario c – parquet)
    parquet_path_hf = f"{adls_path}/IncmInvcFeeDscntPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path_hf,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )
    # Target: K_INVC_FEE_DSCNT  (sequential file)
    seq_path = f"{adls_path}/load/K_INVC_FEE_DSCNT.dat"
    write_files(
        df_NewKeys,
        seq_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )
    # --------------------------------------------------
    # Stage: Merge (transformer)
    ac = df_AllCol_clean.alias("ac")
    k  = df_Keys.alias("k")
    join_expr = (
        (ac.SRC_SYS_CD_SK               == k.SRC_SYS_CD_SK) &
        (ac.BILL_INVC_ID                == k.BILL_INVC_ID) &
        (ac.PMFA_ID                     == k.FEE_DSCNT_ID) &
        (ac.BLFD_SOURCE                 == k.INVC_FEE_DSCNT_SRC_CD) &
        (ac.BLFD_DISP_CD               == k.INVC_FEE_DSCNT_BILL_DISP_CD)
    )
    df_merge = (
        ac.join(k, join_expr, "left")
    )
    df_Key = df_merge.select(
        col("ac.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("ac.DISCARD_IN").alias("DISCARD_IN"),
        col("ac.PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("ac.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ac.ERR_CT").alias("ERR_CT"),
        col("ac.RECYCLE_CT").alias("RECYCLE_CT"),
        col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("ac.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("k.INVC_FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
        col("ac.PMFA_ID").alias("FEE_DSCNT_ID"),
        col("ac.PMFA_ID").alias("PMFA_ID"),
        col("ac.BLFD_SOURCE").alias("BLFD_SOURCE"),
        col("ac.BLFD_DISP_CD").alias("BLFD_DISP_CD"),
        col("ac.PMFA_FEE_DISC_IND").alias("PMFA_FEE_DISC_IND"),
        col("ac.GRGR_ID").alias("GRGR_ID"),
        col("ac.SGSG_ID").alias("SGSG_ID"),
        col("ac.SBSB_ID").alias("SBSB_ID"),
        col("ac.INVC_SK").alias("INVC_SK"),
        col("ac.BILL_INVC_ID").alias("BILL_INVC_ID"),
        col("ac.GRP_ID").alias("GRP_ID"),
        col("ac.INVC_ID").alias("INVC_ID"),
        col("ac.SUBGRP_ID").alias("SUBGRP_ID"),
        col("ac.DSCNT_IN").alias("DSCNT_IN"),
        col("ac.FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
        col("ac.SBSB_CK").alias("SBSB_CK"),
        col("ac.CSCS_ID").alias("CSCS_ID"),
        col("ac.ELIG_CSPI_ID").alias("CLS_PLN_ID"),
        col("ac.ELIG_PDPD_ID").alias("PROD_ID"),
        col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
    # --------------------------------------------------
    return df_Key

# COMMAND ----------