# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Shared Container: MbrRiskMesrPkey
Description:
* VC LOGS *
^1_2 06/18/08 12:12:12 Batch  14780_43935 PROMOTE bckcetl ids20 dsadm BLS FOR SA
^1_2 06/18/08 11:57:41 Batch  14780_43064 INIT bckcett testIDS dsadm bls for sa
^1_2 06/17/08 13:24:03 Batch  14779_48263 PROMOTE bckcett testIDS u03651 Steffs promote
^1_2 06/17/08 12:43:46 Batch  14779_45832 INIT bckcett devlIDS u03651 steffy
^1_1 05/25/08 15:48:27 Batch  14756_56911 INIT bckcett devlIDS u03651 steffy

Primary Key logic for IDS Risk Cat Table.

Modifications                                                                             code walkthru           date

Bhoomi Dasari    05/2008     Created                                        Steph Goddrad         06/17/2008
Annotations:
If primary key found, assign surragote key, otherwise get next key and update hash file.
This container is used in:
ImpProMbrRiskMesrExtr
McSourcePreProcMbrRiskMesrExtr

These programs need to be re-compiled when logic changes
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def run_MbrRiskMesrPkey(df_Transform: DataFrame, params: dict) -> DataFrame:
    CurrRunCycle = params["CurrRunCycle"]
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]

    extract_query = f"""
    SELECT
        SRC_SYS_CD as SRC_SYS_CD_lkp,
        MBR_UNIQ_KEY as MBR_UNIQ_KEY_lkp,
        RISK_CAT_ID as RISK_CAT_ID_lkp,
        PRCS_YR_MO_SK as PRCS_YR_MO_SK_lkp,
        CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK_lkp,
        MBR_RISK_MESR_SK as MBR_RISK_MESR_SK_lkp
    FROM {IDSOwner}.dummy_hf_mbr_risk_mesr
    """

    df_lkup = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    join_cond = (
        (F.col("transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD_lkp")) &
        (F.col("transform.MBR_UNIQ_KEY") == F.col("lkup.MBR_UNIQ_KEY_lkp")) &
        (F.col("transform.RISK_CAT_ID") == F.col("lkup.RISK_CAT_ID_lkp")) &
        (F.col("transform.PRCS_YR_MO_SK") == F.col("lkup.PRCS_YR_MO_SK_lkp"))
    )

    df_enriched = (
        df_Transform.alias("transform")
        .join(df_lkup.alias("lkup"), join_cond, "left")
        .withColumn("MBR_RISK_MESR_SK", F.col("MBR_RISK_MESR_SK_lkp"))
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(F.col("MBR_RISK_MESR_SK_lkp").isNull(), F.lit(CurrRunCycle))
            .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MBR_RISK_MESR_SK",<schema>,<secret_name>)

    df_updt = (
        df_enriched.filter(F.col("MBR_RISK_MESR_SK_lkp").isNull())
        .select(
            "SRC_SYS_CD",
            "MBR_UNIQ_KEY",
            "RISK_CAT_ID",
            "PRCS_YR_MO_SK",
            F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            "MBR_RISK_MESR_SK"
        )
    )

    (
        df_updt.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.dummy_hf_mbr_risk_mesr")
        .mode("append")
        .save()
    )

    df_key = (
        df_enriched.select(
            "JOB_EXCTN_RCRD_ERR_SK",
            "INSRT_UPDT_CD",
            "DISCARD_IN",
            "PASS_THRU_IN",
            "FIRST_RECYC_DT",
            "ERR_CT",
            "RECYCLE_CT",
            "SRC_SYS_CD",
            "PRI_KEY_STRING",
            "MBR_RISK_MESR_SK",
            "MBR_UNIQ_KEY",
            "RISK_CAT_ID",
            "PRCS_YR_MO_SK",
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "MBR_CK",
            "MBR_MED_MESRS_CD",
            "RISK_CAT_SK",
            "FTR_RELTV_RISK_NO"
        )
    )

    return df_key
# COMMAND ----------