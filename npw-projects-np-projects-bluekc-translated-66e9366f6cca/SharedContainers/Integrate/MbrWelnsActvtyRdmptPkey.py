# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

JOB NAME:  MbrWelnsActvtyRdmptPkey

DESCRIPTION:   Creates Primary key for IDS MBR_WELNS_ACTVTY_RDMPT table

PROCESSING:    Used in jobs HallmarkInsightsMbrWelnsActvtyRdmptExtr
               
MODIFICATIONS:
Developer           Date                         Project #      Change Description                                        Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------               ------------------------------------       ----------------------------      -------------------------
Judy Reynolds    2009-12-23               4319              Initial program                                                IntegrateWrhsDevl             Steph Goddard          02/08/2010
Judy Reynolds    2010-05-11               TTR_808       modified to add additional fields to                IntegrateWrhsDevl             Steph Goddard          05/13/2010
                                                                                  key file
Kalyan Neelam     2012-10-03              4830                Added 3 new columns on end                      IntegrateNewDevl 
                                                                                    TOT_CARD_CT,                   
                                                                                    TOT_ADM_CST_AMT,TOT_TAX_AMT
Annotations:
This container is used in:
HallmarkInsightMbrWelnsActvtyRdmptExtr IdsMbrWelnsActvtyRdmptExtr

This program will need to be re-compiled when logic changes
Assign primary surrogate key
"""

from pyspark.sql import DataFrame, functions as F


def run_MbrWelnsActvtyRdmptPkey(df_Transform: DataFrame, params: dict) -> DataFrame:
    # ---------------------------------------------------------------------
    # Unpack parameters
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    CurrRunCycle = params["CurrRunCycle"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]
    # ---------------------------------------------------------------------
    # Read dummy table that replaces hash file hf_mbr_welns_actvty_rdmpt
    extract_query = f"""
    SELECT
      SRC_SYS_CD                     AS SRC_SYS_CD_lkp,
      MBR_UNIQ_KEY                   AS MBR_UNIQ_KEY_lkp,
      RDMPT_DT_SK                    AS RDMPT_DT_SK_lkp,
      CONFIRM_NO                     AS CONFIRM_NO_lkp,
      CRT_RUN_CYC_EXCTN_SK           AS CRT_RUN_CYC_EXCTN_SK_lkp,
      MBR_WELNS_ACTVTY_RDMPT_SK      AS MBR_WELNS_ACTVTY_RDMPT_SK_lkp
    FROM {IDSOwner}.dummy_hf_mbr_welns_actvty_rdmpt
    """
    df_hf_mbr_welns_actvty_rdmpt_lkup = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )
    # ---------------------------------------------------------------------
    # Join input with lookup
    df_enriched = (
        df_Transform.alias("tr")
        .join(
            df_hf_mbr_welns_actvty_rdmpt_lkup.alias("lkp"),
            (F.col("tr.SRC_SYS_CD") == F.col("lkp.SRC_SYS_CD_lkp"))
            & (F.col("tr.MBR_UNIQ_KEY") == F.col("lkp.MBR_UNIQ_KEY_lkp"))
            & (F.col("tr.RDMPT_DT_SK") == F.col("lkp.RDMPT_DT_SK_lkp"))
            & (F.col("tr.CONFIRM_NO") == F.col("lkp.CONFIRM_NO_lkp")),
            "left",
        )
    )
    # ---------------------------------------------------------------------
    # Carry over existing surrogate key from lookup
    df_enriched = df_enriched.withColumn(
        "MBR_WELNS_ACTVTY_RDMPT_SK",
        F.col("MBR_WELNS_ACTVTY_RDMPT_SK_lkp")
    )
    # Generate surrogate keys for missing rows
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "MBR_WELNS_ACTVTY_RDMPT_SK",
        <schema>,
        <secret_name>
    )
    # ---------------------------------------------------------------------
    # Derive NewCrtRunCycExtcnSk
    df_enriched = df_enriched.withColumn(
        "NewCrtRunCycExtcnSk",
        F.when(
            F.col("MBR_WELNS_ACTVTY_RDMPT_SK_lkp").isNull(),
            F.lit(CurrRunCycle)
        ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK_lkp"))
    )
    # ---------------------------------------------------------------------
    # Prepare rows to insert back into dummy table (was hash-file write)
    df_updt = (
        df_enriched
        .filter(F.col("MBR_WELNS_ACTVTY_RDMPT_SK_lkp").isNull())
        .select(
            F.col("SRC_SYS_CD"),
            F.col("MBR_UNIQ_KEY"),
            F.col("RDMPT_DT_SK"),
            F.col("CONFIRM_NO"),
            F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("MBR_WELNS_ACTVTY_RDMPT_SK")
        )
    )
    (
        df_updt.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.dummy_hf_mbr_welns_actvty_rdmpt")
        .mode("append")
        .save()
    )
    # ---------------------------------------------------------------------
    # Build final output DataFrame
    df_key = df_enriched.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "MBR_WELNS_ACTVTY_RDMPT_SK",
        F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        "SRC_SYS_CD_SK",
        "MBR_UNIQ_KEY",
        "MBR_SK",
        "RDMPT_DT_SK",
        "CONFIRM_NO",
        "RDMPT_PT_AMT",
        "TOT_CARD_CT",
        "TOT_ADM_CST_AMT",
        "TOT_TAX_AMT"
    )
    # ---------------------------------------------------------------------
    return df_key