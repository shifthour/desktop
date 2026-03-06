# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""*****************************************************************************************************************************************************************************
COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

JOB NAME:     IdsDntlClmLnPkey

DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.

INPUTS:  Sequential file in common record format created in transform job.

HASH FILES:    hf_clm_ln - this is both read from and written to.   

TRANSFORMS:  

PROCESSING:

OUTPUTS: Sequential file in common record format.

MODIFICATIONS:

            Steph Goddard  03/09/2006   changed to use hf_clm_ln as primary key lookup hash file
            Brent Leland      04/14/2006   Change hf_clm_ln back to original temporarily

MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Parik                 2008-08-05              Primary Key process change                                                      3567(Primary Key)               devlIDS         Steph Goddard          08/12/2008
RHenry              2012-05-01              cleaned up display and data elements in in/output             4896 Remediation                                           SAndrew                    2012-05-18
"""

# This container is used in:
# FctsDntlClmLineExtr
# 
# These programs need to be re-compiled when logic changes
# This job uses hf_clm_ln as the primary key hash file since it has the same natural key as the CLM_LN table
# Primary key hash file created and loaded in FctsClmLnPKExtr, NascoClmLnPKExtr, PCSClmLnPKExtr and ArgusClmLnPKExtr processes

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_DntlClmLnPK(df_Transform: DataFrame, params: dict) -> DataFrame:
    CurrRunCycle = params["CurrRunCycle"]
    adls_path = params["adls_path"]

    lkup_file_path = f"{adls_path}/DntlClmLnPK_lkup.parquet"
    df_lkup = spark.read.parquet(lkup_file_path)

    df_enriched = (
        df_Transform.alias("transform")
        .join(
            df_lkup.alias("lkup"),
            (
                (F.col("transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"))
                & (F.col("transform.CLM_ID") == F.col("lkup.CLM_ID"))
                & (F.col("transform.CLM_LN_SEQ_NO") == F.col("lkup.CLM_LN_SEQ_NO"))
            ),
            "left",
        )
        .withColumn(
            "SK",
            F.when(F.col("lkup.CLM_LN_SK").isNull(), F.lit(0)).otherwise(
                F.col("lkup.CLM_LN_SK")
            ),
        )
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(
                F.col("lkup.CLM_LN_SK").isNull(), F.lit(CurrRunCycle)
            ).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK")),
        )
        .withColumn(
            "INSRT_UPDT_CD",
            F.when(
                F.col("transform.CRT_RUN_CYC_EXCTN_SK")
                == F.col("transform.LAST_UPDT_RUN_CYC_EXCTN_SK"),
                F.lit("I"),
            ).otherwise(F.lit("U")),
        )
    )

    df_output = df_enriched.select(
        F.col("transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD"),
        F.col("transform.DISCARD_IN").alias("DISCARD_IN"),
        F.col("transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("transform.ERR_CT").alias("ERR_CT"),
        F.col("transform.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("SK").alias("DNTL_CLM_LN_SK"),
        F.col("transform.CLM_ID").alias("CLM_ID"),
        F.col("transform.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("transform.ALT_PROC_CD").alias("ALT_PROC_CD"),
        F.col("transform.DNTL_CLM_LN_DNTL_CAT_CD").alias("DNTL_CLM_LN_DNTL_CAT_CD"),
        F.col("transform.DNTL_CLM_LN_DNTL_CAT_RULE_CD").alias(
            "DNTL_CLM_LN_DNTL_CAT_RULE_CD"
        ),
        F.col("transform.DNTL_CLM_LN_ALT_PROC_EXCD").alias(
            "DNTL_CLM_LN_ALT_PROC_EXCD"
        ),
        F.col("transform.DNTL_CLM_LN_BNF_TYP_CD").alias("DNTL_CLM_LN_BNF_TYP_CD"),
        F.col("transform.DNTL_CLM_LN_TOOTH_SRFC_CD").alias(
            "DNTL_CLM_LN_TOOTH_SRFC_CD"
        ),
        F.col("transform.DNTL_CLM_LN_UTIL_EDIT_CD").alias(
            "DNTL_CLM_LN_UTIL_EDIT_CD"
        ),
        F.col("transform.CAT_PAYMT_PFX_ID").alias("CAT_PAYMT_PFX_ID"),
        F.col("transform.DNTL_PROC_PAYMT_PFX_ID").alias(
            "DNTL_PROC_PAYMT_PFX_ID"
        ),
        F.col("transform.DNTL_PROC_PRICE_ID").alias("DNTL_PROC_PRICE_ID"),
        F.col("transform.TOOTH_BEG_NO").alias("TOOTH_BEG_NO"),
        F.col("transform.TOOTH_END_NO").alias("TOOTH_END_NO"),
        F.col("transform.TOOTH_NO").alias("TOOTH_NO"),
    )

    return df_output