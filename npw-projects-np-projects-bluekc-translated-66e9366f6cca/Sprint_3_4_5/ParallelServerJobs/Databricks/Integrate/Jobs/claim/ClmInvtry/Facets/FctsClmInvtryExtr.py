# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsClmInvtryExtr
# MAGIC CALLED BY:  OpsDashboardClmExtrSeq
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:   Pulls the Claim Inventory information from Facets database which populates the Claim Inventory table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                 Change Description                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------      ---------------------------------------------------------                      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              12/21/2007          3531                            Original Programming                                                 devlIDS30                Steph Goddard            01/04/2008
# MAGIC Abhiram Dasarathy           07/27/2015          5407	                  Added Column ASG_USER_SK to the end of the file       EnterpriseDev1          Kalyan Neelam             2015-07-29
# MAGIC Prabhu ES                        02/26/2022          S2S Remediation        MSSQL connection parameters added                     IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC Pulling Facets Claim Inventory Data
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC Writing Sequential File to /key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, concat, when, expr, date_format, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmInvtryPkey
# COMMAND ----------

CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
CurrDate = get_widget_value("CurrDate","")
FacetsOwner = get_widget_value("$FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

# Read from hashed file (scenario C -> parquet)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# Read from ODBC (CMC_CLCL_CLAIM)
extract_query = (
    f"SELECT \n"
    f"CLAIM.CLCL_ID,\n"
    f"CLAIM.CLCL_CL_TYPE,\n"
    f"CLAIM.CLCL_CL_SUB_TYPE,\n"
    f"CLAIM.CLCL_INPUT_DT,\n"
    f"CLAIM.CLCL_RECD_DT,\n"
    f"CLAIM.PDPD_ID,\n"
    f"CLAIM.PRPR_ID,\n"
    f"GRP.GRGR_ID,\n"
    f"STATUS.CLST_STS,\n"
    f"STATUS.CLST_MCTR_REAS\n"
    f"FROM \n"
    f"{FacetsOwner}.CMC_CLCL_CLAIM CLAIM,\n"
    f"{FacetsOwner}.CMC_GRGR_GROUP GRP,\n"
    f"{FacetsOwner}.CMC_CLST_STATUS STATUS\n"
    f"WHERE\n"
    f"CLAIM.CLCL_INPUT_DT >= '2007-01-01'\n"
    f"AND CLAIM.CLCL_CUR_STS IN ('11','15') \n"
    f"AND CLAIM.GRGR_CK = GRP.GRGR_CK \n"
    f"AND GRP.GRGR_ID = 'NASCOPAR'\n"
    f"AND CLAIM.CLCL_ID = STATUS.CLCL_ID \n"
    f"AND CLAIM.CLST_SEQ_NO = STATUS.CLST_SEQ_NO"
)

df_CMC_CLCL_CLAIM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# StripField Transformer
df_stripField = df_CMC_CLCL_CLAIM.select(
    strip_field(col("CLCL_ID")).alias("CLCL_ID"),
    strip_field(col("CLCL_CL_TYPE")).alias("CLCL_CL_TYPE"),
    strip_field(col("CLCL_CL_SUB_TYPE")).alias("CLCL_CL_SUB_TYPE"),
    col("CLCL_INPUT_DT").alias("CLCL_INPUT_DT"),
    col("CLCL_RECD_DT").alias("CLCL_RECD_DT"),
    strip_field(col("PDPD_ID")).alias("PDPD_ID"),
    strip_field(col("PRPR_ID")).alias("PRPR_ID"),
    strip_field(col("GRGR_ID")).alias("GRGR_ID"),
    strip_field(col("CLST_STS")).alias("CLST_STS"),
    strip_field(col("CLST_MCTR_REAS")).alias("CLST_MCTR_REAS")
)

# BusinessRules Transformer (with lookup from nasco_dup_lkup)
df_BusinessRules_base = (
    df_stripField.alias("Strip")
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        col("Strip.CLCL_ID") == col("nasco_dup_lkup.CLM_ID"),
        how="left"
    )
    .withColumn("PassThru", lit("Y"))
    .withColumn("ClclId", trim(col("Strip.CLCL_ID")))
    .withColumn("ClstMctrReas", trim(col("Strip.CLST_MCTR_REAS")))
    .withColumn("svDtSk", date_format(col("Strip.CLCL_INPUT_DT"), "yyyy-MM-dd"))
)

df_Transform = (
    df_BusinessRules_base
    .filter(col("nasco_dup_lkup.CLM_ID").isNull())
    .select(
        lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        lit("I").alias("INSRT_UPDT_CD"),
        lit("N").alias("DISCARD_IN"),
        col("PassThru").alias("PASS_THRU_IN"),
        lit(CurrDate).alias("FIRST_RECYC_DT"),
        lit(0).alias("ERR_CT"),
        lit(0).alias("RECYCLE_CT"),
        lit("FACETS").alias("SRC_SYS_CD"),
        concat(lit("FACETS"), lit(";"), col("ClclId")).alias("PRI_KEY_STRING"),
        lit(0).alias("CLM_INVTRY_SK"),
        col("ClclId").alias("CLM_INVTRY_KEY_ID"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        trim(col("Strip.GRGR_ID")).alias("GRP_ID"),
        lit("NASCO").alias("OPS_WORK_UNIT_ID"),
        trim(col("Strip.PDPD_ID")).alias("PDPD_ID"),
        trim(col("Strip.PRPR_ID")).alias("PRPR_ID"),
        expr(
            "CASE WHEN substring(ClstMctrReas,1,1)='C' THEN 'C' "
            "WHEN substring(ClstMctrReas,1,1)='H' THEN 'H' "
            "WHEN substring(ClstMctrReas,1,1)='M' THEN 'M' ELSE 'O' END"
        ).alias("CLM_INVTRY_PEND_CAT_CD"),
        col("ClstMctrReas").alias("CLM_STTUS_CHG_RSN_CD"),
        trim(col("Strip.CLST_STS")).alias("CLST_STS"),
        trim(col("Strip.CLCL_CL_SUB_TYPE")).alias("CLCL_CL_SUB_TYPE"),
        trim(col("Strip.CLCL_CL_TYPE")).alias("CLCL_CL_TYPE"),
        col("svDtSk").alias("INPT_DT_SK"),
        date_format(col("Strip.CLCL_RECD_DT"), "yyyy-MM-dd").alias("RCVD_DT_SK"),
        lit(CurrDate).alias("EXTR_DT_SK"),
        col("svDtSk").alias("STTUS_DT_SK"),
        lit(1).alias("INVTRY_CT"),
        lit(1).alias("ASG_USER_SK"),
        lit(1).alias("WORK_ITEM_CT")
    )
)

# Call Shared Container: ClmInvtryPkey
params_ClmInvtryPkey = {
    "DriverTable": "#DriverTable#",
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "CurrDate": CurrDate,
    "$FacetsDB": "#$FacetsDB#",
    "$FacetsOwner": FacetsOwner
}
df_ClmInvtryPkey = ClmInvtryPkey(df_Transform, params_ClmInvtryPkey)

# Final output file (CSeqFileStage)
df_final = (
    df_ClmInvtryPkey
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
        "CLM_INVTRY_SK",
        "CLM_INVTRY_KEY_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "GRP_ID",
        "OPS_WORK_UNIT_ID",
        "PDPD_ID",
        "PRPR_ID",
        "CLM_INVTRY_PEND_CAT_CD",
        "CLM_STTUS_CHG_RSN_CD",
        "CLST_STS",
        "CLCL_CL_SUB_TYPE",
        "CLCL_CL_TYPE",
        "INPT_DT_SK",
        "RCVD_DT_SK",
        "EXTR_DT_SK",
        "STTUS_DT_SK",
        "INVTRY_CT",
        "ASG_USER_SK",
        "WORK_ITEM_CT"
    )
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("PDPD_ID", rpad(col("PDPD_ID"), 8, " "))
    .withColumn("PRPR_ID", rpad(col("PRPR_ID"), 12, " "))
    .withColumn("CLST_STS", rpad(col("CLST_STS"), 2, " "))
    .withColumn("CLCL_CL_SUB_TYPE", rpad(col("CLCL_CL_SUB_TYPE"), 1, " "))
    .withColumn("CLCL_CL_TYPE", rpad(col("CLCL_CL_TYPE"), 1, " "))
    .withColumn("INPT_DT_SK", rpad(col("INPT_DT_SK"), 10, " "))
    .withColumn("RCVD_DT_SK", rpad(col("RCVD_DT_SK"), 10, " "))
    .withColumn("EXTR_DT_SK", rpad(col("EXTR_DT_SK"), 10, " "))
    .withColumn("STTUS_DT_SK", rpad(col("STTUS_DT_SK"), 10, " "))
)

output_path = f"{adls_path}/key/FctsClmInvtryExtr.ClmInvtry.dat.{RunID}"
write_files(
    df_final,
    output_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)