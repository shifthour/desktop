# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsXcClmInvtryExtr
# MAGIC CALLED BY:  OpsDashboardClmExtrSeq
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:   Pulls the Claim Inventory information from Facets XC database which populates the Claim Inventory table
# MAGIC 
# MAGIC FACETS XC is used as the source since this is the primary source of extract for this job. No default settings for Facets need to be used.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                 Change Description                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------      ---------------------------------------------------------                      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              12/18/2007          3531                            Original Programming                                                 devlIDS30                Steph Goddard            01/04/2008
# MAGIC Abhiram Dasarathy           07/27/2015          5407	                  Added Column ASG_USER_SK to the end of the file       EnterpriseDev1          Kalyan Neelam             2015-07-29
# MAGIC Prabhu ES                       02/26/2022           S2S                             MSSQL connection parameters added                      IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC This process takes place to get all group id's since they do not exist in Facets XC
# MAGIC Pulling Facets XC Claim Inventory Data
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmInvtryPkey
# COMMAND ----------

CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
CurrDate = get_widget_value("CurrDate","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
FacetsXCOwner = get_widget_value("FacetsXCOwner","")
facetsxc_secret_name = get_widget_value("facetsxc_secret_name","")
UWSOwner = get_widget_value("UWSOwner","")
uws_secret_name = get_widget_value("uws_secret_name","")

jdbc_url_facetsxc, jdbc_props_facetsxc = get_db_config(facetsxc_secret_name)
extract_query_claim = """
SELECT 
CLAIM.CLCL_ID,
CLAIM.GRGR_CK,
CLAIM.CLCL_CL_TYPE,
CLAIM.CLCL_CL_SUB_TYPE,
CLAIM.CLCL_INPUT_DT,
CLAIM.CLCL_RECD_DT,
CLAIM.PDPD_ID,
CLAIM.PRPR_ID,
STATUS.CLST_STS
FROM {0}.CMC_CLCL_CLAIM CLAIM,
{0}.CMC_CLST_STATUS STATUS
WHERE
CLAIM.CLCL_INPUT_DT >= '2007-01-01'
AND CLAIM.CLCL_ID = STATUS.CLCL_ID
AND CLAIM.CLST_SEQ_NO = STATUS.CLST_SEQ_NO
AND STATUS.CLST_STS <> '16'
""".format(FacetsXCOwner)
df_extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facetsxc)
    .options(**jdbc_props_facetsxc)
    .option("query", extract_query_claim)
    .load()
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids = """
SELECT 
GRP.GRP_UNIQ_KEY as GRP_UNIQ_KEY,
GRP.GRP_ID as GRP_ID
FROM {0}.GRP GRP
""".format(IDSOwner)
df_grp_extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)
df_grp_extr_dedup = dedup_sort(df_grp_extr, ["GRP_UNIQ_KEY"], [])

df_extract_with_joinkey = df_extract.withColumn("GRGR_CK_stripped", strip_field(F.col("GRGR_CK")))
df_strip_joined = df_extract_with_joinkey.join(
    df_grp_extr_dedup,
    df_extract_with_joinkey["GRGR_CK_stripped"] == df_grp_extr_dedup["GRP_UNIQ_KEY"],
    "left"
)
df_strip = df_strip_joined.select(
    F.expr("strip_field(CLCL_ID)").alias("CLCL_ID"),
    F.expr("strip_field(CLCL_CL_TYPE)").alias("CLCL_CL_TYPE"),
    F.expr("strip_field(CLCL_CL_SUB_TYPE)").alias("CLCL_CL_SUB_TYPE"),
    F.col("CLCL_INPUT_DT").alias("CLCL_INPUT_DT"),
    F.col("CLCL_RECD_DT").alias("CLCL_RECD_DT"),
    F.expr("strip_field(PDPD_ID)").alias("PDPD_ID"),
    F.expr("strip_field(PRPR_ID)").alias("PRPR_ID"),
    F.expr("strip_field(CLST_STS)").alias("CLST_STS"),
    F.when(F.col("GRP_UNIQ_KEY").isNull(), F.lit("NA")).otherwise(F.col("GRP_ID")).alias("GRP_ID")
)

jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
extract_query_uws = """
SELECT 
WORK_UNIT.GRP_ID,
WORK_UNIT.OPS_WORK_UNIT_ID
FROM {0}.OPS_GRP_WORK_UNIT_XREF WORK_UNIT
""".format(UWSOwner)
df_work_unit = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", extract_query_uws)
    .load()
)
df_work_unit_dedup = dedup_sort(df_work_unit, ["GRP_ID"], [])

df_bizrules_join = df_strip.join(df_work_unit_dedup, df_strip["GRP_ID"] == df_work_unit_dedup["GRP_ID"], "left")

df_bizrules_vars = df_bizrules_join.withColumn("PassThru", F.lit("Y")) \
    .withColumn("ClclId", trim(F.col("CLCL_ID"))) \
    .withColumn("PdpdId", trim(F.col("PDPD_ID"))) \
    .withColumn("svDtSk", F.date_format(F.col("CLCL_INPUT_DT"), "yyyy-MM-dd"))

ops_unit_expr = (
    F.when(F.col("GRP_ID") == "10025000", F.lit("Group25"))
    .when(F.substring(F.col("ClclId"), 6, 1).isin("H", "K"), F.lit("ITSHome"))
    .when(F.col("OPS_WORK_UNIT_ID").isNotNull(), F.col("OPS_WORK_UNIT_ID"))
    .when(
        F.substring(F.col("PdpdId"), 1, 3).isin("MSK", "MSM", "MSX", "MVM", "MVC", "TCK", "TCM", "TCX", "TPK", "TPM", "TPX"),
        F.lit("TIP")
    )
    .when(
        F.substring(F.col("PdpdId"), 1, 2).isin("MG"),
        F.lit("TIP")
    )
    .when(
        F.substring(F.col("PdpdId"), 1, 2).isin("PB", "PC", "PF"),
        F.lit("PPO")
    )
    .when(
        F.substring(F.col("PdpdId"), 1, 2) == "BM",
        F.lit("BA+")
    )
    .when(
        F.substring(F.col("PdpdId"), 1, 2) == "BA",
        F.lit("BA")
    )
    .when(
        F.substring(F.col("PdpdId"), 1, 2) == "BC",
        F.lit("BC")
    )
    .when(
        F.substring(F.col("PdpdId"), 1, 1) == "D",
        F.lit("Dental")
    )
    .otherwise(F.lit("Unidentified"))
)

df_business_rules = df_bizrules_vars.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETSXC").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETSXC"), F.lit(";"), F.col("ClclId")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_INVTRY_SK"),
    F.col("ClclId").alias("CLM_INVTRY_KEY_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    trim(F.col("GRP_ID")).alias("GRP_ID"),
    ops_unit_expr.alias("OPS_WORK_UNIT_ID"),
    F.col("PdpdId").alias("PDPD_ID"),
    trim(F.col("PRPR_ID")).alias("PRPR_ID"),
    F.lit("NA").alias("CLM_INVTRY_PEND_CAT_CD"),
    F.lit("NA").alias("CLM_STTUS_CHG_RSN_CD"),
    trim(F.col("CLST_STS")).alias("CLST_STS"),
    trim(F.col("CLCL_CL_SUB_TYPE")).alias("CLCL_CL_SUB_TYPE"),
    trim(F.col("CLCL_CL_TYPE")).alias("CLCL_CL_TYPE"),
    F.col("svDtSk").alias("INPT_DT_SK"),
    F.date_format(F.col("CLCL_RECD_DT"), "yyyy-MM-dd").alias("RCVD_DT_SK"),
    F.lit(CurrDate).alias("EXTR_DT_SK"),
    F.col("svDtSk").alias("STTUS_DT_SK"),
    F.lit(1).alias("INVTRY_CT"),
    F.lit(1).alias("ASG_USER_SK"),
    F.lit(1).alias("WORK_ITEM_CT")
)

params = {
    "DriverTable": "#DriverTable#",
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "CurrDate": CurrDate,
    "FacetsDB": "<...>",
    "FacetsOwner": "<...>"
}
df_key = ClmInvtryPkey(df_business_rules, params)

final_cols = [
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
]

df_final = df_key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_INVTRY_SK"),
    F.col("CLM_INVTRY_KEY_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_ID"),
    F.col("OPS_WORK_UNIT_ID"),
    F.rpad(F.col("PDPD_ID"),8," ").alias("PDPD_ID"),
    F.rpad(F.col("PRPR_ID"),12," ").alias("PRPR_ID"),
    F.col("CLM_INVTRY_PEND_CAT_CD"),
    F.col("CLM_STTUS_CHG_RSN_CD"),
    F.rpad(F.col("CLST_STS"),2," ").alias("CLST_STS"),
    F.rpad(F.col("CLCL_CL_SUB_TYPE"),1," ").alias("CLCL_CL_SUB_TYPE"),
    F.rpad(F.col("CLCL_CL_TYPE"),1," ").alias("CLCL_CL_TYPE"),
    F.rpad(F.col("INPT_DT_SK"),10," ").alias("INPT_DT_SK"),
    F.rpad(F.col("RCVD_DT_SK"),10," ").alias("RCVD_DT_SK"),
    F.rpad(F.col("EXTR_DT_SK"),10," ").alias("EXTR_DT_SK"),
    F.rpad(F.col("STTUS_DT_SK"),10," ").alias("STTUS_DT_SK"),
    F.col("INVTRY_CT"),
    F.col("ASG_USER_SK"),
    F.col("WORK_ITEM_CT")
)

write_files(
    df_final,
    f"{adls_path}/key/FctsXcClmInvtryExtr.ClmInvtry.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parqet=False,
    header=False,
    quote="\"",
    nullValue=None
)