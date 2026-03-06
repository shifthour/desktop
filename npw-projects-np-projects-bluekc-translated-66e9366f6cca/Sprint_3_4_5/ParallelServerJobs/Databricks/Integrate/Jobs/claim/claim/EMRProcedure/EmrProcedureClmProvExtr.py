# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2021, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  EmrProcedureClmProvExtr
# MAGIC CALLED BY:  EmrProcedureClmExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  This job looks up the NTNL_PROV_ID from the IDS table PROV, and assigns SK values using a shared container.
# MAGIC 
# MAGIC                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer	Date		Project/Altiris #		Change Description						Development Project		Code Reviewer	Date Reviewed       
# MAGIC =============================================================================================================================================================================================== 
# MAGIC Rajasekhar.K	2021-10-11        	441615                               	Initial Programming    					IntegrateDev1			Jeyaprasanna       	2021-10-14     
# MAGIC Ken Bradmon	2022-12-15	us542805			Added new columns: PROC_CD_CPT, 				IntegrateDev2			Harsha Ravuri	2023-06-14			
# MAGIC 							PROC_CD_CPT_MOD_1, PROC_CD_CPT_MOD_2, 		IntegrateDev1		
# MAGIC 							PROC_CD_CPTII_MOD_2, PROC_CD_CPTII_MOD_2, 
# MAGIC 							SNOMED, and CVX.

# MAGIC This container is used in:
# MAGIC ArgusClmProvExtr
# MAGIC PCSClmProvExtr
# MAGIC NascoClmProvExtr
# MAGIC FctsClmProvExtr
# MAGIC WellDyneClmProvExtr
# MAGIC MCSourceClmProvExtr
# MAGIC MedicaidClmProvExtr
# MAGIC MedtrakClmProvExtr
# MAGIC BCBSSCClmProvExtr
# MAGIC BCBSSCMedClmProvExtr
# MAGIC BCAClmProvExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to /key
# MAGIC Job Name:  EmrProcedureClmProvExtr
# MAGIC Description:  This job looks up the NTNL_PROV_ID from the IDS table PROV, and assigns SK values using a shared container.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmProvPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
InFile_F = get_widget_value('InFile_F','')

schema_EmrProcedureClmProvLanding = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("POL_NO", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_MID_NM", StringType(), True),
    StructField("MBI", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("GNDR", StringType(), True),
    StructField("PROC_TYPE", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("RSLT_VAL", StringType(), True),
    StructField("RNDR_NTNL_PROV_ID", StringType(), True),
    StructField("RNDR_PROV_TYP", StringType(), True),
    StructField("PROC_CD_CPT", StringType(), True),
    StructField("PROC_CD_CPT_MOD_1", StringType(), True),
    StructField("PROC_CD_CPT_MOD_2", StringType(), True),
    StructField("PROC_CD_CPTII_MOD_1", StringType(), True),
    StructField("PROC_CD_CPTII_MOD_2", StringType(), True),
    StructField("SNOMED", StringType(), True),
    StructField("CVX", StringType(), True),
    StructField("SOURCE_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", StringType(), True),
    StructField("CLM_LN_SEQ", StringType(), True)
])

df_EmrProcedureClmProvLanding = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "000")
    .schema(schema_EmrProcedureClmProvLanding)
    .load(f"{adls_path}/verified/{InFile_F}")
)

extract_query_IDS_PROV_ID = f"""
SELECT NTNL_PROV_ID,PROV_ID from (
SELECT P.NTNL_PROV_ID,P.PROV_ID,p.TERM_DT_SK, dense_rank() over(partition by P.NTNL_PROV_ID order by P.TERM_DT_SK desc) as high
FROM {IDSOwner}.PROV P, {IDSOwner}.CD_MPPNG CD_MPPNG
where P.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK
and CD_MPPNG.SRC_DOMAIN_NM = 'SOURCE SYSTEM'
and CD_MPPNG.SRC_CD_NM = 'FACETS'
GROUP BY NTNL_PROV_ID,prov_id,term_dt_sk
) where high = 1
"""

jdbc_url_IDS_PROV_ID, jdbc_props_IDS_PROV_ID = get_db_config(ids_secret_name)
df_IDS_PROV_ID = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS_PROV_ID)
    .options(**jdbc_props_IDS_PROV_ID)
    .option("query", extract_query_IDS_PROV_ID)
    .load()
)

df_hf_ids_clm = df_IDS_PROV_ID.dropDuplicates(["NTNL_PROV_ID"])

df_ProvId_Trans = (
    df_EmrProcedureClmProvLanding.alias("EmrClmProv")
    .join(
        df_hf_ids_clm.alias("lnk_alpha"),
        F.col("EmrClmProv.RNDR_NTNL_PROV_ID") == F.col("lnk_alpha.NTNL_PROV_ID"),
        "left"
    )
)

df_lnk_prov1_op = df_ProvId_Trans.select(
    F.col("EmrClmProv.CLM_ID").alias("CLM_ID"),
    F.col("EmrClmProv.POL_NO").alias("POL_NO"),
    F.col("EmrClmProv.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("EmrClmProv.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("EmrClmProv.PATN_MID_NM").alias("PATN_MID_NM"),
    F.col("EmrClmProv.MBI").alias("MBI"),
    F.col("EmrClmProv.DOB").alias("DOB"),
    F.col("EmrClmProv.GNDR").alias("GNDR"),
    F.col("EmrClmProv.PROC_TYPE").alias("PROC_TYPE"),
    F.col("EmrClmProv.DT_OF_SVC").alias("DT_OF_SVC"),
    F.col("EmrClmProv.RSLT_VAL").alias("RSLT_VAL"),
    F.col("EmrClmProv.RNDR_NTNL_PROV_ID").alias("RNDR_NTNL_PROV_ID"),
    F.col("EmrClmProv.RNDR_PROV_TYP").alias("RNDR_PROV_TYP"),
    F.col("EmrClmProv.SOURCE_ID").alias("SOURCE_ID"),
    F.col("EmrClmProv.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("EmrClmProv.CLM_LN_SEQ").alias("CLM_LN_SEQ"),
    F.when(
        (F.col("lnk_alpha.PROV_ID").isNull()) | (F.trim(F.col("lnk_alpha.PROV_ID")) == ""),
        "NA"
    ).otherwise(F.col("lnk_alpha.PROV_ID")).alias("PROV_ID")
)

df_snapshot_allcol = df_lnk_prov1_op.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.lit("SVC").alias("CLM_PROV_ROLE_TYP_CD"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CLM_ID"), F.lit(";"), F.lit("SVC")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROV_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_ID"),
    F.lit("NA").alias("TAX_ID"),
    F.lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    F.lit("NA").alias("NTNL_PROV_ID")
)

df_snapshot_transform = df_lnk_prov1_op.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.lit("SVC").alias("CLM_PROV_ROLE_TYP_CD")
)

df_snapshot_snapshot = df_lnk_prov1_op.select(
    F.col("CLM_ID"),
    F.lit("SVC").alias("CLM_PROV_ROLE_TYP_CD"),
    F.col("PROV_ID")
)

params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}
df_key = ClmProvPK(df_snapshot_allcol, df_snapshot_transform, params)

df_key_final = df_key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_PROV_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_ID"),
    F.rpad(F.col("TAX_ID"), 9, " ").alias("TAX_ID"),
    F.col("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    F.col("NTNL_PROV_ID")
)

write_files(
    df_key_final,
    f"{adls_path}/key/EmrProcedureClmProvExtr_{SrcSysCd}.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_Transformer = df_snapshot_snapshot.withColumn(
    "svClmProvRole",
    GetFkeyCodes(SrcSysCd, F.lit(0), "CLAIM PROVIDER ROLE TYPE", F.col("CLM_PROV_ROLE_TYP_CD"), 'X')
)

df_B_CLM_PROV = df_Transformer.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("svClmProvRole").alias("CLM_PROV_ROLE_TYP_CD_SK"),
    F.col("PROV_ID")
)

write_files(
    df_B_CLM_PROV.select("SRC_SYS_CD_SK", "CLM_ID", "CLM_PROV_ROLE_TYP_CD_SK", "PROV_ID"),
    f"{adls_path}/load/B_CLM_PROV.{SrcSysCd}.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)