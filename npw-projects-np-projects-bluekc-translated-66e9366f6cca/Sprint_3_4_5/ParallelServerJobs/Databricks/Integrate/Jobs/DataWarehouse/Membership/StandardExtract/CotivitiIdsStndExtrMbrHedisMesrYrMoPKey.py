# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Process Description: Build primary key for  MBR_HEDIS_MESR_YR_MO table.  K table will be create to new SK value from the max value  .                       
# MAGIC PROCESSING:
# MAGIC Build primary key for  PROD_QHP table load data 
# MAGIC 
# MAGIC Called By: CotivitiIdsStndExtrMbrHedisMesrYrMoSeq
# MAGIC 
# MAGIC Applies Transformation rules for MBR_HEDIS_MESR_YR_MO
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                                                                  Project/                                                                                                                                       		Code                  Date
# MAGIC Developer                               Date                                   Altiris #                Change Description                                   Environment                                   Reviewer                    Reviewed
# MAGIC -----------------------              ------------------                             -------------         ------------------------------------------------	            -------------------------	 -------------------------         -------------------
# MAGIC 
# MAGIC Karthik Chintalapani          2021-02- 10                         US#341503                Original Program                                    IntegrateDev1                           Jaideep Mankala          02/12/2021
# MAGIC 
# MAGIC Venkat M                       2021-12-13                        US441657                     Changed Keep condutor                    IntegarateDev2                              Goutham K                  1/7/2022
# MAGIC                                                                                                                  connection alive to - NO
# MAGIC  
# MAGIC Reddy Sanam                 2023-08-12                        US590640                  Updated the job to reflect new layout  IntegrateDev2                                  Goutham K                8/25/2023
# MAGIC 
# MAGIC Reddy Sanam                2024-01-25                       US606288              Changed datatype for field UNIT_CT       IntegrateDev2                                Goutham Kalidindi       1/30/2024
# MAGIC                                                                                                                  to Decimal(13,2)
# MAGIC 
# MAGIC Reddy Sanam                2024-10-23                       US631537             Added and propagated new field "MBR_GNDR"  IntegrateDev2                      Goutham Kalidindi     10/29/2024

# MAGIC New PKEYs are generated in this transformer. Fetch the max key column value from K table and  will generate  next PKEY value starting from the max value.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Left Join on Natural Keys
# MAGIC This query fetches the max value of the MBR_HEDIS_MESR_YR_MO_SK from MBR_HEDIS_MESR_YR_MO table
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
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_DB2_K_MBR_HEDIS_MESR_YR_MO = f"""SELECT
HEDIS_RVW_SET_NM as RVW_SET_NM
,HEDIS_POP_NM AS POP_NM
,HEDIS_MESR_NM AS MESR_NM
,HEDIS_SUB_MESR_NM AS SUB_MESR_NM
,MBR_UNIQ_KEY
,HEDIS_MBR_BUCKET_ID AS MBR_BUCKET_ID
,BASE_EVT_EPSD_DT_SK
,ACTVTY_YR_NO
,ACTVTY_MO_NO
,SRC_SYS_CD
,CRT_RUN_CYC_EXCTN_SK
,MBR_HEDIS_MESR_YR_MO_SK 
FROM {IDSOwner}.K_MBR_HEDIS_MESR_YR_MO
"""

df_DB2_K_MBR_HEDIS_MESR_YR_MO = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_K_MBR_HEDIS_MESR_YR_MO)
    .load()
)

df_ds_MBR_HEDIS_MESR_Xfm = spark.read.parquet(
    f"{adls_path}/ds/MBR_HEDIS_MESR_YR_MO.xfrm.{RunID}.parquet"
)

df_Cp_Pk = df_ds_MBR_HEDIS_MESR_Xfm.select(
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("RVW_SET_NM").alias("RVW_SET_NM"),
    F.col("POP_NM").alias("POP_NM"),
    F.col("MESR_NM").alias("MESR_NM"),
    F.col("SUB_MESR_NM").alias("SUB_MESR_NM"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_BUCKET_ID").alias("MBR_BUCKET_ID"),
    F.col("BASE_EVT_EPSD_DT_SK").alias("BASE_EVT_EPSD_DT_SK"),
    F.col("ACTVTY_YR_NO").alias("ACTVTY_YR_NO"),
    F.col("ACTVTY_MO_NO").alias("ACTVTY_MO_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("CMPLNC_ADMIN_IN").alias("CMPLNC_ADMIN_IN"),
    F.col("CMPLNC_MNL_DATA_IN").alias("CMPLNC_MNL_DATA_IN"),
    F.col("CMPLNC_MNL_DATA_SMPL_POP_IN").alias("CMPLNC_MNL_DATA_SMPL_POP_IN"),
    F.col("CMPLNC_SMPL_POP_IN").alias("CMPLNC_SMPL_POP_IN"),
    F.col("CONTRAIN_IN").alias("CONTRAIN_IN"),
    F.col("CONTRAIN_SMPL_POP_IN").alias("CONTRAIN_SMPL_POP_IN"),
    F.col("EXCL_IN").alias("EXCL_IN"),
    F.col("EXCL_SMPL_POP_IN").alias("EXCL_SMPL_POP_IN"),
    F.col("MESR_ELIG_IN").alias("MESR_ELIG_IN"),
    F.col("MESR_ELIG_SMPL_POP_IN").alias("MESR_ELIG_SMPL_POP_IN"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    F.col("EVT_CT").alias("EVT_CT"),
    F.col("UNIT_CT").alias("UNIT_CT"),
    F.col("ACRDTN_CAT_ID").alias("ACRDTN_CAT_ID"),
    F.col("CNTY_AREA_ID").alias("CNTY_AREA_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("MBR_FULL_NM").alias("MBR_FULL_NM"),
    F.col("ON_OFF_EXCH_ID").alias("ON_OFF_EXCH_ID"),
    F.col("PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("HEDIS_RVW_SET_STRT_DT").alias("HEDIS_RVW_SET_STRT_DT"),
    F.col("HEDIS_RVW_SET_END_DT").alias("HEDIS_RVW_SET_END_DT"),
    F.col("CST_AMT").alias("CST_AMT"),
    F.col("RISK_PCT").alias("RISK_PCT"),
    F.col("ADJ_RISK_NO").alias("ADJ_RISK_NO"),
    F.col("PLN_ALL_CAUSE_READMISSION_VRNC_NO").alias("PLN_ALL_CAUSE_READMISSION_VRNC_NO"),
    F.col("HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
    F.col("DENOMINATOR_FOR_NO_CONT_ENR").alias("DENOMINATOR_FOR_NO_CONT_ENR"),
    F.col("ADMIN_COMPLIANT_CAT").alias("ADMIN_COMPLIANT_CAT"),
    F.col("EXCLNO_CE").alias("EXCLNO_CE"),
    F.col("DENOMINATOR_HYBRID").alias("DENOMINATOR_HYBRID"),
    F.col("NUMERATOR_COMPLIANT_FLAG_HYBRID").alias("NUMERATOR_COMPLIANT_FLAG_HYBRID"),
    F.col("NUMERATOR_CMPLNC_CAT_HYBRID").alias("NUMERATOR_CMPLNC_CAT_HYBRID"),
    F.col("EXCLHYBRID").alias("EXCLHYBRID"),
    F.col("EXCLHYBRID_RSN").alias("EXCLHYBRID_RSN"),
    F.col("LAB_TST_VAL_2").alias("LAB_TST_VAL_2"),
    F.col("SES_STRAT").alias("SES_STRAT"),
    F.col("RACE_ID").alias("RACE_ID"),
    F.col("RACE_SRC").alias("RACE_SRC"),
    F.col("ETHNCTY").alias("ETHNCTY"),
    F.col("ETHNCTY_SRC").alias("ETHNCTY_SRC"),
    F.col("ADV_ILNS_FRAILTY_EXCL").alias("ADV_ILNS_FRAILTY_EXCL"),
    F.col("HSPC_EXCL").alias("HSPC_EXCL"),
    F.col("LTI_SNP_EXCL").alias("LTI_SNP_EXCL"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_LN").alias("PROD_LN"),
    F.col("PROD_ROLLUP_ID").alias("PROD_ROLLUP_ID"),
    F.col("NUMERATOR_CMPLNC_CAT").alias("NUMERATOR_CMPLNC_CAT"),
    F.col("DCSD_EXCL").alias("DCSD_EXCL"),
    F.col("OPTNL_EXCL").alias("OPTNL_EXCL"),
    F.col("RQRD_EXCL").alias("RQRD_EXCL"),
    F.col("NUMERATOR_CMPLNC_FLAG").alias("NUMERATOR_CMPLNC_FLAG"),
    F.col("EXCL").alias("EXCL"),
    F.col("DENOMINATOR").alias("DENOMINATOR"),
    F.col("NUMERATOR_EVT_1_SVC_DT").alias("NUMERATOR_EVT_1_SVC_DT"),
    F.col("LAB_TST_VAL_NO").alias("LAB_TST_VAL_NO"),
    F.col("MBR_GNDR").alias("MBR_GNDR")
)

df_Jnr_Keys = df_Cp_Pk.alias("lnk_ToJnr").join(
    df_DB2_K_MBR_HEDIS_MESR_YR_MO.alias("lnk_KMbrHedisMesrPkey_out"),
    (
        (F.col("lnk_ToJnr.RVW_SET_NM") == F.col("lnk_KMbrHedisMesrPkey_out.RVW_SET_NM")) &
        (F.col("lnk_ToJnr.POP_NM") == F.col("lnk_KMbrHedisMesrPkey_out.POP_NM")) &
        (F.col("lnk_ToJnr.MESR_NM") == F.col("lnk_KMbrHedisMesrPkey_out.MESR_NM")) &
        (F.col("lnk_ToJnr.SUB_MESR_NM") == F.col("lnk_KMbrHedisMesrPkey_out.SUB_MESR_NM")) &
        (F.col("lnk_ToJnr.MBR_UNIQ_KEY") == F.col("lnk_KMbrHedisMesrPkey_out.MBR_UNIQ_KEY")) &
        (F.col("lnk_ToJnr.MBR_BUCKET_ID") == F.col("lnk_KMbrHedisMesrPkey_out.MBR_BUCKET_ID")) &
        (F.col("lnk_ToJnr.BASE_EVT_EPSD_DT_SK") == F.col("lnk_KMbrHedisMesrPkey_out.BASE_EVT_EPSD_DT_SK")) &
        (F.col("lnk_ToJnr.ACTVTY_YR_NO") == F.col("lnk_KMbrHedisMesrPkey_out.ACTVTY_YR_NO")) &
        (F.col("lnk_ToJnr.ACTVTY_MO_NO") == F.col("lnk_KMbrHedisMesrPkey_out.ACTVTY_MO_NO")) &
        (F.col("lnk_ToJnr.SRC_SYS_CD") == F.col("lnk_KMbrHedisMesrPkey_out.SRC_SYS_CD"))
    ),
    how="left"
).select(
    F.col("lnk_ToJnr.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("lnk_ToJnr.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("lnk_ToJnr.RVW_SET_NM").alias("RVW_SET_NM"),
    F.col("lnk_ToJnr.POP_NM").alias("POP_NM"),
    F.col("lnk_ToJnr.MESR_NM").alias("MESR_NM"),
    F.col("lnk_ToJnr.SUB_MESR_NM").alias("SUB_MESR_NM"),
    F.col("lnk_ToJnr.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_ToJnr.MBR_BUCKET_ID").alias("MBR_BUCKET_ID"),
    F.col("lnk_ToJnr.BASE_EVT_EPSD_DT_SK").alias("BASE_EVT_EPSD_DT_SK"),
    F.col("lnk_ToJnr.ACTVTY_YR_NO").alias("ACTVTY_YR_NO"),
    F.col("lnk_ToJnr.ACTVTY_MO_NO").alias("ACTVTY_MO_NO"),
    F.col("lnk_ToJnr.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_ToJnr.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_ToJnr.MBR_SK").alias("MBR_SK"),
    F.col("lnk_ToJnr.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("lnk_ToJnr.CMPLNC_ADMIN_IN").alias("CMPLNC_ADMIN_IN"),
    F.col("lnk_ToJnr.CMPLNC_MNL_DATA_IN").alias("CMPLNC_MNL_DATA_IN"),
    F.col("lnk_ToJnr.CMPLNC_MNL_DATA_SMPL_POP_IN").alias("CMPLNC_MNL_DATA_SMPL_POP_IN"),
    F.col("lnk_ToJnr.CMPLNC_SMPL_POP_IN").alias("CMPLNC_SMPL_POP_IN"),
    F.col("lnk_ToJnr.CONTRAIN_IN").alias("CONTRAIN_IN"),
    F.col("lnk_ToJnr.CONTRAIN_SMPL_POP_IN").alias("CONTRAIN_SMPL_POP_IN"),
    F.col("lnk_ToJnr.EXCL_IN").alias("EXCL_IN"),
    F.col("lnk_ToJnr.EXCL_SMPL_POP_IN").alias("EXCL_SMPL_POP_IN"),
    F.col("lnk_ToJnr.MESR_ELIG_IN").alias("MESR_ELIG_IN"),
    F.col("lnk_ToJnr.MESR_ELIG_SMPL_POP_IN").alias("MESR_ELIG_SMPL_POP_IN"),
    F.col("lnk_ToJnr.MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    F.col("lnk_ToJnr.EVT_CT").alias("EVT_CT"),
    F.col("lnk_ToJnr.UNIT_CT").alias("UNIT_CT"),
    F.col("lnk_ToJnr.ACRDTN_CAT_ID").alias("ACRDTN_CAT_ID"),
    F.col("lnk_ToJnr.CNTY_AREA_ID").alias("CNTY_AREA_ID"),
    F.col("lnk_ToJnr.GRP_ID").alias("GRP_ID"),
    F.col("lnk_ToJnr.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("lnk_ToJnr.MBR_ID").alias("MBR_ID"),
    F.col("lnk_ToJnr.MBR_FULL_NM").alias("MBR_FULL_NM"),
    F.col("lnk_ToJnr.ON_OFF_EXCH_ID").alias("ON_OFF_EXCH_ID"),
    F.col("lnk_ToJnr.PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("lnk_ToJnr.HEDIS_RVW_SET_STRT_DT").alias("HEDIS_RVW_SET_STRT_DT"),
    F.col("lnk_ToJnr.HEDIS_RVW_SET_END_DT").alias("HEDIS_RVW_SET_END_DT"),
    F.col("lnk_ToJnr.CST_AMT").alias("CST_AMT"),
    F.col("lnk_ToJnr.RISK_PCT").alias("RISK_PCT"),
    F.col("lnk_ToJnr.ADJ_RISK_NO").alias("ADJ_RISK_NO"),
    F.col("lnk_ToJnr.PLN_ALL_CAUSE_READMISSION_VRNC_NO").alias("PLN_ALL_CAUSE_READMISSION_VRNC_NO"),
    F.col("lnk_ToJnr.HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
    F.col("lnk_ToJnr.DENOMINATOR_FOR_NO_CONT_ENR").alias("DENOMINATOR_FOR_NO_CONT_ENR"),
    F.col("lnk_ToJnr.ADMIN_COMPLIANT_CAT").alias("ADMIN_COMPLIANT_CAT"),
    F.col("lnk_ToJnr.EXCLNO_CE").alias("EXCLNO_CE"),
    F.col("lnk_ToJnr.DENOMINATOR_HYBRID").alias("DENOMINATOR_HYBRID"),
    F.col("lnk_ToJnr.NUMERATOR_COMPLIANT_FLAG_HYBRID").alias("NUMERATOR_COMPLIANT_FLAG_HYBRID"),
    F.col("lnk_ToJnr.NUMERATOR_CMPLNC_CAT_HYBRID").alias("NUMERATOR_CMPLNC_CAT_HYBRID"),
    F.col("lnk_ToJnr.EXCLHYBRID").alias("EXCLHYBRID"),
    F.col("lnk_ToJnr.EXCLHYBRID_RSN").alias("EXCLHYBRID_RSN"),
    F.col("lnk_ToJnr.LAB_TST_VAL_2").alias("LAB_TST_VAL_2"),
    F.col("lnk_ToJnr.SES_STRAT").alias("SES_STRAT"),
    F.col("lnk_ToJnr.RACE_ID").alias("RACE_ID"),
    F.col("lnk_ToJnr.RACE_SRC").alias("RACE_SRC"),
    F.col("lnk_ToJnr.ETHNCTY").alias("ETHNCTY"),
    F.col("lnk_ToJnr.ETHNCTY_SRC").alias("ETHNCTY_SRC"),
    F.col("lnk_ToJnr.ADV_ILNS_FRAILTY_EXCL").alias("ADV_ILNS_FRAILTY_EXCL"),
    F.col("lnk_ToJnr.HSPC_EXCL").alias("HSPC_EXCL"),
    F.col("lnk_ToJnr.LTI_SNP_EXCL").alias("LTI_SNP_EXCL"),
    F.col("lnk_ToJnr.PROD_ID").alias("PROD_ID"),
    F.col("lnk_ToJnr.PROD_LN").alias("PROD_LN"),
    F.col("lnk_ToJnr.PROD_ROLLUP_ID").alias("PROD_ROLLUP_ID"),
    F.col("lnk_ToJnr.NUMERATOR_CMPLNC_CAT").alias("NUMERATOR_CMPLNC_CAT"),
    F.col("lnk_ToJnr.DCSD_EXCL").alias("DCSD_EXCL"),
    F.col("lnk_ToJnr.OPTNL_EXCL").alias("OPTNL_EXCL"),
    F.col("lnk_ToJnr.RQRD_EXCL").alias("RQRD_EXCL"),
    F.col("lnk_ToJnr.NUMERATOR_CMPLNC_FLAG").alias("NUMERATOR_CMPLNC_FLAG"),
    F.col("lnk_ToJnr.EXCL").alias("EXCL"),
    F.col("lnk_ToJnr.DENOMINATOR").alias("DENOMINATOR"),
    F.col("lnk_ToJnr.NUMERATOR_EVT_1_SVC_DT").alias("NUMERATOR_EVT_1_SVC_DT"),
    F.col("lnk_ToJnr.LAB_TST_VAL_NO").alias("LAB_TST_VAL_NO"),
    F.col("lnk_KMbrHedisMesrPkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_KMbrHedisMesrPkey_out.MBR_HEDIS_MESR_YR_MO_SK").alias("MBR_HEDIS_MESR_YR_MO_SK"),
    F.col("lnk_ToJnr.MBR_GNDR").alias("MBR_GNDR")
)

df_temp = df_Jnr_Keys.withColumn("orig_is_null", F.col("MBR_HEDIS_MESR_YR_MO_SK").isNull())
df_enriched = SurrogateKeyGen(df_temp,<DB sequence name>,"MBR_HEDIS_MESR_YR_MO_SK",<schema>,<secret_name>)

df_lnk_KMbrHedisMesr_New_pre = df_enriched.filter(F.col("orig_is_null")).select(
    F.col("RVW_SET_NM").alias("HEDIS_RVW_SET_NM"),
    F.col("POP_NM").alias("HEDIS_POP_NM"),
    F.col("MESR_NM").alias("HEDIS_MESR_NM"),
    F.col("SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    F.col("BASE_EVT_EPSD_DT_SK").alias("BASE_EVT_EPSD_DT_SK"),
    F.col("ACTVTY_YR_NO").alias("ACTVTY_YR_NO"),
    F.col("ACTVTY_MO_NO").alias("ACTVTY_MO_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_HEDIS_MESR_YR_MO_SK").alias("MBR_HEDIS_MESR_YR_MO_SK")
)

df_lnk_KMbrHedisMesr_New = df_lnk_KMbrHedisMesr_New_pre.withColumn(
    "BASE_EVT_EPSD_DT_SK", F.rpad(F.col("BASE_EVT_EPSD_DT_SK"), 10, " ")
).select(
    "HEDIS_RVW_SET_NM",
    "HEDIS_POP_NM",
    "HEDIS_MESR_NM",
    "HEDIS_SUB_MESR_NM",
    "MBR_UNIQ_KEY",
    "HEDIS_MBR_BUCKET_ID",
    "BASE_EVT_EPSD_DT_SK",
    "ACTVTY_YR_NO",
    "ACTVTY_MO_NO",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "MBR_HEDIS_MESR_YR_MO_SK"
)

df_lnk_MbrHedisMesrData_out_pre = df_enriched.withColumn(
    "final_CRT_RUN_CYC_EXCTN_SK",
    F.when(F.col("orig_is_null"), IDSRunCycle).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.lit(IDSRunCycle)
)

df_lnk_MbrHedisMesrData_out = df_lnk_MbrHedisMesrData_out_pre.select(
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("MBR_HEDIS_MESR_YR_MO_SK").alias("MBR_HEDIS_MESR_YR_MO_SK"),
    F.col("RVW_SET_NM").alias("RVW_SET_NM"),
    F.col("POP_NM").alias("POP_NM"),
    F.col("MESR_NM").alias("MESR_NM"),
    F.col("SUB_MESR_NM").alias("SUB_MESR_NM"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_BUCKET_ID").alias("MBR_BUCKET_ID"),
    F.rpad(F.col("BASE_EVT_EPSD_DT_SK"), 10, " ").alias("BASE_EVT_EPSD_DT_SK"),
    F.col("ACTVTY_YR_NO").alias("ACTVTY_YR_NO"),
    F.col("ACTVTY_MO_NO").alias("ACTVTY_MO_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("final_CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.rpad(F.col("CMPLNC_ADMIN_IN"), 1, " ").alias("CMPLNC_ADMIN_IN"),
    F.rpad(F.col("CMPLNC_MNL_DATA_IN"), 1, " ").alias("CMPLNC_MNL_DATA_IN"),
    F.rpad(F.col("CMPLNC_MNL_DATA_SMPL_POP_IN"), 1, " ").alias("CMPLNC_MNL_DATA_SMPL_POP_IN"),
    F.rpad(F.col("CMPLNC_SMPL_POP_IN"), 1, " ").alias("CMPLNC_SMPL_POP_IN"),
    F.rpad(F.col("CONTRAIN_IN"), 1, " ").alias("CONTRAIN_IN"),
    F.rpad(F.col("CONTRAIN_SMPL_POP_IN"), 1, " ").alias("CONTRAIN_SMPL_POP_IN"),
    F.rpad(F.col("EXCL_IN"), 1, " ").alias("EXCL_IN"),
    F.rpad(F.col("EXCL_SMPL_POP_IN"), 1, " ").alias("EXCL_SMPL_POP_IN"),
    F.rpad(F.col("MESR_ELIG_IN"), 1, " ").alias("MESR_ELIG_IN"),
    F.rpad(F.col("MESR_ELIG_SMPL_POP_IN"), 1, " ").alias("MESR_ELIG_SMPL_POP_IN"),
    F.rpad(F.col("MBR_BRTH_DT_SK"), 10, " ").alias("MBR_BRTH_DT_SK"),
    F.col("EVT_CT").alias("EVT_CT"),
    F.col("UNIT_CT").alias("UNIT_CT"),
    F.col("ACRDTN_CAT_ID").alias("ACRDTN_CAT_ID"),
    F.col("CNTY_AREA_ID").alias("CNTY_AREA_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("MBR_FULL_NM").alias("MBR_FULL_NM"),
    F.col("ON_OFF_EXCH_ID").alias("ON_OFF_EXCH_ID"),
    F.col("PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("HEDIS_RVW_SET_STRT_DT").alias("HEDIS_RVW_SET_STRT_DT"),
    F.col("HEDIS_RVW_SET_END_DT").alias("HEDIS_RVW_SET_END_DT"),
    F.col("CST_AMT").alias("CST_AMT"),
    F.col("RISK_PCT").alias("RISK_PCT"),
    F.col("ADJ_RISK_NO").alias("ADJ_RISK_NO"),
    F.col("PLN_ALL_CAUSE_READMISSION_VRNC_NO").alias("PLN_ALL_CAUSE_READMISSION_VRNC_NO"),
    F.col("HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
    F.col("DENOMINATOR_FOR_NO_CONT_ENR").alias("DENOMINATOR_FOR_NO_CONT_ENR"),
    F.col("ADMIN_COMPLIANT_CAT").alias("ADMIN_COMPLIANT_CAT"),
    F.col("EXCLNO_CE").alias("EXCLNO_CE"),
    F.col("DENOMINATOR_HYBRID").alias("DENOMINATOR_HYBRID"),
    F.col("NUMERATOR_COMPLIANT_FLAG_HYBRID").alias("NUMERATOR_COMPLIANT_FLAG_HYBRID"),
    F.col("NUMERATOR_CMPLNC_CAT_HYBRID").alias("NUMERATOR_CMPLNC_CAT_HYBRID"),
    F.col("EXCLHYBRID").alias("EXCLHYBRID"),
    F.col("EXCLHYBRID_RSN").alias("EXCLHYBRID_RSN"),
    F.col("LAB_TST_VAL_2").alias("LAB_TST_VAL_2"),
    F.col("SES_STRAT").alias("SES_STRAT"),
    F.col("RACE_ID").alias("RACE_ID"),
    F.col("RACE_SRC").alias("RACE_SRC"),
    F.col("ETHNCTY").alias("ETHNCTY"),
    F.col("ETHNCTY_SRC").alias("ETHNCTY_SRC"),
    F.col("ADV_ILNS_FRAILTY_EXCL").alias("ADV_ILNS_FRAILTY_EXCL"),
    F.col("HSPC_EXCL").alias("HSPC_EXCL"),
    F.col("LTI_SNP_EXCL").alias("LTI_SNP_EXCL"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_LN").alias("PROD_LN"),
    F.col("PROD_ROLLUP_ID").alias("PROD_ROLLUP_ID"),
    F.col("NUMERATOR_CMPLNC_CAT").alias("NUMERATOR_CMPLNC_CAT"),
    F.col("NUMERATOR_CMPLNC_FLAG").alias("NUMERATOR_CMPLNC_FLAG"),
    F.col("DCSD_EXCL").alias("DCSD_EXCL"),
    F.col("EXCL").alias("EXCL"),
    F.col("DENOMINATOR").alias("DENOMINATOR"),
    F.col("OPTNL_EXCL").alias("OPTNL_EXCL"),
    F.col("RQRD_EXCL").alias("RQRD_EXCL"),
    F.col("NUMERATOR_EVT_1_SVC_DT").alias("NUMERATOR_EVT_1_SVC_DT"),
    F.col("LAB_TST_VAL_NO").alias("LAB_TST_VAL_NO"),
    F.col("MBR_GNDR").alias("MBR_GNDR")
)

write_files(
    df_lnk_MbrHedisMesrData_out,
    f"{adls_path}/key/MBR_HEDIS_MESR_YR_MO.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

drop_temp_sql = "DROP TABLE IF EXISTS STAGING.CotivitiIdsStndExtrMbrHedisMesrYrMoPKey_DB2_K_MBR_HEDIS_MESR_YR_MO_Load_temp"
execute_dml(drop_temp_sql, jdbc_url, jdbc_props)

df_lnk_KMbrHedisMesr_New.write.jdbc(
    url=jdbc_url,
    table="STAGING.CotivitiIdsStndExtrMbrHedisMesrYrMoPKey_DB2_K_MBR_HEDIS_MESR_YR_MO_Load_temp",
    mode="overwrite",
    properties=jdbc_props
)

merge_sql = f"""
MERGE INTO {IDSOwner}.K_MBR_HEDIS_MESR_YR_MO AS T
USING STAGING.CotivitiIdsStndExtrMbrHedisMesrYrMoPKey_DB2_K_MBR_HEDIS_MESR_YR_MO_Load_temp AS S
ON (
 T.HEDIS_RVW_SET_NM = S.HEDIS_RVW_SET_NM AND
 T.HEDIS_POP_NM = S.HEDIS_POP_NM AND
 T.HEDIS_MESR_NM = S.HEDIS_MESR_NM AND
 T.HEDIS_SUB_MESR_NM = S.HEDIS_SUB_MESR_NM AND
 T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY AND
 T.HEDIS_MBR_BUCKET_ID = S.HEDIS_MBR_BUCKET_ID AND
 T.BASE_EVT_EPSD_DT_SK = S.BASE_EVT_EPSD_DT_SK AND
 T.ACTVTY_YR_NO = S.ACTVTY_YR_NO AND
 T.ACTVTY_MO_NO = S.ACTVTY_MO_NO AND
 T.SRC_SYS_CD = S.SRC_SYS_CD
)
WHEN MATCHED THEN UPDATE SET
 T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
 T.MBR_HEDIS_MESR_YR_MO_SK = S.MBR_HEDIS_MESR_YR_MO_SK
WHEN NOT MATCHED THEN
 INSERT (
  HEDIS_RVW_SET_NM,
  HEDIS_POP_NM,
  HEDIS_MESR_NM,
  HEDIS_SUB_MESR_NM,
  MBR_UNIQ_KEY,
  HEDIS_MBR_BUCKET_ID,
  BASE_EVT_EPSD_DT_SK,
  ACTVTY_YR_NO,
  ACTVTY_MO_NO,
  SRC_SYS_CD,
  CRT_RUN_CYC_EXCTN_SK,
  MBR_HEDIS_MESR_YR_MO_SK
 )
 VALUES (
  S.HEDIS_RVW_SET_NM,
  S.HEDIS_POP_NM,
  S.HEDIS_MESR_NM,
  S.HEDIS_SUB_MESR_NM,
  S.MBR_UNIQ_KEY,
  S.HEDIS_MBR_BUCKET_ID,
  S.BASE_EVT_EPSD_DT_SK,
  S.ACTVTY_YR_NO,
  S.ACTVTY_MO_NO,
  S.SRC_SYS_CD,
  S.CRT_RUN_CYC_EXCTN_SK,
  S.MBR_HEDIS_MESR_YR_MO_SK
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)