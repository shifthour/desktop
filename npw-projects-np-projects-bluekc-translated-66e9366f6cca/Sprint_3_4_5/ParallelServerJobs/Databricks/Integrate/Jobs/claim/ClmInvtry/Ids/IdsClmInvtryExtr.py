# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  OpsDashboardClmExtrSeq
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:   Pulls the Claim Inventory information from IDS database which populates the Claim Inventory table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                            Project/                                                                                                                                  Development
# MAGIC Developer                     Date              Altiris #       Change Description                                                                                             Project                              Code Reviewer            Date Reviewed
# MAGIC ----------------------------------   -------------------   ---------------   ------------------------------------------------------------------------------------------------                            -------------------                      ---------------------------------    -------------------------   
# MAGIC Parikshith Chada          12/20/2007   3531          Original Programming                                                                                          devlIDS30                         Steph Goddard              01/04/2008
# MAGIC Parikshith Chada          01/08/2008   3531          Made structural changes to the job                                                                    devlIDS30                         Steph Goddard             01/09/2008
# MAGIC Parikshith Chada          02/01/2008   3531          Made code changes in the logic                                                                        devlIDS30
# MAGIC Hugh Sisson                 06/30/2008   ProdSupp  Extract query corrected and optimized                                                               devlIDScur                        Steph Goddard              07/02/2008
# MAGIC Hugh Sisson                 07/14/2008   ProdSupp  Extract query modified                                                                                        devlIDScur                         Steph Goddard             07/14/2008
# MAGIC Dan Long                     4/19/2013     TTR-1492 Changed the Performance parameters to the new                                              IntegrateNewDevl             Bhoomi Dasari              4/30/2013  
# MAGIC                                                                              standard values of 512 for the buffer size and 300
# MAGIC                                                                              for the timeout value. No code logic TTR-1492                
# MAGIC                                                                              was modified.            
# MAGIC Abhiram Dasarathy       07/27/2015    5407        Added Column ASG_USER_SK to the end of the file                                          EnterpriseDev1                  Kalyan Neelam             2015-07-29

# MAGIC Pulling IDS claims
# MAGIC Writing Sequential File to /key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_IdsClm = f"""
SELECT DISTINCT 
      CLM.CLM_ID,
      CLM.SRC_SYS_CD_SK,
      CLM.CLM_SUBTYP_CD_SK,
      CLM.CLM_TYP_CD_SK,
      CLM.INPT_DT_SK,
      CLM.RCVD_DT_SK,
      CLM.STTUS_DT_SK,
      GRP.GRP_ID,
      PROD.PROD_ID,
      CLM_STTUS_AUDIT.CLM_STTUS_CD_SK,
      CLM_STTUS_AUDIT.CLM_STTUS_CHG_RSN_CD_SK,
      CLM.CLM_INPT_SRC_CD_SK,
      ''
FROM 
      {IDSOwner}.CLM CLM,
      {IDSOwner}.PROD PROD,
      {IDSOwner}.GRP GRP,
      {IDSOwner}.CLM_STTUS_AUDIT CLM_STTUS_AUDIT,
      {IDSOwner}.CD_MPPNG  CD_MPPNG1,
      {IDSOwner}.CD_MPPNG  CD_MPPNG3,
      {IDSOwner}.CD_MPPNG  CD_MPPNG4
WHERE 
      CLM.PD_DT_SK = '1753-01-01'
  AND CLM.INPT_DT_SK >= '2007-01-01' 
  AND CLM.GRP_SK = GRP.GRP_SK 
  AND CLM.PROD_SK = PROD.PROD_SK 
  AND CLM.CLM_SK = CLM_STTUS_AUDIT.CLM_SK 
  AND CLM.SRC_SYS_CD_SK = CLM_STTUS_AUDIT.SRC_SYS_CD_SK
  AND CLM_STTUS_AUDIT.CLM_STTUS_AUDIT_SEQ_NO = (SELECT MAX(CLM_STTUS_AUDIT2.CLM_STTUS_AUDIT_SEQ_NO) 
                                               FROM {IDSOwner}.CLM_STTUS_AUDIT CLM_STTUS_AUDIT2
                                               WHERE CLM.CLM_SK = CLM_STTUS_AUDIT2.CLM_SK)
  AND CLM.CLM_STTUS_CD_SK = CD_MPPNG1.CD_MPPNG_SK 
  AND CD_MPPNG1.TRGT_CD IN ('A01','A04','A05','A06','A11','A12')
  AND CLM_STTUS_AUDIT.TRNSMSN_SRC_CD_SK = CD_MPPNG3.CD_MPPNG_SK 
  AND CLM.CLM_INPT_SRC_CD_SK = CD_MPPNG4.CD_MPPNG_SK 
  AND  
    (
      CD_MPPNG1.TRGT_CD IN ('A04','A05','A06','A11','A12')
      OR (
           CD_MPPNG1.TRGT_CD = 'A01'
           AND
           (
             ( CD_MPPNG4.TRGT_CD IN ('H','K')  
               AND CD_MPPNG3.TRGT_CD IN ('32','42')
             ) 
             OR 
             (
               GRP.GRP_ID = 'BLUECARD'
               AND CD_MPPNG3.TRGT_CD = '20'
             )
           )
         )
    )
UNION
SELECT DISTINCT 
      CLM.CLM_ID,
      CLM.SRC_SYS_CD_SK,
      CLM.CLM_SUBTYP_CD_SK,
      CLM.CLM_TYP_CD_SK,
      CLM.INPT_DT_SK,
      CLM.RCVD_DT_SK,
      CLM.STTUS_DT_SK,
      GRP.GRP_ID,
      PROD.PROD_ID,
      CLM_STTUS_AUDIT.CLM_STTUS_CD_SK,
      CLM_STTUS_AUDIT.CLM_STTUS_CHG_RSN_CD_SK,
      CLM.CLM_INPT_SRC_CD_SK,
      EXCD.EXCD_ID
FROM 
    (
      (
        {IDSOwner}.CLM CLM 
        left outer join {IDSOwner}.CLM_OVRD CLM_OVRD 
        ON CLM.SRC_SYS_CD_SK = CLM_OVRD.SRC_SYS_CD_SK AND CLM.CLM_ID = CLM_OVRD.CLM_ID
      )
      left outer join {IDSOwner}.EXCD EXCD 
      ON CLM_OVRD.CLM_OVRD_EXCD_SK = EXCD.EXCD_SK
    ),
      {IDSOwner}.PROD PROD,
      {IDSOwner}.GRP GRP,
      {IDSOwner}.CLM_STTUS_AUDIT CLM_STTUS_AUDIT,
      {IDSOwner}.CD_MPPNG  CD_MPPNG1,
      {IDSOwner}.CD_MPPNG  CD_MPPNG3,
      {IDSOwner}.CD_MPPNG  CD_MPPNG4
WHERE 
      CLM.PD_DT_SK = '1753-01-01'
  AND CLM.INPT_DT_SK >= '2007-01-01' 
  AND CLM.CLM_SK = CLM_OVRD.CLM_SK  
  AND CLM.SRC_SYS_CD_SK = CLM_OVRD.SRC_SYS_CD_SK 
  AND CLM_OVRD.CLM_OVRD_ID = 'PD'
  AND CLM.GRP_SK = GRP.GRP_SK 
  AND CLM.PROD_SK = PROD.PROD_SK 
  AND CLM.CLM_SK = CLM_STTUS_AUDIT.CLM_SK 
  AND CLM.SRC_SYS_CD_SK = CLM_STTUS_AUDIT.SRC_SYS_CD_SK
  AND CLM_STTUS_AUDIT.CLM_STTUS_AUDIT_SEQ_NO = (SELECT MAX(CLM_STTUS_AUDIT2.CLM_STTUS_AUDIT_SEQ_NO) 
                                               FROM {IDSOwner}.CLM_STTUS_AUDIT CLM_STTUS_AUDIT2
                                               WHERE CLM.CLM_SK = CLM_STTUS_AUDIT2.CLM_SK)
  AND CLM_STTUS_AUDIT.TRNSMSN_SRC_CD_SK = CD_MPPNG3.CD_MPPNG_SK 
  AND CLM.CLM_INPT_SRC_CD_SK = CD_MPPNG4.CD_MPPNG_SK 
  AND CLM.CLM_STTUS_CD_SK = CD_MPPNG1.CD_MPPNG_SK 
  AND CD_MPPNG1.TRGT_CD = 'A01'
  AND GRP.GRP_ID = '10023000'
  AND EXCD.EXCD_ID IN ('Y16','Y17','Y18','Y19','Y20')
"""

df_IdsClm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IdsClm)
    .load()
)

df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet").alias("hf_etrnl_cd_mppng")

jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsClmInvtryExtr_OPS_GRP_WORK_UNIT_XREF_temp", jdbc_url_uws, jdbc_props_uws)

df_IdsClm.select("GRP_ID").dropDuplicates().write \
    .format("jdbc") \
    .option("url", jdbc_url_uws) \
    .options(**jdbc_props_uws) \
    .option("dbtable", "STAGING.IdsClmInvtryExtr_OPS_GRP_WORK_UNIT_XREF_temp") \
    .mode("append") \
    .save()

extract_query_ops = f"""
SELECT WORK_UNIT.GRP_ID, WORK_UNIT.OPS_WORK_UNIT_ID
FROM {UWSOwner}.OPS_GRP_WORK_UNIT_XREF WORK_UNIT
JOIN STAGING.IdsClmInvtryExtr_OPS_GRP_WORK_UNIT_XREF_temp S
    ON WORK_UNIT.GRP_ID = S.GRP_ID
"""

df_ops_grp_work_unit_xref = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", extract_query_ops)
    .load()
)

df_ops_grp_work_unit_xref = dedup_sort(
    df_ops_grp_work_unit_xref,
    ["GRP_ID"],
    [("GRP_ID","A")]
).alias("WorkUnitLkup")

df_ClmSttusChgRsnCdLkup = df_hf_etrnl_cd_mppng.alias("ClmSttusChgRsnCdLkup")
df_AuditClmSttusCdLkup = df_hf_etrnl_cd_mppng.alias("AuditClmSttusCdLkup")
df_ClmSubtypCdLkup = df_hf_etrnl_cd_mppng.alias("ClmSubtypCdLkup")
df_ClmTypCdLkup = df_hf_etrnl_cd_mppng.alias("ClmTypCdLkup")
df_ClmInptSrcCdLkup = df_hf_etrnl_cd_mppng.alias("ClmInptSrcCdLkup")
df_SrcSysCdLkup = df_hf_etrnl_cd_mppng.alias("SrcSysCdLkup")

df_businessRules_joined = (
    df_IdsClm.alias("Extract")
    .join(df_ClmSttusChgRsnCdLkup, F.col("Extract.CLM_STTUS_CHG_RSN_CD_SK") == F.col("ClmSttusChgRsnCdLkup.CD_MPPNG_SK"), "left")
    .join(df_ops_grp_work_unit_xref, F.col("Extract.GRP_ID") == F.col("WorkUnitLkup.GRP_ID"), "left")
    .join(df_AuditClmSttusCdLkup, F.col("Extract.CLM_STTUS_CD_SK") == F.col("AuditClmSttusCdLkup.CD_MPPNG_SK"), "left")
    .join(df_ClmSubtypCdLkup, F.col("Extract.CLM_SUBTYP_CD_SK") == F.col("ClmSubtypCdLkup.CD_MPPNG_SK"), "left")
    .join(df_ClmTypCdLkup, F.col("Extract.CLM_TYP_CD_SK") == F.col("ClmTypCdLkup.CD_MPPNG_SK"), "left")
    .join(df_ClmInptSrcCdLkup, F.col("Extract.CLM_INPT_SRC_CD_SK") == F.col("ClmInptSrcCdLkup.CD_MPPNG_SK"), "left")
    .join(df_SrcSysCdLkup, F.col("Extract.SRC_SYS_CD_SK") == F.col("SrcSysCdLkup.CD_MPPNG_SK"), "left")
)

df_businessRules = df_businessRules_joined.withColumn(
    "JOB_EXCTN_RCRD_ERR_SK", F.lit(0)
).withColumn(
    "INSRT_UPDT_CD", F.lit("I")
).withColumn(
    "DISCARD_IN", F.lit("N")
).withColumn(
    "PASS_THRU_IN", F.lit("Y")
).withColumn(
    "FIRST_RECYC_DT", F.lit(CurrDate)
).withColumn(
    "ERR_CT", F.lit(0)
).withColumn(
    "RECYCLE_CT", F.lit(0)
).withColumn(
    "SRC_SYS_CD", F.col("SrcSysCdLkup.TRGT_CD")
).withColumn(
    "PRI_KEY_STRING", F.concat_ws(";", F.col("SrcSysCdLkup.TRGT_CD"), F.col("Extract.CLM_ID"))
).withColumn(
    "CLM_INVTRY_SK", F.lit(0)
).withColumn(
    "CLM_INVTRY_KEY_ID", F.col("Extract.CLM_ID")
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK", F.lit(0)
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0)
).withColumn(
    "GRP_ID", F.col("Extract.GRP_ID")
).withColumn(
    "OPS_WORK_UNIT_ID",
    F.when(
        (F.col("Extract.GRP_ID") == "10023000")
        & (F.col("AuditClmSttusCdLkup.TRGT_CD") == "A01")
        & (F.col("Extract.EXCD_ID").isin("Y16","Y17","Y18","Y19","Y20")), 
        F.lit("FEPDeferrals")
    ).when(
        F.col("Extract.GRP_ID") == "10025000",
        F.lit("Group25")
    ).when(
        (F.col("Extract.GRP_ID") == "BLUECARD")
        & (F.col("AuditClmSttusCdLkup.TRGT_CD") == "A01"),
        F.lit("ITSHostAR")
    ).when(
        (F.col("ClmInptSrcCdLkup.TRGT_CD").isin("H","K"))
        & (F.col("AuditClmSttusCdLkup.TRGT_CD") == "A01"),
        F.lit("ITSHomeAR")
    ).when(
        F.col("ClmInptSrcCdLkup.TRGT_CD").isin("H","K","RH"),
        F.lit("ITSHome")
    ).when(
        (F.col("WorkUnitLkup.GRP_ID").isNotNull()) | (F.length(F.col("WorkUnitLkup.GRP_ID")) != 0),
        F.col("WorkUnitLkup.OPS_WORK_UNIT_ID")
    ).when(
        (F.substring(F.col("Extract.PROD_ID"),1,2).isin("MG"))
        | (F.substring(F.col("Extract.PROD_ID"),1,3).isin("MSK","MSM","MSX","MVM","MVC","TCK","TCM","TCX","TPK","TPM","TPX")),
        F.lit("TIP")
    ).when(
        F.substring(F.col("Extract.PROD_ID"),1,2).isin("PB","PC","PF"),
        F.lit("PPO")
    ).when(
        F.substring(F.col("Extract.PROD_ID"),1,2) == "BM",
        F.lit("BA+")
    ).when(
        F.substring(F.col("Extract.PROD_ID"),1,2) == "BA",
        F.lit("BA")
    ).when(
        F.substring(F.col("Extract.PROD_ID"),1,2) == "BC",
        F.lit("BC")
    ).when(
        F.substring(F.col("Extract.PROD_ID"),1,1) == "D",
        F.lit("Dental")
    ).otherwise(F.lit("Unidentified"))
).withColumn(
    "PDPD_ID", F.col("Extract.PROD_ID")
).withColumn(
    "PRPR_ID", F.lit("NA")
).withColumn(
    "CLM_INVTRY_PEND_CAT_CD",
    F.when(
        F.substring(F.col("ClmSttusChgRsnCdLkup.SRC_CD"),1,1) == "C", F.lit("C")
    ).when(
        F.substring(F.col("ClmSttusChgRsnCdLkup.SRC_CD"),1,1) == "H", F.lit("H")
    ).when(
        F.substring(F.col("ClmSttusChgRsnCdLkup.SRC_CD"),1,1) == "M", F.lit("M")
    ).otherwise(F.lit("O"))
).withColumn(
    "CLM_STTUS_CHG_RSN_CD",
    F.when(
        F.isnull(F.col("ClmSttusChgRsnCdLkup.CD_MPPNG_SK")), F.lit("NA")
    ).otherwise(F.col("ClmSttusChgRsnCdLkup.SRC_CD"))
).withColumn(
    "CLST_STS",
    F.when(
        F.isnull(F.col("AuditClmSttusCdLkup.CD_MPPNG_SK")), F.lit("NA")
    ).otherwise(F.col("AuditClmSttusCdLkup.SRC_CD"))
).withColumn(
    "CLCL_CL_SUB_TYPE",
    F.when(
        F.col("ClmTypCdLkup.TRGT_CD") == "DNTL", F.lit("D")
    ).when(
        F.col("ClmSubtypCdLkup.TRGT_CD").isin("IP","OP"), F.lit("H")
    ).when(
        F.col("ClmSubtypCdLkup.TRGT_CD") == "PR", F.lit("M")
    ).otherwise(F.lit("NA"))
).withColumn(
    "CLCL_CL_TYPE",
    F.when(
        F.isnull(F.col("ClmTypCdLkup.CD_MPPNG_SK")), F.lit("NA")
    ).otherwise(F.col("ClmTypCdLkup.SRC_CD"))
).withColumn(
    "INPT_DT_SK", F.col("Extract.INPT_DT_SK")
).withColumn(
    "RCVD_DT_SK", F.col("Extract.RCVD_DT_SK")
).withColumn(
    "EXTR_DT_SK", F.lit(CurrDate)
).withColumn(
    "STTUS_DT_SK", F.col("Extract.STTUS_DT_SK")
).withColumn(
    "INVTRY_CT", F.lit(1)
).withColumn(
    "ASG_USER_SK", F.lit(1)
).withColumn(
    "WORK_ITEM_CT", F.lit(1)
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmInvtryPkey
# COMMAND ----------

params_ClmInvtryPkey = {
    "DriverTable": "#DriverTable#",
    "CurrRunCycle": "#CurrRunCycle#",
    "RunID": "#RunID#",
    "CurrDate": "#CurrDate#",
    "$FacetsDB": "#$FacetsDB#",
    "$FacetsOwner": "#$FacetsOwner#"
}

df_ClmInvtryPkeyOut = ClmInvtryPkey(df_businessRules, params_ClmInvtryPkey)

df_final = df_ClmInvtryPkeyOut.select(
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

df_final = df_final.withColumn(
    "INSRT_UPDT_CD",
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ")
).withColumn(
    "DISCARD_IN",
    F.rpad(F.col("DISCARD_IN"), 1, " ")
).withColumn(
    "PASS_THRU_IN",
    F.rpad(F.col("PASS_THRU_IN"), 1, " ")
).withColumn(
    "PDPD_ID",
    F.rpad(F.col("PDPD_ID"), 8, " ")
).withColumn(
    "PRPR_ID",
    F.rpad(F.col("PRPR_ID"), 12, " ")
).withColumn(
    "CLST_STS",
    F.rpad(F.col("CLST_STS"), 2, " ")
).withColumn(
    "CLCL_CL_SUB_TYPE",
    F.rpad(F.col("CLCL_CL_SUB_TYPE"), 1, " ")
).withColumn(
    "CLCL_CL_TYPE",
    F.rpad(F.col("CLCL_CL_TYPE"), 1, " ")
).withColumn(
    "INPT_DT_SK",
    F.rpad(F.col("INPT_DT_SK"), 10, " ")
).withColumn(
    "RCVD_DT_SK",
    F.rpad(F.col("RCVD_DT_SK"), 10, " ")
).withColumn(
    "EXTR_DT_SK",
    F.rpad(F.col("EXTR_DT_SK"), 10, " ")
).withColumn(
    "STTUS_DT_SK",
    F.rpad(F.col("STTUS_DT_SK"), 10, " ")
)

write_files(
    df_final,
    f"{adls_path}/key/IdsClmInvtryExtr.ClmInvtry.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)