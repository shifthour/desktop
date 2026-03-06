# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC 6;hf_clm_extrnl_prov_newITS_addr;hf_clm_extrnl_prov_oldITS_addr;hf_clm_extrnl_prov_old_ITS_tax_id;hf_clm_extrnl_prov_newITS;hf_clm_extrnl_prov_oldITS;hf_clm_extrnl_prov_all_claims
# MAGIC 
# MAGIC 
# MAGIC  Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC DESCRIPTION: Runs Facets External Provider extract.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC 2004-06-09      Brent Leland             Original Programming.
# MAGIC Oliver Nielsen          08/15/2007       Balancing              Added Snapshot extract for balancing                                        devlIDS30                      Steph Goddard          8/30/07
# MAGIC 
# MAGIC 
# MAGIC Bhoomi D                25/03/2008       ITS HOME           Changed the source from which data was originally coming         devlIDScur                     Steph Goddard           03/31/2008
# MAGIC                                                                                        CMC_CLPP_ITS_PROV (new source) along with old one   
# MAGIC SAndrew                 2008-04-17        #3255 ITS HOME  Changed rule to determine if new or old ITS Claim                      devlIDScur                     Steph Goddard           04/21/2008
# MAGIC                                                                                        changed all hash file names to be consistant/standard
# MAGIC 6;hf_extrnl_prov_addr;hf_clm_extrn_prov_clpp;hf_extrnal_prov_addrs_claims;hf_clm_extrnl_prov_tax_id;hf_clm_extrn_prov_exchg;hf_clm_extrn_prov_all_claims
# MAGIC to
# MAGIC hf_clm_extrnl_prov_newITS_addr;hf_clm_extrnl_prov_oldITS_addr;hf_clm_extrnl_prov_old_ITS_tax_id;hf_clm_extrnl_prov_newITS;hf_clm_extrnl_prov_oldITS;hf_clm_extrnl_prov_all_claims
# MAGIC              
# MAGIC 
# MAGIC Bhoomi Dasari         2008-08-05      3567(Primary Key)    Changed primary key process from hash file to DB2 table            devlIDS                          Steph Goddard          08/15/2008
# MAGIC 
# MAGIC Manasa Andru                  2017-06-26            TFS - 19354               Updated the performance parameters with         IntegrateDev1
# MAGIC                                                                                                    the right values to avoid job abend due to mutex error.     
# MAGIC Prabhu ES               2022-02-28      S2S Remediation     MSSQL connection parameters added                                      IntegrateDev5                    Kalyan Neelam          2022-06-10

# MAGIC Prov ID and Tax ID are on BLE_EXCHG table by SCCF no, however, only original SCCF no is on the table so only match first 15 characters of SCCF
# MAGIC Apply business logic
# MAGIC balancing
# MAGIC Extract Facets Claim External Provider Data.   
# MAGIC Mostly deals with ITS Home claims
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
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
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmExtrnlProvPK
# COMMAND ----------

# Retrieve all parameter values
CurrRunCycle = get_widget_value('CurrRunCycle','100')
CurrentDate = get_widget_value('CurrentDate','2008-08-06')
RunID = get_widget_value('RunID','20080806')
DriverTable = get_widget_value('DriverTable','TMP_IDS_CLAIM')
AdjTable = get_widget_value('AdjTable','TMP_ADJ_FROM')
FacetsOwner = get_widget_value('FacetsOwner','$PROJDEF')
facets_secret_name = get_widget_value('facets_secret_name','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105859')

# Read from hashed file hf_clm_fcts_reversals (Scenario C => read parquet)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_hf_clm_fcts_reversals = df_hf_clm_fcts_reversals.select(
    "CLCL_ID",
    "CLCL_CUR_STS",
    "CLCL_PAID_DT",
    "CLCL_ID_ADJ_TO",
    "CLCL_ID_ADJ_FROM"
)

# Read from hashed file clm_nasco_dup_bypass (Scenario C => read parquet)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
df_clm_nasco_dup_bypass = df_clm_nasco_dup_bypass.select("CLM_ID")

# ODBC Connector: CMC_CLPP_ITS_PROV
jdbc_url_CMC_CLPP_ITS_PROV, jdbc_props_CMC_CLPP_ITS_PROV = get_db_config(facets_secret_name)
query_CMC_CLPP_ITS_PROV = f"""
SELECT 
Cast(Trim(TMP.CLM_ID) as char(12)) as CLCL_ID, 
PROV.CLPP_BCBS_PR_ID, 
PROV.CLPP_BCBS_PR_NPI,  
PROV.CLPP_CNTRY_CD, 
PROV.CLPP_FED_TAX_ID,  
PROV.CLPP_PERF_ID,  
PROV.CLPP_PERF_ID_NPI,
PROV.CLPP_PHONE,  
PROV.CLPP_PR_ADDR1,  
PROV.CLPP_PR_ADDR2,  
PROV.CLPP_PR_CITY, 
PROV.CLPP_PR_NAME, 
PROV.CLPP_PR_STATE,  
PROV.CLPP_ZIP, 
PROV.CLPP_CLM_SUB_ORG, 
PROV.CLPP_PERF_NAME
FROM {FacetsOwner}.CMC_CLPP_ITS_PROV PROV,            
     tempdb..{DriverTable} TMP ,
     {FacetsOwner}.CMC_CLMI_MISC MISC
WHERE TMP.CLM_ID = PROV.CLCL_ID 
AND TMP.CLM_ID = MISC.CLCL_ID
AND MISC.CLMI_ITS_SUB_TYPE IN ('H','T')
"""
df_CMC_CLPP_ITS_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_CLPP_ITS_PROV)
    .options(**jdbc_props_CMC_CLPP_ITS_PROV)
    .option("query", query_CMC_CLPP_ITS_PROV)
    .load()
)

# Remove intermediate hashed file "hf_clm_extrnl_prov_newITS_addr" (Scenario A)
# Deduplicate on key column: CLCL_ID
df_CMC_CLPP_ITS_PROV_dedup = dedup_sort(
    df_CMC_CLPP_ITS_PROV,
    ["CLCL_ID"],
    [("CLCL_ID", "A")]
)

# From that deduped DF, produce the two outputs corresponding to "NewITSAddr" and "lkupconstrnt"
# Output: NewITSAddr
df_newITSAddr = df_CMC_CLPP_ITS_PROV_dedup.select(
    F.col("CLCL_ID"),
    F.col("CLPP_BCBS_PR_ID"),
    F.col("CLPP_BCBS_PR_NPI"),
    F.col("CLPP_CNTRY_CD"),
    F.col("CLPP_FED_TAX_ID"),
    F.col("CLPP_PERF_ID"),
    F.col("CLPP_PERF_ID_NPI"),
    F.col("CLPP_PHONE"),
    F.col("CLPP_PR_ADDR1"),
    F.col("CLPP_PR_ADDR2"),
    F.col("CLPP_PR_CITY"),
    F.col("CLPP_PR_NAME"),
    F.col("CLPP_PR_STATE"),
    F.col("CLPP_ZIP"),
    F.col("CLPP_CLM_SUB_ORG"),
    F.col("CLPP_PERF_NAME")
)

# Output: lkupconstrnt (phone length differs, but we select same data, then cast phone to char(10) if needed)
df_lkupconstrnt = df_CMC_CLPP_ITS_PROV_dedup.select(
    F.col("CLCL_ID"),
    F.col("CLPP_BCBS_PR_ID"),
    F.col("CLPP_BCBS_PR_NPI"),
    F.col("CLPP_CNTRY_CD"),
    F.col("CLPP_FED_TAX_ID"),
    F.col("CLPP_PERF_ID"),
    F.col("CLPP_PERF_ID_NPI"),
    F.col("CLPP_PHONE").alias("CLPP_PHONE"),  # still a string, same name
    F.col("CLPP_PR_ADDR1"),
    F.col("CLPP_PR_ADDR2"),
    F.col("CLPP_PR_CITY"),
    F.col("CLPP_PR_NAME"),
    F.col("CLPP_PR_STATE"),
    F.col("CLPP_ZIP"),
    F.col("CLPP_CLM_SUB_ORG"),
    F.col("CLPP_PERF_NAME")
)

# ODBC Connector: CER_ATAD_ADDRESS
jdbc_url_CER_ATAD_ADDRESS, jdbc_props_CER_ATAD_ADDRESS = get_db_config(facets_secret_name)
query_CER_ATAD_ADDRESS = f"""
SELECT CL.CLCL_ID, ADDR.ATAD_NAME,  ADDR.ATAD_ADDR1,  
ADDR.ATAD_ADDR2,  ADDR.ATAD_ADDR3,  ADDR.ATAD_CITY,  ADDR.ATAD_STATE,  ADDR.ATAD_ZIP,  ADDR.ATAD_COUNTY,  
ADDR.ATAD_CTRY_CD,  ADDR.ATAD_PHONE
FROM {FacetsOwner}.CER_ATAD_ADDRESS_D ADDR, 
     {FacetsOwner}.CER_ATXR_ATTACH_U ATT, 
     {FacetsOwner}.CMC_CLCL_CLAIM CL, 
     tempdb..{DriverTable}  TMP
WHERE CL.CLCL_ID = TMP.CLM_ID
      and (SUBSTRING(CL.CLCL_ID, 6,1) = 'H' or SUBSTRING(CL.CLCL_ID,6,2) = 'RH'
            or SUBSTRING(CL.CLCL_ID,6,1) = 'K' or SUBSTRING(CL.CLCL_ID,6,1) = 'G')
      and CL.ATXR_SOURCE_ID <> '1753-01-01 00:00:00.000'
      and CL.ATXR_SOURCE_ID = ATT.ATXR_SOURCE_ID
      and ATT.ATXR_DEST_ID = ADDR.ATXR_DEST_ID
      and ADDR.ATSY_ID = 'ATUE'
      and ATT.ATSY_ID = ADDR.ATSY_ID

UNION ALL

SELECT CL.CLCL_ID,  ADDR.ATAD_NAME,  ADDR.ATAD_ADDR1,  
ADDR.ATAD_ADDR2,  ADDR.ATAD_ADDR3,  ADDR.ATAD_CITY,  ADDR.ATAD_STATE,  ADDR.ATAD_ZIP,  ADDR.ATAD_COUNTY,  
ADDR.ATAD_CTRY_CD,  ADDR.ATAD_PHONE
FROM {FacetsOwner}.CER_ATAD_ADDRESS_D ADDR, 
     {FacetsOwner}.CER_ATXR_ATTACH_U ATT, 
     {FacetsOwner}.CMC_CLCL_CLAIM CL, 
     tempdb..{AdjTable}  TMP2
WHERE CL.CLCL_ID = (substring(TMP2.CLM_ID,1,10) + '00')
      and (SUBSTRING(CL.CLCL_ID, 6,1) = 'H' or SUBSTRING(CL.CLCL_ID,6,2) = 'RH'
            or SUBSTRING(CL.CLCL_ID,6,1) = 'K' or SUBSTRING(CL.CLCL_ID,6,1) = 'G')
      and CL.ATXR_SOURCE_ID <> '1753-01-01 00:00:00.000'
      and CL.ATXR_SOURCE_ID = ATT.ATXR_SOURCE_ID
      and ATT.ATXR_DEST_ID = ADDR.ATXR_DEST_ID
      and ADDR.ATSY_ID = 'ATUE'
      and ATT.ATSY_ID = ADDR.ATSY_ID
ORDER BY CL.CLCL_ID
"""
df_CER_ATAD_ADDRESS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CER_ATAD_ADDRESS)
    .options(**jdbc_props_CER_ATAD_ADDRESS)
    .option("query", query_CER_ATAD_ADDRESS)
    .load()
)

# Remove intermediate hashed file hf_clm_extrnl_prov_oldITS_addr (Scenario A)
df_CER_ATAD_ADDRESS_dedup = dedup_sort(
    df_CER_ATAD_ADDRESS,
    ["CLCL_ID"],
    [("CLCL_ID","A")]
)
df_oldITSAddr = df_CER_ATAD_ADDRESS_dedup.select(
    "CLCL_ID",
    "ATAD_NAME",
    "ATAD_ADDR1",
    "ATAD_ADDR2",
    "ATAD_ADDR3",
    "ATAD_CITY",
    "ATAD_STATE",
    "ATAD_ZIP",
    "ATAD_COUNTY",
    "ATAD_CTRY_CD",
    "ATAD_PHONE"
)

# ODBC Connector: CMC_CLCL_CLAIM
jdbc_url_CMC_CLCL_CLAIM, jdbc_props_CMC_CLCL_CLAIM = get_db_config(facets_secret_name)
query_CMC_CLCL_CLAIM = f"""
SELECT distinct 
CLM.CLCL_ID, 
CLM.CLCL_CL_SUB_TYPE, 
CLM.CLCL_CUR_STS, 
CLM.CLCL_LAST_ACT_DTM, 
MISC.CLMI_ITS_SCCF_NO,
MISC.CLMI_ITS_SUB_TYPE

FROM {FacetsOwner}.CMC_CLMI_MISC MISC,
     {FacetsOwner}.CMC_CLCL_CLAIM CLM, 
     tempdb..{DriverTable}     TMP 
WHERE TMP.CLM_ID = CLM.CLCL_ID 
AND CLM.CLCL_ID = MISC.CLCL_ID
AND (
    SUBSTRING(CLM.CLCL_ID, 6,1) = 'H'    or 
    SUBSTRING(CLM.CLCL_ID,6,2) = 'RH'  or 
    SUBSTRING(CLM.CLCL_ID,6,1) = 'K'    or 
    SUBSTRING(CLM.CLCL_ID,6,1) = 'G'    or 
    MISC.CLMI_ITS_SUB_TYPE IN ('H','T')
    )
"""
df_CMC_CLCL_CLAIM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_CLCL_CLAIM)
    .options(**jdbc_props_CMC_CLCL_CLAIM)
    .option("query", query_CMC_CLCL_CLAIM)
    .load()
)

# Transformer: old_or_new_its
#   Input:
#       Primary link -> df_CMC_CLCL_CLAIM
#       Lookup link -> df_lkupconstrnt on CLCL_ID
#   Stage var: svOldITS = if len(trim(lkupconstrnt.CLCL_ID))=0 or isnull(...) then 'Y' else 'N'
df_left_old_or_new = df_CMC_CLCL_CLAIM.alias("Extract1")
df_lookup_lkupconstrnt = df_lkupconstrnt.alias("lkupconstrnt")

df_join_old_or_new = df_left_old_or_new.join(
    df_lookup_lkupconstrnt,
    on=F.col("Extract1.CLCL_ID") == F.col("lkupconstrnt.CLCL_ID"),
    how="left"
)

# Compute stage variable
cond_svOldITS = F.when(
    (F.length(F.trim(F.col("lkupconstrnt.CLCL_ID")))==0) | (F.col("lkupconstrnt.CLCL_ID").isNull()),
    F.lit("Y")
).otherwise(F.lit("N"))

df_join_old_or_new = df_join_old_or_new.withColumn("svOldITS", cond_svOldITS)

# Constraint for old_its -> (svOldITS='Y')
df_old_its = df_join_old_or_new.filter(F.col("svOldITS")=="Y").select(
    F.expr("Trim(Extract1.CLCL_ID)").alias("CLCL_ID"),
    F.expr("Trim(Extract1.CLCL_CL_SUB_TYPE)").alias("CLCL_CL_SUB_TYPE"),
    F.expr("Trim(Extract1.CLMI_ITS_SCCF_NO)").alias("CLMI_ITS_SCCF_NO")
)

# Constraint for new_its -> (svOldITS='N')
df_new_its = df_join_old_or_new.filter(F.col("svOldITS")=="N").select(
    F.expr("Trim(Extract1.CLCL_ID)").alias("CLCL_ID"),
    F.expr("Trim(Extract1.CLCL_CL_SUB_TYPE)").alias("CLCL_CL_SUB_TYPE"),
    F.expr("Trim(Extract1.CLMI_ITS_SCCF_NO)").alias("CLMI_ITS_SCCF_NO")
)

# Transformer: StripField
#   Input:
#       Primary link -> df_new_its
#       Lookup link -> df_newITSAddr on CLCL_ID
df_left_StripField = df_new_its.alias("new_its")
df_lookup_newITSAddr = df_newITSAddr.alias("NewITSAddr")

df_StripField_join = df_left_StripField.join(
    df_lookup_newITSAddr,
    on=[F.col("new_its.CLCL_ID") == F.col("NewITSAddr.CLCL_ID")],
    how="left"
)

df_StripField = df_StripField_join.select(
    F.expr("Trim(new_its.CLCL_ID)").alias("CLCL_ID"),
    F.expr("Trim(new_its.CLCL_CL_SUB_TYPE)").alias("CLCL_CL_SUB_TYPE"),
    F.expr("Trim(NewITSAddr.CLPP_BCBS_PR_ID)").alias("CLPP_BCBS_PR_ID"),
    F.expr("Trim(NewITSAddr.CLPP_BCBS_PR_NPI)").alias("CLPP_BCBS_PR_NPI"),
    F.expr("Trim(NewITSAddr.CLPP_CNTRY_CD)").alias("CLPP_CNTRY_CD"),
    F.expr("Trim(NewITSAddr.CLPP_FED_TAX_ID)").alias("CLPP_FED_TAX_ID"),
    F.expr("Trim(NewITSAddr.CLPP_PERF_ID)").alias("CLPP_PERF_ID"),
    F.expr("Trim(NewITSAddr.CLPP_PERF_ID_NPI)").alias("CLPP_PERF_ID_NPI"),
    F.expr("Trim(NewITSAddr.CLPP_PHONE)").alias("CLPP_PHONE"),
    F.expr("Trim(NewITSAddr.CLPP_PR_ADDR1)").alias("CLPP_PR_ADDR1"),
    F.expr("Trim(NewITSAddr.CLPP_PR_ADDR2)").alias("CLPP_PR_ADDR2"),
    F.expr("Trim(NewITSAddr.CLPP_PR_CITY)").alias("CLPP_PR_CITY"),
    F.expr("Trim(NewITSAddr.CLPP_PR_NAME)").alias("CLPP_PR_NAME"),
    F.expr("Trim(NewITSAddr.CLPP_PR_STATE)").alias("CLPP_PR_STATE"),
    F.expr("Trim(NewITSAddr.CLPP_ZIP)").alias("CLPP_ZIP"),
    F.when(
        F.col("new_its.CLCL_CL_SUB_TYPE")==F.lit("H"),
        F.expr("Trim(NewITSAddr.CLPP_PR_NAME)")
    ).otherwise(
        F.when(
            F.trim(F.col("new_its.CLCL_CL_SUB_TYPE"))==F.lit("M"),
            F.when(
                F.length(F.trim(F.col("NewITSAddr.CLPP_CLM_SUB_ORG")))>0,
                F.col("NewITSAddr.CLPP_CLM_SUB_ORG")
            ).otherwise(
                F.when(
                    F.length(F.trim(F.col("NewITSAddr.CLPP_PR_NAME")))>0,
                    F.col("NewITSAddr.CLPP_PR_NAME")
                ).otherwise(
                    F.col("NewITSAddr.CLPP_PERF_NAME")
                )
            )
        ).otherwise(F.col("NewITSAddr.CLPP_PERF_NAME"))
    ).alias("PROV_NM"),
    F.when(
        (F.col("NewITSAddr.CLPP_PERF_NAME").isNull()) | (F.length(F.col("NewITSAddr.CLPP_PERF_NAME"))==0),
        F.lit(None)
    ).otherwise(F.trim(F.col("NewITSAddr.CLPP_PERF_NAME"))).alias("SVC_PROV_NM")
)

# Remove intermediate hashed file hf_clm_extrn_prov_newITS (Scenario A)
df_StripField_dedup = dedup_sort(
    df_StripField,
    ["CLCL_ID"],
    [("CLCL_ID","A")]
)
df_new = df_StripField_dedup.select(
    "CLCL_ID",
    "CLCL_CL_SUB_TYPE",
    "CLPP_BCBS_PR_ID",
    "CLPP_BCBS_PR_NPI",
    "CLPP_CNTRY_CD",
    "CLPP_FED_TAX_ID",
    "CLPP_PERF_ID",
    "CLPP_PERF_ID_NPI",
    F.col("CLPP_PHONE").cast(StringType()).alias("CLPP_PHONE"),  # stored length=10 in the DS metadata
    "CLPP_PR_ADDR1",
    "CLPP_PR_ADDR2",
    "CLPP_PR_CITY",
    "CLPP_PR_NAME",
    "CLPP_PR_STATE",
    "CLPP_ZIP",
    "PROV_NM",
    "SVC_PROV_NM"
)

# ODBC Connector: ITS_BLE_EXCHG_INQ
jdbc_url_ITS_BLE_EXCHG_INQ, jdbc_props_ITS_BLE_EXCHG_INQ = get_db_config(facets_secret_name)
query_ITS_BLE_EXCHG_INQ = f"""
select 
CLMI_ITS_SCCF_NO, 
ITS_PRV_NO, 
ITS_PRV_TAX_ID,
ITS_PROV_NPI_ID,
ITS_SERVICING_PROV_NPI,
ITS_SERVICING_PROV_NO
from 
{FacetsOwner}.ITS_BLE_EXCHG_INQR BLE, 
{FacetsOwner}.CMC_CLMI_MISC CLMI, 
tempdb..{DriverTable} TMP
where CLMI.CLCL_ID = TMP.CLM_ID and
      Trim( substring(CLMI.CLMI_ITS_SCCF_NO,1,15) + '00') = BLE.ITS_SCCF_NO and
      (SUBSTRING(CLMI.CLCL_ID, 6,1) = 'H' or SUBSTRING(CLMI.CLCL_ID,6,2) = 'RH'
       or SUBSTRING(CLMI.CLCL_ID,6,1) = 'K' or SUBSTRING(CLMI.CLCL_ID,6,1) = 'G')
"""
df_ITS_BLE_EXCHG_INQ = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ITS_BLE_EXCHG_INQ)
    .options(**jdbc_props_ITS_BLE_EXCHG_INQ)
    .option("query", query_ITS_BLE_EXCHG_INQ)
    .load()
)

# Remove intermediate hashed file hf_clm_extrnl_prov_old_ITS_tax_id (Scenario A)
df_ITS_BLE_EXCHG_INQ_dedup = dedup_sort(
    df_ITS_BLE_EXCHG_INQ,
    ["CLMI_ITS_SCCF_NO"],
    [("CLMI_ITS_SCCF_NO","A")]
)
df_OldITStaxId = df_ITS_BLE_EXCHG_INQ_dedup.select(
    "CLMI_ITS_SCCF_NO",
    "ITS_PRV_NO",
    "ITS_PRV_TAX_ID",
    "ITS_PROV_NPI_ID",
    "ITS_SERVICING_PROV_NPI",
    "ITS_SERVICING_PROV_NO"
)

# Transformer: Strip
#   Input:
#     Primary -> df_old_its
#     Lookup1 -> df_oldITSAddr (no join conditions => use a left join on CLCL_ID if we want to match? Actually there's no explicit condition in DS, so that typically results in a left join on a literal True? The design, however, frequently implies no matching columns means no join. But the pinned columns do mention "Expression" referencing oldITSAddr? There's presumably a pass? 
#     Lookup2 -> df_OldITStaxId joined on old_its.CLMI_ITS_SCCF_NO = OldITStaxId.CLMI_ITS_SCCF_NO
df_left_Strip = df_old_its.alias("old_its")
df_lkp_OldITSAddr = df_oldITSAddr.alias("OldITSAddr")  # no explicit join conditions listed, DS job had an empty array. That typically means no columns => but DataStage “no join conditions” on a lookup link mimics a cross or maybe a left with a constraint? In DataStage, that is tricky. Usually it’s either a cross join or we ignore columns? The design used expressions referencing OldITSAddr.* with an IF checking IsNull? 
# In practice, DataStage often merges them by the same CLCL_ID. But the JSON doesn't show "JoinConditions": []. So it might be a leftover. 
# We'll interpret the references "If IsNull(... OldITSAddr.CLCL_ID)..." => that implies a left join on "CLCL_ID". Let’s do that for consistent DS logic.

df_join_1 = df_left_Strip.join(
    df_lkp_OldITSAddr,
    on=[F.col("old_its.CLCL_ID") == F.col("OldITSAddr.CLCL_ID")],
    how="left"
)

df_lkp_OldITStaxId = df_OldITStaxId.alias("OldITStaxId")
df_join_2 = df_join_1.join(
    df_lkp_OldITStaxId,
    on=[F.col("old_its.CLMI_ITS_SCCF_NO") == F.col("OldITStaxId.CLMI_ITS_SCCF_NO")],
    how="left"
)

df_Strip_out = df_join_2.select(
    F.expr("Trim(old_its.CLCL_ID)").alias("CLCL_ID"),
    F.expr("Trim(old_its.CLCL_CL_SUB_TYPE)").alias("CLCL_CL_SUB_TYPE"),
    F.when(
        F.isnull(F.trim(F.col("OldITStaxId.CLMI_ITS_SCCF_NO")))==F.lit(False),
        F.expr("Trim(OldITStaxId.ITS_PRV_NO)")
    ).otherwise(F.lit(" ")).alias("BCBS_PR_ID"),
    F.when(
        F.isnull(F.trim(F.col("OldITStaxId.CLMI_ITS_SCCF_NO")))==F.lit(False),
        F.expr("Trim(OldITStaxId.ITS_PROV_NPI_ID)")
    ).otherwise(F.lit(" ")).alias("BCBS_PR_NPI"),
    F.when(
        F.isnull(F.trim(F.col("OldITSAddr.CLCL_ID")))==F.lit(False),
        F.expr("Trim(OldITSAddr.ATAD_CTRY_CD)")
    ).otherwise(F.expr("Space(1)")).alias("CNTRY_CD"),
    F.when(
        F.isnull(F.trim(F.col("OldITStaxId.CLMI_ITS_SCCF_NO")))==F.lit(False),
        F.expr("Trim(OldITStaxId.ITS_PRV_TAX_ID)")
    ).otherwise(F.lit(" ")).alias("FED_TAX_ID"),
    F.when(
        F.isnull(F.col("OldITStaxId.ITS_SERVICING_PROV_NO")),
        F.expr("Space(1)")
    ).otherwise(F.expr("Trim(OldITStaxId.ITS_SERVICING_PROV_NO)")).alias("PERF_ID"),
    F.when(
        F.isnull(F.trim(F.col("OldITStaxId.CLMI_ITS_SCCF_NO")))==F.lit(False),
        F.expr("Trim(OldITStaxId.ITS_SERVICING_PROV_NPI)")
    ).otherwise(F.lit(" ")).alias("PERF_ID_NPI"),
    F.when(
        F.isnull(F.trim(F.col("OldITSAddr.CLCL_ID")))==F.lit(False),
        F.expr("Trim(OldITSAddr.ATAD_PHONE)")
    ).otherwise(F.lit(None)).alias("PHONE"),
    F.when(
        F.isnull(F.trim(F.col("OldITSAddr.CLCL_ID")))==F.lit(False),
        F.expr("Trim(OldITSAddr.ATAD_ADDR1)")
    ).otherwise(F.lit(None)).alias("PR_ADDR1"),
    F.when(
        F.isnull(F.trim(F.col("OldITSAddr.CLCL_ID")))==F.lit(False),
        F.expr("Trim(OldITSAddr.ATAD_ADDR2)")
    ).otherwise(F.lit(None)).alias("PR_ADDR2"),
    F.when(
        F.isnull(F.trim(F.col("OldITSAddr.CLCL_ID")))==F.lit(False),
        F.expr("Trim(OldITSAddr.ATAD_CITY)")
    ).otherwise(F.lit(None)).alias("PR_CITY"),
    F.when(
        F.isnull(F.trim(F.col("OldITSAddr.CLCL_ID")))==F.lit(False),
        F.expr("Trim(OldITSAddr.ATAD_NAME)")
    ).otherwise(F.lit(None)).alias("PR_NAME"),
    F.when(
        F.isnull(F.trim(F.col("OldITSAddr.CLCL_ID")))==F.lit(False),
        F.expr("Trim(OldITSAddr.ATAD_STATE)")
    ).otherwise(F.expr("Space(1)")).alias("PR_STATE"),
    F.when(
        F.isnull(F.trim(F.col("OldITSAddr.CLCL_ID")))==F.lit(False),
        F.expr("Trim(OldITSAddr.ATAD_ZIP)")
    ).otherwise(F.lit(None)).alias("ZIP"),
    F.when(
        F.isnull(F.trim(F.col("OldITSAddr.CLCL_ID")))==F.lit(False),
        F.expr("Trim(OldITSAddr.ATAD_NAME)")
    ).otherwise(F.lit(None)).alias("PROV_NM"),
    F.lit(" ").alias("SVC_PROV_NM")  # WhereExpression=" "
)

# Remove intermediate hashed file hf_clm_extrnl_prov_oldITS (Scenario A)
df_Strip_out_dedup = dedup_sort(
    df_Strip_out,
    ["CLCL_ID"],
    [("CLCL_ID","A")]
)
df_old = df_Strip_out_dedup.select(
    F.col("CLCL_ID"),
    F.col("CLCL_CL_SUB_TYPE"),
    F.col("BCBS_PR_ID").alias("CLPP_BCBS_PR_ID"),
    F.col("BCBS_PR_NPI").alias("CLPP_BCBS_PR_NPI"),
    F.col("CNTRY_CD").alias("CLPP_CNTRY_CD"),
    F.col("FED_TAX_ID").alias("CLPP_FED_TAX_ID"),
    F.col("PERF_ID").alias("CLPP_PERF_ID"),
    F.col("PERF_ID_NPI").alias("CLPP_PERF_ID_NPI"),
    F.col("PHONE").alias("CLPP_PHONE"),
    F.col("PR_ADDR1").alias("CLPP_PR_ADDR1"),
    F.col("PR_ADDR2").alias("CLPP_PR_ADDR2"),
    F.col("PR_CITY").alias("CLPP_PR_CITY"),
    F.col("PR_NAME").alias("CLPP_PR_NAME"),
    F.col("PR_STATE").alias("CLPP_PR_STATE"),
    F.col("ZIP").alias("CLPP_ZIP"),
    F.col("PROV_NM"),
    F.col("SVC_PROV_NM")
)

# Collector: merge_old_new
#   Input1 -> df_old
#   Input2 -> df_new
#   Round robin union
df_merge_old_new = df_old.unionByName(df_new, allowMissingColumns=True)

# Remove intermediate hashed file hf_clm_extrn_prov_all_claims (Scenario A)
df_merge_old_new_dedup = dedup_sort(
    df_merge_old_new,
    ["CLCL_ID"],
    [("CLCL_ID","A")]
)
df_all_claims = df_merge_old_new_dedup.select(
    "CLCL_ID",
    "CLCL_CL_SUB_TYPE",
    "CLPP_BCBS_PR_ID",
    "CLPP_BCBS_PR_NPI",
    "CLPP_CNTRY_CD",
    "CLPP_FED_TAX_ID",
    "CLPP_PERF_ID",
    "CLPP_PERF_ID_NPI",
    "CLPP_PHONE",
    "CLPP_PR_ADDR1",
    "CLPP_PR_ADDR2",
    "CLPP_PR_CITY",
    "CLPP_PR_NAME",
    "CLPP_PR_STATE",
    "CLPP_ZIP",
    "PROV_NM",
    "SVC_PROV_NM"
)

# BusinessRules
#   Primary link -> df_all_claims as "Strip"
#   Lookup link1 -> df_hf_clm_fcts_reversals as "fcts_reversals" joined on CLCL_ID
#   Lookup link2 -> df_clm_nasco_dup_bypass as "nasco_dup_lkup" joined on CLCL_ID

df_primary_BusinessRules = df_all_claims.alias("Strip")
df_lookup_fcts_reversals = df_hf_clm_fcts_reversals.alias("fcts_reversals")
df_lookup_nasco_dup = df_clm_nasco_dup_bypass.alias("nasco_dup_lkup")

df_join_1_BusinessRules = df_primary_BusinessRules.join(
    df_lookup_fcts_reversals,
    on=[F.col("Strip.CLCL_ID")==F.col("fcts_reversals.CLCL_ID")],
    how="left"
)
df_join_2_BusinessRules = df_join_1_BusinessRules.join(
    df_lookup_nasco_dup,
    on=[F.col("Strip.CLCL_ID")==F.col("nasco_dup_lkup.CLM_ID")],
    how="left"
)

# Create stage variable "ClmId" = trim(Strip.CLCL_ID), but we can do that inline
df_BusinessRules = df_join_2_BusinessRules.withColumn("ClmId", F.trim(F.col("Strip.CLCL_ID")))

# Output link "Regular" constraint: IsNull(nasco_dup_lkup.CLM_ID)=@TRUE
df_BusinessRules_Regular = df_BusinessRules.filter(
    F.col("nasco_dup_lkup.CLM_ID").isNull()
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"),F.lit(";"),F.col("ClmId")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_EXTRNL_PROV_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.col("ClmId").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.col("Strip.PROV_NM").alias("PROV_NM"),
    F.when(
       (F.col("Strip.CLPP_PR_ADDR1").isNull()) | (F.length(F.col("Strip.CLPP_PR_ADDR1"))==0),
       F.lit(None)
    ).otherwise(F.col("Strip.CLPP_PR_ADDR1")).alias("ADDR_LN_1"),
    F.when(
       (F.col("Strip.CLPP_PR_ADDR2").isNull()) | (F.length(F.col("Strip.CLPP_PR_ADDR2"))==0),
       F.lit(None)
    ).otherwise(F.col("Strip.CLPP_PR_ADDR2")).alias("ADDR_LN_2"),
    F.lit(None).alias("ADDR_LN_3"),
    F.when(
       (F.col("Strip.CLPP_PR_CITY").isNull()) | (F.length(F.col("Strip.CLPP_PR_CITY"))==0),
       F.lit(" ")
    ).otherwise(F.col("Strip.CLPP_PR_CITY")).alias("CITY_NM"),
    F.when(
       (F.trim(F.col("Strip.CLPP_PR_STATE")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_PR_STATE")))==0),
       F.lit("NA")
    ).otherwise(F.trim(F.col("Strip.CLPP_PR_STATE"))).alias("CLPP_PR_STATE"),
    F.lit(0).alias("CLM_EXTRNL_PROV_ST_CD_SK"),
    F.when(
       (F.trim(F.col("Strip.CLPP_ZIP")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_ZIP")))==0),
       F.lit(" ")
    ).otherwise(F.trim(F.col("Strip.CLPP_ZIP"))).alias("POSTAL_CD"),
    F.lit(None).alias("CNTY_NM"),
    F.when(
       (F.trim(F.col("Strip.CLPP_CNTRY_CD")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_CNTRY_CD")))==0),
       F.lit("NA")
    ).otherwise(F.trim(F.col("Strip.CLPP_CNTRY_CD"))).alias("CLPP_CNTRY_CD"),
    F.lit(0).alias("CLM_EXTRNL_PROV_CTRY_CD_SK"),
    F.when(
       (F.trim(F.col("Strip.CLPP_PHONE")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_PHONE")))==0),
       F.lit(None)
    ).otherwise(F.trim(F.col("Strip.CLPP_PHONE"))).alias("PHN_NO"),
    F.when(
       F.length(F.trim(F.col("Strip.CLPP_BCBS_PR_ID")))==0,
       F.lit(" ")
    ).otherwise(F.col("Strip.CLPP_BCBS_PR_ID")).alias("PROV_ID"),
    F.when(
       F.length(F.trim(F.col("Strip.CLPP_BCBS_PR_NPI")))==0,
       F.lit(" ")
    ).otherwise(F.trim(F.col("Strip.CLPP_BCBS_PR_NPI"))).alias("PROV_NPI"),
    F.when(
       F.length(F.trim(F.col("Strip.CLPP_PERF_ID")))==0,
       F.lit(" ")
    ).otherwise(F.trim(F.col("Strip.CLPP_PERF_ID"))).alias("SVC_PROV_ID"),
    F.when(
       F.length(F.trim(F.col("Strip.CLPP_PERF_ID_NPI")))==0,
       F.lit(" ")
    ).otherwise(F.trim(F.col("Strip.CLPP_PERF_ID_NPI"))).alias("SVC_PROV_NPI"),
    F.col("Strip.SVC_PROV_NM").alias("SVC_PROV_NM"),
    F.when(
       F.length(F.trim(F.col("Strip.CLPP_FED_TAX_ID")))==0,
       F.lit(" ")
    ).otherwise(F.trim(F.col("Strip.CLPP_FED_TAX_ID"))).alias("TAX_ID")
)

# Output link "Reversals" constraint:
# (IsNull(fcts_reversals.CLCL_ID)=@FALSE) and (fcts_reversals.CLCL_CUR_STS in ('89','91','99'))
df_BusinessRules_Reversals = df_BusinessRules.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    ((F.col("fcts_reversals.CLCL_CUR_STS")==F.lit("89"))|
     (F.col("fcts_reversals.CLCL_CUR_STS")==F.lit("91"))|
     (F.col("fcts_reversals.CLCL_CUR_STS")==F.lit("99")))
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"),F.lit(";"),F.col("ClmId"),F.lit("R")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_EXTRNL_PROV_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.concat(F.col("ClmId"),F.lit("R")).alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.col("Strip.PROV_NM").alias("PROV_NM"),
    F.when(
       (F.trim(F.col("Strip.CLPP_PR_ADDR1")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_PR_ADDR1")))==0),
       F.lit(None)
    ).otherwise(F.trim(F.col("Strip.CLPP_PR_ADDR1"))).alias("ADDR_LN_1"),
    F.when(
       (F.trim(F.col("Strip.CLPP_PR_ADDR2")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_PR_ADDR2")))==0),
       F.lit(None)
    ).otherwise(F.trim(F.col("Strip.CLPP_PR_ADDR2"))).alias("ADDR_LN_2"),
    F.lit(None).alias("ADDR_LN_3"),
    F.when(
       (F.trim(F.col("Strip.CLPP_PR_CITY")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_PR_CITY")))==0),
       F.lit(" ")
    ).otherwise(F.trim(F.col("Strip.CLPP_PR_CITY"))).alias("CITY_NM"),
    F.when(
       (F.trim(F.col("Strip.CLPP_PR_STATE")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_PR_STATE")))==0),
       F.lit("NA")
    ).otherwise(F.trim(F.col("Strip.CLPP_PR_STATE"))).alias("CLPP_PR_STATE"),
    F.lit(0).alias("CLM_EXTRNL_PROV_ST_CD_SK"),
    F.when(
       (F.trim(F.col("Strip.CLPP_ZIP")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_ZIP")))==0),
       F.lit(" ")
    ).otherwise(F.trim(F.col("Strip.CLPP_ZIP"))).alias("POSTAL_CD"),
    F.lit(None).alias("CNTY_NM"),
    F.when(
       (F.trim(F.col("Strip.CLPP_CNTRY_CD")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_CNTRY_CD")))==0),
       F.lit("NA")
    ).otherwise(F.trim(F.col("Strip.CLPP_CNTRY_CD"))).alias("CLPP_CNTRY_CD"),
    F.lit(0).alias("CLM_EXTRNL_PROV_CTRY_CD_SK"),
    F.when(
       (F.trim(F.col("Strip.CLPP_PHONE")).isNull()) | (F.length(F.trim(F.col("Strip.CLPP_PHONE")))==0),
       F.lit(None)
    ).otherwise(F.trim(F.col("Strip.CLPP_PHONE"))).alias("PHN_NO"),
    F.when(
       F.length(F.trim(F.col("Strip.CLPP_BCBS_PR_ID")))==0,
       F.lit(" ")
    ).otherwise(F.trim(F.col("Strip.CLPP_BCBS_PR_ID"))).alias("PROV_ID"),
    F.when(
       F.length(F.trim(F.col("Strip.CLPP_BCBS_PR_NPI")))==0,
       F.lit(" ")
    ).otherwise(F.trim(F.col("Strip.CLPP_BCBS_PR_NPI"))).alias("PROV_NPI"),
    F.when(
       F.length(F.trim(F.col("Strip.CLPP_PERF_ID")))==0,
       F.lit(" ")
    ).otherwise(F.trim(F.col("Strip.CLPP_PERF_ID"))).alias("SVC_PROV_ID"),
    F.when(
       F.length(F.trim(F.col("Strip.CLPP_PERF_ID_NPI")))==0,
       F.lit(" ")
    ).otherwise(F.trim(F.col("Strip.CLPP_PERF_ID_NPI"))).alias("SVC_PROV_NPI"),
    F.col("Strip.SVC_PROV_NM").alias("SVC_PROV_NM"),
    F.when(
       F.length(F.trim(F.col("Strip.CLPP_FED_TAX_ID")))==0,
       F.lit(" ")
    ).otherwise(F.trim(F.col("Strip.CLPP_FED_TAX_ID"))).alias("TAX_ID")
)

# Collector: merges Reversals and Regular => union
df_Collector = df_BusinessRules_Reversals.unionByName(df_BusinessRules_Regular, allowMissingColumns=True)

# Snapshot
df_Snapshot = df_Collector.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_EXTRNL_PROV_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "PROV_NM",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "ADDR_LN_3",
    "CITY_NM",
    "CLPP_PR_STATE",
    "CLM_EXTRNL_PROV_ST_CD_SK",
    "POSTAL_CD",
    "CNTY_NM",
    "CLPP_CNTRY_CD",
    "CLM_EXTRNL_PROV_CTRY_CD_SK",
    "PHN_NO",
    "PROV_ID",
    "PROV_NPI",
    "SVC_PROV_ID",
    "SVC_PROV_NPI",
    "SVC_PROV_NM",
    "TAX_ID"
)

# Next stage: Snapshot => output pins:
#   Pkey -> goes into the shared container "ClmExtrnlProvPK"
#   SnapShot -> next stage "Transformer" with only CLM_ID ?

df_Snapshot_pkey = df_Snapshot.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_EXTRNL_PROV_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "PROV_NM",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "ADDR_LN_3",
    "CITY_NM",
    "CLPP_PR_STATE",
    "CLM_EXTRNL_PROV_ST_CD_SK",
    "POSTAL_CD",
    "CNTY_NM",
    "CLPP_CNTRY_CD",
    "CLM_EXTRNL_PROV_CTRY_CD_SK",
    "PHN_NO",
    "PROV_ID",
    "PROV_NPI",
    "SVC_PROV_ID",
    "SVC_PROV_NPI",
    "SVC_PROV_NM",
    "TAX_ID"
)

df_Snapshot_SnapShot = df_Snapshot.select(
    F.col("CLM_ID").alias("CLM_ID")
)

# Next stage: Transformer => input is SnapShot_SnapShot => output => RowCount => B_CLM_EXTRNL_PROV
df_Transformer_in = df_Snapshot_SnapShot.alias("SnapShot")
df_Transformer_out = df_Transformer_in.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("SnapShot.CLM_ID").alias("CLM_ID")
)

# Write to CSeqFileStage => B_CLM_EXTRNL_PROV
# Before writing, apply rpad if columns are char or varchar with known length.
# The job metadata for "B_CLM_EXTRNL_PROV" only shows these 2 columns, no explicit lengths given. 
df_B_CLM_EXTRNL_PROV = df_Transformer_out.select("SRC_SYS_CD_SK","CLM_ID")

write_files(
    df_B_CLM_EXTRNL_PROV,
    f"{adls_path}/load/B_CLM_EXTRNL_PROV.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Shared container: ClmExtrnlProvPK
params_ClmExtrnlProvPK = {
    "CurrRunCycle": CurrRunCycle
}
df_IdsClmExtrnlProvPkey = ClmExtrnlProvPK(df_Snapshot_pkey, params_ClmExtrnlProvPK)

# Next stage: IdsClmExtrnlProvPkey => CSeqFileStage => write final
# The pinned columns have char/varchar definitions with lengths. We apply rpad for each such column.
df_IdsClmExtrnlProvPkey_out = df_IdsClmExtrnlProvPkey.select(
    F.rpad(F.col("JOB_EXCTN_RCRD_ERR_SK").cast(StringType()), 1, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),  # no length in DS, minimal
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.rpad(F.col("SRC_SYS_CD"), 6, " ").alias("SRC_SYS_CD"),  # no DS length but approximate
    F.col("PRI_KEY_STRING"),
    F.col("CLM_EXTRNL_PROV_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK"),
    F.col("PROV_NM"),
    F.col("ADDR_LN_1"),
    F.col("ADDR_LN_2"),
    F.col("ADDR_LN_3"),
    F.col("CITY_NM"),
    F.rpad(F.col("CLPP_PR_STATE"), 2, " ").alias("CLPP_PR_STATE"),
    F.col("CLM_EXTRNL_PROV_ST_CD_SK"),
    F.rpad(F.col("POSTAL_CD"), 11, " ").alias("POSTAL_CD"),
    F.col("CNTY_NM"),
    F.rpad(F.col("CLPP_CNTRY_CD"), 4, " ").alias("CLPP_CNTRY_CD"),
    F.col("CLM_EXTRNL_PROV_CTRY_CD_SK"),
    F.rpad(F.col("PHN_NO"), 20, " ").alias("PHN_NO"),
    F.col("PROV_ID"),
    F.col("PROV_NPI"),
    F.rpad(F.col("SVC_PROV_ID"), 13, " ").alias("SVC_PROV_ID"),
    F.col("SVC_PROV_NPI"),
    F.col("SVC_PROV_NM"),
    F.rpad(F.col("TAX_ID"), 9, " ").alias("TAX_ID")
)

write_files(
    df_IdsClmExtrnlProvPkey_out,
    f"{adls_path}/key/FctsClmExtrnlProvExtr.FctsClmExtrnlProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)