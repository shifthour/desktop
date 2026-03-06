# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC This job is retrieves the medical product id from Facets and compares with the plan configuration from EAM and write the results to a file where the Medical Product ids don't match.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ======================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project               
# MAGIC ======================================================================================================================================
# MAGIC ReddySanam               2021-01-26                  US329820                                         Original Programming                                                    IntegrateDev2                    Kalyan Neelam         2021-01-27
# MAGIC ReddySanam               2021-01-31                  US329820                                         Consider only DISCREPANCY_NAME                         IntegrateDev2                    Jeyaprasanna           2021-02-02  
# MAGIC                                                                                                                                   of MedicalProductID mismatch 
# MAGIC JohnAbraham               2021-08-11                  US391328              Added EAMRPT Env variables and updated appropriate tables             IntegrateDev1
# MAGIC                                                                                                                with these parameters 
# MAGIC Prabhu ES                    2022-03-21                  S2S                                                   MSSQL ODBC conn params added                             IntegrateDev5	Ken Bradmon	2022-05-20

# MAGIC This job is retrieves the medical product id from Facets and compares with the plan configuration from EAM and write the results to a file where the Medical Product ids don't match.
# MAGIC Attach the plan id for each MBI from EAM. After the plan id is attached, it is compared between FACETS and PLAN CONFIG and call out if they don't match.
# MAGIC This file will have discrepancies that are still open
# MAGIC This file will have resolved discrepancies between last run and current run. These records need to be deleted
# MAGIC This file will have newly found discrepancies
# MAGIC Check if a discrepancy already exists for an MBI and take appropriate action if exits/does not exists/existed and fixes by currne Run.
# MAGIC This is to remove duplicates if an MBI has more than one SBSB_ID.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
EAMOwner = get_widget_value('EAMOwner','')
eam_secret_name = get_widget_value('eam_secret_name','')
RUNID = get_widget_value('RUNID','')
BillDt = get_widget_value('BillDt','')
CurrDt = get_widget_value('CurrDt','')

jdbc_url_eam, jdbc_props_eam = get_db_config(eam_secret_name)
extract_query_EAM_DISC_ANALYSIS = (
    "SELECT  DISC.ID AS ID_SK,DISC.MBI,DISC.IDENTIFIEDBY,DISC.IDENTIFIEDDATE,DISC.DISCREPANCY as DISCREPANCY_ID "
    "FROM " + EAMOwner + ".DISCREPANCY_ANALYSIS DISC "
    "INNER JOIN " + EAMOwner + ".DISCREPANCY_CONFIG CONFIG "
    "ON DISC.DISCREPANCY = CONFIG.ID "
    "WHERE STATUS = 'OPEN' "
    "and DISC.IDENTIFIEDBY = 'BILL' "
    "and CONFIG.DISCREPANCY_NAME = 'INC_MP'"
)
df_EAM_DISC_ANALYSIS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eam)
    .options(**jdbc_props_eam)
    .option("query", extract_query_EAM_DISC_ANALYSIS)
    .load()
)

extract_query_DISC_CONFIG = (
    "select ID AS DISCREPANCY_ID, DISCREPANCY_NAME, DISCREPANCY_DESC, IDENTIFIEDBY, PRIORITY, ACTIVE, SOURCE1, SOURCE2 "
    "from " + EAMOwner + ".DISCREPANCY_CONFIG "
    "WHERE ACTIVE = 'Y' "
    "AND IDENTIFIEDBY = 'BILL' "
    "AND DISCREPANCY_NAME = 'INC_MP'"
)
df_DISC_CONFIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eam)
    .options(**jdbc_props_eam)
    .option("query", extract_query_DISC_CONFIG)
    .load()
)

extract_query_EAM_Product = (
    "SELECT Distinct PlanID.FieldValue as PlanID, PBP.FieldValue as PBP, SUBSTRING(SCC.FieldValue, 1, 2) as StateCode, "
    "Groups.GRGR_ID as GRGR_ID, OSB.FieldValue as OSB, MedProd.FieldValue as MedicalProduct, DentProd.FieldValue as DentalProduct "
    "FROM " + EAMOwner + ".CoreSystemConfigManagerRules RULES "
    "INNER JOIN " + EAMOwner + ".CoreSystemConfigManagerFieldMap PlanID on PlanID.RuleID = RULES.RuleID AND PlanID.FieldId =1 "
    "INNER JOIN " + EAMOwner + ".CoreSystemConfigManagerFieldMap PBP on PBP.RuleID = RULES.RuleID AND PBP.FieldId =2 "
    "INNER JOIN " + EAMOwner + ".CoreSystemConfigManagerFieldMap SCC on SCC.RuleID = RULES.RuleID AND SCC.FieldId =4 "
    "LEFT OUTER JOIN " + EAMOwner + ".CoreSystemConfigManagerFieldMap OSB on OSB.RuleID = RULES.RuleID AND OSB.FieldId =8 "
    "LEFT OUTER JOIN " + EAMOwner + ".CoreSystemConfigManagerFieldMap GRP on GRP.RuleID = RULES.RuleID AND GRP.FieldId =11 "
    "LEFT OUTER JOIN " + EAMOwner + ".Groups ON Groups.GroupID = GRP.FieldValue "
    "LEFT OUTER JOIN " + EAMOwner + ".CoreSystemConfigManagerFieldMap SBG on SBG.RuleID = RULES.RuleID AND SBG.FieldId =12 "
    "LEFT OUTER JOIN " + EAMOwner + ".CoreSystemConfigManagerFieldMap CLS on CLS.RuleID = RULES.RuleID AND CLS.FieldId =13 "
    "LEFT OUTER JOIN " + EAMOwner + ".CoreSystemConfigManagerFieldMap MedProd on MedProd.RuleID = RULES.RuleID AND MedProd.FieldId =14 "
    "LEFT OUTER JOIN " + EAMOwner + ".CoreSystemConfigManagerFieldMap DentProd on DentProd.RuleID = RULES.RuleID AND DentProd.FieldId =15 "
    "WHERE getDate() BETWEEN RULES. MinEffectiveDate AND RULES. MaxEffectiveDate "
    "AND RULES.IsActive = 1 "
    "AND RULES.IsValid=1 "
    "AND OSB.FieldValue = '0'"
)
df_EAM_Product = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_eam)
    .options(**jdbc_props_eam)
    .option("query", extract_query_EAM_Product)
    .load()
)

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_MemberAttr = (
    "/*Fix has been added to filter the null values in the table CMC_MEMD_MECR_DETL to resolved code issue in s2s testing*/ "
    "SELECT  DISTINCT "
    "LTRIM(RTRIM(GRGR.GRGR_ID)) as GRGR_ID, SBSB.SBSB_ID, MEME.MEME_HICN AS MBI, LTRIM(RTRIM(BGBG.BGBG_ID)) BGBG_ID, "
    "LTRIM(RTRIM(PBP.MEMD_MCTR_PBP)) MEMD_MCTR_PBP, LTRIM(RTRIM(SCCC.MEMD_MCTR_MCST)) MEMD_MCTR_MCST, LTRIM(RTRIM(MEPE.CSPI_ID)) CSPI_ID "
    "FROM " + FacetsOwner + ".CMC_SBSB_SUBSC SBSB "
    "INNER JOIN " + FacetsOwner + ".CMC_GRGR_GROUP GRGR ON GRGR.GRGR_CK=SBSB.GRGR_CK "
    "INNER JOIN " + FacetsOwner + ".CMC_MEME_MEMBER MEME ON MEME.SBSB_CK=SBSB.SBSB_CK "
    "INNER JOIN " + FacetsOwner + ".CMC_MEPE_PRCS_ELIG MEPE ON MEPE.MEME_CK=MEME.MEME_CK AND MEPE.CSPD_CAT='M' AND MEPE.MEPE_ELIG_IND='Y' "
    "AND '" + BillDt + "' BETWEEN MEPE.MEPE_EFF_DT AND MEPE.MEPE_TERM_DT "
    "INNER JOIN " + FacetsOwner + ".CMC_MEMD_MECR_DETL PBP ON PBP.MEME_CK=MEME.MEME_CK AND PBP.MEMD_EVENT_CD='PBP' "
    "AND '" + BillDt + "'  BETWEEN PBP.MEMD_HCFA_EFF_DT AND  PBP.MEMD_HCFA_TERM_DT "
    "LEFT OUTER JOIN " + FacetsOwner + ".CMC_MEMD_MECR_DETL SCCC ON SCCC.MEME_CK=MEME.MEME_CK "
    "and SCCC.MEMD_EVENT_CD='SCCC' "
    "and '" + BillDt + "' BETWEEN SCCC.MEMD_HCFA_EFF_DT AND  SCCC.MEMD_HCFA_TERM_DT "
    "LEFT OUTER JOIN " + FacetsOwner + ".CMC_BGBG_BIL_GROUP BGBG ON BGBG.BGBG_CK = SCCC.BGBG_CK "
    "where (LTRIM(RTRIM(SCCC.MEMD_MCTR_MCST)) <> '' )"
)
df_MemberAttr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_MemberAttr)
    .load()
)

df_Att_MedicalEAM = df_MemberAttr.alias("MemAttr").crossJoin(df_EAM_Product.alias("Ref_EAM_Prdt")).select(
    F.col("MemAttr.GRGR_ID").alias("GRGR_ID"),
    F.col("MemAttr.SBSB_ID").alias("SBSB_ID"),
    F.col("MemAttr.MBI").alias("MBI"),
    F.col("MemAttr.BGBG_ID").alias("BGBG_ID"),
    F.col("MemAttr.MEMD_MCTR_PBP").alias("MEMD_MCTR_PBP"),
    F.col("MemAttr.MEMD_MCTR_MCST").alias("MEMD_MCTR_MCST"),
    F.col("MemAttr.CSPI_ID").alias("CSPI_ID"),
    F.col("Ref_EAM_Prdt.MedicalProduct").alias("MedicalProduct")
)

df_XmrO_Lkp = df_Att_MedicalEAM.filter(
    (
        trim(F.when(F.col("CSPI_ID").isNotNull(), F.col("CSPI_ID")).otherwise(F.lit("")))
        != trim(F.when(F.col("MedicalProduct").isNotNull(), F.col("MedicalProduct")).otherwise(F.lit("")))
    )
).select(
    F.lit("INC_MP").alias("Field"),
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.col("SBSB_ID").alias("SBSB_ID"),
    F.col("MBI").alias("MBI"),
    F.col("BGBG_ID").alias("BGBG_ID"),
    F.col("MEMD_MCTR_PBP").alias("MEMD_MCTR_PBP"),
    F.col("MEMD_MCTR_MCST").alias("MEMD_MCTR_MCST"),
    F.col("CSPI_ID").alias("CSPI_ID"),
    F.col("MedicalProduct").alias("MedicalProduct")
)

df_RuleFailures = df_XmrO_Lkp.alias("XmrO_Lkp").crossJoin(df_DISC_CONFIG.alias("Ref_Disc_Config")).select(
    F.col("XmrO_Lkp.Field").alias("Field"),
    F.col("XmrO_Lkp.SBSB_ID").alias("FACETS_MemberID"),
    F.col("XmrO_Lkp.GRGR_ID").alias("GRGR_ID"),
    F.col("XmrO_Lkp.MBI").alias("MBI"),
    F.col("Ref_Disc_Config.DISCREPANCY_ID").alias("DISCREPANCY_ID"),
    F.col("Ref_Disc_Config.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
    F.col("Ref_Disc_Config.IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    F.col("Ref_Disc_Config.PRIORITY").alias("PRIORITY"),
    F.col("XmrO_Lkp.MedicalProduct").alias("SOURCE1"),
    F.col("XmrO_Lkp.CSPI_ID").alias("SOURCE2")
)

df_FullOuter = df_RuleFailures.alias("RuleFailures").join(
    df_EAM_DISC_ANALYSIS.alias("DISC_Analysis"),
    (
        (F.col("RuleFailures.MBI") == F.col("DISC_Analysis.MBI"))
        & (F.col("RuleFailures.DISCREPANCY_ID") == F.col("DISC_Analysis.DISCREPANCY_ID"))
        & (F.col("RuleFailures.IDENTIFIEDBY") == F.col("DISC_Analysis.IDENTIFIEDBY"))
    ),
    "full"
).select(
    F.col("RuleFailures.Field").alias("Field"),
    F.col("RuleFailures.FACETS_MemberID").alias("FACETS_MemberID"),
    F.col("RuleFailures.MBI").alias("leftRec_MBI"),
    F.col("RuleFailures.GRGR_ID").alias("FACETS_GROUP_ID"),
    F.col("RuleFailures.DISCREPANCY_ID").alias("leftRec_DISCREPANCY_ID"),
    F.col("RuleFailures.DISCREPANCY_DESC").alias("DISCREPANCY_DESC"),
    F.col("RuleFailures.IDENTIFIEDBY").alias("leftRec_IDENTIFIEDBY"),
    F.col("RuleFailures.PRIORITY").alias("PRIORITY"),
    F.col("RuleFailures.SOURCE1").alias("SOURCE1"),
    F.col("RuleFailures.SOURCE2").alias("SOURCE2"),
    F.col("DISC_Analysis.ID_SK").alias("ID_SK"),
    F.col("DISC_Analysis.MBI").alias("rightRec_MBI"),
    F.col("DISC_Analysis.IDENTIFIEDBY").alias("rightRec_IDENTIFIEDBY"),
    F.col("DISC_Analysis.IDENTIFIEDDATE").alias("IDENTIFIEDDATE"),
    F.col("DISC_Analysis.DISCREPANCY_ID").alias("rightRec_DISCREPANCY_ID")
)

df_NewAdds = df_FullOuter.filter(
    (F.col("rightRec_MBI") == "")
    & (F.when(F.col("leftRec_MBI").isNotNull(), F.col("leftRec_MBI")).otherwise(F.lit("")) != "")
).select(
    F.col("FACETS_GROUP_ID").alias("GRGR_ID"),
    F.col("FACETS_MemberID").alias("SBSB_ID"),
    F.col("leftRec_MBI").alias("MBI"),
    F.col("SOURCE1").alias("SOURCE1"),
    F.col("SOURCE2").alias("SOURCE2"),
    F.col("leftRec_IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    F.to_date(F.lit(CurrDt), "yyyy-MM-dd").alias("IDENTIFIEDDATE"),
    F.lit(None).alias("RESOLVEDDATE"),
    F.lit("OPEN").alias("STATUS"),
    F.col("leftRec_DISCREPANCY_ID").alias("DISCREPANCY"),
    F.col("DISCREPANCY_DESC").alias("NOTE"),
    F.col("PRIORITY").alias("PRIORITY"),
    F.lit(0).alias("AGE"),
    F.when(F.col("FACETS_MemberID") == "", "N").otherwise(
        F.when(F.substring("FACETS_MemberID", 1, 3) == "000", "Y").otherwise("N")
    ).alias("LEGACY"),
    F.to_date(F.lit(CurrDt), "yyyy-MM-dd").alias("LAST_UPDATED_DATE")
)

df_Resolved = df_FullOuter.filter(
    (
        F.when(F.col("leftRec_MBI").isNotNull(), F.col("leftRec_MBI")).otherwise(F.lit(""))
        == ""
    )
    & (F.col("rightRec_MBI") != "")
).select(
    F.col("ID_SK").alias("ID_SK")
)

df_StillOpen = df_FullOuter.filter(
    (
        F.when(F.col("leftRec_MBI").isNotNull(), F.col("leftRec_MBI")).otherwise(F.lit(""))
        != ""
    )
    & (F.col("rightRec_MBI") != "")
).select(
    F.col("ID_SK").alias("ID_SK"),
    DaysSinceFromDate2(F.to_date(F.lit(CurrDt), "yyyy-MM-dd"), F.col("IDENTIFIEDDATE")).alias("AGE"),
    F.to_date(F.lit(CurrDt), "yyyy-MM-dd").alias("LAST_UPDATED_DATE")
)

write_files(
    df_StillOpen.select("ID_SK","AGE","LAST_UPDATED_DATE"),
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_MEDLPLN_AGED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_Resolved.select("ID_SK"),
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_MEDLPLN_RESOLVED.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

df_dedup_NewAdds = dedup_sort(
    df_NewAdds,
    ["MBI","IDENTIFIEDBY","IDENTIFIEDDATE","DISCREPANCY"],
    [("MBI","A"),("IDENTIFIEDBY","A"),("IDENTIFIEDDATE","A"),("DISCREPANCY","A")]
)

df_NewAddsFile = df_dedup_NewAdds.select(
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.col("SBSB_ID").alias("SBSB_ID"),
    F.col("MBI").alias("MBI"),
    F.col("SOURCE1").alias("SOURCE1"),
    F.col("SOURCE2").alias("SOURCE2"),
    F.col("IDENTIFIEDBY").alias("IDENTIFIEDBY"),
    F.col("IDENTIFIEDDATE").alias("IDENTIFIEDDATE"),
    F.col("RESOLVEDDATE").alias("RESOLVEDDATE"),
    F.col("STATUS").alias("STATUS"),
    F.col("DISCREPANCY").alias("DISCREPANCY"),
    F.col("NOTE").alias("NOTE"),
    F.col("PRIORITY").alias("PRIORITY"),
    F.col("AGE").alias("AGE"),
    F.rpad(F.col("LEGACY"), 1, " ").alias("LEGACY"),
    F.col("LAST_UPDATED_DATE").alias("LAST_UPDATED_DATE")
)

write_files(
    df_NewAddsFile,
    f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_MEDLPLN_APPEND.{RUNID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)