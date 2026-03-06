# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_UMIN_INPATIENTfor loading into IDS.
# MAGIC       
# MAGIC 
# MAGIC                           
# MAGIC PROCESSING:  Extract, transform, and primary keying for Utilization Management subject area.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Hugh Sisson    10/24/2005  -  Originally Program
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              ---------------------------------               -------------------------
# MAGIC    
# MAGIC Parik                               04/12/2007              3264                            Added Balancing process to the overall job that takes           devlIDS30                               Steph Goddard                     9/14/07
# MAGIC                                                                                                               a snapshot of the source data         
# MAGIC O. Nielsen                        07/29/2008          Facets 4.5.1                   changed UMIN_UDCD_ID_ADMN and 
# MAGIC                                                                                                              UMIN_IDCD_ID_PRI from char(6) to Varchar(10)                  devlIDSnew                            Steph Goddard                    08/25/2008
# MAGIC 
# MAGIC Bhoomi Dasari                04/09/2009          3808                               Added SrcSysCdSk to balancing snapshot                              devlIDS                                   Steph Goddard                    04/10/2009
# MAGIC 
# MAGIC Rick Henry                     2012-05-11            4896                               Modified SK for Diag_Cd added Diag_Cd_Typ_Cd                  NewDevl
# MAGIC                                                                                                             and Proc_Cd_Typ_cd                                    
# MAGIC Raja Gummadi                2012-10-04           TTR-1406                       Changed P_ICD_VRSN lookup date.                                      IntegrateNewDevl                   Bhoomi Dasari                     10/24/2012
# MAGIC                                                                                                            Removed Data Elements
# MAGIC Raja Gummadi                2012-11-12           TTR-1413                     Removed P_ICD_VRSN lookup. Modified Proc_cd                IntegrateNewDevl                   
# MAGIC                                                                                                            lookup as per new mappings
# MAGIC Raja Gummadi                2013-01-10           TTR-526                        Added new column DSCHG_DIAG_CD.                                IntegrateNewDevl                      Bhoomi Dasari                      2/25/2013
# MAGIC 
# MAGIC Manasa Andru                2015-04-22           TFS - 12493             Updated the field - UMIN_DISALL_EXCD to No Upcase             IntegrateDev2                            Jag Yelavarthi                     2016-05-03
# MAGIC                                                                                                        so that the field would find a match on the EXCD table.
# MAGIC Prabhu ES                      2022-03-07           S2S Remediation       MSSQL ODBC conn params added                                            IntegrateDev5		Harsha Ravuri		06-14-2022

# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Trim and UpCase all string variables
# MAGIC Extract Facets UM IP Data
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic
# MAGIC Balancing snapshot of source table
# MAGIC Writing Sequential File to /pkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve all required parameter values
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# Read from PROC_CD (DB2Connector, database=IDS)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_proc_cd = f"""
SELECT
    PROC_CD,
    PROC_CD_TYP_CD,
    PROC_CD_CAT_CD
FROM
    {IDSOwner}.PROC_CD
WHERE
    PROC_CD_CAT_CD = 'MED'
"""
df_PROC_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_proc_cd)
    .load()
)

# This goes through a hashed file "hf_UmIp_ProcCd" in the job (scenario A: intermediate hashed file),
# so we directly connect df_PROC_CD to the downstream usage with a deduplicate on the key column "PROC_CD".
df_hf_UmIp_ProcCd = df_PROC_CD.drop_duplicates(["PROC_CD"])

# Read from CD_MPPG (DB2Connector, database=IDS)
extract_query_cd_mppg = f"""
SELECT
    SRC_CD,
    TRGT_CD
FROM
    {IDSOwner}.CD_MPPNG
WHERE
    SRC_DOMAIN_NM = 'DIAGNOSIS CODE TYPE'
    AND TRGT_DOMAIN_NM = 'DIAGNOSIS CODE TYPE'
    AND SRC_CLCTN_CD = 'FACETS DBO'
"""
df_CD_MPPG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_cd_mppg)
    .load()
)

# This goes through a hashed file "hf_umip_cd_mppng_trgt_cd" in the job (scenario A: intermediate hashed file),
# so we directly connect df_CD_MPPG to the downstream usage with a deduplicate on the key column "SRC_CD".
df_hf_umip_cd_mppng_trgt_cd = df_CD_MPPG.drop_duplicates(["SRC_CD"])

# Because "IDS" stage (DB2Connector) has a query with "WHERE PROV_AGMNT_ID = ?" etc.,
# we read the entire table and will join later in Spark according to the job's join logic.
extract_query_ids_prov_agmnt = f"""
SELECT
    PROV_AGMNT_ID,
    EFF_DT_SK,
    TERM_DT_SK,
    PROV_AGMNT_SK
FROM
    {IDSOwner}.PROV_AGMNT
"""
df_ProvAgmnt_full = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids_prov_agmnt)
    .load()
)

# Read from FACETS (ODBCConnector) referencing #$FacetsOwner# tables and tempdb..#DriverTable#
# We assume we can read directly with the same SQL (no '?' placeholders).
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_facets = f"""
SELECT
    U.UMUM_REF_ID,
    U.UMIN_AUTH_IND,
    U.UMIN_REF_IND,
    U.UMIN_TYPE,
    U.UMIT_STS,
    U.UMIT_STS_DTM,
    U.UMIN_INPUT_USID,
    U.UMIN_INPUT_DT,
    U.UMIN_RECD_DT,
    U.UMIN_NEXT_REV_DT,
    U.UMIN_AUTH_DT,
    U.UMIN_PRPR_ID_FAC,
    U.UMIN_PRPR_ID_ADM,
    U.UMIN_PRPR_ID_ATT,
    U.UMIN_PRPR_ID_SURG,
    U.UMIN_PRPR_ID_PCP,
    U.UMIN_PRPR_ID_REQ,
    U.UMIN_PR_NW_STS,
    U.UMIN_CAT_ADM,
    U.UMIN_CAT_CURR,
    U.UMIN_IPCD_ID_PRI,
    U.UMIN_IDCD_ID_PRI,
    U.UMIN_IDCD_ID_ADM,
    U.UMIN_REQ_PROC_DT,
    U.UMIN_AUTH_PROC_DT,
    U.UMIN_PREOP_REQ,
    U.UMIN_PSCD_ID_REQ,
    U.UMIN_PSCD_POS_REQ,
    U.UMIN_PSCD_ID_AUTH,
    U.UMIN_PSCD_POS_AUTH,
    U.UMIN_MCTR_SDNY,
    U.UMIN_USID_SDNY,
    U.UMIN_DISALL_EXCD,
    U.UMIN_LOS_REQ_TOT,
    U.UMIN_LOS_AUTH_TOT,
    U.UMIN_LOS_GL_PRI_F,
    U.UMIN_LOS_GL_PRI_T,
    U.UMIN_LOS_ACTUAL,
    U.UMIN_BIRTH_WEIGHT,
    U.UMIN_REQ_ADM_DT,
    U.UMIN_AUTH_ADM_DT,
    U.UMIN_ACT_ADM_DT,
    U.UMIN_DC_EXP_DTM,
    U.UMIN_DC_STS,
    U.UMIN_DC_DTM,
    U.UMIN_ME_AGE,
    U.AGAG_ID,
    U.UMIN_PCP_IND,
    U.UMIN_DENY_DT,
    U.UMIN_TOT_ALW_DAYS,
    U.ATXR_SOURCE_ID,
    M.UMUM_ICD_IND_PROC,
    M.UMUM_CREATE_DT,
    U.UMIN_IDCD_ID_DC
FROM
    {FacetsOwner}.CMC_UMIN_INPATIENT AS U,
    tempdb..{DriverTable} AS T,
    {FacetsOwner}.CMC_UMUM_UTIL_MGT M
WHERE
    U.UMUM_REF_ID = T.UM_REF_ID
    AND U.UMUM_REF_ID = M.UMUM_REF_ID
ORDER BY
    U.UMUM_REF_ID
"""
df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_facets)
    .load()
)

# --- StripFields stage (CTransformerStage) ---
# Create df_Strip by applying all transformations from "StripFields"
df_Strip = df_FACETS.select(
    F.upper(trim(strip_field(F.col("UMUM_REF_ID"))))[0:9].alias("UMUM_REF_ID"),
    F.upper(trim(strip_field(F.col("UMIN_AUTH_IND"))))[0:1].alias("UMIN_AUTH_IND"),
    F.upper(trim(strip_field(F.col("UMIN_REF_IND"))))[0:1].alias("UMIN_REF_IND"),
    F.upper(trim(strip_field(F.col("UMIN_TYPE"))))[0:1].alias("UMIN_TYPE"),
    F.upper(trim(strip_field(F.col("UMIT_STS"))))[0:2].alias("UMIT_STS"),
    F.col("UMIT_STS_DTM").alias("UMIT_STS_DTM"),
    trim(strip_field(F.col("UMIN_INPUT_USID")))[0:10].alias("UMIN_INPUT_USID"),
    F.col("UMIN_INPUT_DT").alias("UMIN_INPUT_DT"),
    F.col("UMIN_RECD_DT").alias("UMIN_RECD_DT"),
    F.col("UMIN_NEXT_REV_DT").alias("UMIN_NEXT_REV_DT"),
    F.col("UMIN_AUTH_DT").alias("UMIN_AUTH_DT"),
    F.upper(trim(strip_field(F.col("UMIN_PRPR_ID_FAC"))))[0:12].alias("UMIN_PRPR_ID_FAC"),
    F.upper(trim(strip_field(F.col("UMIN_PRPR_ID_ADM"))))[0:12].alias("UMIN_PRPR_ID_ADM"),
    F.upper(trim(strip_field(F.col("UMIN_PRPR_ID_ATT"))))[0:12].alias("UMIN_PRPR_ID_ATT"),
    F.upper(trim(strip_field(F.col("UMIN_PRPR_ID_SURG"))))[0:12].alias("UMIN_PRPR_ID_SURG"),
    F.upper(trim(strip_field(F.col("UMIN_PRPR_ID_PCP"))))[0:12].alias("UMIN_PRPR_ID_PCP"),
    F.upper(trim(strip_field(F.col("UMIN_PRPR_ID_REQ"))))[0:12].alias("UMIN_PRPR_ID_REQ"),
    F.upper(trim(strip_field(F.col("UMIN_PR_NW_STS"))))[0:1].alias("UMIN_PR_NW_STS"),
    F.upper(trim(strip_field(F.col("UMIN_CAT_ADM"))))[0:1].alias("UMIN_CAT_ADM"),
    F.upper(trim(strip_field(F.col("UMIN_CAT_CURR"))))[0:1].alias("UMIN_CAT_CURR"),
    F.upper(trim(strip_field(F.col("UMIN_IPCD_ID_PRI"))))[0:7].alias("UMIN_IPCD_ID_PRI"),
    F.upper(trim(strip_field(F.col("UMIN_IDCD_ID_PRI")))).alias("UMIN_IDCD_ID_PRI"),
    F.upper(trim(strip_field(F.col("UMIN_IDCD_ID_ADM")))).alias("UMIN_IDCD_ID_ADM"),
    F.col("UMIN_REQ_PROC_DT").alias("UMIN_REQ_PROC_DT"),
    F.col("UMIN_AUTH_PROC_DT").alias("UMIN_AUTH_PROC_DT"),
    F.col("UMIN_PREOP_REQ").alias("UMIN_PREOP_REQ"),
    F.upper(trim(strip_field(F.col("UMIN_PSCD_ID_REQ"))))[0:2].alias("UMIN_PSCD_ID_REQ"),
    F.upper(trim(strip_field(F.col("UMIN_PSCD_POS_REQ"))))[0:1].alias("UMIN_PSCD_POS_REQ"),
    F.upper(trim(strip_field(F.col("UMIN_PSCD_ID_AUTH"))))[0:2].alias("UMIN_PSCD_ID_AUTH"),
    F.upper(trim(strip_field(F.col("UMIN_PSCD_POS_AUTH"))))[0:1].alias("UMIN_PSCD_POS_AUTH"),
    F.upper(trim(strip_field(F.col("UMIN_MCTR_SDNY"))))[0:4].alias("UMIN_MCTR_SDNY"),
    trim(strip_field(F.col("UMIN_USID_SDNY")))[0:10].alias("UMIN_USID_SDNY"),
    trim(strip_field(F.col("UMIN_DISALL_EXCD")))[0:3].alias("UMIN_DISALL_EXCD"),
    F.col("UMIN_LOS_REQ_TOT").alias("UMIN_LOS_REQ_TOT"),
    F.col("UMIN_LOS_AUTH_TOT").alias("UMIN_LOS_AUTH_TOT"),
    F.col("UMIN_LOS_GL_PRI_F").alias("UMIN_LOS_GL_PRI_F"),
    F.col("UMIN_LOS_GL_PRI_T").alias("UMIN_LOS_GL_PRI_T"),
    F.col("UMIN_LOS_ACTUAL").alias("UMIN_LOS_ACTUAL"),
    F.col("UMIN_BIRTH_WEIGHT").alias("UMIN_BIRTH_WEIGHT"),
    F.col("UMIN_REQ_ADM_DT").alias("UMIN_REQ_ADM_DT"),
    F.col("UMIN_AUTH_ADM_DT").alias("UMIN_AUTH_ADM_DT"),
    F.col("UMIN_ACT_ADM_DT").alias("UMIN_ACT_ADM_DT"),
    F.col("UMIN_DC_EXP_DTM").alias("UMIN_DC_EXP_DTM"),
    F.upper(trim(strip_field(F.col("UMIN_DC_STS"))))[0:2].alias("UMIN_DC_STS"),
    F.col("UMIN_DC_DTM").alias("UMIN_DC_DTM"),
    F.col("UMIN_ME_AGE").alias("UMIN_ME_AGE"),
    F.upper(trim(strip_field(F.col("AGAG_ID"))))[0:12].alias("AGAG_ID"),
    F.upper(trim(strip_field(F.col("UMIN_PCP_IND"))))[0:1].alias("UMIN_PCP_IND"),
    F.col("UMIN_DENY_DT").alias("UMIN_DENY_DT"),
    F.col("UMIN_TOT_ALW_DAYS").alias("UMIN_TOT_ALW_DAYS"),
    F.col("ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID"),
    F.when(
        F.length(F.upper(trim(strip_field(F.col("UMUM_ICD_IND_PROC"))))) == 0,
        F.lit("9")
    ).otherwise(
        F.upper(trim(strip_field(F.col("UMUM_ICD_IND_PROC"))))[0:1]
    ).alias("UMUM_ICD_IND_PROC"),
    # M.UMUM_CREATE_DT => "FORMAT.DATE(Extract.UMUM_CREATE_DT, 'SYBASE','TIMESTAMP','CCYY-MM-DD')"
    # In instructions, "FORMAT.DATE(@DATE, 'DATE','CURRENT','CCYY-MM-DD') => current_date()"
    # but here it's a datetime from the table. We'll interpret "SYBASE => TIMESTAMP => CCYY-MM-DD" 
    # as taking the date portion, so let's do substring(yyyy-mm-dd).
    F.date_format(F.col("UMUM_CREATE_DT"), "yyyy-MM-dd").alias("M.UMUM_CREATE_DT"),
    # UMIN_IDCD_ID_DC => "if Len(...)=0 Or IsNull(...) then 'NA' else value"
    F.when(
        (F.col("UMIN_IDCD_ID_DC").isNull()) | (F.length(trim(strip_field(F.col("UMIN_IDCD_ID_DC")))) == 0),
        F.lit("NA")
    ).otherwise(F.col("UMIN_IDCD_ID_DC")).alias("UMIN_IDCD_ID_DC")
)

# --- BusinessRules stage (CTransformerStage) ---
# We handle two left joins here: 
#   1) with df_ProvAgmnt_full, using the conditional expression 
#   2) with df_hf_umip_cd_mppng_trgt_cd on "UMUM_ICD_IND_PROC" == "SRC_CD"
# For the first join, replicate:
#   Strip.AGAG_ID == ProvAgmnt.PROV_AGMNT_ID
#   (when(UMIT_STS='DS', substring(UMIN_AUTH_ADM_DT,1,10), ...) == ProvAgmnt.EFF_DT_SK)
#   (when(UMIT_STS='DS', substring(UMIN_AUTH_ADM_DT,1,10), ...) == ProvAgmnt.TERM_DT_SK)
df_BusinessRules_join = (
    df_Strip.alias("Strip")
    .join(
        df_ProvAgmnt_full.alias("ProvAgmnt"),
        (
            (F.col("Strip.AGAG_ID") == F.col("ProvAgmnt.PROV_AGMNT_ID"))
            & (
                F.when(
                    F.col("Strip.UMIT_STS") == "DS",
                    F.substring(F.col("Strip.UMIN_AUTH_ADM_DT"), 1, 10)
                )
                .when(
                    F.col("Strip.UMIT_STS") == "DC",
                    F.substring(F.col("Strip.UMIN_ACT_ADM_DT"), 1, 10)
                )
                .otherwise(F.lit("1753-01-01"))
                == F.col("ProvAgmnt.EFF_DT_SK")
            )
            & (
                F.when(
                    F.col("Strip.UMIT_STS") == "DS",
                    F.substring(F.col("Strip.UMIN_AUTH_ADM_DT"), 1, 10)
                )
                .when(
                    F.col("Strip.UMIT_STS") == "DC",
                    F.substring(F.col("Strip.UMIN_ACT_ADM_DT"), 1, 10)
                )
                .otherwise(F.lit("9999-12-31"))
                == F.col("ProvAgmnt.TERM_DT_SK")
            )
        ),
        "left"
    )
    .join(
        df_hf_umip_cd_mppng_trgt_cd.alias("diag_cd_typ_cd"),
        F.col("Strip.UMUM_ICD_IND_PROC") == F.col("diag_cd_typ_cd.SRC_CD"),
        "left"
    )
)

# Define the stage variables in "BusinessRules"
df_BusinessRules_vars = df_BusinessRules_join.withColumn(
    "RowPassThru", F.lit("Y")
).withColumn(
    "svSrcSysCd", F.lit("FACETS")
).withColumn(
    "svDiagCdTypCd",
    F.when(
        F.col("diag_cd_typ_cd.TRGT_CD").isNull(),
        F.lit("")
    ).otherwise(F.col("diag_cd_typ_cd.TRGT_CD"))
).withColumn(
    "svLenProcCd", F.length(F.col("Strip.UMIN_IPCD_ID_PRI"))
).withColumn(
    "svPriProcCd",
    F.when(
        (F.col("Strip.UMIN_IPCD_ID_PRI").isNull()) | (F.col("svLenProcCd") == 0),
        F.lit("NA")
    ).otherwise(
        F.when(
            F.col("svLenProcCd") > 5,
            F.substring(F.col("Strip.UMIN_IPCD_ID_PRI"), 1, 5)
        ).otherwise(F.col("Strip.UMIN_IPCD_ID_PRI"))
    )
)

# Now select the final columns for the output link "Transform" 
# (the job's link "V8S0P11" from the BusinessRules stage).
df_BusinessRules = df_BusinessRules_vars.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),        # char(10)
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),            # char(1)
    F.col("RowPassThru").alias("PASS_THRU_IN"),                # char(1)
    F.col("CurrDate").alias("FIRST_RECYC_DT"),                 # uses parameter CurrDate
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD"),
    F.concat(F.col("svSrcSysCd"), F.lit(";"), F.col("Strip.UMUM_REF_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("UM_SK"),
    F.col("Strip.UMUM_REF_ID").alias("UM_REF_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_IDCD_ID_ADM").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_IDCD_ID_ADM"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_IDCD_ID_ADM")))),
        6, " "
    ).alias("ADMS_PRI_DIAG_CD"),  # char(6)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_PRPR_ID_ADM").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_PRPR_ID_ADM"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_PRPR_ID_ADM")))),
        12, " "
    ).alias("ADMS_PROV"),         # char(12)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_PRPR_ID_ATT").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_PRPR_ID_ATT"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_PRPR_ID_ATT")))),
        12, " "
    ).alias("ATND_PROV"),         # char(12)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_USID_SDNY").isNull()) | (F.length(F.col("Strip.UMIN_USID_SDNY")) == 0),
            F.lit("NA")
        ).otherwise(F.col("Strip.UMIN_USID_SDNY")),
        10, " "
    ).alias("DENIED_USER"),       # char(10)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_DISALL_EXCD").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_DISALL_EXCD"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_DISALL_EXCD")))),
        3, " "
    ).alias("DSALW_EXCD"),        # char(3)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_PRPR_ID_FAC").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_PRPR_ID_FAC"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_PRPR_ID_FAC")))),
        12, " "
    ).alias("FCLTY_PROV"),        # char(12)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_INPUT_USID").isNull()) | (F.length(F.col("Strip.UMIN_INPUT_USID")) == 0),
            F.lit("UNK")
        ).otherwise(F.col("Strip.UMIN_INPUT_USID")),
        10, " "
    ).alias("INPT_USER"),         # char(10)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_PRPR_ID_PCP").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_PRPR_ID_PCP"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_PRPR_ID_PCP")))),
        12, " "
    ).alias("PCP_PROV"),          # char(12)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_IDCD_ID_PRI").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_IDCD_ID_PRI"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_IDCD_ID_PRI")))),
        10, " "
    ).alias("PRI_DIAG_CD"),       # char(10)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_IPCD_ID_PRI").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_IPCD_ID_PRI"))) == 0),
            F.lit("NA")
        ).otherwise(F.trim(F.col("Strip.UMIN_IPCD_ID_PRI"))),
        5, " "
    ).alias("PRI_SURG_PROC_CD"),  # char(5)
    F.rpad(
        F.when(
            (F.col("Strip.AGAG_ID").isNull()) | (F.length(F.col("Strip.AGAG_ID")) == 0),
            F.lit("1")
        ).otherwise(
            F.when(
                ((F.col("Strip.UMIT_STS") == "DS") | (F.col("Strip.UMIT_STS") == "DC"))
                & (F.col("ProvAgmnt.PROV_AGMNT_SK").isNull()),
                F.lit("0")
            ).otherwise(
                F.when(
                    (F.col("Strip.UMIT_STS") != "DS") & (F.col("Strip.UMIT_STS") != "DC"),
                    F.lit("1")
                ).otherwise(F.col("ProvAgmnt.PROV_AGMNT_SK"))
            )
        ),
        12, " "
    ).alias("PROV_AGMNT_SK"),     # char(12)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_PRPR_ID_REQ").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_PRPR_ID_REQ"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_PRPR_ID_REQ")))),
        12, " "
    ).alias("RQST_PROV"),         # char(12)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_PRPR_ID_SURG").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_PRPR_ID_SURG"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_PRPR_ID_SURG")))),
        12, " "
    ).alias("SURGEON_PROV"),      # char(12)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_CAT_ADM").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_CAT_ADM"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_CAT_ADM")))),
        1, " "
    ).alias("UM_IP_ADMS_TREAT_CAT_CD"),   # char(1)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_PSCD_POS_AUTH").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_PSCD_POS_AUTH"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_PSCD_POS_AUTH")))),
        1, " "
    ).alias("UM_IP_AUTH_POS_CAT_CD"),     # char(1)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_PSCD_ID_AUTH").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_PSCD_ID_AUTH"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_PSCD_ID_AUTH")))),
        2, " "
    ).alias("UM_IP_AUTH_POS_TYP_CD"),     # char(2)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_TYPE").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_TYPE"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_TYPE")))),
        1, " "
    ).alias("UM_IP_CARE_TYP_CD"),         # char(1)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_CAT_CURR").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_CAT_CURR"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_CAT_CURR")))),
        1, " "
    ).alias("UM_IP_CUR_TREAT_CAT_CD"),    # char(1)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_MCTR_SDNY").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_MCTR_SDNY"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_MCTR_SDNY")))),
        4, " "
    ).alias("UM_IP_DENIAL_RSN_CD"),       # char(4)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_DC_STS").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_DC_STS"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_DC_STS")))),
        2, " "
    ).alias("UM_IP_DSCHG_STTUS_CD"),      # char(2)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_PR_NW_STS").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_PR_NW_STS"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_PR_NW_STS")))),
        1, " "
    ).alias("UM_IP_FCLTY_NTWK_STTUS_CD"), # char(1)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_PSCD_POS_REQ").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_PSCD_POS_REQ"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_PSCD_POS_REQ")))),
        1, " "
    ).alias("UM_IP_RQST_POS_CAT_CD"),     # char(1)
    F.rpad(
        F.when(
            (F.col("Strip.UMIN_PSCD_ID_REQ").isNull()) | (F.length(F.trim(F.col("Strip.UMIN_PSCD_ID_REQ"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIN_PSCD_ID_REQ")))),
        2, " "
    ).alias("UM_IP_RQST_POS_TYP_CD"),     # char(2)
    F.rpad(
        F.when(
            (F.col("Strip.UMIT_STS").isNull()) | (F.length(F.trim(F.col("Strip.UMIT_STS"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(F.trim(F.col("Strip.UMIT_STS")))),
        2, " "
    ).alias("UM_IP_STTUS_CD"),            # char(2)
    F.rpad(
        F.when(
            F.upper(F.trim(F.col("Strip.UMIN_AUTH_IND"))) == "Y",
            F.lit("Y")
        ).otherwise(F.lit("N")),
        1, " "
    ).alias("PREAUTH_IN"),               # char(1)
    F.rpad(
        F.when(
            F.upper(F.trim(F.col("Strip.UMIN_REF_IND"))) == "Y",
            F.lit("Y")
        ).otherwise(F.lit("N")),
        1, " "
    ).alias("RFRL_IN"),                  # char(1)
    F.rpad(
        F.when(
            F.upper(F.trim(F.col("Strip.UMIN_PCP_IND"))) == "Y",
            F.lit("Y")
        ).otherwise(F.lit("N")),
        1, " "
    ).alias("RQST_PROV_PCP_IN"),         # char(1)
    F.rpad(
        F.substring(F.col("Strip.UMIN_ACT_ADM_DT"), 1, 10),
        10, " "
    ).alias("ACTL_ADMS_DT"),             # char(10)
    # ATXR_SOURCE_ID => FORMAT.DATE(Strip.ATXR_SOURCE_ID, "SYBASE","TIMESTAMP","DB2TIMESTAMP")
    # We'll interpret that as a Spark cast to timestamp from the string, then reformat. 
    F.date_format(F.to_timestamp(F.col("Strip.ATXR_SOURCE_ID")), "yyyy-MM-dd HH:mm:ss").alias("ATCHMT_SRC_DTM"),
    F.rpad(
        F.substring(F.col("Strip.UMIN_AUTH_ADM_DT"), 1, 10),
        10, " "
    ).alias("AUTH_ADMS_DT"),             # char(10)
    F.rpad(
        F.substring(F.col("Strip.UMIN_AUTH_DT"), 1, 10),
        10, " "
    ).alias("AUTH_DT"),                  # char(10)
    F.rpad(
        F.substring(F.col("Strip.UMIN_DENY_DT"), 1, 10),
        10, " "
    ).alias("DENIAL_DT"),                # char(10)
    # DSCHG_DTM => FORMAT.DATE(Strip.UMIN_DC_DTM, 'SYBASE','TIMESTAMP','DB2TIMESTAMP')
    F.date_format(F.to_timestamp(F.col("Strip.UMIN_DC_DTM")), "yyyy-MM-dd HH:mm:ss").alias("DSCHG_DTM"),
    F.rpad(
        F.substring(F.col("Strip.UMIN_DC_EXP_DTM"), 1, 10),
        10, " "
    ).alias("XPCT_DSCHG_DT"),            # char(10)
    F.rpad(
        F.substring(F.col("Strip.UMIN_INPUT_DT"), 1, 10),
        10, " "
    ).alias("INPT_DT"),                  # char(10)
    F.rpad(
        F.substring(F.col("Strip.UMIN_RECD_DT"), 1, 10),
        10, " "
    ).alias("RCVD_DT"),                  # char(10)
    F.rpad(
        F.substring(F.col("Strip.UMIN_REQ_ADM_DT"), 1, 10),
        10, " "
    ).alias("RQST_ADMS_DT"),             # char(10)
    F.rpad(
        F.substring(F.col("Strip.UMIN_NEXT_REV_DT"), 1, 10),
        10, " "
    ).alias("NEXT_RVW_DT"),              # char(10)
    F.rpad(
        F.substring(F.col("Strip.UMIT_STS_DTM"), 1, 10),
        10, " "
    ).alias("STTUS_DT"),                 # char(10)
    F.rpad(
        F.substring(F.col("Strip.UMIN_REQ_PROC_DT"), 1, 10),
        10, " "
    ).alias("UM_IP_RQST_SURG_DT"),       # char(10)
    F.rpad(
        F.substring(F.col("Strip.UMIN_AUTH_PROC_DT"), 1, 10),
        10, " "
    ).alias("UM_IP_AUTH_SURG_DT"),       # char(10)
    F.when(
        (F.col("Strip.UMIN_LOS_ACTUAL") == 0) & (F.col("Strip.UMIT_STS") == "DC"),
        F.lit(1)
    ).otherwise(F.col("Strip.UMIN_LOS_ACTUAL")).alias("ACTL_LOS_DAYS_QTY"),
    F.col("Strip.UMIN_TOT_ALW_DAYS").alias("ALW_TOT_LOS_DAYS_QTY"),
    F.col("Strip.UMIN_LOS_AUTH_TOT").alias("AUTH_TOT_LOS_DAYS_QTY"),
    F.col("Strip.UMIN_BIRTH_WEIGHT").alias("BRTH_WT_QTY"),
    F.col("Strip.UMIN_ME_AGE").alias("MBR_AGE"),
    F.col("Strip.UMIN_LOS_GL_PRI_T").alias("PRI_GOAL_END_LOS_DAYS_QTY"),
    F.col("Strip.UMIN_LOS_GL_PRI_F").alias("PRI_GOAL_STRT_LOS_DAYS_QTY"),
    F.col("Strip.UMIN_PREOP_REQ").alias("RQST_PREOP_DAYS_QTY"),
    F.col("Strip.UMIN_LOS_REQ_TOT").alias("RQST_TOT_LOS_DAYS_QTY"),
    # PRI_SURG_PROC_CD_MOD_TX => If svLenProcCd >= 7 => Substrings(Strip.UMIN_IPCD_ID_PRI,6,2) else Space(2)
    # but we also do the "svPriProcCd" logic. The job uses another condition in the final link. We replicate exactly:
    # " If svLenProcCd >= 7 Then Substrings(Strip.UMIN_IPCD_ID_PRI,6,2) Else Space(2)" in BusinessRules, 
    F.rpad(
        F.when(
            F.col("svLenProcCd") >= 7,
            F.substring(F.col("Strip.UMIN_IPCD_ID_PRI"), 6, 2)
        ).otherwise(F.lit("  ")),
        2, " "
    ).alias("PRI_SURG_PROC_CD_MOD_TX"),  # char(2)
    F.col("svDiagCdTypCd").alias("DIAG_CD_TYP_CD"),
    # Next stage "CheckSk" expects columns "PROC_CD_TYP_CD" and "PROC_CD_CAT_CD" from stage variables
    # but they are not in this DataFrame yet. We'll add placeholders "''" for now, to be overwritten in the next stage.
    F.lit("").alias("PROC_CD_TYP_CD"),
    F.lit("").alias("PROC_CD_CAT_CD"),
    F.rpad(F.when(
        (F.col("Strip.UMIN_IDCD_ID_DC").isNull()) | (F.length(F.col("Strip.UMIN_IDCD_ID_DC")) == 0),
        F.lit("NA")
    ).otherwise(F.col("Strip.UMIN_IDCD_ID_DC")), 6, " ").alias("DSCHG_DIAG_CD")  # char(6)
)

# --- CheckSk stage ---
# We do two left lookups with df_hf_UmIp_ProcCd:
#   Link "ProcCdTypCd" => join on (PRI_SURG_PROC_CD, PROC_CD_TYP_CD, PROC_CD_CAT_CD)
#   Link "ProcCdTypCd1" => join on Substrings(PRI_SURG_PROC_CD,1,5) = PROC_CD
# Actually, the job logic suggests we must consider the big expression for second join.
# We'll keep them separate. Then define the stage variables. 
df_checksk_l1 = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_UmIp_ProcCd.alias("ProcCdTypCd"),
        (
            (F.col("Transform.PRI_SURG_PROC_CD") == F.col("ProcCdTypCd.PROC_CD")) &
            (F.col("Transform.PROC_CD_TYP_CD") == F.col("ProcCdTypCd.PROC_CD_TYP_CD")) &
            (F.col("Transform.PROC_CD_CAT_CD") == F.col("ProcCdTypCd.PROC_CD_CAT_CD"))
        ),
        "left"
    )
)

df_checksk_l2 = (
    df_checksk_l1.alias("Transform")
    .join(
        df_hf_UmIp_ProcCd.alias("ProcCdTypCd1"),
        (
            F.substring(F.col("Transform.PRI_SURG_PROC_CD"), 1, 5) == F.col("ProcCdTypCd1.PROC_CD")
        ),
        "left"
    )
)

# Define the stage variables in CheckSk
df_checksk_vars = df_checksk_l2.withColumn(
    "svProcCd",
    F.when(
        F.col("ProcCdTypCd.PROC_CD").isNotNull(),
        F.col("ProcCdTypCd.PROC_CD")
    ).otherwise(F.lit("NA"))
).withColumn(
    "svProcCd1",
    F.when(
        F.col("svProcCd") == "NA",
        F.when(
            F.col("ProcCdTypCd1.PROC_CD").isNull(),
            F.lit("NA")
        ).otherwise(F.col("ProcCdTypCd1.PROC_CD"))
    ).otherwise(F.col("svProcCd"))
).withColumn(
    "svProcCdTypCd",
    F.when(
        F.col("ProcCdTypCd.PROC_CD").isNotNull(),
        F.col("ProcCdTypCd.PROC_CD_TYP_CD")
    ).otherwise(
        F.when(
            F.col("ProcCdTypCd1.PROC_CD").isNotNull(),
            F.col("ProcCdTypCd1.PROC_CD_TYP_CD")
        ).otherwise(F.lit("NA"))
    )
).withColumn(
    "svProcCdCatCd",
    F.when(
        F.col("ProcCdTypCd.PROC_CD").isNotNull(),
        F.col("ProcCdTypCd.PROC_CD_CAT_CD")
    ).otherwise(
        F.when(
            F.col("ProcCdTypCd1.PROC_CD").isNotNull(),
            F.col("ProcCdTypCd1.PROC_CD_CAT_CD")
        ).otherwise(F.lit("NA"))
    )
)

# Output link "Sk_Check" from CheckSk
df_Sk_Check = df_checksk_vars.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Transform.UM_SK").alias("UM_SK"),
    F.col("Transform.UM_REF_ID").alias("UM_REF_ID"),
    F.col("Transform.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.ADMS_PRI_DIAG_CD").alias("ADMS_PRI_DIAG_CD"),
    F.col("Transform.ADMS_PROV").alias("ADMS_PROV"),
    F.col("Transform.ATND_PROV").alias("ATND_PROV"),
    F.col("Transform.DENIED_USER").alias("DENIED_USER"),
    F.col("Transform.DSALW_EXCD").alias("DSALW_EXCD"),
    F.col("Transform.FCLTY_PROV").alias("FCLTY_PROV"),
    F.col("Transform.INPT_USER").alias("INPT_USER"),
    F.col("Transform.PCP_PROV").alias("PCP_PROV"),
    F.col("Transform.PRI_DIAG_CD").alias("PRI_DIAG_CD"),
    # Overwrite PRI_SURG_PROC_CD with svProcCd1
    F.rpad(F.col("svProcCd1"), 5, " ").alias("PRI_SURG_PROC_CD"),
    F.col("Transform.PROV_AGMNT_SK").alias("PROV_AGMNT_SK"),
    F.col("Transform.RQST_PROV").alias("RQST_PROV"),
    F.col("Transform.SURGEON_PROV").alias("SURGEON_PROV"),
    F.col("Transform.UM_IP_ADMS_TREAT_CAT_CD").alias("UM_IP_ADMS_TREAT_CAT_CD"),
    F.col("Transform.UM_IP_AUTH_POS_CAT_CD").alias("UM_IP_AUTH_POS_CAT_CD"),
    F.col("Transform.UM_IP_AUTH_POS_TYP_CD").alias("UM_IP_AUTH_POS_TYP_CD"),
    F.col("Transform.UM_IP_CARE_TYP_CD").alias("UM_IP_CARE_TYP_CD"),
    F.col("Transform.UM_IP_CUR_TREAT_CAT_CD").alias("UM_IP_CUR_TREAT_CAT_CD"),
    F.col("Transform.UM_IP_DENIAL_RSN_CD").alias("UM_IP_DENIAL_RSN_CD"),
    F.col("Transform.UM_IP_DSCHG_STTUS_CD").alias("UM_IP_DSCHG_STTUS_CD"),
    F.col("Transform.UM_IP_FCLTY_NTWK_STTUS_CD").alias("UM_IP_FCLTY_NTWK_STTUS_CD"),
    F.col("Transform.UM_IP_RQST_POS_CAT_CD").alias("UM_IP_RQST_POS_CAT_CD"),
    F.col("Transform.UM_IP_RQST_POS_TYP_CD").alias("UM_IP_RQST_POS_TYP_CD"),
    F.col("Transform.UM_IP_STTUS_CD").alias("UM_IP_STTUS_CD"),
    F.col("Transform.PREAUTH_IN").alias("PREAUTH_IN"),
    F.col("Transform.RFRL_IN").alias("RFRL_IN"),
    F.col("Transform.RQST_PROV_PCP_IN").alias("RQST_PROV_PCP_IN"),
    F.col("Transform.ACTL_ADMS_DT").alias("ACTL_ADMS_DT"),
    F.col("Transform.ATCHMT_SRC_DTM").alias("ATCHMT_SRC_DTM"),
    F.col("Transform.AUTH_ADMS_DT").alias("AUTH_ADMS_DT"),
    F.col("Transform.AUTH_DT").alias("AUTH_DT"),
    F.col("Transform.DENIAL_DT").alias("DENIAL_DT"),
    F.col("Transform.DSCHG_DTM").alias("DSCHG_DTM"),
    F.col("Transform.XPCT_DSCHG_DT").alias("XPCT_DSCHG_DT"),
    F.col("Transform.INPT_DT").alias("INPT_DT"),
    F.col("Transform.RCVD_DT").alias("RCVD_DT"),
    F.col("Transform.RQST_ADMS_DT").alias("RQST_ADMS_DT"),
    F.col("Transform.NEXT_RVW_DT").alias("NEXT_RVW_DT"),
    F.col("Transform.STTUS_DT").alias("STTUS_DT"),
    F.col("Transform.UM_IP_RQST_SURG_DT").alias("UM_IP_RQST_SURG_DT"),
    F.col("Transform.UM_IP_AUTH_SURG_DT").alias("UM_IP_AUTH_SURG_DT"),
    F.col("Transform.ACTL_LOS_DAYS_QTY").alias("ACTL_LOS_DAYS_QTY"),
    F.col("Transform.ALW_TOT_LOS_DAYS_QTY").alias("ALW_TOT_LOS_DAYS_QTY"),
    F.col("Transform.AUTH_TOT_LOS_DAYS_QTY").alias("AUTH_TOT_LOS_DAYS_QTY"),
    F.col("Transform.BRTH_WT_QTY").alias("BRTH_WT_QTY"),
    F.col("Transform.MBR_AGE").alias("MBR_AGE"),
    F.col("Transform.PRI_GOAL_END_LOS_DAYS_QTY").alias("PRI_GOAL_END_LOS_DAYS_QTY"),
    F.col("Transform.PRI_GOAL_STRT_LOS_DAYS_QTY").alias("PRI_GOAL_STRT_LOS_DAYS_QTY"),
    F.col("Transform.RQST_PREOP_DAYS_QTY").alias("RQST_PREOP_DAYS_QTY"),
    F.col("Transform.RQST_TOT_LOS_DAYS_QTY").alias("RQST_TOT_LOS_DAYS_QTY"),
    F.rpad(
        F.when(
            (F.col("ProcCdTypCd.PROC_CD_TYP_CD") == "ICD10") | (F.col("svProcCd1") == "NA"),
            F.lit("  ")
        ).otherwise(
            F.when(
                F.length(F.col("Transform.PRI_SURG_PROC_CD")) >= 7,
                F.substring(F.col("Transform.PRI_SURG_PROC_CD"), -2, 2)
            ).otherwise(F.lit("  "))
        ),
        2, " "
    ).alias("PRI_SURG_PROC_CD_MOD_TX"), # char(2)
    F.col("Transform.DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD"),
    F.col("svProcCdTypCd").alias("PROC_CD_TYP_CD"),
    F.col("svProcCdCatCd").alias("PROC_CD_CAT_CD"),
    F.col("Transform.DSCHG_DIAG_CD").alias("DSCHG_DIAG_CD")
)

# Output link "Snapshot" from CheckSk
df_Snapshot = df_checksk_vars.select(
    F.col("Transform.UM_REF_ID").alias("UM_REF_ID"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.UM_IP_CUR_TREAT_CAT_CD").alias("UM_IP_CUR_TREAT_CAT_CD"),
    F.col("Transform.UM_IP_STTUS_CD").alias("UM_IP_STTUS_CD"),
    F.col("Transform.ACTL_LOS_DAYS_QTY").alias("ACTL_LOS_DAYS_QTY"),
    F.col("Transform.PRI_DIAG_CD").alias("PRI_DIAG_CD"),
    F.col("Transform.DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD")
)

# --- PrimaryKey stage ---
# CHashedFileStage "hf_um_lkup" as a lookup => scenario C (read from a parquet file).
# We'll read it from path f"{adls_path}/hf_um_lkup.parquet" and join.
df_hf_um_lkup = spark.read.parquet(f"{adls_path}/hf_um_lkup.parquet")
df_hf_um_lkup_aliased = df_hf_um_lkup.alias("lkup")

df_PrimaryKey_join = (
    df_Sk_Check.alias("Sk_Check")
    .join(
        df_hf_um_lkup_aliased,
        (
            (F.col("Sk_Check.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"))
            & (F.col("Sk_Check.UM_REF_ID") == F.col("lkup.UM_REF_ID"))
        ),
        "left"
    )
)

# Now replicate the stage variables:
#   SK = if IsNull(lkup.UM_SK) then KeyMgtGetNextValueConcurrent("UM_SK") else lkup.UM_SK
#   => SurrogateKeyGen usage
#   NewCrtRunCycExtcnSk = if IsNull(lkup.UM_SK) then CurrRunCycle else lkup.CRT_RUN_CYC_EXCTN_SK
df_temp_primarykey = df_PrimaryKey_join.withColumnRenamed("UM_SK", "SkCheck_UM_SK") \
                                       .withColumn("lkup_UM_SK", F.col("lkup.UM_SK")) \
                                       .withColumn("lkup_CRT_RUN_CYC_EXCTN_SK", F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))

# According to the KeyMgtGetNextValueConcurrent replacement rule, we must do:
df_enriched = SurrogateKeyGen(df_temp_primarykey, <DB sequence name>, 'SkCheck_UM_SK', <schema>, <secret_name>)

# Now define columns for final:
df_primarykey_with_calc = (
    df_enriched
    .withColumn(
        "SK",
        F.when(F.col("lkup_UM_SK").isNull(), F.col("SkCheck_UM_SK")).otherwise(F.col("lkup_UM_SK"))
    )
    .withColumn(
        "NewCrtRunCycExtcnSk",
        F.when(F.col("lkup_UM_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup_CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn(
        "CurrRunCycleVal", F.lit(CurrRunCycle)
    )
)

# Output link "Key" from PrimaryKey => "IdsUmIpExtr"
df_Key = df_primarykey_with_calc.select(
    F.col("Sk_Check.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Sk_Check.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Sk_Check.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Sk_Check.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Sk_Check.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Sk_Check.ERR_CT").alias("ERR_CT"),
    F.col("Sk_Check.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Sk_Check.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Sk_Check.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SK").alias("UM_SK"),
    F.col("Sk_Check.UM_REF_ID").alias("UM_REF_ID"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CurrRunCycleVal").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Sk_Check.ADMS_PRI_DIAG_CD").alias("ADMS_PRI_DIAG_CD"),
    F.col("Sk_Check.ADMS_PROV").alias("ADMS_PROV"),
    F.col("Sk_Check.ATND_PROV").alias("ATND_PROV"),
    F.col("Sk_Check.DENIED_USER").alias("DENIED_USER"),
    F.col("Sk_Check.DSALW_EXCD").alias("DSALW_EXCD"),
    F.col("Sk_Check.FCLTY_PROV").alias("FCLTY_PROV"),
    F.col("Sk_Check.INPT_USER").alias("INPT_USER"),
    F.col("Sk_Check.PCP_PROV").alias("PCP_PROV"),
    # Note job adjusts length from 10 to 9 for PRI_DIAG_CD
    F.substring(F.col("Sk_Check.PRI_DIAG_CD"), 1, 9).alias("PRI_DIAG_CD"),
    F.col("Sk_Check.PRI_SURG_PROC_CD").alias("PRI_SURG_PROC_CD"),
    F.col("Sk_Check.PROV_AGMNT_SK").alias("PROV_AGMNT_SK"),
    F.col("Sk_Check.RQST_PROV").alias("RQST_PROV"),
    F.col("Sk_Check.SURGEON_PROV").alias("SURGEON_PROV"),
    F.col("Sk_Check.UM_IP_ADMS_TREAT_CAT_CD").alias("UM_IP_ADMS_TREAT_CAT_CD"),
    F.col("Sk_Check.UM_IP_AUTH_POS_CAT_CD").alias("UM_IP_AUTH_POS_CAT_CD"),
    F.col("Sk_Check.UM_IP_AUTH_POS_TYP_CD").alias("UM_IP_AUTH_POS_TYP_CD"),
    F.col("Sk_Check.UM_IP_CARE_TYP_CD").alias("UM_IP_CARE_TYP_CD"),
    F.col("Sk_Check.UM_IP_CUR_TREAT_CAT_CD").alias("UM_IP_CUR_TREAT_CAT_CD"),
    F.col("Sk_Check.UM_IP_DENIAL_RSN_CD").alias("UM_IP_DENIAL_RSN_CD"),
    F.col("Sk_Check.UM_IP_DSCHG_STTUS_CD").alias("UM_IP_DSCHG_STTUS_CD"),
    F.col("Sk_Check.UM_IP_FCLTY_NTWK_STTUS_CD").alias("UM_IP_FCLTY_NTWK_STTUS_CD"),
    F.col("Sk_Check.UM_IP_RQST_POS_CAT_CD").alias("UM_IP_RQST_POS_CAT_CD"),
    F.col("Sk_Check.UM_IP_RQST_POS_TYP_CD").alias("UM_IP_RQST_POS_TYP_CD"),
    F.col("Sk_Check.UM_IP_STTUS_CD").alias("UM_IP_STTUS_CD"),
    F.col("Sk_Check.PREAUTH_IN").alias("PREAUTH_IN"),
    F.col("Sk_Check.RFRL_IN").alias("RFRL_IN"),
    F.col("Sk_Check.RQST_PROV_PCP_IN").alias("RQST_PROV_PCP_IN"),
    F.col("Sk_Check.ACTL_ADMS_DT").alias("ACTL_ADMS_DT"),
    F.col("Sk_Check.ATCHMT_SRC_DTM").alias("ATCHMT_SRC_DTM"),
    F.col("Sk_Check.AUTH_ADMS_DT").alias("AUTH_ADMS_DT"),
    F.col("Sk_Check.AUTH_DT").alias("AUTH_DT"),
    F.col("Sk_Check.DENIAL_DT").alias("DENIAL_DT"),
    F.col("Sk_Check.DSCHG_DTM").alias("DSCHG_DTM"),
    F.col("Sk_Check.XPCT_DSCHG_DT").alias("XPCT_DSCHG_DT"),
    F.col("Sk_Check.INPT_DT").alias("INPT_DT"),
    F.col("Sk_Check.RCVD_DT").alias("RCVD_DT"),
    F.col("Sk_Check.RQST_ADMS_DT").alias("RQST_ADMS_DT"),
    F.col("Sk_Check.NEXT_RVW_DT").alias("NEXT_RVW_DT"),
    F.col("Sk_Check.STTUS_DT").alias("STTUS_DT"),
    F.col("Sk_Check.UM_IP_RQST_SURG_DT").alias("UM_IP_RQST_SURG_DT"),
    F.col("Sk_Check.UM_IP_AUTH_SURG_DT").alias("UM_IP_AUTH_SURG_DT"),
    F.col("Sk_Check.ACTL_LOS_DAYS_QTY").alias("ACTL_LOS_DAYS_QTY"),
    F.col("Sk_Check.ALW_TOT_LOS_DAYS_QTY").alias("ALW_TOT_LOS_DAYS_QTY"),
    F.col("Sk_Check.AUTH_TOT_LOS_DAYS_QTY").alias("AUTH_TOT_LOS_DAYS_QTY"),
    F.col("Sk_Check.BRTH_WT_QTY").alias("BRTH_WT_QTY"),
    F.col("Sk_Check.MBR_AGE").alias("MBR_AGE"),
    F.col("Sk_Check.PRI_GOAL_END_LOS_DAYS_QTY").alias("PRI_GOAL_END_LOS_DAYS_QTY"),
    F.col("Sk_Check.PRI_GOAL_STRT_LOS_DAYS_QTY").alias("PRI_GOAL_STRT_LOS_DAYS_QTY"),
    F.col("Sk_Check.RQST_PREOP_DAYS_QTY").alias("RQST_PREOP_DAYS_QTY"),
    F.col("Sk_Check.RQST_TOT_LOS_DAYS_QTY").alias("RQST_TOT_LOS_DAYS_QTY"),
    F.col("Sk_Check.PRI_SURG_PROC_CD_MOD_TX").alias("PRI_SURG_PROC_CD_MOD_TX"),
    F.col("Sk_Check.DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD"),
    F.col("Sk_Check.PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    F.col("Sk_Check.PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD"),
    F.col("Sk_Check.DSCHG_DIAG_CD").alias("DSCHG_DIAG_CD")
)

# Output link "updt" from PrimaryKey => "hf_um" (Constraint isNull(lkup.UM_SK) = @TRUE)
df_updt = df_primarykey_with_calc.filter(F.col("lkup_UM_SK").isNull()).select(
    F.col("Sk_Check.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Sk_Check.UM_REF_ID").alias("UM_REF_ID"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("SK").alias("UM_SK")
)

# We write "hf_um" hashed file => scenario C => write to parquet
# Must preserve column order, apply rpad if any column is char/varchar with known length => none are given here.
df_updt_write = df_updt

write_files(
    df_updt_write,
    f"hf_um.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# --- IdsUmIpExtr (CSeqFileStage) writes #TmpOutFile# => folder "key"
# We must preserve column order and do rpad for any known char/varchar with length.
# The columns come from df_Key in the same order we selected them.
# We'll apply rpad if we know length from the job design. Many columns are char(...) in the job, so do it carefully:
col_for_idsumipextr = []
schema_info_idsumipextr = {
    # "INSRT_UPDT_CD": 10,
    # "DISCARD_IN": 1,
    # "PASS_THRU_IN": 1,
    # "ADMS_PRI_DIAG_CD": 6,
    # "ADMS_PROV": 12,
    # ...
    # We'll build from the final link definition we see in the job. 
    # The job had them in that order with some char lengths. We replicate them below.
}

def with_rpad(df, col_name, length_str):
    if length_str is None or length_str == "":
        return df[col_name]
    else:
        l = int(length_str)
        return F.rpad(df[col_name], l, " ")

# The stage explicit columns & lengths (from the final "IdsUmIpExtr" definition in the JSON):
df_idsumipextr = df_Key.select(
    # We'll rpad only where the JSON link shows "SqlType": "char","Length":...
    # The JSON snippet for "IdsUmIpExtr" does not explicitly list each column's "SqlType" and "Length",
    # but we can see them throughout the upstream definitions. We'll apply rpad if we have that info.
    # For brevity, only applying to clearly declared char(...) in the prior stage. Others remain as-is.
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("UM_SK"),
    F.col("UM_REF_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("ADMS_PRI_DIAG_CD"), 6, " ").alias("ADMS_PRI_DIAG_CD"),
    F.rpad(F.col("ADMS_PROV"), 12, " ").alias("ADMS_PROV"),
    F.rpad(F.col("ATND_PROV"), 12, " ").alias("ATND_PROV"),
    F.rpad(F.col("DENIED_USER"), 10, " ").alias("DENIED_USER"),
    F.rpad(F.col("DSALW_EXCD"), 3, " ").alias("DSALW_EXCD"),
    F.rpad(F.col("FCLTY_PROV"), 12, " ").alias("FCLTY_PROV"),
    F.rpad(F.col("INPT_USER"), 10, " ").alias("INPT_USER"),
    F.rpad(F.col("PCP_PROV"), 12, " ").alias("PCP_PROV"),
    # PRI_DIAG_CD final length is 9 from the link
    F.rpad(F.col("PRI_DIAG_CD"), 9, " ").alias("PRI_DIAG_CD"),
    F.rpad(F.col("PRI_SURG_PROC_CD"), 5, " ").alias("PRI_SURG_PROC_CD"),
    F.rpad(F.col("PROV_AGMNT_SK"), 12, " ").alias("PROV_AGMNT_SK"),
    F.rpad(F.col("RQST_PROV"), 12, " ").alias("RQST_PROV"),
    F.rpad(F.col("SURGEON_PROV"), 12, " ").alias("SURGEON_PROV"),
    F.rpad(F.col("UM_IP_ADMS_TREAT_CAT_CD"), 1, " ").alias("UM_IP_ADMS_TREAT_CAT_CD"),
    F.rpad(F.col("UM_IP_AUTH_POS_CAT_CD"), 1, " ").alias("UM_IP_AUTH_POS_CAT_CD"),
    F.rpad(F.col("UM_IP_AUTH_POS_TYP_CD"), 2, " ").alias("UM_IP_AUTH_POS_TYP_CD"),
    F.rpad(F.col("UM_IP_CARE_TYP_CD"), 1, " ").alias("UM_IP_CARE_TYP_CD"),
    F.rpad(F.col("UM_IP_CUR_TREAT_CAT_CD"), 1, " ").alias("UM_IP_CUR_TREAT_CAT_CD"),
    F.rpad(F.col("UM_IP_DENIAL_RSN_CD"), 4, " ").alias("UM_IP_DENIAL_RSN_CD"),
    F.rpad(F.col("UM_IP_DSCHG_STTUS_CD"), 2, " ").alias("UM_IP_DSCHG_STTUS_CD"),
    F.rpad(F.col("UM_IP_FCLTY_NTWK_STTUS_CD"), 1, " ").alias("UM_IP_FCLTY_NTWK_STTUS_CD"),
    F.rpad(F.col("UM_IP_RQST_POS_CAT_CD"), 1, " ").alias("UM_IP_RQST_POS_CAT_CD"),
    F.rpad(F.col("UM_IP_RQST_POS_TYP_CD"), 2, " ").alias("UM_IP_RQST_POS_TYP_CD"),
    F.rpad(F.col("UM_IP_STTUS_CD"), 2, " ").alias("UM_IP_STTUS_CD"),
    F.rpad(F.col("PREAUTH_IN"), 1, " ").alias("PREAUTH_IN"),
    F.rpad(F.col("RFRL_IN"), 1, " ").alias("RFRL_IN"),
    F.rpad(F.col("RQST_PROV_PCP_IN"), 1, " ").alias("RQST_PROV_PCP_IN"),
    F.rpad(F.col("ACTL_ADMS_DT"), 10, " ").alias("ACTL_ADMS_DT"),
    F.col("ATCHMT_SRC_DTM"),
    F.rpad(F.col("AUTH_ADMS_DT"), 10, " ").alias("AUTH_ADMS_DT"),
    F.rpad(F.col("AUTH_DT"), 10, " ").alias("AUTH_DT"),
    F.rpad(F.col("DENIAL_DT"), 10, " ").alias("DENIAL_DT"),
    F.col("DSCHG_DTM"),
    F.rpad(F.col("XPCT_DSCHG_DT"), 10, " ").alias("XPCT_DSCHG_DT"),
    F.rpad(F.col("INPT_DT"), 10, " ").alias("INPT_DT"),
    F.rpad(F.col("RCVD_DT"), 10, " ").alias("RCVD_DT"),
    F.rpad(F.col("RQST_ADMS_DT"), 10, " ").alias("RQST_ADMS_DT"),
    F.rpad(F.col("NEXT_RVW_DT"), 10, " ").alias("NEXT_RVW_DT"),
    F.rpad(F.col("STTUS_DT"), 10, " ").alias("STTUS_DT"),
    F.rpad(F.col("UM_IP_RQST_SURG_DT"), 10, " ").alias("UM_IP_RQST_SURG_DT"),
    F.rpad(F.col("UM_IP_AUTH_SURG_DT"), 10, " ").alias("UM_IP_AUTH_SURG_DT"),
    F.col("ACTL_LOS_DAYS_QTY"),
    F.col("ALW_TOT_LOS_DAYS_QTY"),
    F.col("AUTH_TOT_LOS_DAYS_QTY"),
    F.col("BRTH_WT_QTY"),
    F.col("MBR_AGE"),
    F.col("PRI_GOAL_END_LOS_DAYS_QTY"),
    F.col("PRI_GOAL_STRT_LOS_DAYS_QTY"),
    F.col("RQST_PREOP_DAYS_QTY"),
    F.col("RQST_TOT_LOS_DAYS_QTY"),
    F.rpad(F.col("PRI_SURG_PROC_CD_MOD_TX"), 2, " ").alias("PRI_SURG_PROC_CD_MOD_TX"),
    F.col("DIAG_CD_TYP_CD"),
    F.col("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD"),
    F.rpad(F.col("DSCHG_DIAG_CD"), 6, " ").alias("DSCHG_DIAG_CD")
)

# Write out using "write_files" to the directory "key" with the filename from TmpOutFile
# Because there's no 'landing' or 'external' in the path, we use f"{adls_path}/key/<TmpOutFile>"
ids_um_ip_extr_path = f"{adls_path}/key/{TmpOutFile}"
write_files(
    df_idsumipextr,
    ids_um_ip_extr_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --- Transform => "Snapshot_File" => B_UM_IP.dat ---
# We have columns: SRC_SYS_CD_SK (pk), UM_REF_ID (pk), PRI_DIAG_CD_SK, UM_IP_CUR_TREAT_CAT_CD_SK, UM_IP_STTUS_CD_SK, ACTL_LOS_DAYS_QTY
df_Transform_out = df_Snapshot.withColumn(
    "svPriDiagCdSk", F.lit("GetFkeyDiagCd(Snapshot.SRC_SYS_CD, 1, Snapshot.PRI_DIAG_CD, Snapshot.DIAG_CD_TYP_CD, 'X')")
).withColumn(
    "svUmIpCurTreatCatCdSk", F.lit("GetFkeyCodes(Snapshot.SRC_SYS_CD, 1, \"UTILIZATION MANAGEMENT TREATMENT\", Snapshot.UM_IP_CUR_TREAT_CAT_CD, 'X')")
).withColumn(
    "svUmIpSttusCdSk", F.lit("GetFkeyCodes(Snapshot.SRC_SYS_CD, 1, \"UTILIZATION MANAGEMENT STATUS\", Snapshot.UM_IP_STTUS_CD, 'X')")
)

# Now the final columns:
df_Transform_final = df_Transform_out.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),  # primary key part
    F.col("UM_REF_ID"),
    F.col("svPriDiagCdSk").alias("PRI_DIAG_CD_SK"),
    F.col("svUmIpCurTreatCatCdSk").alias("UM_IP_CUR_TREAT_CAT_CD_SK"),
    F.col("svUmIpSttusCdSk").alias("UM_IP_STTUS_CD_SK"),
    F.col("ACTL_LOS_DAYS_QTY")
)

# Write to "Snapshot_File", which is a CSeqFileStage => "B_UM_IP.dat" in "load" directory
snapshot_file_path = f"{adls_path}/load/B_UM_IP.dat"
df_snapshot_write = df_Transform_final.select(
    "SRC_SYS_CD_SK",
    "UM_REF_ID",
    "PRI_DIAG_CD_SK",
    "UM_IP_CUR_TREAT_CAT_CD_SK",
    "UM_IP_STTUS_CD_SK",
    "ACTL_LOS_DAYS_QTY"
)

write_files(
    df_snapshot_write,
    snapshot_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)