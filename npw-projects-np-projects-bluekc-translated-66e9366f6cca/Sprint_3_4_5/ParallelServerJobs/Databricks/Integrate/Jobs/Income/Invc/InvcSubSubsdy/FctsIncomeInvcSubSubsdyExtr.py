# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsIncomeInvcSubSbsdyExtr
# MAGIC CALLED BY:    FctsIncomeExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Pulls the Invoice Subscriber subsidy information from Facets for Income 
# MAGIC       
# MAGIC    Date is used as a criteria for which records are pulled.   
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name. 
# MAGIC                   
# MAGIC                  This is pulls all records from a BeginDate supplied in parameters.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date             Project/Altiris #      Change Description                                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    ---------------------------   -----------------------------------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                 2014-06-20          5235                 Original Programming                                                    IntegrateNewDevl        Bhoomi Dasari              6/21/2014
# MAGIC 
# MAGIC Goutham Kalidindi             2021-03-24      358186                 Changed Datatype length for field                                IntegrateDev2              Reddy Sanam              04/01/2021
# MAGIC                                                                                              BLIV_ID
# MAGIC                                                                                                char(12) to Varchar(15)
# MAGIC Prabhu ES                         2022-03-02    S2S Remediation  MSSQL ODBC conn added and other param changes  IntegrateDev5	         Ken Bradmon	             2022-06-13
# MAGIC 
# MAGIC Reddy Sanam                 2022-07-15                                    Changed the shared container for a column mapping                                         Hugh  Sisson               2022-07-15
# MAGIC                                                                                               Keys.CRT_TS [1,23] to Keys.CRT_TS

# MAGIC Hash file hf_invc_sub_allcol cleared
# MAGIC Pulls all Invoice rows from facets CDS_INSB_SB_DETAIL table matching data criteria.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/IncmInvcSubSubsdyPK
# COMMAND ----------

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
SrcSysCd = get_widget_value('SrcSysCd','FACETS')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

df_hf_invc_rebills = spark.read.parquet(f"{adls_path}/hf_invc_rebills.parquet")

query_CDS_INSS_SB_SUB = (
    "SELECT SUB.BLIV_ID, SUB.CSPI_ID, SUB.PDPD_ID, SUB.PDBL_ID, SUB.BLSB_COV_DUE_DT, "
    "SUB.BLSB_COV_FROM_DT, SUB.BLSB_PREM_TYPE, SUB.SBSB_CK, SUB.BLSB_CREATE_DTM, "
    "SUB.BLSS_DISP_CD, SUB.BLSB_COV_THRU_DT, SUB.BLSS_PREM_SB, SUB.GRGR_ID, "
    "SUB.SGSG_ID, SUB.CSCS_ID, SUB.BLSB_FI "
    f"FROM tempdb..{DriverTable} TmpInvc, {FacetsOwner}.CDS_INSS_SB_SUB SUB "
    "WHERE TmpInvc.BILL_INVC_ID = SUB.BLIV_ID "
    "AND (SUB.BLSS_PREM_SB<>0 OR SUB.BLSS_PREM_DEP<>0)"
)

df_CDS_INSS_SB_SUB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_CDS_INSS_SB_SUB)
    .load()
)

df_StripField = (
    df_CDS_INSS_SB_SUB
    .withColumn("BLIV_ID", F.regexp_replace(trim(F.col("BLIV_ID")), "[\r\n\t]", ""))
    .withColumn("CSPI_ID", F.regexp_replace(trim(F.col("CSPI_ID")), "[\r\n\t]", ""))
    .withColumn("PDPD_ID", F.regexp_replace(trim(F.col("PDPD_ID")), "[\r\n\t]", ""))
    .withColumn("PDBL_ID", F.regexp_replace(trim(F.col("PDBL_ID")), "[\r\n\t]", ""))
    .withColumn("BLSB_COV_DUE_DT", F.col("BLSB_COV_DUE_DT"))
    .withColumn("BLSB_COV_FROM_DT", F.col("BLSB_COV_FROM_DT"))
    .withColumn("BLSB_PREM_TYPE", F.regexp_replace(trim(F.col("BLSB_PREM_TYPE")), "[\r\n\t]", ""))
    .withColumn("SBSB_CK", F.col("SBSB_CK"))
    .withColumn("BLSB_CREATE_DTM", F.col("BLSB_CREATE_DTM"))
    .withColumn("BLSS_DISP_CD", F.regexp_replace(trim(F.col("BLSS_DISP_CD")), "[\r\n\t]", ""))
    .withColumn("BLSB_COV_THRU_DT", F.col("BLSB_COV_THRU_DT"))
    .withColumn("BLSS_PREM_SB", F.col("BLSS_PREM_SB"))
    .withColumn("GRGR_ID", F.regexp_replace(trim(F.col("GRGR_ID")), "[\r\n\t]", ""))
    .withColumn("SGSG_ID", F.regexp_replace(trim(F.col("SGSG_ID")), "[\r\n\t]", ""))
    .withColumn("CSCS_ID", F.regexp_replace(trim(F.col("CSCS_ID")), "[\r\n\t]", ""))
    .withColumn("BLSB_FI", F.regexp_replace(trim(F.col("BLSB_FI")), "[\r\n\t]", ""))
)

df_BusinessRules_stagevars = (
    df_StripField
    .withColumn("svBlsbDispCd", F.when(F.length(trim(F.col("BLSS_DISP_CD")))==0, F.lit("NA")).otherwise(trim(F.col("BLSS_DISP_CD"))))
    .withColumn("svCovDueDt", F.substring(F.col("BLSB_COV_DUE_DT"), 1, 10))
    .withColumn("svCovStrtDt", F.substring(F.col("BLSB_COV_FROM_DT"), 1, 10))
    .withColumn("svCovEndDt", F.substring(F.col("BLSB_COV_THRU_DT"), 1, 10))
    .withColumn("svCrtDt", F.concat(
        F.substring(F.col("BLSB_CREATE_DTM"), 1, 10),
        F.lit(" "),
        F.substring(F.col("BLSB_CREATE_DTM"), 12, 2),
        F.lit(":"),
        F.substring(F.col("BLSB_CREATE_DTM"), 15, 2),
        F.lit(":"),
        F.substring(F.col("BLSB_CREATE_DTM"), 18, 2),
        F.lit("."),
        F.substring(F.col("BLSB_CREATE_DTM"), 21, 6)
    ))
)

df_BusinessRules = df_BusinessRules_stagevars.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(None).alias("FIRST_RECYC_DT").cast(StringType()),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(
        F.lit("FACETS;"),
        trim(F.col("BLIV_ID")),
        F.lit(";"),
        F.col("SBSB_CK"),
        F.lit(";"),
        trim(F.col("CSPI_ID")),
        F.lit(";"),
        trim(F.col("PDPD_ID")),
        F.lit(";"),
        trim(F.col("PDBL_ID")),
        F.lit(";"),
        F.col("svCovDueDt"),
        F.lit(";"),
        F.col("svCovStrtDt"),
        F.lit(";"),
        trim(F.col("BLSB_PREM_TYPE")),
        F.lit(";"),
        F.col("svCrtDt"),
        F.lit(";"),
        F.col("svBlsbDispCd")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("INVC_SUB_SBSDY_SK"),
    trim(F.col("BLIV_ID")).alias("BILL_INVC_ID"),
    F.col("SBSB_CK").alias("SUB_UNIQ_KEY"),
    trim(F.col("CSPI_ID")).alias("CLS_PLN_ID"),
    trim(F.col("PDPD_ID")).alias("PROD_ID"),
    trim(F.col("PDBL_ID")).alias("PROD_BILL_CMPNT_ID"),
    F.col("svCovDueDt").alias("COV_DUE_DT"),
    F.col("svCovStrtDt").alias("COV_STRT_DT_SK"),
    F.col("BLSB_PREM_TYPE").alias("INVC_SUB_SBSDY_PRM_TYP_CD"),
    F.col("svCrtDt").alias("CRT_DT"),
    F.col("svBlsbDispCd").alias("INVC_SUB_SBSDY_BILL_DISP_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    trim(F.col("CSCS_ID")).alias("CLS_ID"),
    trim(F.col("CSPI_ID")).alias("CSPI_ID"),
    trim(F.col("GRGR_ID")).alias("GRP_ID"),
    trim(F.col("BLIV_ID")).alias("INVC_ID"),
    trim(F.col("SGSG_ID")).alias("SUBGRP_ID"),
    F.when(trim(F.col("BLSB_FI"))=="*", F.lit("IDB"))
     .when(F.length(trim(F.col("BLSB_FI")))==0, F.lit("NA"))
     .otherwise(trim(F.col("BLSB_FI"))).alias("INVC_SUB_FMLY_CNTR_CD"),
    F.col("svCovEndDt").alias("COV_END_DT_SK"),
    F.col("BLSS_PREM_SB").alias("SUB_SBSDY_AMT")
)

df_Join_ReversalLogic = df_BusinessRules.alias("ChkReversals").join(
    df_hf_invc_rebills.alias("rebill_lkup"),
    F.col("ChkReversals.BILL_INVC_ID") == F.col("rebill_lkup.BILL_INVC_ID"),
    how="left"
)

df_ReversalLogic_stagevars = (
    df_Join_ReversalLogic
    .withColumn("svReversalsString", F.concat(
        F.lit("FACETS;"),
        trim(F.col("ChkReversals.BILL_INVC_ID")),
        F.lit("R;"),
        F.col("ChkReversals.SUB_UNIQ_KEY"),
        F.lit(";"),
        trim(F.col("ChkReversals.CLS_PLN_ID")),
        F.lit(";"),
        trim(F.col("ChkReversals.PROD_ID")),
        F.lit(";"),
        trim(F.col("ChkReversals.PROD_BILL_CMPNT_ID")),
        F.lit(";"),
        F.col("ChkReversals.COV_DUE_DT"),
        F.lit(";"),
        F.col("ChkReversals.COV_STRT_DT_SK"),
        F.lit(";"),
        trim(F.col("ChkReversals.INVC_SUB_SBSDY_PRM_TYP_CD")),
        F.lit(";"),
        F.col("ChkReversals.CRT_DT"),
        F.lit(";"),
        F.col("ChkReversals.INVC_SUB_SBSDY_BILL_DISP_CD")
    ))
    .withColumn("svReversalBillInvcId", F.concat(F.col("ChkReversals.BILL_INVC_ID"), F.lit("R")))
    .withColumn("svReversalnvcId", F.concat(F.col("ChkReversals.INVC_ID"), F.lit("R")))
)

df_Reversal = df_ReversalLogic_stagevars.filter(~F.isnull(F.col("rebill_lkup.BILL_INVC_ID")))
df_NonReversal = df_ReversalLogic_stagevars.filter(F.isnull(F.col("rebill_lkup.BILL_INVC_ID")))

df_NonReversal_out = df_NonReversal.select(
    F.col("ChkReversals.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("ChkReversals.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("ChkReversals.DISCARD_IN").alias("DISCARD_IN"),
    F.col("ChkReversals.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("ChkReversals.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ChkReversals.ERR_CT").alias("ERR_CT"),
    F.col("ChkReversals.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("ChkReversals.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("ChkReversals.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("ChkReversals.INVC_SUB_SBSDY_SK").alias("INVC_SUB_SBSDY_SK"),
    F.col("ChkReversals.BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("ChkReversals.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("ChkReversals.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("ChkReversals.PROD_ID").alias("PROD_ID"),
    F.col("ChkReversals.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("ChkReversals.COV_DUE_DT").alias("COV_DUE_DT"),
    F.col("ChkReversals.COV_STRT_DT_SK").alias("COV_STRT_DT_SK"),
    F.col("ChkReversals.INVC_SUB_SBSDY_PRM_TYP_CD").alias("INVC_SUB_SBSDY_PRM_TYP_CD"),
    F.col("ChkReversals.CRT_DT").alias("CRT_DT"),
    F.col("ChkReversals.INVC_SUB_SBSDY_BILL_DISP_CD").alias("INVC_SUB_SBSDY_BILL_DISP_CD"),
    F.col("ChkReversals.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("ChkReversals.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ChkReversals.CLS_ID").alias("CLS_ID"),
    F.col("ChkReversals.CSPI_ID").alias("CSPI_ID"),
    F.col("ChkReversals.GRP_ID").alias("GRP_ID"),
    F.col("ChkReversals.INVC_ID").alias("INVC_ID"),
    F.col("ChkReversals.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("ChkReversals.INVC_SUB_FMLY_CNTR_CD").alias("INVC_SUB_FMLY_CNTR_CD"),
    F.col("ChkReversals.COV_END_DT_SK").alias("COV_END_DT_SK"),
    F.col("ChkReversals.SUB_SBSDY_AMT").alias("SUB_SBSDY_AMT"),
)

df_Reversal_out = df_Reversal.select(
    F.col("ChkReversals.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("ChkReversals.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("ChkReversals.DISCARD_IN").alias("DISCARD_IN"),
    F.col("ChkReversals.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("ChkReversals.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ChkReversals.ERR_CT").alias("ERR_CT"),
    F.col("ChkReversals.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("ChkReversals.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("svReversalsString").alias("PRI_KEY_STRING"),
    F.col("ChkReversals.INVC_SUB_SBSDY_SK").alias("INVC_SUB_SBSDY_SK"),
    F.col("svReversalBillInvcId").alias("BILL_INVC_ID"),
    F.col("ChkReversals.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("ChkReversals.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("ChkReversals.PROD_ID").alias("PROD_ID"),
    F.col("ChkReversals.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    F.col("ChkReversals.COV_DUE_DT").alias("COV_DUE_DT"),
    F.col("ChkReversals.COV_STRT_DT_SK").alias("COV_STRT_DT_SK"),
    F.col("ChkReversals.INVC_SUB_SBSDY_PRM_TYP_CD").alias("INVC_SUB_SBSDY_PRM_TYP_CD"),
    F.col("ChkReversals.CRT_DT").alias("CRT_DT"),
    F.col("ChkReversals.INVC_SUB_SBSDY_BILL_DISP_CD").alias("INVC_SUB_SBSDY_BILL_DISP_CD"),
    F.col("ChkReversals.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("ChkReversals.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ChkReversals.CLS_ID").alias("CLS_ID"),
    F.col("ChkReversals.CSPI_ID").alias("CSPI_ID"),
    F.col("ChkReversals.GRP_ID").alias("GRP_ID"),
    F.col("svReversalnvcId").alias("INVC_ID"),
    F.col("ChkReversals.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("ChkReversals.INVC_SUB_FMLY_CNTR_CD").alias("INVC_SUB_FMLY_CNTR_CD"),
    F.col("ChkReversals.COV_END_DT_SK").alias("COV_END_DT_SK"),
    (F.col("ChkReversals.SUB_SBSDY_AMT") * -1).alias("SUB_SBSDY_AMT")
)

df_ColletRows = df_NonReversal_out.unionByName(df_Reversal_out)

df_Key = df_ColletRows

df_KeyAllCol = df_Key.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("BILL_INVC_ID"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID"),
    F.col("COV_DUE_DT").alias("COV_DUE_DT"),
    F.col("COV_STRT_DT_SK"),
    F.col("INVC_SUB_SBSDY_PRM_TYP_CD"),
    F.col("CRT_DT"),
    F.col("INVC_SUB_SBSDY_BILL_DISP_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("INVC_SUB_SBSDY_SK"),
    F.col("COV_END_DT_SK"),
    F.col("CLS_ID"),
    F.col("CSPI_ID"),
    F.col("GRP_ID"),
    F.col("INVC_ID"),
    F.col("SUBGRP_ID"),
    F.col("INVC_SUB_FMLY_CNTR_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SUB_SBSDY_AMT")
)

df_KeyTransform = df_Key.select(
    F.col("BILL_INVC_ID"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID"),
    F.col("COV_DUE_DT").alias("COV_DUE_DT_SK"),
    F.col("COV_STRT_DT_SK"),
    F.col("INVC_SUB_SBSDY_PRM_TYP_CD"),
    F.col("CRT_DT").alias("CRT_TS"),
    F.col("INVC_SUB_SBSDY_BILL_DISP_CD"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

df_KeySnapshot = df_Key.select(
    F.col("BILL_INVC_ID"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID"),
    F.col("COV_DUE_DT").alias("COV_DUE_DT_SK"),
    F.col("COV_STRT_DT_SK"),
    F.col("INVC_SUB_SBSDY_PRM_TYP_CD"),
    F.col("CRT_DT").alias("CRT_TS"),
    F.col("INVC_SUB_SBSDY_BILL_DISP_CD"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("SUB_SBSDY_AMT")
)

df_KeySnapshot_write = df_KeySnapshot.select(
    F.col("BILL_INVC_ID"),
    F.col("SUB_UNIQ_KEY"),
    F.rpad(F.col("CLS_PLN_ID"), 8, " ").alias("CLS_PLN_ID"),
    F.rpad(F.col("PROD_ID"), 8, " ").alias("PROD_ID"),
    F.rpad(F.col("PROD_BILL_CMPNT_ID"), 4, " ").alias("PROD_BILL_CMPNT_ID"),
    F.rpad(F.col("COV_DUE_DT_SK"), 10, " ").alias("COV_DUE_DT_SK"),
    F.rpad(F.col("COV_STRT_DT_SK"), 10, " ").alias("COV_STRT_DT_SK"),
    F.col("INVC_SUB_SBSDY_PRM_TYP_CD"),
    F.col("CRT_TS"),
    F.col("INVC_SUB_SBSDY_BILL_DISP_CD"),
    F.rpad(F.col("SRC_SYS_CD_SK"), len(SrcSysCdSk), " ").alias("SRC_SYS_CD_SK"),
    F.col("SUB_SBSDY_AMT")
)

write_files(
    df_KeySnapshot_write,
    f"{adls_path}/load/B_INVC_SUB_SBSDY.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)

params_IncmInvcSubSubsdyPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "$IDSOwner": IDSOwner
}

df_IdsInvcSubSubsdy = IncmInvcSubSubsdyPK(df_KeyTransform, df_KeyAllCol, params_IncmInvcSubSubsdyPK)

df_IdsInvcSubSubsdy_write = df_IdsInvcSubSubsdy.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("INVC_SUB_SBSDY_SK"),
    F.col("BILL_INVC_ID"),
    F.rpad(F.col("CLS_PLN_ID"), 8, " ").alias("CLS_PLN_ID"),
    F.rpad(F.col("PROD_ID"), 8, " ").alias("PROD_ID"),
    F.rpad(F.col("PROD_BILL_CMPNT_ID"), 4, " ").alias("PROD_BILL_CMPNT_ID"),
    F.rpad(F.col("COV_DUE_DT"), 10, " ").alias("COV_DUE_DT"),
    F.rpad(F.col("COV_STRT_DT_SK"), 10, " ").alias("COV_STRT_DT_SK"),
    F.col("INVC_SUB_SBSDY_PRM_TYP_CD"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CRT_DT"),
    F.col("INVC_SUB_SBSDY_BILL_DISP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("COV_END_DT_SK"), 10, " ").alias("COV_END_DT_SK"),
    F.rpad(F.col("CLS_ID"), 4, " ").alias("CLS_ID"),
    F.rpad(F.col("CSPI_ID"), 8, " ").alias("CSPI_ID"),
    F.rpad(F.col("GRP_ID"), 8, " ").alias("GRP_ID"),
    F.col("INVC_ID"),
    F.rpad(F.col("SUBGRP_ID"), 4, " ").alias("SUBGRP_ID"),
    F.rpad(F.col("INVC_SUB_FMLY_CNTR_CD"), 1, " ").alias("INVC_SUB_FMLY_CNTR_CD"),
    F.col("SUB_SBSDY_AMT")
)

write_files(
    df_IdsInvcSubSubsdy_write,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)