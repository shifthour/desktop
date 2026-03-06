# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004, 2005, 2006, 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC   Pulls data from CMC_ACPR_PYMT_RED  to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------    
# MAGIC               SAndrew   08/04/2004-   Originally Programmed
# MAGIC               SAndrew   08/08/2005    Facets 4.2 changes.   Added key field ACPR_SUB_TYP.  impacts extract, CRF, primary and load.
# MAGIC                                                         changed PCA_OVERPD_NET_AMT to have default of 0.00
# MAGIC               Steph Goddard  02/16/2006  Combine extract, transform, primary key for sequencer
# MAGIC 
# MAGIC Oliver Nielsen          08/21/2007        Balancing            Added Balancing Snapshot                                                           devlIDS30                     Steph Goddard           8/30/07 
# MAGIC 
# MAGIC Bhoomi Dasari         2008-07-08      3657(Primary Key)  Changed primary key process from hash file to DB2 table               devlIDS                        Brent Leland               7-11-2008           
# MAGIC                                                                                       Removed balancing snapshot since it was not used.
# MAGIC 
# MAGIC Manasa Andru       2015-04-22        TFS - 12493         Updated the field - PAYMT_RDUCTN_EXCD_ID                      IntegrateDev2                    Jag Yelavarthi             2016-05-03
# MAGIC                                                                                       to No Upcase so that the field would find
# MAGIC                                                                                          a match on the EXCD table.
# MAGIC 
# MAGIC Krishnakanth          2018-04-25       60037                    Added logic to extract both initial and Delta.                                EnterpriseDev2                 Kalyan Neelam           2018-04-27
# MAGIC   Manivannan
# MAGIC Prabhu ES             2022-02-28        S2S Remediation   MSSQL connection parameters added                                        IntegrateDev5                  Kalyan Neelam           2022-06-10

# MAGIC Run STRIP.FIELD transform on fields that might have Carriage Return, Line Feed, and  Tab
# MAGIC Pulling Facets Claim Override Data
# MAGIC Hash file (hf_paymt_reductn_allcol) cleared from the container - PaymtRductnPK
# MAGIC Writing Sequential File to /key
# MAGIC No reversals created for this table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, length, upper, substring, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value("DriverTable","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
SrcSysCd = get_widget_value("SrcSysCd","")
RunID = get_widget_value("RunID","")
CurrentDate = get_widget_value("CurrentDate","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
LastRunDateTime = get_widget_value("LastRunDateTime","")

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = (
    f"SELECT payred.ACPR_REF_ID, payred.ACPR_SUB_TYPE, payred.ACPR_TX_YR, payred.ACPR_TYPE, "
    f"payred.ACPR_CREATE_DT, payred.LOBD_ID, ACPR_PAYEE_PR_ID, payred.ACPR_PAYEE_CK, "
    f"payred.ACPR_PAYEE_TYPE, payred.ACPR_AUTO_REDUC, payred.ACPR_ORIG_AMT, "
    f"payred.ACPR_RECOV_AMT, payred.ACPR_RECD_AMT, payred.ACPR_WOFF_AMT, payred.ACPR_NET_AMT, "
    f"ACPR_STS, payred.EXCD_ID, USUS_ID, payred.PDDS_PREM_IND, payred.ACPR_VARCHAR_MSG, "
    f"payred.SBFS_PLAN_YEAR_DT "
    f"FROM {FacetsOwner}.CMC_CLOV_OVERPAY opay, {FacetsOwner}.CMC_ACPR_PYMT_RED payred, tempdb..{DriverTable} tmp "
    f"WHERE tmp.CLM_ID = opay.CLCL_ID "
    f"AND opay.ACPR_REF_ID = payred.ACPR_REF_ID "
    f"UNION "
    f"SELECT payred.ACPR_REF_ID, payred.ACPR_SUB_TYPE, payred.ACPR_TX_YR, payred.ACPR_TYPE, "
    f"payred.ACPR_CREATE_DT, payred.LOBD_ID, ACPR_PAYEE_PR_ID, payred.ACPR_PAYEE_CK, "
    f"payred.ACPR_PAYEE_TYPE, payred.ACPR_AUTO_REDUC, payred.ACPR_ORIG_AMT, "
    f"payred.ACPR_RECOV_AMT, payred.ACPR_RECD_AMT, payred.ACPR_WOFF_AMT, payred.ACPR_NET_AMT, "
    f"ACPR_STS, payred.EXCD_ID, USUS_ID, payred.PDDS_PREM_IND, payred.ACPR_VARCHAR_MSG, "
    f"payred.SBFS_PLAN_YEAR_DT "
    f"FROM {FacetsOwner}.CMC_ACPR_PYMT_RED payred "
    f"WHERE payred.ACPR_PAYEE_PR_ID LIKE 'MH%' "
    f"AND payred.ACPR_CREATE_DT > '{LastRunDateTime}'"
)
df_CMC_ACPR_PYMT_RED = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_StripField = (
    df_CMC_ACPR_PYMT_RED
    .withColumn(
        "ACPR_REF_ID",
        rpad(strip_field(col("ACPR_REF_ID")), 16, " ")
    )
    .withColumn(
        "ACPR_SUB_TYPE",
        rpad(strip_field(col("ACPR_SUB_TYPE")), 1, " ")
    )
    .withColumn(
        "ACPR_TX_YR",
        rpad(strip_field(col("ACPR_TX_YR")), 4, " ")
    )
    .withColumn(
        "ACPR_TYPE",
        rpad(strip_field(col("ACPR_TYPE")), 2, " ")
    )
    .withColumn(
        "EXTRACTION_TIMESTATMP",
        current_date()
    )
    .withColumn(
        "ACPR_CREATE_DT",
        col("ACPR_CREATE_DT")
    )
    .withColumn(
        "LOBD_ID",
        rpad(strip_field(col("LOBD_ID")), 4, " ")
    )
    .withColumn(
        "ACPR_PAYEE_PR_ID",
        rpad(strip_field(col("ACPR_PAYEE_PR_ID")), 12, " ")
    )
    .withColumn(
        "ACPR_PAYEE_CK",
        col("ACPR_PAYEE_CK")
    )
    .withColumn(
        "ACPR_PAYEE_TYPE",
        rpad(strip_field(col("ACPR_PAYEE_TYPE")), 1, " ")
    )
    .withColumn(
        "ACPR_AUTO_REDUC",
        rpad(strip_field(col("ACPR_AUTO_REDUC")), 1, " ")
    )
    .withColumn(
        "ACPR_ORIG_AMT",
        col("ACPR_ORIG_AMT")
    )
    .withColumn(
        "ACPR_RECOV_AMT",
        col("ACPR_RECOV_AMT")
    )
    .withColumn(
        "ACPR_RECD_AMT",
        col("ACPR_RECD_AMT")
    )
    .withColumn(
        "ACPR_WOFF_AMT",
        col("ACPR_WOFF_AMT")
    )
    .withColumn(
        "ACPR_NET_AMT",
        col("ACPR_NET_AMT")
    )
    .withColumn(
        "ACPR_STS",
        rpad(strip_field(col("ACPR_STS")), 1, " ")
    )
    .withColumn(
        "EXCD_ID",
        rpad(strip_field(col("EXCD_ID")), 3, " ")
    )
    .withColumn(
        "USUS_ID",
        rpad(strip_field(col("USUS_ID")), 10, " ")
    )
    .withColumn(
        "PDDS_PREM_IND",
        rpad(strip_field(col("PDDS_PREM_IND")), 1, " ")
    )
    .withColumn(
        "ACPR_VARCHAR_MSG",
        when(
            (length(strip_field(col("ACPR_VARCHAR_MSG"))) == 0)
            | (strip_field(col("ACPR_VARCHAR_MSG"))) == "",
            lit(" ")
        ).otherwise(strip_field(col("ACPR_VARCHAR_MSG")))
    )
    .withColumn(
        "SBFS_PLAN_YEAR_DT",
        col("SBFS_PLAN_YEAR_DT")
    )
)

df_BusinessRules_var = df_StripField.withColumn(
    "ReductionPayeeType",
    when(
        col("ACPR_PAYEE_TYPE").isNull() | (length(trim(col("ACPR_PAYEE_TYPE"))) == 0),
        "UNK"
    ).otherwise(upper(trim(col("ACPR_PAYEE_TYPE"))))
)

df_BusinessRules = (
    df_BusinessRules_var
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", rpad(lit("I"), 10, " "))
    .withColumn("DISCARD_IN", rpad(lit("N"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(lit("Y"), 1, " "))
    .withColumn("FIRST_RECYC_DT", col("EXTRACTION_TIMESTATMP"))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit("FACETS"))
    .withColumn(
        "PRI_KEY_STRING",
        concat(
            lit("FACETS"), lit(";"), trim(col("ACPR_REF_ID")), lit(";"),
            trim(col("ACPR_SUB_TYPE")), lit(";"), trim(col("ACPR_TX_YR")),
            lit(";"), trim(col("ACPR_TYPE")))
    )
    .withColumn("PAYMT_RDUCTN_SK", lit(0))
    .withColumn(
        "PAYMT_RDUCTN_REF_ID",
        when(
            col("ACPR_REF_ID").isNull() | (length(trim(col("ACPR_REF_ID"))) == 0),
            "UNK"
        ).otherwise(upper(trim(col("ACPR_REF_ID"))))
    )
    .withColumn(
        "PAYMT_RDUCTN_SUBTYP_CD",
        when(
            col("ACPR_SUB_TYPE").isNull() | (length(trim(col("ACPR_SUB_TYPE"))) == 0),
            "UNK"
        ).otherwise(upper(trim(col("ACPR_SUB_TYPE"))))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn(
        "PAYEE_PROVDER_ID",
        when(
            trim(col("ReductionPayeeType")) == "P",
            when(
                col("ACPR_PAYEE_PR_ID").isNull() | (length(trim(col("ACPR_PAYEE_PR_ID"))) == 0),
                "UNK"
            ).otherwise(upper(trim(col("ACPR_PAYEE_PR_ID"))))
        ).otherwise(
            when(
                col("ACPR_PAYEE_PR_ID").isNull() | (length(trim(col("ACPR_PAYEE_PR_ID"))) == 0),
                "NA"
            ).otherwise(upper(trim(col("ACPR_PAYEE_PR_ID"))))
        )
    )
    .withColumn(
        "USER_ID",
        when(
            trim(col("ACPR_TYPE")) == "MR",
            when(
                col("USUS_ID").isNull() | (length(trim(col("USUS_ID"))) == 0),
                "UNK"
            ).otherwise(upper(trim(col("USUS_ID"))))
        ).otherwise(
            when(
                col("USUS_ID").isNull() | (length(trim(col("USUS_ID"))) == 0),
                "NA"
            ).otherwise(upper(trim(col("USUS_ID"))))
        )
    )
    .withColumn(
        "PAYMT_RDUCTN_EXCD_ID",
        when(
            trim(col("ACPR_TYPE")) == "MR",
            when(
                trim(col("EXCD_ID")).isNull(),
                "UNK"
            ).otherwise(trim(col("EXCD_ID")))
        ).otherwise(
            when(
                col("EXCD_ID").isNull() | (length(trim(col("EXCD_ID"))) == 0),
                "NA"
            ).otherwise(trim(col("EXCD_ID")))
        )
    )
    .withColumn("PAYMT_RDUCTN_PAYE_TYP_CD", col("ReductionPayeeType"))
    .withColumn(
        "PAYMT_RDUCTN_PRM_TYP_CD",
        when(
            col("PDDS_PREM_IND").isNull() | (length(trim(col("PDDS_PREM_IND"))) == 0),
            "UNK"
        ).otherwise(upper(trim(col("PDDS_PREM_IND"))))
    )
    .withColumn(
        "PAYMT_RDUCTN_STTUS_CD",
        when(
            col("ACPR_STS").isNull() | (length(trim(col("ACPR_STS"))) == 0),
            "UNK"
        ).otherwise(upper(trim(col("ACPR_STS"))))
    )
    .withColumn(
        "PAYMT_RDUCTN_TYP_CD",
        when(
            col("ACPR_TYPE").isNull() | (length(trim(col("ACPR_TYPE"))) == 0),
            "UNK"
        ).otherwise(upper(trim(col("ACPR_TYPE"))))
    )
    .withColumn(
        "AUTO_PAYMT_RDUCTN_IN",
        when(
            col("ACPR_AUTO_REDUC").isNull() | (length(trim(col("ACPR_AUTO_REDUC"))) == 0),
            "UNK"
        ).otherwise(upper(trim(col("ACPR_AUTO_REDUC"))))
    )
    .withColumn(
        "CRT_DT",
        substring(col("ACPR_CREATE_DT"), 1, 10)
    )
    .withColumn(
        "PLN_YR_DT",
        trim(substring(col("SBFS_PLAN_YEAR_DT"), 1, 4))
    )
    .withColumn("ORIG_RDUCTN_AMT", col("ACPR_ORIG_AMT"))
    .withColumn("PCA_OVERPD_NET_AMT", lit(0.0))
    .withColumn("RCVD_AMT", col("ACPR_RECD_AMT"))
    .withColumn("RCVRED_AMT", col("ACPR_RECOV_AMT"))
    .withColumn("REMN_NET_AMT", col("ACPR_NET_AMT"))
    .withColumn("WRT_OFF_AMT", col("ACPR_WOFF_AMT"))
    .withColumn("RDUCTN_DESC", col("ACPR_VARCHAR_MSG"))
    .withColumn(
        "TAX_YR",
        when(
            col("ACPR_TX_YR").isNull() | (length(trim(col("ACPR_TX_YR"))) == 0),
            "UNK"
        ).otherwise(upper(trim(col("ACPR_TX_YR"))))
    )
)

df_Snapshot = df_BusinessRules

df_AllColl = df_Snapshot.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("PAYMT_RDUCTN_REF_ID"),
    col("PAYMT_RDUCTN_SUBTYP_CD"),
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PAYEE_PROVDER_ID"),
    col("USER_ID"),
    col("PAYMT_RDUCTN_EXCD_ID"),
    col("PAYMT_RDUCTN_PAYE_TYP_CD"),
    col("PAYMT_RDUCTN_PRM_TYP_CD"),
    col("PAYMT_RDUCTN_STTUS_CD"),
    col("PAYMT_RDUCTN_TYP_CD"),
    col("AUTO_PAYMT_RDUCTN_IN"),
    col("CRT_DT"),
    col("PLN_YR_DT"),
    col("ORIG_RDUCTN_AMT"),
    col("PCA_OVERPD_NET_AMT"),
    col("RCVD_AMT"),
    col("RCVRED_AMT"),
    col("REMN_NET_AMT"),
    col("WRT_OFF_AMT"),
    col("RDUCTN_DESC"),
    col("TAX_YR")
)

df_Transform = df_Snapshot.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("PAYMT_RDUCTN_REF_ID"),
    col("PAYMT_RDUCTN_SUBTYP_CD")
)

# MAGIC %run ../../../../shared_containers/PrimaryKey/PaymtRductnPK
# COMMAND ----------
sc_params = {
    "CurrRunCycle": CurrRunCycle,
    "CurrDate": CurrentDate,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "IDSOwner": IDSOwner
}
df_IdsClmPayReductnPkey = PaymtRductnPK(df_AllColl, df_Transform, sc_params)

final_df = df_IdsClmPayReductnPkey.select(
    rpad(col("JOB_EXCTN_RCRD_ERR_SK").cast("string"), 1, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    rpad(col("PAYMT_RDUCTN_SK").cast("string"), 1, " ").alias("PAYMT_RDUCTN_SK"),
    col("PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
    rpad(col("PAYMT_RDUCTN_SUBTYP_CD"), 1, " ").alias("PAYMT_RDUCTN_SUBTYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("PAYEE_PROVDER_ID"), 12, " ").alias("PAYEE_PROVDER_ID"),
    col("USER_ID").alias("USER_ID"),
    col("PAYMT_RDUCTN_EXCD_ID").alias("PAYMT_RDUCTN_EXCD_ID"),
    col("PAYMT_RDUCTN_PAYE_TYP_CD").alias("PAYMT_RDUCTN_PAYE_TYP_CD"),
    col("PAYMT_RDUCTN_PRM_TYP_CD").alias("PAYMT_RDUCTN_PRM_TYP_CD"),
    col("PAYMT_RDUCTN_STTUS_CD").alias("PAYMT_RDUCTN_STTUS_CD"),
    col("PAYMT_RDUCTN_TYP_CD").alias("PAYMT_RDUCTN_TYP_CD"),
    rpad(col("AUTO_PAYMT_RDUCTN_IN"), 1, " ").alias("AUTO_PAYMT_RDUCTN_IN"),
    rpad(col("CRT_DT"), 10, " ").alias("CRT_DT"),
    rpad(col("PLN_YR_DT"), 10, " ").alias("PLN_YR_DT"),
    col("ORIG_RDUCTN_AMT").alias("ORIG_RDUCTN_AMT"),
    col("PCA_OVERPD_NET_AMT").alias("PCA_OVERPD_NET_AMT"),
    col("RCVD_AMT").alias("RCVD_AMT"),
    col("RCVRED_AMT").alias("RCVRED_AMT"),
    col("REMN_NET_AMT").alias("REMN_NET_AMT"),
    col("WRT_OFF_AMT").alias("WRT_OFF_AMT"),
    col("RDUCTN_DESC").alias("RDUCTN_DESC"),
    rpad(col("TAX_YR"), 4, " ").alias("TAX_YR")
)

write_files(
    final_df,
    f"{adls_path}/key/FctsClmPayRductnExtr.FctsClmPayRductn.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)