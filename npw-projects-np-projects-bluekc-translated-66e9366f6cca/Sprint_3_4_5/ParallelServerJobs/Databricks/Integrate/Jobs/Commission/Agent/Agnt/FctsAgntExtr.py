# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsAgntExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_COCE_COMM_ENTY for loading into IDS.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:     Facets -  CMC_COCE_COMM_ENTY
# MAGIC 
# MAGIC HASH FILES: none
# MAGIC 
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                              Trim
# MAGIC                              GetFkeyAgntIndv
# MAGIC                              GetFkeyTaxDmgrphc
# MAGIC                            
# MAGIC PROCESSING:  Extract, transform, and primary keying for Agent subject area.
# MAGIC 
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Hugh Sisson    10/24/2005  -  Originally Program
# MAGIC 
# MAGIC 
# MAGIC Developer                            Date                 Project/Altiris #                Change Description                                                                                                      Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------        -------------------    -----------------------------------    ---------------------------------------------------------                                                                                 ----------------------------------     ---------------------------------    -------------------------   
# MAGIC Parikshith Chada                6/7/2007          3264                              Added Balancing process to the overall                                                                            devlIDS30                      Steph Goddard             09/17/2007
# MAGIC                                                                                                           job that takes a snapshot of the source data                        
# MAGIC 
# MAGIC Bhoomi Dasari                    09/04/2008       3567                             Added new primay key contianer and SrcSysCdsk and SrcSysCd                                    devlIDS                         Steph Goddard              09/22/2008  
# MAGIC                                                                                                                                      
# MAGIC Dan Long                           2014-06-17         TFS-1079                    Add field COCE_MCTR_VIP to the CMC_COCE_COMM_ENTY stage.  Passed               IntegrateNewDevl         Kalyan Neelam               2014-07-08
# MAGIC                                                                                                           the value thru the Stripfields, BusinessRules, AgentPK and  IdsAgntExtr stages
# MAGIC                                                                                                          .
# MAGIC Shanmugam Annamalai     2018-02-01         TFS-21033                   Updated logic for field AGNT_TERM_RSN_CD, AGNT_PD_AGNT_TYP_CD.                 Integrate Dev1             Jaideep Mankala             2018-02-22 
# MAGIC Prabhu ES                         2022-03-01         S2S Remediation          MSSQL connection parameters added                                                                               IntegrateDev5

# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Trim all string variables
# MAGIC Extract Facets Agent Data
# MAGIC Apply business logic
# MAGIC Hash file hf_agnt_allcol cleared
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


# MAGIC %run ../../../../../shared_containers/PrimaryKey/AgntPK
# COMMAND ----------

facets_secret_name = get_widget_value('facets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
IDSOwner = get_widget_value('IDSOwner','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

df_CMC_COCE_COMM_ENTY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"SELECT COCE_ID,COCE_TERM_DT,COCE_NAME,COTD_ENTY_TYPE,COCE_CK,COCE_MCTR_TRSN,COTD_ENTY_TYPE_PAY,COCE_PAY_CO_METH,COCI_ID,COAD_TYPE_CHECK,COAD_TYPE_MAIL,MCTN_ID,COCE_STMT_FROM_DT,COCE_STMT_THRU_DT,COCE_MCTR_VIP FROM {FacetsOwner}.CMC_COCE_COMM_ENTY WHERE COTD_ENTY_TYPE = 'AGNT' Or COTD_ENTY_TYPE = 'SUBA' Or COTD_ENTY_TYPE = 'AGCY'"
    )
    .load()
)

df_StripFields = (
    df_CMC_COCE_COMM_ENTY
    .withColumn("COCE_ID", trim(strip_field("COCE_ID")))
    .withColumn("COCE_TERM_DT", F.substring(strip_field("COCE_TERM_DT"), 1, 10))
    .withColumn("COCE_NAME", trim(strip_field("COCE_NAME")))
    .withColumn("COTD_ENTY_TYPE", trim(strip_field("COTD_ENTY_TYPE")))
    .withColumn("COCE_CK", F.col("COCE_CK"))
    .withColumn("COCE_MCTR_TRSN", trim(strip_field("COCE_MCTR_TRSN")))
    .withColumn("COTD_ENTY_TYPE_PAY", trim(strip_field("COTD_ENTY_TYPE_PAY")))
    .withColumn("COCE_PAY_CO_METH", trim(strip_field("COCE_PAY_CO_METH")))
    .withColumn("COCI_ID", trim(strip_field("COCI_ID")))
    .withColumn("COAD_TYPE_CHECK", trim(strip_field("COAD_TYPE_CHECK")))
    .withColumn("COAD_TYPE_MAIL", trim(strip_field("COAD_TYPE_MAIL")))
    .withColumn("MCTN_ID", trim(strip_field("MCTN_ID")))
    .withColumn("COCE_STMT_FROM_DT", F.substring(strip_field("COCE_STMT_FROM_DT"), 1, 10))
    .withColumn("COCE_STMT_THRU_DT", F.substring(strip_field("COCE_STMT_THRU_DT"), 1, 10))
    .withColumn("COCE_MCTR_VIP", trim(strip_field("COCE_MCTR_VIP")))
)

df_BusinessRules = (
    df_StripFields
    .withColumn("RowPassThru", F.lit("Y"))
    .withColumn("svSrcSysCd", F.lit("FACETS"))
    .withColumn(
        "svMctnIdIsNull",
        F.when(
            (F.col("MCTN_ID").isNull()) | (trim("MCTN_ID") == ""),
            True
        ).otherwise(False)
    )
    .withColumn(
        "svCotdEntyTypePayIsNull",
        F.when(
            (F.col("COTD_ENTY_TYPE_PAY").isNull()) | (trim("COTD_ENTY_TYPE_PAY") == ""),
            True
        ).otherwise(False)
    )
    .withColumn(
        "svCoceMctrTrsnIsNull",
        F.when(
            (F.col("COCE_MCTR_TRSN").isNull()) | (trim("COCE_MCTR_TRSN") == ""),
            True
        ).otherwise(False)
    )
    .withColumn("svCoceTermDtString", F.col("COCE_TERM_DT"))
    .withColumn(
        "svAgntId",
        F.when(
            F.col("COTD_ENTY_TYPE") == "AGCY", "NA"
        ).otherwise(
            F.when(
                (F.col("COCI_ID").isNull()) | (trim("COCI_ID") == ""), "UNK"
            ).otherwise(
                F.upper(trim("COCI_ID"))
            )
        )
    )
)

df_BusinessRules_allcol = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("COCE_ID").alias("AGNT_ID"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    F.lit(current_date()).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD"),
    F.concat(F.col("svSrcSysCd"), F.lit(";"), F.col("COCE_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("AGNT_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("svAgntId").alias("AGNT_INDV"),
    F.when(
        F.col("svMctnIdIsNull") == True,
        F.when(
            F.substring(F.col("COCE_TERM_DT"), 1, 10) == "9999-12-31", "UNK"
        ).otherwise("NA")
    ).otherwise(F.col("MCTN_ID")).alias("TAX_DMGRPHC"),
    F.when(
        (F.col("COAD_TYPE_MAIL").isNull()) | (trim("COAD_TYPE_MAIL") == ""),
        "UNK"
    ).otherwise(F.upper(trim("COAD_TYPE_MAIL"))).alias("AGNT_ADDR_MAIL_TYP_CD"),
    F.when(
        (F.col("COAD_TYPE_CHECK").isNull()) | (trim("COAD_TYPE_CHECK") == ""),
        "UNK"
    ).otherwise(F.upper(trim("COAD_TYPE_CHECK"))).alias("AGNT_ADDR_REMIT_TYP_CD"),
    F.when(
        F.col("svCotdEntyTypePayIsNull") == True,
        F.lit("NA")
    ).otherwise(
        F.when(
            F.col("COTD_ENTY_TYPE") == "SUBA",
            "NA"
        ).otherwise(F.col("COTD_ENTY_TYPE_PAY"))
    ).alias("AGNT_PD_AGNT_TYP_CD"),
    F.when(
        (F.col("COCE_PAY_CO_METH").isNull()) | (trim("COCE_PAY_CO_METH") == ""),
        "UNK"
    ).otherwise(F.upper(trim("COCE_PAY_CO_METH"))).alias("AGNT_PAYMT_METH_CD"),
    F.when(
        F.col("svCoceMctrTrsnIsNull") == True,
        F.when(
            F.col("svCoceTermDtString") == "9999-12-31", "NA"
        ).otherwise("MSNGRSN")
    ).otherwise(F.upper(F.col("COCE_MCTR_TRSN"))).alias("AGNT_TERM_RSN_CD"),
    F.when(
        (F.col("COTD_ENTY_TYPE").isNull()) | (trim("COTD_ENTY_TYPE") == ""),
        "UNK"
    ).otherwise(F.upper(trim("COTD_ENTY_TYPE"))).alias("AGNT_TYP_CD"),
    F.col("COCE_STMT_FROM_DT").alias("LAST_STMNT_FROM_DT"),
    F.col("COCE_STMT_THRU_DT").alias("LAST_STMNT_THRU_DT"),
    F.col("COCE_TERM_DT").alias("TERM_DT"),
    F.col("COCE_CK").alias("AGNT_UNIQ_KEY"),
    F.col("svAgntId").alias("AGNT_INDV_ID"),
    F.col("COCE_NAME").alias("AGNT_NM"),
    F.col("COCE_MCTR_VIP").alias("COCE_MCTR_VIP")
)

df_BusinessRules_transform = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("COCE_ID").alias("AGNT_ID")
)

params_AgntPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}

df_AgntPK_Key = AgntPK(df_BusinessRules_allcol, df_BusinessRules_transform, params_AgntPK)

df_final_IdsAgntExtr = df_AgntPK_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("AGNT_SK"),
    F.rpad(F.col("AGNT_ID"), 10, " ").alias("AGNT_ID"),
    F.col("CRT_RUN_CYC_EXTCN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.rpad(F.col("AGNT_INDV"), 12, " ").alias("AGNT_INDV"),
    F.rpad(F.col("TAX_DMGRPHC"), 9, " ").alias("TAX_DMGRPHC"),
    F.rpad(F.col("AGNT_ADDR_MAIL_TYP_CD"), 1, " ").alias("AGNT_ADDR_MAIL_TYP_CD"),
    F.rpad(F.col("AGNT_ADDR_REMIT_TYP_CD"), 1, " ").alias("AGNT_ADDR_REMIT_TYP_CD"),
    F.rpad(F.col("AGNT_PD_AGNT_TYP_CD"), 4, " ").alias("AGNT_PD_AGNT_TYP_CD"),
    F.rpad(F.col("AGNT_PAYMT_METH_CD"), 1, " ").alias("AGNT_PAYMT_METH_CD"),
    F.rpad(F.col("AGNT_TERM_RSN_CD"), 4, " ").alias("AGNT_TERM_RSN_CD"),
    F.rpad(F.col("AGNT_TYP_CD"), 4, " ").alias("AGNT_TYP_CD"),
    F.col("LAST_STMNT_FROM_DT"),
    F.col("LAST_STMNT_THRU_DT"),
    F.col("TERM_DT"),
    F.col("AGNT_UNIQ_KEY"),
    F.rpad(F.col("AGNT_INDV_ID"), 12, " ").alias("AGNT_INDV_ID"),
    F.rpad(F.col("AGNT_NM"), 55, " ").alias("AGNT_NM"),
    F.rpad(F.col("COCE_MCTR_VIP"), 4, " ").alias("COCE_MCTR_VIP")
)

write_files(
    df_final_IdsAgntExtr,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"SELECT COCE_ID FROM {FacetsOwner}.CMC_COCE_COMM_ENTY WHERE COTD_ENTY_TYPE = 'AGNT' Or COTD_ENTY_TYPE = 'SUBA' Or COTD_ENTY_TYPE = 'AGCY'"
    )
    .load()
)

df_Transform = (
    df_Facets_Source
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("AGNT_ID", trim(strip_field("COCE_ID")))
)

df_final_Snapshot_File = df_Transform.select(
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("AGNT_ID"), 10, " ").alias("AGNT_ID")
)

write_files(
    df_final_Snapshot_File,
    f"{adls_path}/load/B_AGNT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)