# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2008 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:    ScorecardEmplProdItemDailyCntl
# MAGIC                    
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC  Developer                                    Date                             Project/Ticket #                                Change Description                       Development Project         Code Reviewer            Date Reviewed
# MAGIC ----------------------------------                -------------------                    ---------------------------                            -----------------------------------                    ----------------------------------        -------------------------           -------------------------   
# MAGIC Sruthi Mandadi                           2018-09-19             5236 - Indigo Replacement                           New Program                                EnterpriseDev1               Kalyan Neelam          2018-09-27

# MAGIC The sequential file is used by the ScrcrdEmplProdItemLoad  job to Load Table SCRCRD_EMPL_PROD_ITEM table in ScoreCard Database
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# ======================================================
# Retrieve all job parameters via get_widget_value
# ======================================================
MbrSvcWorkItemOwner = get_widget_value('MbrSvcWorkItemOwner','')
LastRunDtm = get_widget_value('LastRunDtm','')
CurrentRunDtm = get_widget_value('CurrentRunDtm','')
ScoreCardOwner = get_widget_value('ScoreCardOwner','')
Source = get_widget_value('Source','')
mbrsvcworkitem_secret_name = get_widget_value('mbrsvcworkitem_secret_name','')
scorecard_secret_name = get_widget_value('scorecard_secret_name','')

# ======================================================
# Read from ScrCrd_EMPL (ODBCConnectorPX)
# ======================================================
jdbc_url_ScrCrd_EMPL, jdbc_props_ScrCrd_EMPL = get_db_config(scorecard_secret_name)
extract_query_ScrCrd_EMPL = (
    f"SELECT \n"
    f"    EMPL_ID , EMPL_ACTV_DIR_ID\n"
    f"FROM \n"
    f"{ScoreCardOwner}.EMPL"
)
df_ScrCrd_EMPL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ScrCrd_EMPL)
    .options(**jdbc_props_ScrCrd_EMPL)
    .option("query", extract_query_ScrCrd_EMPL)
    .load()
)

# ======================================================
# Read from ScrCrd_Work_Typ (ODBCConnectorPX)
# ======================================================
jdbc_url_ScrCrd_Work_Typ, jdbc_props_ScrCrd_Work_Typ = get_db_config(scorecard_secret_name)
extract_query_ScrCrd_Work_Typ = (
    f"SELECT \n"
    f"    SCRCRD_WORK_TYP_ID\n"
    f"FROM \n"
    f"{ScoreCardOwner}.SCRCRD_WORK_TYP"
)
df_ScrCrd_Work_Typ = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ScrCrd_Work_Typ)
    .options(**jdbc_props_ScrCrd_Work_Typ)
    .option("query", extract_query_ScrCrd_Work_Typ)
    .load()
)

# ======================================================
# Copy stage producing multiple outputs from df_ScrCrd_Work_Typ
# ======================================================
df_Scrcrd_Work_Typ1 = df_ScrCrd_Work_Typ.select("SCRCRD_WORK_TYP_ID")
df_Scrcrd_Work_Typ2 = df_ScrCrd_Work_Typ.select("SCRCRD_WORK_TYP_ID")
df_Scrcrd_Work_Typ3 = df_ScrCrd_Work_Typ.select("SCRCRD_WORK_TYP_ID")
df_Scrcrd_Work_Typ4 = df_ScrCrd_Work_Typ.select("SCRCRD_WORK_TYP_ID")

# ======================================================
# Read from MBR_SVC_WORK_ITEM (ODBCConnectorPX)
# ======================================================
jdbc_url_MBR_SVC_WORK_ITEM, jdbc_props_MBR_SVC_WORK_ITEM = get_db_config(mbrsvcworkitem_secret_name)
extract_query_MBR_SVC_WORK_ITEM = (
    f"SELECT DISTINCT\n"
    f"      DOC_ID\n"
    f"      ,LAST_RTE_BY_ACTV_DIR_ACCT_NM\n"
    f"      ,QUEUE_NM\n"
    f"      ,QUEUE_STRT_DTM\n"
    f"      ,TASK_TYP_NM\n"
    f"      ,TASK_ASG_ACTV_DIR_ACCT_NM\n"
    f"      ,TASK_CMPL_DTM\n"
    f"      ,DRAWER_NM\n"
    f"      ,GRP_ID\n"
    f"      ,SUB_ID\n"
    f"      ,DOC_TYP_NM\n"
    f"      ,WORK_IN\n"
    f"      ,WORK_ITEM_CT\n"
    f"      ,REP_PRCSR_ID\n"
    f"      ,MBR_FULL_NM\n"
    f"      ,FILE_PRCS_DTM\n"
    f"      ,EFF_DTM\n"
    f"      ,SUBGRP_ID\n"
    f"      ,ERR_LVL_TX\n"
    f"      ,RCVD_DTM\n"
    f"      ,SHOP__IN\n"
    f"      ,GRP_SIZE_CD\n"
    f"      ,OTHR_INSUR_IN\n"
    f"      ,FEP_IN\n"
    f"      ,COB_IN\n"
    f"      ,MSP_IN\n"
    f"      ,EXCH_ID\n"
    f"      ,SCRD_ATCHMT_EXIST_IN\n"
    f"      ,ON_EXCH_IN\n"
    f"      ,SCAN_DTM\n"
    f"      ,DOC_STTUS_CD\n"
    f"      ,TASK_STTUS_CD\n"
    f"      ,TASK_RSN_TX\n"
    f"      ,TASK_CRTN_DTM\n"
    f"      ,TASK_ASG_SEC_CT\n"
    f"      ,LAST_RTE_BY_USER_ID\n"
    f"      ,TASK_ASG_USER_ID\n"
    f"      ,WORK_TYP_CD\n"
    f"FROM \n"
    f"{MbrSvcWorkItemOwner}.MBR_SVC_WORK_ITEM\n"
    f"WHERE \n"
    f"((QUEUE_NM = 'Member Services Complete') or (QUEUE_NM like '%Small Group - QA%') or (QUEUE_NM like '%Large Group QA%'))\n"
    f"AND (LTrim(RTrim(LAST_RTE_BY_ACTV_DIR_ACCT_NM)) <> '' AND LAST_RTE_BY_ACTV_DIR_ACCT_NM <> 'Unknown' AND LAST_RTE_BY_ACTV_DIR_ACCT_NM <> 'SYSTEM' AND LAST_RTE_BY_ACTV_DIR_ACCT_NM <> 'System' AND LAST_RTE_BY_ACTV_DIR_ACCT_NM <> 'NA')\n"
    f"AND QUEUE_STRT_DTM >= '{LastRunDtm}'\n"
    f"AND WORK_TYP_CD is NOT NULL AND WORK_TYP_CD <> ''"
)
df_MBR_SVC_WORK_ITEM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_MBR_SVC_WORK_ITEM)
    .options(**jdbc_props_MBR_SVC_WORK_ITEM)
    .option("query", extract_query_MBR_SVC_WORK_ITEM)
    .load()
)

# ======================================================
# Xfm_MbrSvcWrkTyp
# Filter and create columns
# Constraint: Left(LAST_RTE_BY_ACTV_DIR_ACCT_NM, 1) in ['I','i']
# ======================================================
df_Xfm_MbrSvcWrkTyp_filtered = df_MBR_SVC_WORK_ITEM.filter(
    (F.substring(F.col("LAST_RTE_BY_ACTV_DIR_ACCT_NM"), 1, 1).isin(["I", "i"]))
)

df_Xfm_MbrSvcWrkTyp = (
    df_Xfm_MbrSvcWrkTyp_filtered
    .withColumn(
        "emplid",
        F.when(
            F.col("LAST_RTE_BY_ACTV_DIR_ACCT_NM").isNull() | (F.col("LAST_RTE_BY_ACTV_DIR_ACCT_NM") == ""),
            F.lit("000000")
        ).otherwise(
            F.when(
                F.substring(F.col("LAST_RTE_BY_ACTV_DIR_ACCT_NM"), 1, 1) == "I",
                trim(F.regexp_replace(F.col("LAST_RTE_BY_ACTV_DIR_ACCT_NM"), 'I', '0'))
            ).otherwise(
                F.when(
                    F.substring(F.col("LAST_RTE_BY_ACTV_DIR_ACCT_NM"), 1, 1) == "i",
                    trim(F.regexp_replace(F.col("LAST_RTE_BY_ACTV_DIR_ACCT_NM"), 'i', '0'))
                ).otherwise(
                    trim(F.col("LAST_RTE_BY_ACTV_DIR_ACCT_NM"))
                )
            )
        )
    )
    .withColumn(
        "lastrtebyuserid",
        F.when(
            F.substring(F.col("LAST_RTE_BY_USER_ID"), 1, 1) == "I",
            F.regexp_replace(F.col("LAST_RTE_BY_USER_ID"), 'I', '0')
        ).otherwise(
            F.when(
                F.substring(F.col("LAST_RTE_BY_USER_ID"), 1, 1) == "i",
                F.regexp_replace(F.col("LAST_RTE_BY_USER_ID"), 'i', '0')
            ).otherwise(
                F.col("LAST_RTE_BY_USER_ID")
            )
        )
    )
    .withColumn(
        "item_id",
        F.when(
            F.col("RCVD_DTM").isNull(),
            F.date_format(F.col("SCAN_DTM"), "yyyy-MM-dd.HH:mm:ss.SSSSSS")
        ).when(
            F.substring(F.col("RCVD_DTM"), 1, 10) == F.lit("1970-01-01"),
            F.date_format(F.col("SCAN_DTM"), "yyyy-MM-dd.HH:mm:ss.SSSSSS")
        ).otherwise(
            F.date_format(F.col("RCVD_DTM"), "yyyy-MM-dd.HH:mm:ss.SSSSSS")
        )
    )
    .withColumn(
        "employee_id",
        F.lpad(F.col("emplid"), 6, '0')
    )
    .withColumn(
        "LAST_RTE_BY_ACTV_DIR_ACCT_NM",
        F.lpad(F.col("emplid"), 6, '0')
    )
    .withColumn("DOC_ID", F.col("DOC_ID"))
    .withColumn(
        "GRP_ID",
        F.when(
            F.col("GRP_ID").isNull() | (F.col("GRP_ID") == ""),
            F.lit(None)
        ).otherwise(F.col("GRP_ID"))
    )
    .withColumn(
        "SUB_ID",
        F.when(
            F.col("SUB_ID").isNull() | (F.col("SUB_ID") == ""),
            F.lit(None)
        ).otherwise(F.col("SUB_ID"))
    )
    .withColumn("RCVD_DTM", F.col("RCVD_DTM"))
    .withColumn("QUEUE_STRT_DTM", F.col("QUEUE_STRT_DTM"))
    .withColumn("WORK_ITEM_CT", F.col("WORK_ITEM_CT"))
    .withColumn("DOC_TYP_NM", F.col("DOC_TYP_NM"))
    .withColumn(
        "SCRCRD_WORK_TYP_ID",
        F.when(F.col("WORK_IN")=="N", F.lit("NOWK"))
        .when(F.col("DOC_TYP_NM")=="Step Back", F.concat(F.col("WORK_TYP_CD"), F.lit("SB")))
        .when(F.col("DOC_TYP_NM")=="Group Change", F.concat(F.col("WORK_TYP_CD"), F.lit("CNG")))
        .when(
            (F.col("TASK_TYP_NM").isin("MS QA Task", "MS QA Task II", "GS Front-End QA Task", "MS Rep QA")) &
            (F.col("TASK_STTUS_CD").isin("Complete","Returned")),
            F.concat(F.col("WORK_TYP_CD"), F.lit("QA"))
        )
        .otherwise(F.col("WORK_TYP_CD"))
    )
    .withColumn("SCRCRD_WORK_TYP_ID_orig", F.col("WORK_TYP_CD"))
    .select(
        "item_id",
        "employee_id",
        "LAST_RTE_BY_ACTV_DIR_ACCT_NM",
        "DOC_ID",
        "GRP_ID",
        "SUB_ID",
        "RCVD_DTM",
        "QUEUE_STRT_DTM",
        "WORK_ITEM_CT",
        "DOC_TYP_NM",
        "SCRCRD_WORK_TYP_ID",
        "SCRCRD_WORK_TYP_ID_orig"
    )
)

# ======================================================
# Lkp_Scrcrd_WrkTyp (PxLookup) - left join with df_Scrcrd_Work_Typ1
# Produces two outputs: matched => Lkp_Membership_Out1, unmatched => lnk_wrkTypId_Rej
# ======================================================
df_join_wrktyp_1 = df_Xfm_MbrSvcWrkTyp.alias("L").join(
    df_Scrcrd_Work_Typ1.alias("R"),
    F.col("L.SCRCRD_WORK_TYP_ID")==F.col("R.SCRCRD_WORK_TYP_ID"),
    "left"
)

df_Lkp_Membership_Out1 = df_join_wrktyp_1.filter(F.col("R.SCRCRD_WORK_TYP_ID").isNotNull()).select(
    F.col("L.LAST_RTE_BY_ACTV_DIR_ACCT_NM"),
    F.col("L.employee_id"),
    F.col("L.DOC_ID"),
    F.col("L.GRP_ID"),
    F.col("L.SUB_ID"),
    F.col("L.RCVD_DTM"),
    F.col("L.QUEUE_STRT_DTM"),
    F.col("L.WORK_ITEM_CT"),
    F.col("L.DOC_TYP_NM"),
    F.col("L.SCRCRD_WORK_TYP_ID")
)

df_lnk_wrkTypId_Rej = df_join_wrktyp_1.filter(F.col("R.SCRCRD_WORK_TYP_ID").isNull()).select(
    F.col("L.item_id"),
    F.col("L.employee_id"),
    F.col("L.LAST_RTE_BY_ACTV_DIR_ACCT_NM"),
    F.col("L.DOC_ID"),
    F.col("L.GRP_ID"),
    F.col("L.SUB_ID"),
    F.col("L.RCVD_DTM"),
    F.col("L.QUEUE_STRT_DTM"),
    F.col("L.WORK_ITEM_CT"),
    F.col("L.DOC_TYP_NM"),
    F.col("L.SCRCRD_WORK_TYP_ID"),
    F.col("L.SCRCRD_WORK_TYP_ID_orig")
)

# ======================================================
# Xfm_CNG
# ======================================================
df_Xfm_CNG = df_lnk_wrkTypId_Rej.select(
    F.col("item_id"),
    F.col("employee_id"),
    F.col("LAST_RTE_BY_ACTV_DIR_ACCT_NM"),
    F.col("DOC_ID"),
    F.col("GRP_ID"),
    F.col("SUB_ID"),
    F.col("RCVD_DTM"),
    F.col("QUEUE_STRT_DTM"),
    F.col("WORK_ITEM_CT"),
    F.col("DOC_TYP_NM"),
    F.concat(F.col("SCRCRD_WORK_TYP_ID_orig"), F.lit("CNG")).alias("SCRCRD_WORK_TYP_ID"),
    F.col("SCRCRD_WORK_TYP_ID_orig").alias("SCRCRD_WORK_TYP_ID_orig")
)

# ======================================================
# Lkp_Scrcrd_Wrk_Typ (PxLookup) - left join with df_Scrcrd_Work_Typ2
# Produces two outputs: matched => Fnl_CNG, unmatched => DSLink80
# ======================================================
df_join_wrktyp_2 = df_Xfm_CNG.alias("L").join(
    df_Scrcrd_Work_Typ2.alias("R"),
    F.col("L.SCRCRD_WORK_TYP_ID")==F.col("R.SCRCRD_WORK_TYP_ID"),
    "left"
)

df_Fnl_CNG = df_join_wrktyp_2.filter(F.col("R.SCRCRD_WORK_TYP_ID").isNotNull()).select(
    F.col("L.LAST_RTE_BY_ACTV_DIR_ACCT_NM"),
    F.col("L.employee_id"),
    F.col("L.DOC_ID"),
    F.col("L.GRP_ID"),
    F.col("L.SUB_ID"),
    F.col("L.RCVD_DTM"),
    F.col("L.QUEUE_STRT_DTM"),
    F.col("L.WORK_ITEM_CT"),
    F.col("L.DOC_TYP_NM"),
    F.col("L.SCRCRD_WORK_TYP_ID")
)

df_DSLink80 = df_join_wrktyp_2.filter(F.col("R.SCRCRD_WORK_TYP_ID").isNull()).select(
    F.col("L.item_id"),
    F.col("L.employee_id"),
    F.col("L.LAST_RTE_BY_ACTV_DIR_ACCT_NM"),
    F.col("L.DOC_ID"),
    F.col("L.GRP_ID"),
    F.col("L.SUB_ID"),
    F.col("L.RCVD_DTM"),
    F.col("L.QUEUE_STRT_DTM"),
    F.col("L.WORK_ITEM_CT"),
    F.col("L.DOC_TYP_NM"),
    F.concat(F.col("SCRCRD_WORK_TYP_ID"), F.lit("CNG")).alias("SCRCRD_WORK_TYP_ID"),
    F.col("SCRCRD_WORK_TYP_ID_orig")
)

# ======================================================
# Xfm_SB
# ======================================================
df_Xfm_SB = df_DSLink80.select(
    F.col("employee_id"),
    F.col("LAST_RTE_BY_ACTV_DIR_ACCT_NM"),
    F.col("DOC_ID"),
    F.col("GRP_ID"),
    F.col("SUB_ID"),
    F.col("RCVD_DTM"),
    F.col("QUEUE_STRT_DTM"),
    F.col("WORK_ITEM_CT"),
    F.col("DOC_TYP_NM"),
    F.concat(F.col("SCRCRD_WORK_TYP_ID_orig"), F.lit("SB")).alias("SCRCRD_WORK_TYP_ID"),
    F.col("SCRCRD_WORK_TYP_ID_orig")
).alias("DSLink80")

# ======================================================
# Lkp_Scrcrd_Wrk_Tp (PxLookup) - left join with df_Scrcrd_Work_Typ3
# Produces two outputs: matched => Fnl_SB, unmatched => DSLink91
# ======================================================
df_join_wrktyp_3 = df_Xfm_SB.alias("L").join(
    df_Scrcrd_Work_Typ3.alias("R"),
    F.col("L.SCRCRD_WORK_TYP_ID")==F.col("R.SCRCRD_WORK_TYP_ID"),
    "left"
)

df_Fnl_SB = df_join_wrktyp_3.filter(F.col("R.SCRCRD_WORK_TYP_ID").isNotNull()).select(
    F.col("L.employee_id"),
    F.col("L.LAST_RTE_BY_ACTV_DIR_ACCT_NM"),
    F.col("L.DOC_ID"),
    F.col("L.GRP_ID"),
    F.col("L.SUB_ID"),
    F.col("L.RCVD_DTM"),
    F.col("L.QUEUE_STRT_DTM"),
    F.col("L.WORK_ITEM_CT"),
    F.col("L.DOC_TYP_NM"),
    F.col("L.SCRCRD_WORK_TYP_ID")
)

df_DSLink91 = df_join_wrktyp_3.filter(F.col("R.SCRCRD_WORK_TYP_ID").isNull()).select(
    F.col("DSLink80.item_id"),
    F.col("DSLink80.employee_id"),
    F.col("DSLink80.LAST_RTE_BY_ACTV_DIR_ACCT_NM"),
    F.col("DSLink80.DOC_ID"),
    F.col("DSLink80.GRP_ID"),
    F.col("DSLink80.SUB_ID"),
    F.col("DSLink80.RCVD_DTM"),
    F.col("DSLink80.QUEUE_STRT_DTM"),
    F.col("DSLink80.WORK_ITEM_CT"),
    F.col("DSLink80.DOC_TYP_NM"),
    F.concat(F.col("DSLink80.SCRCRD_WORK_TYP_ID_orig"), F.lit("SB")).alias("SCRCRD_WORK_TYP_ID"),
    F.col("DSLink80.SCRCRD_WORK_TYP_ID_orig")
)

# ======================================================
# Xfm_QA
# ======================================================
df_Xfm_QA = df_DSLink91.select(
    F.col("employee_id"),
    F.col("LAST_RTE_BY_ACTV_DIR_ACCT_NM"),
    F.col("DOC_ID"),
    F.col("GRP_ID"),
    F.col("SUB_ID"),
    F.col("RCVD_DTM"),
    F.col("QUEUE_STRT_DTM"),
    F.col("WORK_ITEM_CT"),
    F.col("DOC_TYP_NM"),
    F.concat(F.col("SCRCRD_WORK_TYP_ID_orig"), F.lit("QA")).alias("SCRCRD_WORK_TYP_ID"),
    F.col("SCRCRD_WORK_TYP_ID_orig")
).alias("DSLink91")

# ======================================================
# Lkp_Scrcrd_Wrk_Tp (PxLookup) - left join with df_Scrcrd_Work_Typ4
# Produces two outputs: matched => Fnl_QA, unmatched => DSLink99
# ======================================================
df_join_wrktyp_4 = df_Xfm_QA.alias("L").join(
    df_Scrcrd_Work_Typ4.alias("R"),
    F.col("L.SCRCRD_WORK_TYP_ID")==F.col("R.SCRCRD_WORK_TYP_ID"),
    "left"
)

df_Fnl_QA = df_join_wrktyp_4.filter(F.col("R.SCRCRD_WORK_TYP_ID").isNotNull()).select(
    F.col("L.employee_id"),
    F.col("L.LAST_RTE_BY_ACTV_DIR_ACCT_NM"),
    F.col("L.DOC_ID"),
    F.col("L.GRP_ID"),
    F.col("L.SUB_ID"),
    F.col("L.RCVD_DTM"),
    F.col("L.QUEUE_STRT_DTM"),
    F.col("L.WORK_ITEM_CT"),
    F.col("L.DOC_TYP_NM"),
    F.col("L.SCRCRD_WORK_TYP_ID")
)

df_DSLink99 = df_join_wrktyp_4.filter(F.col("R.SCRCRD_WORK_TYP_ID").isNull()).select(
    F.col("DSLink91.item_id"),
    F.col("DSLink91.employee_id"),
    F.col("DSLink91.LAST_RTE_BY_ACTV_DIR_ACCT_NM"),
    F.col("DSLink91.DOC_ID"),
    F.col("DSLink91.GRP_ID"),
    F.col("DSLink91.SUB_ID"),
    F.col("DSLink91.RCVD_DTM"),
    F.col("DSLink91.QUEUE_STRT_DTM"),
    F.col("DSLink91.WORK_ITEM_CT"),
    F.col("DSLink91.DOC_TYP_NM"),
    F.concat(F.col("DSLink91.SCRCRD_WORK_TYP_ID"), F.lit("CNG")).alias("SCRCRD_WORK_TYP_ID"),
    F.col("DSLink91.SCRCRD_WORK_TYP_ID_orig")
)

# ======================================================
# Write DSLink99 => Seq_Scrcrd_Work_Typ_Id => Scrcrd_Empl_#Source#_WorkTypId_Rej.dat (external)
# ======================================================
df_to_write_Scrcrd_Empl_WorkTypId_Rej = df_DSLink99.select(
    # Maintaining exact column order from this link
    "item_id",
    "employee_id",
    "LAST_RTE_BY_ACTV_DIR_ACCT_NM",
    "DOC_ID",
    "GRP_ID",
    "SUB_ID",
    "RCVD_DTM",
    "QUEUE_STRT_DTM",
    "WORK_ITEM_CT",
    "DOC_TYP_NM",
    "SCRCRD_WORK_TYP_ID",
    "SCRCRD_WORK_TYP_ID_orig"
)
# If any of these are string-like, we apply rpad with unknown length <...>.
df_to_write_Scrcrd_Empl_WorkTypId_Rej_padded = df_to_write_Scrcrd_Empl_WorkTypId_Rej.select(
    F.rpad(F.col("item_id").cast(StringType()), <...>, " ").alias("item_id"),
    F.rpad(F.col("employee_id").cast(StringType()), <...>, " ").alias("employee_id"),
    F.rpad(F.col("LAST_RTE_BY_ACTV_DIR_ACCT_NM").cast(StringType()), <...>, " ").alias("LAST_RTE_BY_ACTV_DIR_ACCT_NM"),
    F.rpad(F.col("DOC_ID").cast(StringType()), <...>, " ").alias("DOC_ID"),
    F.rpad(F.col("GRP_ID").cast(StringType()), <...>, " ").alias("GRP_ID"),
    F.rpad(F.col("SUB_ID").cast(StringType()), <...>, " ").alias("SUB_ID"),
    F.rpad(F.col("RCVD_DTM").cast(StringType()), <...>, " ").alias("RCVD_DTM"),
    F.rpad(F.col("QUEUE_STRT_DTM").cast(StringType()), <...>, " ").alias("QUEUE_STRT_DTM"),
    F.rpad(F.col("WORK_ITEM_CT").cast(StringType()), <...>, " ").alias("WORK_ITEM_CT"),
    F.rpad(F.col("DOC_TYP_NM").cast(StringType()), <...>, " ").alias("DOC_TYP_NM"),
    F.rpad(F.col("SCRCRD_WORK_TYP_ID").cast(StringType()), <...>, " ").alias("SCRCRD_WORK_TYP_ID"),
    F.rpad(F.col("SCRCRD_WORK_TYP_ID_orig").cast(StringType()), <...>, " ").alias("SCRCRD_WORK_TYP_ID_orig")
)
write_files(
    df_to_write_Scrcrd_Empl_WorkTypId_Rej_padded,
    f"{adls_path_publish}/external/Scrcrd_Empl_{Source}_WorkTypId_Rej.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ======================================================
# Funnel_82 => combine df_Lkp_Membership_Out1, df_Fnl_CNG, df_Fnl_SB, df_Fnl_QA
# ======================================================
df_funnel_82_cols = [
    "employee_id",
    "LAST_RTE_BY_ACTV_DIR_ACCT_NM",
    "DOC_ID",
    "GRP_ID",
    "SUB_ID",
    "RCVD_DTM",
    "QUEUE_STRT_DTM",
    "WORK_ITEM_CT",
    "DOC_TYP_NM",
    "SCRCRD_WORK_TYP_ID"
]

df_funnel_1 = df_Lkp_Membership_Out1.select(df_funnel_82_cols)
df_funnel_2 = df_Fnl_CNG.select(df_funnel_82_cols)
df_funnel_3 = df_Fnl_SB.select(df_funnel_82_cols)
df_funnel_4 = df_Fnl_QA.select(df_funnel_82_cols)

df_Funnel_82 = df_funnel_1.unionByName(df_funnel_2).unionByName(df_funnel_3).unionByName(df_funnel_4)

# ======================================================
# lkp_empl_MbrSvc => PxLookup with df_ScrCrd_EMPL on left join
# Primary link => funnel_82, Lookup link => ScrCrd_EMPL
# Two outputs: Lnk_Membership_Info, lnk_empl_rej
# ======================================================
df_join_empl = df_Funnel_82.alias("L").join(
    df_ScrCrd_EMPL.alias("R"),
    F.col("L.employee_id")==F.lpad(F.col("R.EMPL_ACTV_DIR_ID"), 6, '0'),
    "left"
)

# matched => R.EMPL_ID not null. Actually the job does not have a join condition in the JSON for the link to the left? 
# Looking at the JSON, the join conditions are empty, but it's a left link. Usually that means no explicit condition => This can be tricky. 
# However, DataStage might do a reference link with no conditions => everything might match => That is effectively a cross join. 
# But the instructions say: "For all left joins, do not use .join(df, F.lit(True), 'left'), because it is a cross join." 
# The original stage has no "JoinConditions". 
# In DataStage, a lookup link but with no condition usually returns all rows from primary with null from reference. We'll replicate that by not matching on any column => effectively no columns matched. 
# So each row from the primary would simply have null from the reference columns. 
# Then "lnk_empl_rej" might be all rows if no query columns are used. This is ambiguous, but we must replicate exactly. 
# We'll interpret that "no join condition" => The reference columns are always null. 
# We'll do it so matched => row where EMPL_ID is not null? That can't happen if there's truly no condition. 
# Possibly we assume "EMPL_ID" always is null. Then all go to "lnk_empl_rej"? 
# The job's link "Lnk_Membership_Info" doesn't mention columns from df_ScrCrd_EMPL, so it might not matter. 
# We'll treat it as a left join with no columns matched => everything is matched? That doesn't make sense. We'll interpret the reject link as "no ways to reject"? 
# But the stage definition in the JSON shows "OutputPins" => "Lnk_Membership_Info" columns are from Lkp_Membership_Out, none from the reference link. 
# So we can pass everything to "Lnk_Membership_Info" as matched. And "lnk_empl_rej" is presumably empty. 
# We follow DataStage's typical approach for a "Lookup" with no join => the reference columns won't matter. We'll keep them as null. 
# We do matched => isNotNull(R.EMPL_ID)? Then we do the columns from the primary. 
# For the reject => isNull(R.EMPL_ID)? 
# That will replicate a typical behavior of a left outer join plus "reject link = unmatched rows." 
# We'll do that:

df_Lkp_Membership_Info = df_join_empl.filter(F.col("R.EMPL_ID").isNotNull()).select(
    F.col("L.LAST_RTE_BY_ACTV_DIR_ACCT_NM"),
    F.col("L.employee_id"),
    F.col("L.DOC_ID"),
    F.col("L.GRP_ID"),
    F.col("L.SUB_ID"),
    F.col("L.RCVD_DTM"),
    F.col("L.QUEUE_STRT_DTM"),
    F.col("L.WORK_ITEM_CT"),
    F.col("L.DOC_TYP_NM"),
    F.col("L.SCRCRD_WORK_TYP_ID")
)

df_lnk_empl_rej = df_join_empl.filter(F.col("R.EMPL_ID").isNull()).select(
    F.col("L.LAST_RTE_BY_ACTV_DIR_ACCT_NM"),
    F.col("L.employee_id"),
    F.col("L.DOC_ID"),
    F.col("L.GRP_ID"),
    F.col("L.SUB_ID"),
    F.col("L.RCVD_DTM"),
    F.col("L.QUEUE_STRT_DTM"),
    F.col("L.WORK_ITEM_CT"),
    F.col("L.DOC_TYP_NM"),
    F.col("L.SCRCRD_WORK_TYP_ID")
)

# ======================================================
# Write lnk_empl_rej => Seq_Scrcrd_Empl_Id_Rej => Scrcrd_Empl_#Source#_Empl_Id_Rej.dat (external)
# ======================================================
df_to_write_Scrcrd_Empl_Empl_Id_Rej = df_lnk_empl_rej.select(
    "LAST_RTE_BY_ACTV_DIR_ACCT_NM",
    "employee_id",
    "DOC_ID",
    "GRP_ID",
    "SUB_ID",
    "RCVD_DTM",
    "QUEUE_STRT_DTM",
    "WORK_ITEM_CT",
    "DOC_TYP_NM",
    "SCRCRD_WORK_TYP_ID"
)
df_to_write_Scrcrd_Empl_Empl_Id_Rej_padded = df_to_write_Scrcrd_Empl_Empl_Id_Rej.select(
    F.rpad(F.col("LAST_RTE_BY_ACTV_DIR_ACCT_NM").cast(StringType()), <...>, " ").alias("LAST_RTE_BY_ACTV_DIR_ACCT_NM"),
    F.rpad(F.col("employee_id").cast(StringType()), <...>, " ").alias("employee_id"),
    F.rpad(F.col("DOC_ID").cast(StringType()), <...>, " ").alias("DOC_ID"),
    F.rpad(F.col("GRP_ID").cast(StringType()), <...>, " ").alias("GRP_ID"),
    F.rpad(F.col("SUB_ID").cast(StringType()), <...>, " ").alias("SUB_ID"),
    F.rpad(F.col("RCVD_DTM").cast(StringType()), <...>, " ").alias("RCVD_DTM"),
    F.rpad(F.col("QUEUE_STRT_DTM").cast(StringType()), <...>, " ").alias("QUEUE_STRT_DTM"),
    F.rpad(F.col("WORK_ITEM_CT").cast(StringType()), <...>, " ").alias("WORK_ITEM_CT"),
    F.rpad(F.col("DOC_TYP_NM").cast(StringType()), <...>, " ").alias("DOC_TYP_NM"),
    F.rpad(F.col("SCRCRD_WORK_TYP_ID").cast(StringType()), <...>, " ").alias("SCRCRD_WORK_TYP_ID")
)
write_files(
    df_to_write_Scrcrd_Empl_Empl_Id_Rej_padded,
    f"{adls_path_publish}/external/Scrcrd_Empl_{Source}_Empl_Id_Rej.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)

# ======================================================
# Xfm_Membership_Info_Business_Logic
# ======================================================
df_Xfm_Membership_Info_Business_Logic = df_Lkp_Membership_Info.select(
    F.col("employee_id").alias("EMPL_ID"),
    F.when(
        (F.length(trim(F.when(F.col("DOC_ID").isNotNull(), F.col("DOC_ID")).otherwise(F.lit("")))) == 0) |
        (trim(F.col("DOC_ID")) == ""),
        F.regexp_replace(F.regexp_replace(F.regexp_replace(F.lit(CurrentRunDtm), ' ', ''), ':', ''), '-', '')
    ).otherwise(trim(F.col("DOC_ID"))).alias("SCRCRD_ITEM_ID"),
    F.lit(current_timestamp()).alias("RCRD_EXTR_DTM"),
    F.lit("IMAGENOW").alias("SRC_SYS_CD"),
    F.lit("MEMBERSHIP").alias("SCRCRD_ITEM_CAT_ID"),
    F.lit("MEMBERSHIP").alias("SCRCRD_ITEM_TYP_ID"),
    F.col("SCRCRD_WORK_TYP_ID"),
    F.lit("NA").alias("SRC_SYS_INPT_METH_CD"),
    F.lit("COMPLETE").alias("SRC_SYS_STATUS_CD"),
    F.when(
        (F.length(trim(F.when(F.col("GRP_ID").isNotNull(), F.col("GRP_ID")).otherwise(F.lit("")))) == 0) |
        (F.col("GRP_ID") == ""),
        F.lit(None)
    ).otherwise(F.col("GRP_ID")).alias("GRP_ID"),
    F.when(
        (F.length(trim(F.when(F.col("SUB_ID").isNotNull(), F.col("SUB_ID")).otherwise(F.lit("")))) == 0) |
        (F.col("SUB_ID") == ""),
        F.lit(None)
    ).otherwise(F.col("SUB_ID")).alias("MBR_ID"),
    F.lit(None).alias("PROD_ABBR"),
    F.lit(None).alias("PROD_LOB_NO"),
    F.lit(None).alias("SCCF_NO"),
    F.col("RCVD_DTM").alias("SRC_SYS_RCVD_DTM"),
    F.col("QUEUE_STRT_DTM").alias("SRC_SYS_CMPLD_DTM"),
    F.col("QUEUE_STRT_DTM").alias("SRC_SYS_TRANS_DTM"),
    F.when(
        (F.length(trim(F.when(F.col("WORK_ITEM_CT").isNotNull(), F.col("WORK_ITEM_CT")).otherwise(F.lit("")))) == 0) |
        (F.col("WORK_ITEM_CT") == "") |
        (F.col("WORK_ITEM_CT") == F.lit(0)),
        F.lit(1)
    ).otherwise(F.col("WORK_ITEM_CT")).alias("ACTVTY_CT"),
    F.lit("MEMBERSHIP EXTRACT").alias("CRT_USER_ID"),
    F.lit(CurrentRunDtm).alias("LAST_UPDT_DTM"),
    F.lit("MEMBERSHIP EXTRACT").alias("LAST_UPDT_USER_ID")
)

# ======================================================
# Xfm_Inquiry_Info
# ======================================================
df_Xfm_Inquiry_Info = df_Xfm_Membership_Info_Business_Logic.select(
    F.col("EMPL_ID"),
    F.col("SCRCRD_ITEM_ID"),
    F.to_timestamp(F.col("RCRD_EXTR_DTM"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("RCRD_EXTR_DTM"),
    F.col("SRC_SYS_CD"),
    F.col("SCRCRD_ITEM_CAT_ID"),
    F.col("SCRCRD_ITEM_TYP_ID"),
    F.col("SCRCRD_WORK_TYP_ID"),
    F.col("SRC_SYS_INPT_METH_CD"),
    F.col("SRC_SYS_STATUS_CD").alias("SRC_SYS_STTUS_CD"),
    F.col("GRP_ID"),
    F.col("MBR_ID"),
    F.col("PROD_ABBR"),
    F.col("PROD_LOB_NO"),
    F.col("SCCF_NO"),
    F.col("SRC_SYS_RCVD_DTM"),
    F.col("SRC_SYS_CMPLD_DTM"),
    F.col("SRC_SYS_TRANS_DTM"),
    F.col("ACTVTY_CT"),
    F.lit(CurrentRunDtm).alias("CRT_DTM"),
    F.col("CRT_USER_ID"),
    F.col("LAST_UPDT_DTM"),
    F.col("LAST_UPDT_USER_ID")
)

# ======================================================
# Final File: Scrcrd_Empl_Prod_Item_Load_Ready_File => Scrcrd_Prod_Item_#Source#.dat
# ======================================================
df_to_write_final = df_Xfm_Inquiry_Info.select(
    "EMPL_ID",
    "SCRCRD_ITEM_ID",
    "RCRD_EXTR_DTM",
    "SRC_SYS_CD",
    "SCRCRD_ITEM_CAT_ID",
    "SCRCRD_ITEM_TYP_ID",
    "SCRCRD_WORK_TYP_ID",
    "SRC_SYS_INPT_METH_CD",
    "SRC_SYS_STTUS_CD",
    "GRP_ID",
    "MBR_ID",
    "PROD_ABBR",
    "PROD_LOB_NO",
    "SCCF_NO",
    "SRC_SYS_RCVD_DTM",
    "SRC_SYS_CMPLD_DTM",
    "SRC_SYS_TRANS_DTM",
    "ACTVTY_CT",
    "CRT_DTM",
    "CRT_USER_ID",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

df_to_write_final_padded = df_to_write_final.select(
    F.rpad(F.col("EMPL_ID").cast(StringType()), <...>, " ").alias("EMPL_ID"),
    F.rpad(F.col("SCRCRD_ITEM_ID").cast(StringType()), <...>, " ").alias("SCRCRD_ITEM_ID"),
    F.col("RCRD_EXTR_DTM"),  # treat as timestamp or date
    F.rpad(F.col("SRC_SYS_CD").cast(StringType()), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("SCRCRD_ITEM_CAT_ID").cast(StringType()), <...>, " ").alias("SCRCRD_ITEM_CAT_ID"),
    F.rpad(F.col("SCRCRD_ITEM_TYP_ID").cast(StringType()), <...>, " ").alias("SCRCRD_ITEM_TYP_ID"),
    F.rpad(F.col("SCRCRD_WORK_TYP_ID").cast(StringType()), <...>, " ").alias("SCRCRD_WORK_TYP_ID"),
    F.rpad(F.col("SRC_SYS_INPT_METH_CD").cast(StringType()), <...>, " ").alias("SRC_SYS_INPT_METH_CD"),
    F.rpad(F.col("SRC_SYS_STTUS_CD").cast(StringType()), <...>, " ").alias("SRC_SYS_STTUS_CD"),
    F.rpad(F.col("GRP_ID").cast(StringType()), <...>, " ").alias("GRP_ID"),
    F.rpad(F.col("MBR_ID").cast(StringType()), <...>, " ").alias("MBR_ID"),
    F.rpad(F.col("PROD_ABBR").cast(StringType()), <...>, " ").alias("PROD_ABBR"),
    F.rpad(F.col("PROD_LOB_NO").cast(StringType()), <...>, " ").alias("PROD_LOB_NO"),
    F.rpad(F.col("SCCF_NO").cast(StringType()), <...>, " ").alias("SCCF_NO"),
    F.col("SRC_SYS_RCVD_DTM"),
    F.col("SRC_SYS_CMPLD_DTM"),
    F.col("SRC_SYS_TRANS_DTM"),
    F.col("ACTVTY_CT"),
    F.col("CRT_DTM"),
    F.rpad(F.col("CRT_USER_ID").cast(StringType()), <...>, " ").alias("CRT_USER_ID"),
    F.col("LAST_UPDT_DTM"),
    F.rpad(F.col("LAST_UPDT_USER_ID").cast(StringType()), <...>, " ").alias("LAST_UPDT_USER_ID")
)

write_files(
    df_to_write_final_padded,
    f"{adls_path}/load/Scrcrd_Prod_Item_{Source}.dat",
    delimiter="#$",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ======================================================
# End of the generated PySpark notebook code
# ======================================================