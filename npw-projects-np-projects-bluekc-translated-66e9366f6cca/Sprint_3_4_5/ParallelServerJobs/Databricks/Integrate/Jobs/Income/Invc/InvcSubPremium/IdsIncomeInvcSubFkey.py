# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 01/14/09 10:18:26 Batch  14990_37109 PROMOTE bckcetl ids20 dsadm bls for sg
# MAGIC ^1_3 01/14/09 10:10:44 Batch  14990_36646 INIT bckcett testIDS dsadm BLS FOR SG
# MAGIC ^1_1 12/16/08 22:11:05 Batch  14961_79874 PROMOTE bckcett testIDS u03651 steph - income primary key conversion
# MAGIC ^1_1 12/16/08 22:03:32 Batch  14961_79415 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 11/12/07 10:00:11 Batch  14561_36015 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 11/02/07 13:07:35 Batch  14551_47275 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 11/02/07 12:52:45 Batch  14551_46368 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/31/07 13:17:35 Batch  14549_47857 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/31/07 13:09:37 Batch  14549_47387 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 06/07/07 15:08:56 Batch  14403_54540 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 06/07/07 15:06:33 Batch  14403_54395 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 04/09/07 08:24:30 Batch  14344_30274 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/28/07 06:42:38 Batch  14332_24162 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 03/05/07 16:34:28 Batch  14309_59670 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 02/11/07 12:33:13 Batch  14287_45195 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_4 12/18/06 12:24:28 Batch  14232_44709 INIT bckcetl ids20 dsadm Income Backup for 12/18/2006 install
# MAGIC ^1_1 11/03/06 13:03:29 Batch  14187_47011 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_3 05/22/06 11:41:30 Batch  14022_42099 PROMOTE bckcetl ids20 dsadm Jim Hart
# MAGIC ^1_3 05/22/06 11:21:03 Batch  14022_40870 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_2 05/22/06 11:13:00 Batch  14022_40388 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_1 05/22/06 10:32:59 Batch  14022_37985 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_5 05/12/06 15:17:06 Batch  14012_55029 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_5 05/12/06 15:03:19 Batch  14012_54207 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 16:20:33 Batch  14011_58841 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 05/10/06 16:34:22 Batch  14010_59666 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 05/10/06 16:00:21 Batch  14010_57626 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 05/02/06 14:21:23 Batch  14002_51686 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 05/01/06 11:14:49 Batch  14001_45485 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 05/01/06 11:11:02 Batch  14001_40263 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_3 02/17/06 15:08:39 Batch  13928_54530 PROMOTE bckcetl ids20 dsadm J.Mahaffey
# MAGIC ^1_3 02/17/06 15:06:27 Batch  13928_54398 INIT bckcett testIDS30 dsadm J.Mahaffey
# MAGIC ^1_4 02/17/06 15:00:01 Batch  13928_54002 INIT bckcett testIDS30 u10157 sa
# MAGIC ^1_3 02/17/06 14:56:58 Batch  13928_53825 INIT bckcett testIDS30 dsadm J. Mahaffey
# MAGIC ^1_1 02/16/06 16:01:26 Batch  13927_57688 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_1 02/16/06 15:58:47 Batch  13927_57529 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_1 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsInvcSubFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.   
# MAGIC       
# MAGIC 
# MAGIC INPUTS:   File created by IdsInvcSubExtr
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  Fkey lookups
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:   Finds foreign keys for surrogate key fields
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Ralph Tucker   11/8/2005  -   Originally Programmed
# MAGIC            Sharon Andrew  02/16/2006    Changed the default values used for INVC_SUB_COV_END_DT_SK from 0/1 to "UNK" / "NA"
# MAGIC            SAndrew           -  05/03/2006-   Task Tracker 4531 - USAble Reporting Support.   Added extract of field BLSB_VOL to populate the  INVC_SUB VOL_COV_AMT field.             
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari     2008-09-30                   Added SrcSysCdSk parameter                                                          3567                 devlIDS                              Steph Goddard          10/03/2008
# MAGIC 
# MAGIC Goutham Kalidindi             2021-03-24            Changed Datatype length for field                                           358186                    IntegrateDev1                Reddy Sanam            04/01/2021
# MAGIC                                                                                              BLIV_ID
# MAGIC                                                                                                char(12) to Varchar(15)

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
)
from pyspark.sql.functions import col, lit, when, row_number, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/GetFkeyProd
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/GetFkeyCodes
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/GetFkeyDate
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/GetFkeyCls
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/GetFkeyClsPln
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/GetFkeyGrp
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/GetFkeyInvc
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/GetFkeySub
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/GetFkeySubgrp
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/GetFkeyErrorCnt
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/GetRecycleKey
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/FORMAT
# COMMAND ----------

InFile = get_widget_value("InFile", "INVC_SUB.Tmp")
Logging = get_widget_value("Logging", "Y")
OutFile = get_widget_value("OutFile", "INVC_SUB.dat")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "105838")

schema_idsinvcsub = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("INVC_SK", IntegerType(), False),
    StructField("BILL_INVC_ID", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("PROD_BILL_CMPNT_ID", StringType(), False),
    StructField("COV_DUE_DT", StringType(), False),
    StructField("COV_STRT_DT_SK", StringType(), False),
    StructField("COV_END_DT_SK", StringType(), False),
    StructField("INVC_SUB_PRM_TYP_CD", StringType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("CRT_DT", TimestampType(), False),
    StructField("INVC_SUB_BILL_DISP_CD", StringType(), False),
    StructField("CLS_ID", StringType(), False),
    StructField("CSPI_ID", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("INVC_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("INVC_SUB_FMLY_CNTR_CD", StringType(), False),
    StructField("DPNDT_PRM_AMT", DecimalType(38,10), False),
    StructField("SUB_PRM_AMT", DecimalType(38,10), False),
    StructField("DPNDT_CT", IntegerType(), False),
    StructField("SUB_CT", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("VOL_COV_AMT", DecimalType(38,10), False)
])

df_idsinvcsub = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_idsinvcsub)
    .load(f"{adls_path}/key/{InFile}")
)

df_foreignkey = (
    df_idsinvcsub
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svProd", GetFkeyProd(col("SRC_SYS_CD"), col("INVC_SK"), col("PROD_ID"), Logging))
    .withColumn("svInvcSubPrmTypCd", GetFkeyCodes(col("SRC_SYS_CD"), col("INVC_SK"), lit("INVOICE SUBSCRIBER PREMIUM TYPE"), col("INVC_SUB_PRM_TYP_CD"), Logging))
    .withColumn("svCovDueDt", GetFkeyDate(lit("IDS"), col("INVC_SK"), col("COV_DUE_DT"), Logging))
    .withColumn("svCovStrtDt", GetFkeyDate(lit("IDS"), col("INVC_SK"), col("COV_STRT_DT_SK"), Logging))
    .withColumn("svCovEndDt", GetFkeyDate(lit("IDS"), col("INVC_SK"), col("COV_END_DT_SK"), Logging))
    .withColumn("svCls", GetFkeyCls(col("SRC_SYS_CD"), col("INVC_SK"), col("GRP_ID"), col("CLS_ID"), Logging))
    .withColumn("svClsPln", GetFkeyClsPln(col("SRC_SYS_CD"), col("INVC_SK"), col("CLS_PLN_ID"), Logging))
    .withColumn("svGrp", GetFkeyGrp(col("SRC_SYS_CD"), col("INVC_SK"), col("GRP_ID"), Logging))
    .withColumn("svInvc", GetFkeyInvc(col("SRC_SYS_CD"), col("INVC_SK"), col("INVC_ID"), Logging))
    .withColumn("svSub", GetFkeySub(col("SRC_SYS_CD"), col("INVC_SK"), col("SUB_UNIQ_KEY"), Logging))
    .withColumn("svSubGrp", GetFkeySubgrp(col("SRC_SYS_CD"), col("INVC_SK"), col("GRP_ID"), col("SUBGRP_ID"), Logging))
    .withColumn("svInvcSubFmlyCntrCd", GetFkeyCodes(col("SRC_SYS_CD"), col("INVC_SK"), lit("SUBSCRIBER FAMILY INDICATOR"), col("INVC_SUB_FMLY_CNTR_CD"), Logging))
    .withColumn("svInvcSubBillDispCd", GetFkeyCodes(col("SRC_SYS_CD"), col("INVC_SK"), lit("BILLING DISPOSITION"), col("INVC_SUB_BILL_DISP_CD"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("INVC_SK")))
)

df_fkey = (
    df_foreignkey
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .select(
        col("INVC_SK").alias("INVC_SUB_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("BILL_INVC_ID").alias("BILL_INVC_ID"),
        col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        col("PROD_ID").alias("PROD_ID"),
        col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
        col("svCovDueDt").alias("COV_DUE_DT_SK"),
        col("svCovStrtDt").alias("COV_STRT_DT_SK"),
        col("svInvcSubPrmTypCd").alias("INVC_SUB_PRM_TYP_CD_SK"),
        FORMAT.DATE(col("CRT_DT"), lit("SYBASE"), lit("TIMESTAMP"), lit("DB2TIMESTAMP")).alias("CRT_TS"),
        col("svInvcSubBillDispCd").alias("INVC_SUB_BILL_DISP_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svCls").alias("CLS_SK"),
        col("svClsPln").alias("CLS_PLN_SK"),
        col("svGrp").alias("GRP_SK"),
        col("svInvc").alias("INVC_SK"),
        col("svProd").alias("PROD_SK"),
        col("svSubGrp").alias("SUBGRP_SK"),
        col("svSub").alias("SUB_SK"),
        col("svInvcSubFmlyCntrCd").alias("INVC_SUB_FMLY_CNTR_CD_SK"),
        col("svCovEndDt").alias("COV_END_DT_SK"),
        col("DPNDT_PRM_AMT").alias("DPNDT_PRM_AMT"),
        col("SUB_PRM_AMT").alias("SUB_PRM_AMT"),
        col("DPNDT_CT").alias("DPNDT_CT"),
        col("SUB_CT").alias("SUB_CT"),
        col("VOL_COV_AMT").alias("VOL_COV_AMT")
    )
)

w = Window.orderBy(lit(1))
df_temp_for_default = df_foreignkey.withColumn("_row_num", row_number().over(w))

df_defaultunk_single = df_temp_for_default.filter(col("_row_num") == 1)
df_defaultunk = df_defaultunk_single.select(
    lit(0).alias("INVC_SUB_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("BILL_INVC_ID"),
    lit(0).alias("SUB_UNIQ_KEY"),
    lit("UNK").alias("CLS_PLN_ID"),
    lit("UNK").alias("PROD_ID"),
    lit("UNK").alias("PROD_BILL_CMPNT_ID"),
    lit("UNK").alias("COV_DUE_DT_SK"),
    lit("UNK").alias("COV_STRT_DT_SK"),
    lit(0).alias("INVC_SUB_PRM_TYP_CD_SK"),
    FORMAT.DATE(lit("1753-01-01"), lit("DATE"), lit("DATE"), lit("DB2TIMESTAMP")).alias("CRT_TS"),
    lit(0).alias("INVC_SUB_BILL_DISP_CD_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLS_SK"),
    lit(0).alias("CLS_PLN_SK"),
    lit(0).alias("GRP_SK"),
    lit(0).alias("INVC_SK"),
    lit(0).alias("PROD_SK"),
    lit(0).alias("SUBGRP_SK"),
    lit(0).alias("SUB_SK"),
    lit(0).alias("INVC_SUB_FMLY_CNTR_CD_SK"),
    lit("UNK").alias("COV_END_DT_SK"),
    lit(0).alias("DPNDT_PRM_AMT"),
    lit(0).alias("SUB_PRM_AMT"),
    lit(0).alias("DPNDT_CT"),
    lit(0).alias("SUB_CT"),
    lit(0).alias("VOL_COV_AMT")
)

df_defaultna_single = df_temp_for_default.filter(col("_row_num") == 1)
df_defaultna = df_defaultna_single.select(
    lit(1).alias("INVC_SUB_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("BILL_INVC_ID"),
    lit(1).alias("SUB_UNIQ_KEY"),
    lit("NA").alias("CLS_PLN_ID"),
    lit("NA").alias("PROD_ID"),
    lit("NA").alias("PROD_BILL_CMPNT_ID"),
    lit("NA").alias("COV_DUE_DT_SK"),
    lit("NA").alias("COV_STRT_DT_SK"),
    lit(1).alias("INVC_SUB_PRM_TYP_CD_SK"),
    FORMAT.DATE(lit("1753-01-01"), lit("DATE"), lit("DATE"), lit("DB2TIMESTAMP")).alias("CRT_TS"),
    lit(1).alias("INVC_SUB_BILL_DISP_CD_SK"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("CLS_SK"),
    lit(1).alias("CLS_PLN_SK"),
    lit(1).alias("GRP_SK"),
    lit(1).alias("INVC_SK"),
    lit(1).alias("PROD_SK"),
    lit(1).alias("SUBGRP_SK"),
    lit(1).alias("SUB_SK"),
    lit(1).alias("INVC_SUB_FMLY_CNTR_CD_SK"),
    lit("NA").alias("COV_END_DT_SK"),
    lit(0).alias("DPNDT_PRM_AMT"),
    lit(0).alias("SUB_PRM_AMT"),
    lit(0).alias("DPNDT_CT"),
    lit(0).alias("SUB_CT"),
    lit(0).alias("VOL_COV_AMT")
)

df_recycle = (
    df_foreignkey
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("INVC_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("INVC_SK").alias("INVC_SK"),
        col("BILL_INVC_ID").alias("BILL_INVC_ID"),
        col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        col("PROD_ID").alias("PROD_ID"),
        col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
        col("COV_DUE_DT").alias("COV_DUE_DT"),
        col("COV_STRT_DT_SK").alias("COV_STRT_DT_SK"),
        col("COV_END_DT_SK").alias("COV_END_DT_SK"),
        col("INVC_SUB_PRM_TYP_CD").alias("INVC_SUB_PRM_TYP_CD"),
        col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        col("CRT_DT").alias("CRT_DT"),
        col("INVC_SUB_BILL_DISP_CD").alias("INVC_SUB_BILL_DISP_CD"),
        col("CLS_ID").alias("CLS_ID"),
        col("CSPI_ID").alias("CSPI_ID"),
        col("GRP_ID").alias("GRP_ID"),
        col("INVC_ID").alias("INVC_ID"),
        col("SUBGRP_ID").alias("SUBGRP_ID"),
        col("INVC_SUB_FMLY_CNTR_CD").alias("INVC_SUB_FMLY_CNTR_CD"),
        col("DPNDT_PRM_AMT").alias("DPNDT_PRM_AMT"),
        col("SUB_PRM_AMT").alias("SUB_PRM_AMT"),
        col("DPNDT_CT").alias("DPNDT_CT"),
        col("SUB_CT").alias("SUB_CT"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("VOL_COV_AMT").alias("VOL_COV_AMT")
    )
)

write_files(
    df_recycle,
    f"hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_collector = (
    df_fkey
    .unionByName(df_defaultunk)
    .unionByName(df_defaultna)
)

df_collector_ordered = df_collector.select(
    "INVC_SUB_SK",
    "SRC_SYS_CD_SK",
    "BILL_INVC_ID",
    "SUB_UNIQ_KEY",
    "CLS_PLN_ID",
    "PROD_ID",
    "PROD_BILL_CMPNT_ID",
    "COV_DUE_DT_SK",
    "COV_STRT_DT_SK",
    "INVC_SUB_PRM_TYP_CD_SK",
    "CRT_TS",
    "INVC_SUB_BILL_DISP_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLS_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "INVC_SK",
    "PROD_SK",
    "SUBGRP_SK",
    "SUB_SK",
    "INVC_SUB_FMLY_CNTR_CD_SK",
    "COV_END_DT_SK",
    "DPNDT_PRM_AMT",
    "SUB_PRM_AMT",
    "DPNDT_CT",
    "SUB_CT",
    "VOL_COV_AMT"
)

df_enriched = (
    df_collector_ordered
    .withColumn("CLS_PLN_ID", rpad(col("CLS_PLN_ID"), 8, " "))
    .withColumn("PROD_ID", rpad(col("PROD_ID"), 8, " "))
    .withColumn("PROD_BILL_CMPNT_ID", rpad(col("PROD_BILL_CMPNT_ID"), 4, " "))
    .withColumn("COV_DUE_DT_SK", rpad(col("COV_DUE_DT_SK"), 10, " "))
    .withColumn("COV_STRT_DT_SK", rpad(col("COV_STRT_DT_SK"), 10, " "))
    .withColumn("CRT_TS", rpad(col("CRT_TS"), 10, " "))
    .withColumn("COV_END_DT_SK", rpad(col("COV_END_DT_SK"), 10, " "))
)

write_files(
    df_enriched,
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)