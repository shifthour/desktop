# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_7 03/04/09 11:16:36 Batch  15039_40600 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_7 03/04/09 10:54:27 Batch  15039_39268 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_6 02/25/09 18:56:23 Batch  15032_68190 PROMOTE bckcetl testIDS dcg01 #3494 DRG 2009
# MAGIC ^1_6 02/25/09 18:55:17 Batch  15032_68127 INIT bckcett devlIDS u10157 #3494 DRG 2009 
# MAGIC ^1_1 02/25/09 18:32:21 Batch  15032_66785 INIT bckcett devlIDS u10157 #3494 DRG 2009 move to test
# MAGIC ^1_1 12/26/07 11:37:21 Batch  14605_41845 PROMOTE bckcett devlIDS dsadm dsadm
# MAGIC ^1_1 12/26/07 11:16:35 Batch  14605_40601 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 11/28/07 15:31:13 Batch  14577_56366 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 11/21/07 14:41:36 Batch  14570_52900 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_5 10/30/07 12:54:00 Batch  14548_46450 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_5 10/30/07 12:50:21 Batch  14548_46228 INIT bckcett devlIDScur dsadm bls for sa
# MAGIC ^1_1 10/17/07 12:20:35 Batch  14535_44460 PROMOTE bckcett devlIDScur u10157 sa - moved ids_devl DRG code to idscurdevl cause it had balancing code not yet in production
# MAGIC ^1_1 10/17/07 12:18:00 Batch  14535_44325 INIT bckcett devlIDS30 u10157 sa - moving drg code to ids_current_devl with balancing code
# MAGIC ^1_1 05/24/06 13:37:46 Batch  14024_49070 INIT bckcett devlIDS30 u05779 bj
# MAGIC ^1_13 12/13/05 13:55:16 Batch  13862_50122 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_12 10/05/05 10:21:04 Batch  13793_37270 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_11 09/23/05 09:39:19 Batch  13781_34764 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_10 09/23/05 08:27:20 Batch  13781_30444 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_9 09/23/05 08:22:17 Batch  13781_30141 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_8 09/20/05 15:13:43 Batch  13778_54827 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_7 09/20/05 15:06:38 Batch  13778_54403 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:    IdsDrgExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from DRGWgt.dat 
# MAGIC   DRGWgt.dat is updated once a year
# MAGIC 
# MAGIC 
# MAGIC     
# MAGIC INPUTS:
# MAGIC                  data comes from http://cms.hhs.gov/acuteinpatientpps - see SOP in O:\\Library\\IAD\\PHI11\\Shared\\Warehouse Documentation\\IDS\\Reference\\DRG\\SOP for DRG Weights.doc
# MAGIC                 The IDS uses the .../infiles/DRGWgt.dat as it's source. copied from O:\\Library\\IAD\\PHI11\\Shared\\Input_IDS\\DRG\\DRGWgt.csv
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC HASH FILES:  none
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   STRIP.FIELD
# MAGIC                   FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created CmsDrgExtr.dat to go into the load seq 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              Steph Goddard    06/2004     -   Originally Programmed
# MAGIC              Brent Leland        09/24/2004   Changed display documentation.
# MAGIC              Sharon Anrew     1/6/2005    -  Unchecked the First Line are Column Lines for the dgr.dat.
# MAGIC              BJ Luce              5/17/2006      changed to sunsetting CDS. Use DRGWgt.dat as input. no header
# MAGIC                                                                 add parameter CurrentDate
# MAGIC 
# MAGIC             SAndrew               2008-12-10    added svDRG to mask out 4 character DRG code

# MAGIC Apply business logic
# MAGIC Strip Carriage Return, Line Feed, and  Tab and pads fields
# MAGIC Writing Sequential File to ../key
# MAGIC Assign primary surrogate key
# MAGIC Create a row for key \"0\" and \"0000\"
# MAGIC The DRG file is copied from  O:\\Library\\IAD\\PHI11\\Shared\\Input_IDS\\DRG\\
# MAGIC DRGWgt.csv 
# MAGIC 
# MAGIC to /ids/prod/landing/DRGWgt.dat.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from pyspark.sql.functions import (
    col,
    lit,
    concat,
    expr,
    regexp_replace,
    substring,
    length,
    when,
    row_number,
    rpad,
    upper,
    rlike
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


DrgMethodCode = get_widget_value('DrgMethodCode','\\"CMS\\"')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
CurrentDate = get_widget_value('CurrentDate','2007-10-23')

df_hf_drg_lkup = spark.read.parquet(f"{adls_path}/hf_drg.parquet").select(
    "SRC_SYS_CD",
    "DRG_CD",
    "DRG_METH_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "DRG_SK"
)

schema_drg_dat = StructType([
    StructField("drg", StringType(), False),
    StructField("drg_mdc", StringType(), False),
    StructField("drg_type", StringType(), False),
    StructField("drg_title", StringType(), False),
    StructField("drg_rel_wgt", DecimalType(38,10), False),
    StructField("drg_geo_mean", DecimalType(38,10), False),
    StructField("drg_arth_mean", DecimalType(38,10), False)
])

df_drg_dat = (
    spark.read.format("csv")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_drg_dat)
    .load(f"{adls_path_raw}/landing/DRGWgt.dat")
)

df_stripfields = (
    df_drg_dat
    .filter(
        (df_drg_dat["drg"].substr(1,3) != "DRG") &
        (df_drg_dat["drg"].substr(1,4) != "NOTE") &
        (col("drg").rlike("^[0-9]+$"))
    )
    .withColumn("svDRG", substring(concat(lit("0000"), trim(col("drg"))), -4, 4))
    .withColumn(
        "svMDC",
        expr(
            "CASE WHEN length(trim(drg_mdc))=0 OR trim(drg_mdc)='PRE' THEN '00' "
            "WHEN length(trim(drg_mdc))=1 THEN right(concat('0', drg_mdc),2) "
            "ELSE right(concat('00', drg_mdc),2) END"
        )
    )
    .withColumn("DRG_CD", regexp_replace(col("svDRG"), "[\\x0A\\x0D\\x09]", ""))
    .withColumn("EXT_TIMESTAMP", lit(CurrentDate))
    .withColumn("DCG_DIAG_CAT_CD", col("svMDC"))
    .withColumn("DRG_TYPE_CD", col("drg_type"))
    .withColumn("DRG_DESC", regexp_replace(col("drg_title"), "[\\x0A\\x0D\\x09]", ""))
    .withColumn(
        "drg_wgt",
        when(
            col("drg_rel_wgt").isNull() |
            (trim(col("drg_rel_wgt"))=="") |
            (~(col("drg_rel_wgt").cast("string").rlike("^-?\\d+(\\.\\d+)?$"))),
            lit(0)
        ).otherwise(col("drg_rel_wgt"))
    )
    .withColumn(
        "drg_glos",
        when(
            col("drg_geo_mean").isNull() |
            (trim(col("drg_geo_mean"))=="") |
            (~(col("drg_geo_mean").cast("string").rlike("^-?\\d+(\\.\\d+)?$"))),
            lit(0)
        ).otherwise(col("drg_geo_mean"))
    )
    .withColumn(
        "drg_alos",
        when(
            col("drg_arth_mean").isNull() |
            (trim(col("drg_arth_mean"))=="") |
            (~(col("drg_arth_mean").cast("string").rlike("^-?\\d+(\\.\\d+)?$"))),
            lit(0)
        ).otherwise(col("drg_arth_mean"))
    )
    .select(
        "DRG_CD",
        "EXT_TIMESTAMP",
        "DCG_DIAG_CAT_CD",
        "DRG_TYPE_CD",
        "DRG_DESC",
        "drg_wgt",
        "drg_glos",
        "drg_alos"
    )
)

df_business = (
    df_stripfields
    .withColumn(
        "DRGMethodCode",
        when(
            length(trim(lit(DrgMethodCode)))==0,
            lit("MS")
        ).otherwise(trim(lit(DrgMethodCode)))
    )
    .withColumn("RowPassThru", lit("Y"))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", col("RowPassThru"))
    .withColumn("FIRST_RECYC_DT", col("EXT_TIMESTAMP"))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit("IDS"))
    .withColumn("PRI_KEY_STRING", concat(lit("IDS;"), trim(col("DRG_CD")), lit(";"), col("DRGMethodCode")))
    .withColumn("DRG_SK", lit(0))
    .withColumn("DRG_CD", trim(col("DRG_CD")))
    .withColumn("DRG_METH_CD", col("DRGMethodCode"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn(
        "DRG_DIAG_CAT_CD",
        when(
            col("DCG_DIAG_CAT_CD").isNull() | (length(trim(col("DCG_DIAG_CAT_CD")))==0),
            lit("NA")
        ).otherwise(upper(trim(col("DCG_DIAG_CAT_CD"))))
    )
    .withColumn(
        "DRG_TYP_CD",
        when(
            col("DRG_TYPE_CD").isNull() | (length(trim(col("DRG_TYPE_CD")))==0),
            lit("NLV")
        ).otherwise(upper(regexp_replace(trim(col("DRG_TYPE_CD")), " ", "")))
    )
    .withColumn("DRG_DESC", upper(trim(col("DRG_DESC"))))
    .withColumn("GEOMTRC_AVG_LOS", trim(col("drg_glos")))
    .withColumn("ARTHMTC_AVG_LOS", trim(col("drg_alos")))
    .withColumn("WT_FCTR", trim(col("drg_wgt")))
)

df_snap_shot_snapshot = df_business.select(
    col("DRG_CD").alias("DRG_CD"),
    col("DRG_METH_CD").alias("DRG_METH_CD")
)

df_snap_shot_transform1 = df_business.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "DRG_SK",
    "DRG_CD",
    "DRG_METH_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "DRG_DIAG_CAT_CD",
    "DRG_TYP_CD",
    "DRG_DESC",
    "GEOMTRC_AVG_LOS",
    "ARTHMTC_AVG_LOS",
    "WT_FCTR"
)

df_primaryKey = (
    df_snap_shot_transform1.alias("Transform1")
    .join(
        df_hf_drg_lkup.alias("lkup"),
        [
            col("Transform1.SRC_SYS_CD")==col("lkup.SRC_SYS_CD"),
            col("Transform1.DRG_CD")==col("lkup.DRG_CD"),
            col("Transform1.DRG_METH_CD")==col("lkup.DRG_METH_CD")
        ],
        "left"
    )
    .select(
        col("Transform1.JOB_EXCTN_RCRD_ERR_SK").alias("Transform1_JOB_EXCTN_RCRD_ERR_SK"),
        col("Transform1.INSRT_UPDT_CD").alias("Transform1_INSRT_UPDT_CD"),
        col("Transform1.DISCARD_IN").alias("Transform1_DISCARD_IN"),
        col("Transform1.PASS_THRU_IN").alias("Transform1_PASS_THRU_IN"),
        col("Transform1.FIRST_RECYC_DT").alias("Transform1_FIRST_RECYC_DT"),
        col("Transform1.ERR_CT").alias("Transform1_ERR_CT"),
        col("Transform1.RECYCLE_CT").alias("Transform1_RECYCLE_CT"),
        col("Transform1.SRC_SYS_CD").alias("Transform1_SRC_SYS_CD"),
        col("Transform1.PRI_KEY_STRING").alias("Transform1_PRI_KEY_STRING"),
        col("Transform1.DRG_SK").alias("Transform1_DRG_SK"),
        col("Transform1.DRG_CD").alias("Transform1_DRG_CD"),
        col("Transform1.DRG_METH_CD").alias("Transform1_DRG_METH_CD"),
        col("Transform1.CRT_RUN_CYC_EXCTN_SK").alias("Transform1_CRT_RUN_CYC_EXCTN_SK"),
        col("Transform1.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("Transform1_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("Transform1.DRG_DIAG_CAT_CD").alias("Transform1_DRG_DIAG_CAT_CD"),
        col("Transform1.DRG_TYP_CD").alias("Transform1_DRG_TYP_CD"),
        col("Transform1.DRG_DESC").alias("Transform1_DRG_DESC"),
        col("Transform1.GEOMTRC_AVG_LOS").alias("Transform1_GEOMTRC_AVG_LOS"),
        col("Transform1.ARTHMTC_AVG_LOS").alias("Transform1_ARTHMTC_AVG_LOS"),
        col("Transform1.WT_FCTR").alias("Transform1_WT_FCTR"),
        col("lkup.SRC_SYS_CD").alias("lkup_SRC_SYS_CD"),
        col("lkup.DRG_SK").alias("lkup_DRG_SK"),
        col("lkup.CRT_RUN_CYC_EXCTN_SK").alias("lkup_CRT_RUN_CYC_EXCTN_SK")
    )
)

df_enriched = (
    df_primaryKey
    .withColumn(
        "DRG_SK",
        when(col("lkup_DRG_SK").isNull(), lit(None).cast("long")).otherwise(col("lkup_DRG_SK"))
    )
    .withColumn(
        "NewCrtRunCycExtcnSk",
        when(col("lkup_DRG_SK").isNull(), lit(CurrRunCycle).cast("int")).otherwise(col("lkup_CRT_RUN_CYC_EXCTN_SK"))
    )
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"DRG_SK",<schema>,<secret_name>)

df_key = df_enriched.select(
    col("Transform1_JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform1_INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Transform1_DISCARD_IN").alias("DISCARD_IN"),
    col("Transform1_PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Transform1_FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Transform1_ERR_CT").alias("ERR_CT"),
    col("Transform1_RECYCLE_CT").alias("RECYCLE_CT"),
    col("Transform1_SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform1_PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("DRG_SK").alias("DRG_SK"),
    col("Transform1_DRG_CD").alias("DRG_CD"),
    col("Transform1_DRG_METH_CD").alias("DRG_METH_CD"),
    col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).cast("int").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Transform1_DRG_DIAG_CAT_CD").alias("DRG_DIAG_CAT_CD"),
    col("Transform1_DRG_TYP_CD").alias("DRG_TYP_CD"),
    col("Transform1_DRG_DESC").alias("DRG_DESC"),
    col("Transform1_GEOMTRC_AVG_LOS").alias("GEOMTRC_AVG_LOS"),
    col("Transform1_ARTHMTC_AVG_LOS").alias("ARTHMTC_AVG_LOS"),
    col("Transform1_WT_FCTR").alias("WT_FCTR")
)

w = Window.orderBy(lit(1))
df_enriched_with_rn = df_enriched.withColumn("row_num", row_number().over(w))

df_updt = (
    df_enriched_with_rn
    .filter(col("lkup_SRC_SYS_CD").isNull())
    .select(
        col("Transform1_SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("Transform1_DRG_CD").alias("DRG_CD"),
        col("Transform1_DRG_METH_CD").alias("DRG_METH_CD"),
        col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("DRG_SK").alias("DRG_SK")
    )
)

df_writeZeroRow = (
    df_enriched_with_rn
    .filter(col("row_num")==1)
    .select(
        col("Transform1_SRC_SYS_CD").alias("SRC_SYS_CD"),
        lit("0").alias("DRG_CD"),
        col("Transform1_DRG_METH_CD").alias("DRG_METH_CD"),
        col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("DRG_SK")
    )
)

df_write4ZeroRow = (
    df_enriched_with_rn
    .filter(col("row_num")==1)
    .select(
        col("Transform1_SRC_SYS_CD").alias("SRC_SYS_CD"),
        lit("0000").alias("DRG_CD"),
        col("Transform1_DRG_METH_CD").alias("DRG_METH_CD"),
        col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("DRG_SK")
    )
)

df_hf_drg = df_updt.unionByName(df_writeZeroRow).unionByName(df_write4ZeroRow)

df_hf_drg_for_write = df_hf_drg.select(
    rpad(col("SRC_SYS_CD"),10," ").alias("SRC_SYS_CD"), 
    rpad(col("DRG_CD"),4," ").alias("DRG_CD"),
    col("DRG_METH_CD").alias("DRG_METH_CD"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("DRG_SK").alias("DRG_SK")
)

write_files(
    df_hf_drg_for_write,
    "hf_drg.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_DrgCrfPkey_final = df_key.select(
    rpad(col("DRG_CD"),4," ").alias("DRG_CD"),
    col("DRG_METH_CD").alias("DRG_METH_CD")
)

write_files(
    df_DrgCrfPkey_final,
    f"{adls_path}/key/IdsDrgExtr.dat",
    ",",
    "overwrite",
    False,
    True,
    "\"",
    None
)

df_transformer = df_snap_shot_snapshot.withColumn(
    "svDRGMethCdSk",
    GetFkeyCodes("FACETS", lit(0), "DIAGNOSIS RELATED GROUP METHOD CODE", col("DRG_METH_CD"), "X")
)

df_transformer_rowcount = df_transformer.select(
    rpad(col("DRG_CD"),4," ").alias("DRG_CD"),
    col("svDRGMethCdSk").alias("DRG_METH_CD_SK")
)

write_files(
    df_transformer_rowcount,
    f"{adls_path}/load/B_DRG.FACETS.dat",
    ",",
    "overwrite",
    False,
    True,
    "\"",
    None
)