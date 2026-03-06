# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Check record data from Facets and generates a data file for RedCard.
# MAGIC 
# MAGIC Job Name:  EobCheckRec
# MAGIC Called By:RedCardEobExtrCntl
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                               Date                 Project/Altiris #      Change Description                                                                      Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                           --------------------     ------------------------      -----------------------------------------------------------------------                               --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi                     2020-12-05            RedCard                     Original Devlopment                                                                OutboundDev3           Jaideep Mankala             12/27/2020
# MAGIC Raja Gummadi                     2021-01-21            RedCard                    Made CMICRLN value to space                                              OutboundDev3        Jaideep Mankala              01/21/2021
# MAGIC Raja Gummadi                     2021-02-04            343913                 Added void logic                                                                            OutboundDev3              Jaideep Mankala              02/04/2021
# MAGIC 
# MAGIC Megan Conway                 2022-03-09               	                 S2S Remediation - MSSQL connection parameters added	           OutboundDev5		Ken Bradmon	2022-05-19
# MAGIC 
# MAGIC Mrudula Kodali                  2022-09-16              Redcard       Updated  logic to get 2checks for single docID with different LOBD_ID      OutboundDev3          Jeyaprasanna            2022-11-29
# MAGIC                                                                                            also added LOBD_ID and CKPY_REF_ID  in open fields                         
# MAGIC 
# MAGIC Deepika C                         2024-08-06           US 626235           Updated the query to add six new LOBD_ID's in                               OutboundDev3           Jeyaprasanna                  2024-08-16
# MAGIC                                                                                                      CLM,Void stages


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, when, concat
from pyspark.sql.functions import rpad, substring
# COMMAND ----------
# MAGIC %run ../../../../Utility_Outbound
# COMMAND ----------

# ------------------------------------------------------------------------
# Parameters
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CLMPDDT = get_widget_value('CLMPDDT','')
# ------------------------------------------------------------------------
# Read seq_Chktemp (PxSequentialFile)
schema_seq_Chktemp = StructType([
    StructField("CLCL_ID", StringType(), True),
    StructField("CKPY_REF_ID", StringType(), True),
    StructField("CDOCID", StringType(), True)
])
df_seq_Chktemp = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_seq_Chktemp)
    .load(f"{adls_path_publish}/external/Zelis_EOB_Checktemp.dat")
)

# ------------------------------------------------------------------------
# Read CLM (ODBCConnectorPX)
jdbc_url_clm, jdbc_props_clm = get_db_config(facets_secret_name)
extract_query_clm = """SELECT
CBI.CKCK_PAYEE_NAME AS PRPR_NAME,
CCC.CKCK_CK_NO,
CCCC.CKPY_PAY_DT,
CCCC.CLCL_ID,
CBI.CKPY_NET_AMT,
CPBA.PYBA_BANK_ROUTING,
CPBA.PYBA_BANK_TRANSIT,
CPBA.PYBA_CHECKING_ACCT,
CBI.CKPY_REF_ID,
CBI.LOBD_ID

FROM 

""" + f"{FacetsOwner}" + """.CMC_BPID_INDIC CBI,
""" + f"{FacetsOwner}" + """.CMC_CKCK_CHECK CCC,
""" + f"{FacetsOwner}" + """.CMC_CLCK_CLM_CHECK CCCC,
""" + f"{FacetsOwner}" + """.CMC_PYBA_BANK_ACCT CPBA

WHERE

CBI.CKPY_REF_ID = CCCC.CKPY_REF_ID
AND CBI.CKPY_REF_ID = CCC.CKPY_REF_ID
AND CBI.PYBA_ID = CPBA.PYBA_ID
AND CBI.LOBD_ID NOT IN ('FEP','FEPB','FEPH','FEPS','FEPV','FEHS','FEHB','FEHV','FEPL','FEPM','FEPT')
AND CBI.CKPY_PAYEE_TYPE IN ('S','A','M')
AND CBI.CKPY_NET_AMT > 0.00
AND CCCC.CLCK_NET_AMT > 0.00
AND CCC.CKCK_CK_NO <> 0
AND CBI.CKPY_PAY_DT IN (SELECT (CASE WHEN '""" + CLMPDDT + """' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '""" + CLMPDDT + """' END) FROM
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)IS NULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM """ + f"{FacetsOwner}" + """.CMC_BPID_INDIC
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM """ + f"{FacetsOwner}" + """.CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  """ + f"{FacetsOwner}" + """.CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD')))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM """ + f"{FacetsOwner}" + """.CMC_BPID_INDIC
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  """ + f"{FacetsOwner}" + """.CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD'))A)
"""
df_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_clm)
    .options(**jdbc_props_clm)
    .option("query", extract_query_clm)
    .load()
)

# ------------------------------------------------------------------------
# Read Void (ODBCConnectorPX)
jdbc_url_void, jdbc_props_void = get_db_config(facets_secret_name)
extract_query_void = """SELECT

CBI.CKCK_PAYEE_NAME AS PRPR_NAME,
CBI.BPID_CK_NO AS CKCK_CK_NO,
CBI.BPID_PRINTED_DT AS CKPY_PAY_DT,
CCCC.CLCL_ID,
CBI.CKPY_NET_AMT,
CPBA.PYBA_BANK_ROUTING,
CPBA.PYBA_BANK_TRANSIT,
CPBA.PYBA_CHECKING_ACCT,
CBI.CKPY_REF_ID,
CBI.LOBD_ID

FROM 


""" + f"{FacetsOwner}" + """.CMC_BPID_INDIC CBI,
""" + f"{FacetsOwner}" + """.CMC_CLCK_CLM_CHECK CCCC,
""" + f"{FacetsOwner}" + """.CMC_PYBA_BANK_ACCT CPBA

WHERE


CBI.CKPY_REF_ID = CCCC.CKPY_REF_ID
AND CBI.PYBA_ID = CPBA.PYBA_ID
AND CBI.LOBD_ID NOT IN ('FEP','FEPB','FEPH','FEPS','FEPV','FEHS','FEHB','FEHV','FEPL','FEPM','FEPT')
AND CBI.CKPY_PAYEE_TYPE IN ('S','A','M')
AND CBI.CKPY_NET_AMT > 0.00
AND CCCC.CLCK_NET_AMT > 0.00
AND CBI.BPID_CK_NO <> 0
AND CBI.BPID_PRINTED_DT IN (SELECT (CASE WHEN '""" + CLMPDDT + """' = '2199-12-31' THEN A.CLCL_PAID_DT ELSE '""" + CLMPDDT + """' END) FROM 
(SELECT (CASE WHEN MAX(CKPY_PAY_DT)IS NULL THEN (SELECT CASE WHEN MAX(CKPY_PAY_DT) IS NULL THEN '1753-01-01' ELSE MAX(CKPY_PAY_DT) END
                             FROM """ + f"{FacetsOwner}" + """.CMC_BPID_INDIC
                             WHERE SYIN_REF_ID IN (SELECT SYIN_REF_ID FROM """ + f"{FacetsOwner}" + """.CER_SYIN_INST WHERE SYIN_INST IN (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  """ + f"{FacetsOwner}" + """.CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD')))   
                             ELSE MAX(CKPY_PAY_DT) END) AS CLCL_PAID_DT
                             FROM """ + f"{FacetsOwner}" + """.CMC_BPID_INDIC
                             WHERE SYIN_INST IN 
                            (SELECT DISTINCT max(CER_SYIN_INST.SYIN_INST)
                             FROM  """ + f"{FacetsOwner}" + """.CER_SYIN_INST  CER_SYIN_INST 
                             WHERE CER_SYIN_INST.PZAP_APP_ID='!CKD'))A)
"""
df_Void = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_void)
    .options(**jdbc_props_void)
    .option("query", extract_query_void)
    .load()
)

# ------------------------------------------------------------------------
# Funnel (PxFunnel) => fnl_recs
df_CLM_funnel = df_CLM.select(
    "PRPR_NAME",
    "CKCK_CK_NO",
    "CKPY_PAY_DT",
    "CLCL_ID",
    "CKPY_NET_AMT",
    "PYBA_BANK_ROUTING",
    "PYBA_BANK_TRANSIT",
    "PYBA_CHECKING_ACCT",
    "CKPY_REF_ID",
    "LOBD_ID"
)
df_Void_funnel = df_Void.select(
    "PRPR_NAME",
    "CKCK_CK_NO",
    "CKPY_PAY_DT",
    "CLCL_ID",
    "CKPY_NET_AMT",
    "PYBA_BANK_ROUTING",
    "PYBA_BANK_TRANSIT",
    "PYBA_CHECKING_ACCT",
    "CKPY_REF_ID",
    "LOBD_ID"
)
df_fnl_recs = df_CLM_funnel.unionByName(df_Void_funnel)

# ------------------------------------------------------------------------
# xfrm_Map (CTransformerStage)
df_xfrm_Map = df_fnl_recs.select(
    col("PRPR_NAME").alias("PRPR_NAME"),
    col("CKCK_CK_NO").alias("CKCK_CK_NO"),
    col("CKPY_PAY_DT").alias("CKPY_PAY_DT"),
    col("CLCL_ID").alias("CLCL_ID"),
    col("CKPY_NET_AMT").alias("CKPY_NET_AMT"),
    col("PYBA_BANK_ROUTING").alias("PYBA_BANK_ROUTING"),
    col("PYBA_BANK_TRANSIT").alias("PYBA_BANK_TRANSIT"),
    col("PYBA_CHECKING_ACCT").alias("PYBA_CHECKING_ACCT"),
    col("CKPY_REF_ID").alias("CKPY_REF_ID"),
    col("LOBD_ID").alias("LOBD_ID")
)

# ------------------------------------------------------------------------
# lkp_ID (PxLookup) => inner join with df_seq_Chktemp on CLCL_ID
df_lkp_ID = (
    df_xfrm_Map.alias("lnk_meme_ck")
    .join(
        df_seq_Chktemp.alias("lnk_ref"),
        col("lnk_meme_ck.CLCL_ID") == col("lnk_ref.CLCL_ID"),
        "inner"
    )
    .select(
        col("lnk_meme_ck.CLCL_ID").alias("CLCL_ID"),
        col("lnk_meme_ck.PRPR_NAME").alias("PRPR_NAME"),
        col("lnk_ref.CDOCID").alias("CDOCID"),
        col("lnk_meme_ck.CKCK_CK_NO").alias("CKCK_CK_NO"),
        col("lnk_meme_ck.CKPY_PAY_DT").alias("CKPY_PAY_DT"),
        col("lnk_meme_ck.CKPY_NET_AMT").alias("CKPY_NET_AMT"),
        col("lnk_meme_ck.PYBA_BANK_ROUTING").alias("PYBA_BANK_ROUTING"),
        col("lnk_meme_ck.PYBA_BANK_TRANSIT").alias("PYBA_BANK_TRANSIT"),
        col("lnk_meme_ck.PYBA_CHECKING_ACCT").alias("PYBA_CHECKING_ACCT"),
        col("lnk_meme_ck.CKPY_REF_ID").alias("CKPY_REF_ID"),
        col("lnk_meme_ck.LOBD_ID").alias("LOBD_ID")
    )
)

# ------------------------------------------------------------------------
# rmdup_ID (PxRemDup), RetainRecord=last, KeysThatDefineDuplicates=CLCL_ID, CKPY_REF_ID
df_rmdup_ID = dedup_sort(
    df_lkp_ID,
    partition_cols=["CLCL_ID", "CKPY_REF_ID"],
    sort_cols=[("CLCL_ID","D"), ("CKPY_REF_ID","D")]
)

# ------------------------------------------------------------------------
# rmdup_NO (PxRemDup), RetainRecord=first, KeysThatDefineDuplicates=CKCK_CK_NO
df_rmdup_NO = dedup_sort(
    df_rmdup_ID,
    partition_cols=["CKCK_CK_NO"],
    sort_cols=[("CKCK_CK_NO","A")]
)

# ------------------------------------------------------------------------
# xfrm_BusinessRules (CTransformerStage)
df_xfrm_BusinessRules_intermediate = df_rmdup_NO.select(
    lit("06").alias("CRCRDTYP"),
    lit("17").alias("CRCRDVRSN"),
    col("CDOCID").alias("CDOCID"),
    lit("").alias("CFRACTNONE"),  # Space(10) -> we fill later with rpad
    lit("").alias("CFRACTN2"),    # Space(10)
    lit("").alias("CFRACTN3"),    # Space(10)
    lit("Commerce Bank of Kansas City").alias("CBANK1"),  # char(40)
    lit("").alias("CBANK2"),      # Space(40)
    lit("").alias("CBANK3"),      # Space(40)
    lit("").alias("CBANK4"),      # Space(40)
    lit("").alias("CBANK5"),      # Space(40)
    lit("").alias("CBANK6"),      # Space(40)
    lit("").alias("CSGNTR1"),     # Space(5)
    lit("").alias("CSGNTR2"),     # Space(5)
    lit("").alias("CSGNTR3"),     # Space(5)
    lit("").alias("CTXAMT"),      # Space(150)
    lit("").alias("CCHKMSG1"),    # Space(50)
    lit("").alias("CCHKMSG2"),    # Space(50)
    lit("").alias("CMICRLN"),     # Space(65)
    col("CKPY_NET_AMT").cast("string").alias("CCHKAMT"),  # char(15)
    lit("").alias("CBANKLOGO"),   # Space(10)
    lit("").alias("CADMINLN1"),   # Space(50)
    lit("").alias("CADMINLN2"),   # Space(50)
    lit("").alias("CADMINLN3"),   # Space(50)
    lit("").alias("CADMINLN4"),   # Space(50)
    lit("").alias("CADMINLN5"),   # Space(50)
    lit("").alias("CADMINLN6"),   # Space(50)
    lit("").alias("CADMINLOGO"),  # Space(5)
    col("PRPR_NAME").alias("CPAYENM"),  # char(80)
    lit("").alias("CPAYEADDR1"),  # Space(40)
    lit("").alias("CPAYEADDR2"),  # Space(40)
    lit("").alias("CPAYEADDR3"),  # Space(50)
    concat(
        substring(col("CKPY_PAY_DT"), 1, 4),
        substring(col("CKPY_PAY_DT"), 6, 2),
        substring(col("CKPY_PAY_DT"), 9, 2)
    ).alias("CCHKDT"),  # char(8)
    when((col("CKPY_NET_AMT") != 0.0) & (col("CKCK_CK_NO") == 0), col("CKPY_REF_ID"))
    .otherwise(col("CKCK_CK_NO"))
    .cast("string")
    .alias("CCHKNO"),  # char(16)
    lit("").alias("CORIGCHKNO"),                # Space(16)
    lit("").alias("CCHKNOUNIQTAGBASE"),         # Space(50)
    lit("").alias("CORIGCHKNOFROMDATA"),        # Space(16)
    lit("").alias("CCHKNOAFTRMICRRULE"),        # Space(16)
    lit("").alias("CCHKSTRTPOSTN"),             # Space(2)
    lit("").alias("CCHKDATA1LABEL"),            # Space(30)
    lit("").alias("CCHKDATA1"),                 # Space(30)
    lit("").alias("CCHKDATA2LABEL"),            # Space(30)
    lit("").alias("CCHKDATA2"),                 # Space(30)
    lit("").alias("CCHKDATA3LABEL"),            # Space(30)
    lit("").alias("CCHKDATA3"),                 # Space(30)
    lit("").alias("CCHKDATA4LABEL"),            # Space(30)
    lit("").alias("CCHKDATA4"),                 # Space(30)
    lit("").alias("CCHKDATA5LABEL"),            # Space(30)
    lit("").alias("CCHKDATA5"),                 # Space(30)
    lit("").alias("CCHKDATA6LABEL"),            # Space(30)
    lit("").alias("CCHKDATA6"),                 # Space(30)
    col("CLCL_ID").alias("COPENFLD1"),          # char(60)
    col("LOBD_ID").alias("COPENFLD2"),          # char(60)
    col("CKPY_REF_ID").alias("COPENFLD3"),      # char(120)
    lit("").alias("COPENFLD4"),                 # Space(200)
    lit("").alias("COPENFLD5"),                 # Space(200)
    lit("").alias("CISACH"),                    # Space(1)
    lit("").alias("CVOIDCHK"),                  # Space(1)
    lit("").alias("CERRHDR"),                   # Space(2)
    lit("").alias("CPAYERTNGNO"),               # Space(9)
    lit("").alias("CPAYEONUS"),                 # Space(20)
    lit("").alias("CACCTTYP"),                  # Space(2)
    lit("").alias("CADNDA"),                    # Space(80)
    lit("").alias("CPDFPGNO"),                  # Space(7)
    lit("").alias("CSTALEMSG1"),                # Space(50)
    lit("").alias("CSTALEMSG2"),                # Space(50)
    lit("").alias("CACCTSETUPID"),              # Space(6)
    lit("").alias("CEPAYPAYMTTYP"),             # Space(2)
    lit("").alias("CVNDRINSRTPGCT"),            # Space(2)
    lit("").alias("CCHKHTMILLIMETERS"),         # Space(10)
    lit("").alias("CEPAYVNDRINSRTTYP"),         # Space(1)
    lit("").alias("CPAYENCPDPPDXNO"),           # Space(15)
    lit("").alias("CPAYENPI"),                  # Space(10)
    lit("").alias("CPAYETAXID"),                # Space(9)
    lit("").alias("CPAYESUBTAXID"),             # Space(3)
    lit("").alias("CPMTADJAMT"),                # Space(15)
    lit("").alias("CPMTADJFISCALENDDT"),        # Space(8)
    lit("").alias("CPMTADJPROVID"),             # Space(50)
    lit("").alias("CPMTADJRSNCD"),              # Space(5)
    lit("").alias("CPMTADJREFCD"),              # Space(50)
    lit("").alias("CPAYMTTYP"),                 # Space(2)
    lit("").alias("CVNDRTRANSID"),              # Space(25)
    lit("").alias("CVNDRINSRTPGTYP"),           # Space(5)
    lit("").alias("CNRMLZDONUS"),               # Space(18)
    lit("").alias("CNRMLZDTRANSIT"),            # Space(9)
    lit("").alias("C835PAYMTTYP"),              # Space(3)
    lit("").alias("CACHPAYMTFMT"),              # Space(3)
    lit("").alias("CPAYERRTNG"),                # Space(12)
    lit("").alias("CPAYERACCT"),                # Space(35)
    lit("").alias("CORIGCOID"),                 # Space(10)
    lit("").alias("CORIGCOSUPPCD"),             # Space(30)
    lit("").alias("CREASSOCID"),                # Space(30)
    lit("").alias("CEXSTNGCHKLOC"),             # Space(6)
    lit("").alias("CNEWCHKLOC"),                # Space(13)
    lit("").alias("CNACHASTANDARDENTRYCLSCD")   # Space(3)
)

# Now apply rpad for all columns that are char(...) as specified by the stage:
df_xfrm_BusinessRules = (
    df_xfrm_BusinessRules_intermediate
    .withColumn("CRCRDTYP", rpad(col("CRCRDTYP"), 2, " "))
    .withColumn("CRCRDVRSN", rpad(col("CRCRDVRSN"), 2, " "))
    .withColumn("CDOCID", rpad(col("CDOCID"), 25, " "))
    .withColumn("CFRACTNONE", rpad(col("CFRACTNONE"), 10, " "))
    .withColumn("CFRACTN2", rpad(col("CFRACTN2"), 10, " "))
    .withColumn("CFRACTN3", rpad(col("CFRACTN3"), 10, " "))
    .withColumn("CBANK1", rpad(col("CBANK1"), 40, " "))
    .withColumn("CBANK2", rpad(col("CBANK2"), 40, " "))
    .withColumn("CBANK3", rpad(col("CBANK3"), 40, " "))
    .withColumn("CBANK4", rpad(col("CBANK4"), 40, " "))
    .withColumn("CBANK5", rpad(col("CBANK5"), 40, " "))
    .withColumn("CBANK6", rpad(col("CBANK6"), 40, " "))
    .withColumn("CSGNTR1", rpad(col("CSGNTR1"), 5, " "))
    .withColumn("CSGNTR2", rpad(col("CSGNTR2"), 5, " "))
    .withColumn("CSGNTR3", rpad(col("CSGNTR3"), 5, " "))
    .withColumn("CTXAMT", rpad(col("CTXAMT"), 150, " "))
    .withColumn("CCHKMSG1", rpad(col("CCHKMSG1"), 50, " "))
    .withColumn("CCHKMSG2", rpad(col("CCHKMSG2"), 50, " "))
    .withColumn("CMICRLN", rpad(col("CMICRLN"), 65, " "))
    .withColumn("CCHKAMT", rpad(col("CCHKAMT"), 15, " "))
    .withColumn("CBANKLOGO", rpad(col("CBANKLOGO"), 10, " "))
    .withColumn("CADMINLN1", rpad(col("CADMINLN1"), 50, " "))
    .withColumn("CADMINLN2", rpad(col("CADMINLN2"), 50, " "))
    .withColumn("CADMINLN3", rpad(col("CADMINLN3"), 50, " "))
    .withColumn("CADMINLN4", rpad(col("CADMINLN4"), 50, " "))
    .withColumn("CADMINLN5", rpad(col("CADMINLN5"), 50, " "))
    .withColumn("CADMINLN6", rpad(col("CADMINLN6"), 50, " "))
    .withColumn("CADMINLOGO", rpad(col("CADMINLOGO"), 5, " "))
    .withColumn("CPAYENM", rpad(col("CPAYENM"), 80, " "))
    .withColumn("CPAYEADDR1", rpad(col("CPAYEADDR1"), 40, " "))
    .withColumn("CPAYEADDR2", rpad(col("CPAYEADDR2"), 40, " "))
    .withColumn("CPAYEADDR3", rpad(col("CPAYEADDR3"), 50, " "))
    .withColumn("CCHKDT", rpad(col("CCHKDT"), 8, " "))
    .withColumn("CCHKNO", rpad(col("CCHKNO"), 16, " "))
    .withColumn("CORIGCHKNO", rpad(col("CORIGCHKNO"), 16, " "))
    .withColumn("CCHKNOUNIQTAGBASE", rpad(col("CCHKNOUNIQTAGBASE"), 50, " "))
    .withColumn("CORIGCHKNOFROMDATA", rpad(col("CORIGCHKNOFROMDATA"), 16, " "))
    .withColumn("CCHKNOAFTRMICRRULE", rpad(col("CCHKNOAFTRMICRRULE"), 16, " "))
    .withColumn("CCHKSTRTPOSTN", rpad(col("CCHKSTRTPOSTN"), 2, " "))
    .withColumn("CCHKDATA1LABEL", rpad(col("CCHKDATA1LABEL"), 30, " "))
    .withColumn("CCHKDATA1", rpad(col("CCHKDATA1"), 30, " "))
    .withColumn("CCHKDATA2LABEL", rpad(col("CCHKDATA2LABEL"), 30, " "))
    .withColumn("CCHKDATA2", rpad(col("CCHKDATA2"), 30, " "))
    .withColumn("CCHKDATA3LABEL", rpad(col("CCHKDATA3LABEL"), 30, " "))
    .withColumn("CCHKDATA3", rpad(col("CCHKDATA3"), 30, " "))
    .withColumn("CCHKDATA4LABEL", rpad(col("CCHKDATA4LABEL"), 30, " "))
    .withColumn("CCHKDATA4", rpad(col("CCHKDATA4"), 30, " "))
    .withColumn("CCHKDATA5LABEL", rpad(col("CCHKDATA5LABEL"), 30, " "))
    .withColumn("CCHKDATA5", rpad(col("CCHKDATA5"), 30, " "))
    .withColumn("CCHKDATA6LABEL", rpad(col("CCHKDATA6LABEL"), 30, " "))
    .withColumn("CCHKDATA6", rpad(col("CCHKDATA6"), 30, " "))
    .withColumn("COPENFLD1", rpad(col("COPENFLD1"), 60, " "))
    .withColumn("COPENFLD2", rpad(col("COPENFLD2"), 60, " "))
    .withColumn("COPENFLD3", rpad(col("COPENFLD3"), 120, " "))
    .withColumn("COPENFLD4", rpad(col("COPENFLD4"), 200, " "))
    .withColumn("COPENFLD5", rpad(col("COPENFLD5"), 200, " "))
    .withColumn("CISACH", rpad(col("CISACH"), 1, " "))
    .withColumn("CVOIDCHK", rpad(col("CVOIDCHK"), 1, " "))
    .withColumn("CERRHDR", rpad(col("CERRHDR"), 2, " "))
    .withColumn("CPAYERTNGNO", rpad(col("CPAYERTNGNO"), 9, " "))
    .withColumn("CPAYEONUS", rpad(col("CPAYEONUS"), 20, " "))
    .withColumn("CACCTTYP", rpad(col("CACCTTYP"), 2, " "))
    .withColumn("CADNDA", rpad(col("CADNDA"), 80, " "))
    .withColumn("CPDFPGNO", rpad(col("CPDFPGNO"), 7, " "))
    .withColumn("CSTALEMSG1", rpad(col("CSTALEMSG1"), 50, " "))
    .withColumn("CSTALEMSG2", rpad(col("CSTALEMSG2"), 50, " "))
    .withColumn("CACCTSETUPID", rpad(col("CACCTSETUPID"), 6, " "))
    .withColumn("CEPAYPAYMTTYP", rpad(col("CEPAYPAYMTTYP"), 2, " "))
    .withColumn("CVNDRINSRTPGCT", rpad(col("CVNDRINSRTPGCT"), 2, " "))
    .withColumn("CCHKHTMILLIMETERS", rpad(col("CCHKHTMILLIMETERS"), 10, " "))
    .withColumn("CEPAYVNDRINSRTTYP", rpad(col("CEPAYVNDRINSRTTYP"), 1, " "))
    .withColumn("CPAYENCPDPPDXNO", rpad(col("CPAYENCPDPPDXNO"), 15, " "))
    .withColumn("CPAYENPI", rpad(col("CPAYENPI"), 10, " "))
    .withColumn("CPAYETAXID", rpad(col("CPAYETAXID"), 9, " "))
    .withColumn("CPAYESUBTAXID", rpad(col("CPAYESUBTAXID"), 3, " "))
    .withColumn("CPMTADJAMT", rpad(col("CPMTADJAMT"), 15, " "))
    .withColumn("CPMTADJFISCALENDDT", rpad(col("CPMTADJFISCALENDDT"), 8, " "))
    .withColumn("CPMTADJPROVID", rpad(col("CPMTADJPROVID"), 50, " "))
    .withColumn("CPMTADJRSNCD", rpad(col("CPMTADJRSNCD"), 5, " "))
    .withColumn("CPMTADJREFCD", rpad(col("CPMTADJREFCD"), 50, " "))
    .withColumn("CPAYMTTYP", rpad(col("CPAYMTTYP"), 2, " "))
    .withColumn("CVNDRTRANSID", rpad(col("CVNDRTRANSID"), 25, " "))
    .withColumn("CVNDRINSRTPGTYP", rpad(col("CVNDRINSRTPGTYP"), 5, " "))
    .withColumn("CNRMLZDONUS", rpad(col("CNRMLZDONUS"), 18, " "))
    .withColumn("CNRMLZDTRANSIT", rpad(col("CNRMLZDTRANSIT"), 9, " "))
    .withColumn("C835PAYMTTYP", rpad(col("C835PAYMTTYP"), 3, " "))
    .withColumn("CACHPAYMTFMT", rpad(col("CACHPAYMTFMT"), 3, " "))
    .withColumn("CPAYERRTNG", rpad(col("CPAYERRTNG"), 12, " "))
    .withColumn("CPAYERACCT", rpad(col("CPAYERACCT"), 35, " "))
    .withColumn("CORIGCOID", rpad(col("CORIGCOID"), 10, " "))
    .withColumn("CORIGCOSUPPCD", rpad(col("CORIGCOSUPPCD"), 30, " "))
    .withColumn("CREASSOCID", rpad(col("CREASSOCID"), 30, " "))
    .withColumn("CEXSTNGCHKLOC", rpad(col("CEXSTNGCHKLOC"), 6, " "))
    .withColumn("CNEWCHKLOC", rpad(col("CNEWCHKLOC"), 13, " "))
    .withColumn("CNACHASTANDARDENTRYCLSCD", rpad(col("CNACHASTANDARDENTRYCLSCD"), 3, " "))
)

# ------------------------------------------------------------------------
# seq_ChkRec (PxSequentialFile) => Write Zelis_EOBCheckRec06.dat
# Final select in correct column order
df_seq_ChkRec = df_xfrm_BusinessRules.select(
    "CRCRDTYP",
    "CRCRDVRSN",
    "CDOCID",
    "CFRACTNONE",
    "CFRACTN2",
    "CFRACTN3",
    "CBANK1",
    "CBANK2",
    "CBANK3",
    "CBANK4",
    "CBANK5",
    "CBANK6",
    "CSGNTR1",
    "CSGNTR2",
    "CSGNTR3",
    "CTXAMT",
    "CCHKMSG1",
    "CCHKMSG2",
    "CMICRLN",
    "CCHKAMT",
    "CBANKLOGO",
    "CADMINLN1",
    "CADMINLN2",
    "CADMINLN3",
    "CADMINLN4",
    "CADMINLN5",
    "CADMINLN6",
    "CADMINLOGO",
    "CPAYENM",
    "CPAYEADDR1",
    "CPAYEADDR2",
    "CPAYEADDR3",
    "CCHKDT",
    "CCHKNO",
    "CORIGCHKNO",
    "CCHKNOUNIQTAGBASE",
    "CORIGCHKNOFROMDATA",
    "CCHKNOAFTRMICRRULE",
    "CCHKSTRTPOSTN",
    "CCHKDATA1LABEL",
    "CCHKDATA1",
    "CCHKDATA2LABEL",
    "CCHKDATA2",
    "CCHKDATA3LABEL",
    "CCHKDATA3",
    "CCHKDATA4LABEL",
    "CCHKDATA4",
    "CCHKDATA5LABEL",
    "CCHKDATA5",
    "CCHKDATA6LABEL",
    "CCHKDATA6",
    "COPENFLD1",
    "COPENFLD2",
    "COPENFLD3",
    "COPENFLD4",
    "COPENFLD5",
    "CISACH",
    "CVOIDCHK",
    "CERRHDR",
    "CPAYERTNGNO",
    "CPAYEONUS",
    "CACCTTYP",
    "CADNDA",
    "CPDFPGNO",
    "CSTALEMSG1",
    "CSTALEMSG2",
    "CACCTSETUPID",
    "CEPAYPAYMTTYP",
    "CVNDRINSRTPGCT",
    "CCHKHTMILLIMETERS",
    "CEPAYVNDRINSRTTYP",
    "CPAYENCPDPPDXNO",
    "CPAYENPI",
    "CPAYETAXID",
    "CPAYESUBTAXID",
    "CPMTADJAMT",
    "CPMTADJFISCALENDDT",
    "CPMTADJPROVID",
    "CPMTADJRSNCD",
    "CPMTADJREFCD",
    "CPAYMTTYP",
    "CVNDRTRANSID",
    "CVNDRINSRTPGTYP",
    "CNRMLZDONUS",
    "CNRMLZDTRANSIT",
    "C835PAYMTTYP",
    "CACHPAYMTFMT",
    "CPAYERRTNG",
    "CPAYERACCT",
    "CORIGCOID",
    "CORIGCOSUPPCD",
    "CREASSOCID",
    "CEXSTNGCHKLOC",
    "CNEWCHKLOC",
    "CNACHASTANDARDENTRYCLSCD"
)

write_files(
    df_seq_ChkRec,
    f"{adls_path_publish}/external/Zelis_EOBCheckRec06.dat",
    delimiter="\t",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)