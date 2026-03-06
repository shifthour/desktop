# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2018 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Reads the EyeMedClm_ClaimsLanding.dat file and puts the data into the claim common record format and takes it through primary key process using Shared Container ClmPkey
# MAGIC     
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                               Date                 Project/Altiris #       Change Description                                                                   Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                             --------------------     ------------------------       -----------------------------------------------------------------------------------------          --------------------------------       -------------------------------   ----------------------------       
# MAGIC Sethuraman Rajendran           2018-03-01        5744                     Initial Programming                                                                    IntegrateDev2                    Kalyan Neelam          2018-04-04
# MAGIC 
# MAGIC Mohan Karnati                       2019-06-06        ADO-73034             Pulling TXNMY_CD from EYEMEDClmLanding stage               IntegrateDev1       Kalyan Neelam          2019-07-01	
# MAGIC                                                                                                        till EYEMEDClmExtr and changing name in alpha_pfx stage
# MAGIC Giri  Mallavaram    2020-04-06         PBM Changes       Added BILL_PAYMT_EXCL_IN to be in sync with changes to CLM_PKey Container        IntegrateDev2       Kalyan Neelam     2020-04-07
# MAGIC 
# MAGIC Goutham K                         2021-06-02         US-366403            New Provider file Change to include Loc and Svc loc id                  IntegrateDev1                Jeyaprasanna            2021-06-08

# MAGIC Apply business logic
# MAGIC Lookup subscriber, product and member information
# MAGIC Read the EYEMED CLM file created from EYEMEDClmLand
# MAGIC This container is used in:
# MAGIC ESIClmExtr
# MAGIC FctsClmExtr
# MAGIC MCSourceClmExtr
# MAGIC MedicaidClmExtr
# MAGIC NascoClmExtr
# MAGIC PcsClmExtr
# MAGIC PCTAClmExtr
# MAGIC WellDyneClmExtr
# MAGIC MedtrakClmExtr
# MAGIC BCBSSCClmExtr
# MAGIC EyeMedClmExtr
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to /key
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmPK
# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_ids_subgrp = """SELECT 
CLM_ID,
SUBGRP_ID,
CLS_ID,
CLS_PLN_ID,
PROD_ID
FROM 
(
SELECT 
DRUG.CLM_ID,
SUBGRP.SUBGRP_ID,
CLS.CLS_ID,
PLN.CLS_PLN_ID,
CMPNT.PROD_ID,
1 as Order
FROM
""" + f"{IDSOwner}" + """.W_DRUG_ENR DRUG,
""" + f"{IDSOwner}" + """.MBR_ENR MBR, 
""" + f"{IDSOwner}" + """.CD_MPPNG MAP1,
""" + f"{IDSOwner}" + """.SUBGRP SUBGRP,
""" + f"{IDSOwner}" + """.CLS CLS, 
""" + f"{IDSOwner}" + """.CLS_PLN PLN, 
""" + f"{IDSOwner}" + """.PROD_CMPNT CMPNT
WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
and DRUG.FILL_DT_SK between MBR.EFF_DT_SK and MBR.TERM_DT_SK
and MBR.ELIG_IN = 'Y'
and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
and MAP1.TRGT_CD IN ('VSN')
and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
and MBR.CLS_SK = CLS.CLS_SK
and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
AND MBR.PROD_SK = CMPNT.PROD_SK

UNION

SELECT 
DRUG.CLM_ID,
SUBGRP.SUBGRP_ID,
CLS.CLS_ID,
PLN.CLS_PLN_ID,
CMPNT.PROD_ID,
2 as Order
FROM
""" + f"{IDSOwner}" + """.W_DRUG_ENR DRUG,
""" + f"{IDSOwner}" + """.MBR_ENR MBR, 
""" + f"{IDSOwner}" + """.CD_MPPNG MAP1,
""" + f"{IDSOwner}" + """.SUBGRP SUBGRP,
""" + f"{IDSOwner}" + """.CLS CLS, 
""" + f"{IDSOwner}" + """.CLS_PLN PLN, 
""" + f"{IDSOwner}" + """.PROD_CMPNT CMPNT
WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
and MBR.ELIG_IN = 'Y'
and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
and MAP1.TRGT_CD IN ('VSN')
and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
and MBR.CLS_SK = CLS.CLS_SK
and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
AND MBR.PROD_SK = CMPNT.PROD_SK

UNION

SELECT 
DRUG.CLM_ID,
SUBGRP.SUBGRP_ID,
CLS.CLS_ID,
PLN.CLS_PLN_ID,
CMPNT.PROD_ID,
3 as Order
FROM
""" + f"{IDSOwner}" + """.W_DRUG_ENR DRUG,
""" + f"{IDSOwner}" + """.MBR_ENR MBR, 
""" + f"{IDSOwner}" + """.CD_MPPNG MAP1,
""" + f"{IDSOwner}" + """.SUBGRP SUBGRP,
""" + f"{IDSOwner}" + """.CLS CLS, 
""" + f"{IDSOwner}" + """.CLS_PLN PLN, 
""" + f"{IDSOwner}" + """.PROD_CMPNT CMPNT
WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
and DRUG.FILL_DT_SK between MBR.EFF_DT_SK and MBR.TERM_DT_SK
and MBR.ELIG_IN = 'N'
and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
and MAP1.TRGT_CD IN ('VSN')
and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
and MBR.CLS_SK = CLS.CLS_SK
and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
AND MBR.PROD_SK = CMPNT.PROD_SK

UNION

SELECT 
DRUG.CLM_ID,
SUBGRP.SUBGRP_ID,
CLS.CLS_ID,
PLN.CLS_PLN_ID,
CMPNT.PROD_ID,
4 as Order
FROM
""" + f"{IDSOwner}" + """.W_DRUG_ENR DRUG,
""" + f"{IDSOwner}" + """.MBR_ENR MBR, 
""" + f"{IDSOwner}" + """.CD_MPPNG MAP1,
""" + f"{IDSOwner}" + """.SUBGRP SUBGRP,
""" + f"{IDSOwner}" + """.CLS CLS, 
""" + f"{IDSOwner}" + """.CLS_PLN PLN, 
""" + f"{IDSOwner}" + """.PROD_CMPNT CMPNT
WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
and MBR.ELIG_IN = 'N'
and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
and MAP1.TRGT_CD IN ('VSN')
and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
and MBR.CLS_SK = CLS.CLS_SK
and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
AND MBR.PROD_SK = CMPNT.PROD_SK
)
ORDER BY
CLM_ID,
ORDER DESC
"""

df_ids_subgrp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids_subgrp)
    .load()
)

df_subgrp_med = dedup_sort(
    df_ids_subgrp,
    partition_cols=["CLM_ID"],
    sort_cols=[]
)

schema_EYEMEDClmLanding = StructType([
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_NO", DecimalType(38,10), False),
    StructField("ADJ_FROM_CLM_ID", StringType(), False),
    StructField("ADJ_TO_CLM_ID", StringType(), False),
    StructField("CLM_STTUS_CD", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("DOB", StringType(), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("SUB_ID_IDS", StringType(), False),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("RCRD_TYP", StringType(), True),
    StructField("ADJ_VOID_FLAG", StringType(), True),
    StructField("EYEMED_GRP_ID", StringType(), True),
    StructField("EYEMED_SUBGRP_ID", StringType(), True),
    StructField("BILL_TYP_IN", StringType(), True),
    StructField("CLM_NO", StringType(), True),
    StructField("LN_CTR", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("INVC_NO", StringType(), True),
    StructField("INVC_DT", StringType(), True),
    StructField("BILL_AMT", StringType(), True),
    StructField("FFS_ADM_FEE", StringType(), True),
    StructField("RTL_AMT", StringType(), True),
    StructField("MBR_OOP", StringType(), True),
    StructField("3RD_PARTY_DSCNT", StringType(), True),
    StructField("COPAY_AMT", StringType(), True),
    StructField("COV_AMT", StringType(), True),
    StructField("FLR_1", StringType(), True),
    StructField("FLR_2", StringType(), True),
    StructField("NTWK_IN", StringType(), True),
    StructField("SVC_CD", StringType(), True),
    StructField("SVC_DESC", StringType(), True),
    StructField("MOD_CD_1", StringType(), True),
    StructField("MOD_CD_2", StringType(), True),
    StructField("MOD_CD_3", StringType(), True),
    StructField("MOD_CD_4", StringType(), True),
    StructField("MOD_CD_5", StringType(), True),
    StructField("MOD_CD_6", StringType(), True),
    StructField("MOD_CD_7", StringType(), True),
    StructField("MOD_CD_8", StringType(), True),
    StructField("ICD_CD_SET", StringType(), True),
    StructField("DIAG_CD_1", StringType(), True),
    StructField("DIAG_CD_2", StringType(), True),
    StructField("DIAG_CD_3", StringType(), True),
    StructField("DIAG_CD_4", StringType(), True),
    StructField("DIAG_CD_5", StringType(), True),
    StructField("DIAG_CD_6", StringType(), True),
    StructField("DIAG_CD_7", StringType(), True),
    StructField("DIAG_CD_8", StringType(), True),
    StructField("PATN_ID", StringType(), True),
    StructField("PATN_SSN", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_MIDINIT", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("PATN_GNDR", StringType(), True),
    StructField("PATN_FMLY_RELSHP", StringType(), True),
    StructField("PATN_DOB", StringType(), True),
    StructField("PATN_ADDR", StringType(), True),
    StructField("PATN_ADDR_2", StringType(), True),
    StructField("PATN_CITY", StringType(), True),
    StructField("PATN_ST", StringType(), True),
    StructField("PATN_ZIP", StringType(), True),
    StructField("PATN_ZIP4", StringType(), True),
    StructField("CLNT_GRP_NO", StringType(), True),
    StructField("CO_CD", StringType(), True),
    StructField("DIV_CD", StringType(), True),
    StructField("LOC_CD", StringType(), True),
    StructField("CLNT_RPTNG_1", StringType(), True),
    StructField("CLNT_RPTNG_2", StringType(), True),
    StructField("CLNT_RPTNG_3", StringType(), True),
    StructField("CLNT_RPTNG_4", StringType(), True),
    StructField("CLNT_RPTNG_5", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("SUB_SSN", StringType(), True),
    StructField("SUB_FIRST_NM", StringType(), True),
    StructField("SUB_MIDINIT", StringType(), True),
    StructField("SUB_LAST_NM", StringType(), True),
    StructField("SUB_GNDR", StringType(), True),
    StructField("SUB_DOB", StringType(), True),
    StructField("SUB_ADDR", StringType(), True),
    StructField("SUB_ADDR_2", StringType(), True),
    StructField("SUB_CITY", StringType(), True),
    StructField("SUB_ST", StringType(), True),
    StructField("SUB_ZIP", StringType(), True),
    StructField("SUB_ZIP_PLUS_4", StringType(), True),
    StructField("PROV_ID", StringType(), True),
    StructField("PROV_NPI", StringType(), True),
    StructField("TAX_ENTY_NPI", StringType(), True),
    StructField("PROV_FIRST_NM", StringType(), True),
    StructField("PROV_LAST_NM", StringType(), True),
    StructField("BUS_NM", StringType(), True),
    StructField("PROV_ADDR", StringType(), True),
    StructField("PROV_ADDR_2", StringType(), True),
    StructField("PROV_CITY", StringType(), True),
    StructField("PROV_ST", StringType(), True),
    StructField("PROV_ZIP", StringType(), True),
    StructField("PROV_ZIP_PLUS_4", StringType(), True),
    StructField("PROF_DSGTN", StringType(), True),
    StructField("TAX_ENTY_ID", StringType(), True),
    StructField("TXNMY_CD", StringType(), True),
    StructField("CLM_RCVD_DT", StringType(), True),
    StructField("ADJDCT_DT", StringType(), True),
    StructField("CHK_DT", StringType(), True),
    StructField("DENIAL_RSN_CD", StringType(), True),
    StructField("SVC_TYP", StringType(), True),
    StructField("UNIT_OF_SVC", StringType(), True),
    StructField("LOC_ID", StringType(), True),
    StructField("SVC_LOC_ID", StringType(), True),
    StructField("FLR", StringType(), True)
])

df_EYEMEDClmLanding = (
    spark.read
    .schema(schema_EYEMEDClmLanding)
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .csv(f"{adls_path}/verified/EyeMedClm_ClaimsLanding.dat.{RunID}")
)

extract_query_ids_mbr = """SELECT MBR.MBR_UNIQ_KEY as MBR_UNIQ_KEY,SUB.SUB_UNIQ_KEY as SUB_UNIQ_KEY 
FROM """ + f"{IDSOwner}" + """.MBR MBR,""" + f"{IDSOwner}" + """.SUB SUB,""" + f"{IDSOwner}" + """.W_DRUG_ENR DRUG 
WHERE DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY AND MBR.SUB_SK = SUB.SUB_SK
"""

df_ids_mbr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids_mbr)
    .load()
)

df_mbr_lkup = dedup_sort(
    df_ids_mbr,
    partition_cols=["MBR_UNIQ_KEY"],
    sort_cols=[]
)

extract_query_ids_sub_alpha_pfx = """SELECT 
SUB_UNIQ_KEY,
ALPHA_PFX_CD 
FROM 
""" + f"{IDSOwner}" + """.MBR MBR,
""" + f"{IDSOwner}" + """.SUB SUB,
""" + f"{IDSOwner}" + """.ALPHA_PFX PFX,
""" + f"{IDSOwner}" + """.W_DRUG_ENR DRUG 
WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
AND MBR.SUB_SK = SUB.SUB_SK 
AND SUB.ALPHA_PFX_SK = PFX.ALPHA_PFX_SK
"""

df_ids_sub_alpha_pfx = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids_sub_alpha_pfx)
    .load()
)

df_sub_alpha_pfx_lkup = dedup_sort(
    df_ids_sub_alpha_pfx,
    partition_cols=["SUB_UNIQ_KEY"],
    sort_cols=[]
)

extract_query_ids_mbr_enroll_med = """SELECT 
DRUG.CLM_ID,
CLS.CLS_ID,
PLN.CLS_PLN_ID,
SUBGRP.SUBGRP_ID,
CAT.EXPRNC_CAT_CD,
LOB.FNCL_LOB_CD,
CMPNT.PROD_ID  
FROM """ + f"{IDSOwner}" + """.W_DRUG_ENR DRUG,
""" + f"{IDSOwner}" + """.MBR_ENR MBR, 
""" + f"{IDSOwner}" + """.CD_MPPNG MAP1,
""" + f"{IDSOwner}" + """.CLS CLS, 
""" + f"{IDSOwner}" + """.SUBGRP SUBGRP,
""" + f"{IDSOwner}" + """.CLS_PLN PLN, 
""" + f"{IDSOwner}" + """.CD_MPPNG MAP2, 
""" + f"{IDSOwner}" + """.PROD_CMPNT CMPNT,
""" + f"{IDSOwner}" + """.PROD_BILL_CMPNT BILL_CMPNT,
""" + f"{IDSOwner}" + """.EXPRNC_CAT CAT,
""" + f"{IDSOwner}" + """.FNCL_LOB LOB 
WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
and DRUG.FILL_DT_SK between MBR.EFF_DT_SK and MBR.TERM_DT_SK
and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
and MAP1.TRGT_CD IN ('VSN')
and MBR.CLS_SK = CLS.CLS_SK
and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
AND MBR.PROD_SK = CMPNT.PROD_SK
AND  CMPNT.PROD_CMPNT_TYP_CD_SK = MAP2.CD_MPPNG_SK
AND  MAP2.TRGT_CD= 'PDBL'
AND  DRUG.FILL_DT_SK between  CMPNT.PROD_CMPNT_EFF_DT_SK AND CMPNT.PROD_CMPNT_TERM_DT_SK
AND  CMPNT.PROD_CMPNT_EFF_DT_SK= (SELECT MAX ( CMPNT2.PROD_CMPNT_EFF_DT_SK ) 
                                  FROM  """ + f"{IDSOwner}" + """.PROD_CMPNT CMPNT2
                                  WHERE CMPNT.PROD_SK = CMPNT2.PROD_SK
                                  AND   CMPNT.PROD_CMPNT_TYP_CD_SK = CMPNT2.PROD_CMPNT_TYP_CD_SK
                                  AND   DRUG.FILL_DT_SK between CMPNT2.PROD_CMPNT_EFF_DT_SK AND CMPNT2.PROD_CMPNT_TERM_DT_SK)
AND  CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT.PROD_CMPNT_PFX_ID
AND  DRUG.FILL_DT_SK between  BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT.PROD_BILL_CMPNT_TERM_DT_SK
AND  BILL_CMPNT.PROD_BILL_CMPNT_ID IN ('VSN')
AND  BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK= (SELECT MAX ( BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK ) 
                                            FROM  """ + f"{IDSOwner}" + """.PROD_BILL_CMPNT BILL_CMPNT2
                                            WHERE   BILL_CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
                                            AND     CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
                                            AND     BILL_CMPNT.PROD_BILL_CMPNT_ID = BILL_CMPNT2.PROD_BILL_CMPNT_ID
                                            AND     BILL_CMPNT2.PROD_BILL_CMPNT_ID IN ('VSN')
                                            AND     DRUG.FILL_DT_SK between BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT2.PROD_BILL_CMPNT_TERM_DT_SK)
AND  BILL_CMPNT.EXPRNC_CAT_SK = CAT.EXPRNC_CAT_SK
AND  BILL_CMPNT.FNCL_LOB_SK = LOB.FNCL_LOB_SK
"""

df_ids_mbr_enroll_med = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids_mbr_enroll_med)
    .load()
)

df_mbr_enr_lkup = dedup_sort(
    df_ids_mbr_enroll_med,
    partition_cols=["CLM_ID"],
    sort_cols=[]
)

df_businessRules_join = df_EYEMEDClmLanding.alias("EYEMED").join(
    df_mbr_lkup.alias("mbr_lkup"),
    F.col("EYEMED.MBR_UNIQ_KEY") == F.col("mbr_lkup.MBR_UNIQ_KEY"),
    "left"
)

df_businessRules = df_businessRules_join.withColumn(
    "svSubCk",
    F.when(F.col("mbr_lkup.MBR_UNIQ_KEY").isNull(), F.col("EYEMED.SUB_UNIQ_KEY")).otherwise(F.col("mbr_lkup.SUB_UNIQ_KEY"))
).withColumn(
    "svMbrAge",
    AGE(F.col("EYEMED.PATN_DOB"), F.col("EYEMED.DT_OF_SVC"))
).withColumn(
    "svClmCnt",
    F.when(F.col("EYEMED.CLM_STTUS_CD") == F.lit("A08"), F.lit(-1)).otherwise(F.lit(1))
)

df_EYEMEDClmTrns = (
    df_businessRules
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"),10," ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"),1," ").alias("DISCARD_IN"),
        F.rpad(F.lit("Y"),1," ").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        F.concat(F.col("SrcSysCd"),F.lit(";"),F.col("EYEMED.CLM_ID")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("EYEMED.CLM_ID").alias("CLM_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("EYEMED.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_SK"),
        F.col("EYEMED.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_SK"),
        F.lit("NA").alias("ALPHA_PFX_SK"),
        F.rpad(F.lit("NA"),3," ").alias("CLM_EOB_EXCD"),
        F.rpad(F.lit("NA"),4," ").alias("CLS"),
        F.rpad(F.lit("NA"),8," ").alias("CLS_PLN"),
        F.rpad(F.lit("NA"),4," ").alias("EXPRNC_CAT"),
        F.rpad(F.lit("NA"),4," ").alias("FNCL_LOB_NO"),
        F.rpad(F.col("EYEMED.GRP_ID"),8," ").alias("GRP"),
        F.col("EYEMED.MBR_UNIQ_KEY").alias("MBR_CK"),
        F.rpad(F.lit("NA"),8," ").alias("PROD"),
        F.rpad(F.lit("NA"),4," ").alias("SUBGRP"),
        F.col("svSubCk").alias("SUB_CK"),
        F.rpad(F.lit("NA"),10," ").alias("CLM_ACDNT_CD"),
        F.rpad(F.lit("NA"),2," ").alias("CLM_ACDNT_ST_CD"),
        F.rpad(F.lit("NA"),10," ").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
        F.rpad(F.lit("NA"),10," ").alias("CLM_AGMNT_SRC_CD"),
        F.rpad(F.lit("NA"),1," ").alias("CLM_BTCH_ACTN_CD"),
        F.rpad(F.lit("N"),10," ").alias("CLM_CAP_CD"),
        F.rpad(F.lit("STD"),10," ").alias("CLM_CAT_CD"),
        F.rpad(F.lit("NA"),1," ").alias("CLM_CHK_CYC_OVRD_CD"),
        F.lit("NA").alias("CLM_COB_CD_SK"),
        F.when(
            F.col("EYEMED.ADJ_VOID_FLAG").isNull() | (F.length(trim(F.col("EYEMED.ADJ_VOID_FLAG"))) == 0),
            F.lit("NULL")
        ).otherwise(F.col("EYEMED.ADJ_VOID_FLAG")).alias("FINL_DISP_CD"),
        F.rpad(F.lit("NA"),1," ").alias("CLM_INPT_METH_CD"),
        F.rpad(F.lit("NA"),10," ").alias("CLM_INPT_SRC_CD"),
        F.rpad(F.lit("NA"),10," ").alias("CLM_IPP_CD"),
        F.rpad(F.col("EYEMED.NTWK_IN"),2," ").alias("CLM_NTWK_STTUS_CD"),
        F.rpad(F.lit("NA"),4," ").alias("CLM_NONPAR_PROV_PFX_CD"),
        F.rpad(F.lit("NA"),1," ").alias("CLM_OTHER_BNF_CD"),
        F.rpad(F.lit("NA"),1," ").alias("CLM_PAYE_CD"),
        F.rpad(F.lit("NA"),4," ").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
        F.rpad(F.lit("NA"),4," ").alias("CLM_SVC_DEFN_PFX_CD"),
        F.when(
            trim(F.col("EYEMED.PROF_DSGTN")) == F.lit("OPT"), F.lit("0084")
        ).when(
            trim(F.col("EYEMED.PROF_DSGTN")) == F.lit("OD"), F.lit("0083")
        ).when(
            trim(F.col("EYEMED.PROF_DSGTN")) == F.lit("MD"), F.lit("0018")
        ).when(
            trim(F.col("EYEMED.PROF_DSGTN")) == F.lit("DO"), F.lit("0018")
        ).otherwise(F.lit("UNK")).alias("CLM_SVC_PROV_SPEC_CD"),
        F.rpad(F.col("EYEMED.PROV_ID"),10," ").alias("CLM_SVC_PROV_TYP_CD"),
        F.when(
            F.col("EYEMED.ADJ_VOID_FLAG").isNull() | (F.length(trim(F.col("EYEMED.ADJ_VOID_FLAG")))==0),
            F.lit("BLANK")
        ).otherwise(F.col("EYEMED.ADJ_VOID_FLAG")).alias("CLM_STTUS_CD"),
        F.rpad(F.lit("NA"),10," ").alias("CLM_SUBMT_BCBS_PLN_CD"),
        F.rpad(F.lit("NA"),10," ").alias("CLM_SUB_BCBS_PLN_CD"),
        F.rpad(F.lit("VSN"),10," ").alias("CLM_SUBTYP_CD"),
        F.rpad(F.lit("VSN"),1," ").alias("CLM_TYP_CD"),
        F.rpad(F.lit("N"),1," ").alias("ATCHMT_IN"),
        F.rpad(F.lit("N"),1," ").alias("CLM_CLNCL_EDIT_CD"),
        F.rpad(F.lit("N"),1," ").alias("COBRA_CLM_IN"),
        F.rpad(F.lit("N"),1," ").alias("FIRST_PASS_IN"),
        F.rpad(F.lit("N"),1," ").alias("HOST_IN"),
        F.rpad(F.lit("N"),1," ").alias("LTR_IN"),
        F.rpad(F.lit("N"),1," ").alias("MCARE_ASG_IN"),
        F.rpad(F.lit("N"),1," ").alias("NOTE_IN"),
        F.rpad(F.lit("N"),1," ").alias("PCA_AUDIT_IN"),
        F.rpad(F.lit("N"),1," ").alias("PCP_SUBMT_IN"),
        F.rpad(F.lit("N"),1," ").alias("PROD_OOA_IN"),
        F.rpad(F.lit("1753-01-01"),10," ").alias("ACDNT_DT"),
        F.when(
            (F.col("EYEMED.CLM_RCVD_DT").isNull()) | (trim(F.col("EYEMED.CLM_RCVD_DT")) == "")
            ,F.lit("1753-01-01")
        ).otherwise(
            F.concat(F.col("EYEMED.CLM_RCVD_DT").substr(F.lit(1),F.lit(4)),F.lit("-"),
                     F.col("EYEMED.CLM_RCVD_DT").substr(F.lit(5),F.lit(2)),F.lit("-"),
                     F.col("EYEMED.CLM_RCVD_DT").substr(F.lit(7),F.lit(2)))
        ).alias("INPT_DT"),
        F.rpad(F.lit("1753-01-01"),10," ").alias("MBR_PLN_ELIG_DT"),
        F.rpad(F.lit("2199-12-31"),10," ").alias("NEXT_RVW_DT"),
        F.when(
            (F.col("EYEMED.CHK_DT").isNull()) | (trim(F.col("EYEMED.CHK_DT"))=="")
            ,F.lit("1753-01-01")
        ).otherwise(
            F.concat(F.col("EYEMED.CHK_DT").substr(F.lit(1),F.lit(4)),F.lit("-"),
                     F.col("EYEMED.CHK_DT").substr(F.lit(5),F.lit(2)),F.lit("-"),
                     F.col("EYEMED.CHK_DT").substr(F.lit(7),F.lit(2)))
        ).alias("PD_DT"),
        F.rpad(F.lit("1753-01-01"),10," ").alias("PAYMT_DRAG_CYC_DT"),
        F.when(
            (F.col("EYEMED.ADJDCT_DT").isNull()) | (trim(F.col("EYEMED.ADJDCT_DT"))=="")
            ,F.lit("1753-01-01")
        ).otherwise(
            F.concat(F.col("EYEMED.ADJDCT_DT").substr(F.lit(1),F.lit(4)),F.lit("-"),
                     F.col("EYEMED.ADJDCT_DT").substr(F.lit(5),F.lit(2)),F.lit("-"),
                     F.col("EYEMED.ADJDCT_DT").substr(F.lit(7),F.lit(2)))
        ).alias("PRCS_DT"),
        F.concat(F.col("EYEMED.CLM_RCVD_DT").substr(F.lit(1),F.lit(4)),F.lit("-"),
                 F.col("EYEMED.CLM_RCVD_DT").substr(F.lit(5),F.lit(2)),F.lit("-"),
                 F.col("EYEMED.CLM_RCVD_DT").substr(F.lit(7),F.lit(2))).alias("RCVD_DT"),
        F.concat(F.col("EYEMED.DT_OF_SVC").substr(F.lit(1),F.lit(4)),F.lit("-"),
                 F.col("EYEMED.DT_OF_SVC").substr(F.lit(5),F.lit(2)),F.lit("-"),
                 F.col("EYEMED.DT_OF_SVC").substr(F.lit(7),F.lit(2))).alias("SVC_STRT_DT"),
        F.concat(F.col("EYEMED.DT_OF_SVC").substr(F.lit(1),F.lit(4)),F.lit("-"),
                 F.col("EYEMED.DT_OF_SVC").substr(F.lit(5),F.lit(2)),F.lit("-"),
                 F.col("EYEMED.DT_OF_SVC").substr(F.lit(7),F.lit(2))).alias("SVC_END_DT"),
        F.rpad(F.lit("1753-01-01"),10," ").alias("SMLR_ILNS_DT"),
        F.when(
            (F.col("EYEMED.CHK_DT").isNull()) | (trim(F.col("EYEMED.CHK_DT"))=="")
            ,F.lit("1753-01-01")
        ).otherwise(
            F.concat(F.col("EYEMED.CHK_DT").substr(F.lit(1),F.lit(4)),F.lit("-"),
                     F.col("EYEMED.CHK_DT").substr(F.lit(5),F.lit(2)),F.lit("-"),
                     F.col("EYEMED.CHK_DT").substr(F.lit(7),F.lit(2)))
        ).alias("STTUS_DT"),
        F.rpad(F.lit("1753-01-01"),10," ").alias("WORK_UNABLE_BEG_DT"),
        F.rpad(F.lit("1753-01-01"),10," ").alias("WORK_UNABLE_END_DT"),
        F.lit(0.00).alias("ACDNT_AMT"),
        F.lit(0.00).alias("ACTL_PD_AMT"),
        F.col("EYEMED.COV_AMT").cast(DecimalType(38,10)).alias("ALLOW_AMT"),
        F.col("EYEMED.3RD_PARTY_DSCNT").cast(DecimalType(38,10)).alias("DSALW_AMT"),
        F.lit(0.00).alias("COINS_AMT"),
        F.lit(0.00).alias("CNSD_CHRG_AMT"),
        F.col("EYEMED.COPAY_AMT").cast(DecimalType(38,10)).alias("COPAY_AMT"),
        F.col("EYEMED.RTL_AMT").cast(DecimalType(38,10)).alias("CHRG_AMT"),
        F.lit(0.00).alias("DEDCT_AMT"),
        F.col("EYEMED.MBR_OOP").cast(DecimalType(38,10)).alias("PAYBL_AMT"),
        F.col("svClmCnt").alias("CLM_CT"),
        F.when(
            (F.col("EYEMED.PATN_DOB").isNull()) | (F.length(F.col("EYEMED.PATN_DOB"))==0),
            F.lit(None)
        ).otherwise(F.col("svMbrAge")).alias("MBR_AGE"),
        F.col("EYEMED.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
        F.col("EYEMED.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
        F.rpad(F.lit("NA"),18," ").alias("DOC_TX_ID"),
        F.lit(None).alias("MCAID_RESUB_NO"),
        F.rpad(F.lit("NA"),12," ").alias("MCARE_ID"),
        F.col("EYEMED.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.lit(None).alias("PATN_ACCT_NO"),
        F.rpad(F.lit("NA"),16," ").alias("PAYMT_REF_ID"),
        F.rpad(F.lit("NA"),12," ").alias("PROV_AGMNT_ID"),
        F.rpad(F.lit("NA"),2," ").alias("RFRNG_PROV_TX"),
        F.rpad(F.col("EYEMED.SUB_ID"),14," ").alias("SUB_ID"),
        F.lit("NA").alias("PCA_TYP_CD_SK"),
        F.lit("NA").alias("REL_PCA_CLM_SK"),
        F.lit("NA").alias("REL_BASE_CLM_SK"),
        F.lit(0.00).alias("REMIT_SUPRSION_AMT"),
        F.lit("NA").alias("MCAID_STTUS_ID"),
        F.col("EYEMED.MBR_OOP").cast(DecimalType(38,10)).alias("PATN_PD_AMT"),
        F.when(
            F.col("EYEMED.ICD_CD_SET") == F.lit("9"), F.lit("ICD9")
        ).when(
            F.col("EYEMED.ICD_CD_SET") == F.lit("0"), F.lit("ICD10")
        ).otherwise(F.lit("UNK")).alias("CLM_SUBMT_ICD_VRSN_CD"),
        F.lit("NA").alias("NTWK_SK"),
        F.lit(None).alias("PRPR_ENTITY"),
        F.lit("NA").alias("CLCL_MICRO_ID"),
        F.rpad(F.lit("N"),1," ").alias("CLM_UPDT_SW"),
        F.col("EYEMED.TXNMY_CD").alias("TXNMY_CD")
    )
)

df_alpha_pfx_join_1 = df_EYEMEDClmTrns.alias("EYEMEDClmTrns").join(
    df_sub_alpha_pfx_lkup.alias("sub_alpha_pfx_lkup"),
    F.col("EYEMEDClmTrns.SUB_CK") == F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY"),
    "left"
)

df_alpha_pfx_join_2 = df_alpha_pfx_join_1.join(
    df_mbr_enr_lkup.alias("mbr_enr_lkup"),
    F.col("EYEMEDClmTrns.CLM_ID") == F.col("mbr_enr_lkup.CLM_ID"),
    "left"
)

df_alpha_pfx_join_3 = df_alpha_pfx_join_2.join(
    df_subgrp_med.alias("subgrpmed"),
    F.col("EYEMEDClmTrns.CLM_ID") == F.col("subgrpmed.CLM_ID"),
    "left"
)

df_Transform = df_alpha_pfx_join_3.select(
    F.col("EYEMEDClmTrns.JOB_EXCTN_RCRD_ERR_SK"),
    F.col("EYEMEDClmTrns.INSRT_UPDT_CD"),
    F.col("EYEMEDClmTrns.DISCARD_IN"),
    F.col("EYEMEDClmTrns.PASS_THRU_IN"),
    F.col("EYEMEDClmTrns.FIRST_RECYC_DT"),
    F.col("EYEMEDClmTrns.ERR_CT"),
    F.col("EYEMEDClmTrns.RECYCLE_CT"),
    F.col("EYEMEDClmTrns.SRC_SYS_CD"),
    F.col("EYEMEDClmTrns.PRI_KEY_STRING"),
    F.col("EYEMEDClmTrns.CLM_SK"),
    F.col("EYEMEDClmTrns.SRC_SYS_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_ID"),
    F.col("EYEMEDClmTrns.CRT_RUN_CYC_EXCTN_SK"),
    F.col("EYEMEDClmTrns.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("EYEMEDClmTrns.ADJ_FROM_CLM_SK"),
    F.col("EYEMEDClmTrns.ADJ_TO_CLM_SK"),
    F.when(F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY").isNull(), F.lit("UNK")).otherwise(F.col("sub_alpha_pfx_lkup.ALPHA_PFX_CD")).alias("ALPHA_PFX_SK"),
    F.col("EYEMEDClmTrns.CLM_EOB_EXCD").alias("CLM_EOB_EXCD_SK"),
    F.when(
        (F.col("EYEMEDClmTrns.CLM_TYP_CD")==F.lit("X")) & (F.col("subgrpmed.CLM_ID").isNull()),
        F.lit("UNK")
    ).otherwise(F.col("subgrpmed.CLS_ID")).alias("CLS_SK"),
    F.when(
        (F.col("EYEMEDClmTrns.CLM_TYP_CD")==F.lit("X")) & (F.col("subgrpmed.CLM_ID").isNull()),
        F.lit("UNK")
    ).otherwise(F.col("subgrpmed.CLS_PLN_ID")).alias("CLS_PLN_SK"),
    F.when(
        (F.col("EYEMEDClmTrns.CLM_TYP_CD")==F.lit("X")) & (F.col("mbr_enr_lkup.CLM_ID").isNull()),
        F.lit("UNK")
    ).otherwise(F.col("mbr_enr_lkup.EXPRNC_CAT_CD")).alias("EXPRNC_CAT_SK"),
    F.when(
        (F.col("EYEMEDClmTrns.CLM_TYP_CD")==F.lit("X")) & (F.col("mbr_enr_lkup.CLM_ID").isNull()),
        F.lit("UNK")
    ).otherwise(F.col("mbr_enr_lkup.FNCL_LOB_CD")).alias("FNCL_LOB_SK"),
    F.col("EYEMEDClmTrns.GRP").alias("GRP_SK"),
    F.col("EYEMEDClmTrns.MBR_CK").alias("MBR_SK"),
    F.col("EYEMEDClmTrns.NTWK_SK"),
    F.when(
        (F.col("EYEMEDClmTrns.CLM_TYP_CD")==F.lit("X")) & (F.col("subgrpmed.CLM_ID").isNull()),
        F.lit("UNK")
    ).otherwise(F.trim(F.col("subgrpmed.PROD_ID"))).alias("PROD_SK"),
    F.when(
        (F.col("EYEMEDClmTrns.CLM_TYP_CD")==F.lit("X")) & (F.col("subgrpmed.CLM_ID").isNull()),
        F.lit("UNK")
    ).otherwise(F.col("subgrpmed.SUBGRP_ID")).alias("SUBGRP_SK"),
    F.col("EYEMEDClmTrns.SUB_CK").alias("SUB_SK"),
    F.col("EYEMEDClmTrns.CLM_ACDNT_CD").alias("CLM_ACDNT_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_ACDNT_ST_CD").alias("CLM_ACDNT_ST_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_ACTIVATING_BCBS_PLN_CD").alias("CLM_ACTV_BCBS_PLN_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_AGMNT_SRC_CD").alias("CLM_AGMNT_SRC_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_BTCH_ACTN_CD").alias("CLM_BTCH_ACTN_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_CAP_CD").alias("CLM_CAP_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_CAT_CD").alias("CLM_CAT_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_CHK_CYC_OVRD_CD").alias("CLM_CHK_CYC_OVRD_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_COB_CD_SK"),
    F.col("EYEMEDClmTrns.FINL_DISP_CD").alias("CLM_FINL_DISP_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_INPT_METH_CD").alias("CLM_INPT_METH_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_INPT_SRC_CD").alias("CLM_INPT_SRC_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_IPP_CD").alias("CLM_INTER_PLN_PGM_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_NONPAR_PROV_PFX_CD").alias("CLM_NONPAR_PROV_PFX_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_OTHER_BNF_CD").alias("CLM_OTHR_BNF_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_PAYE_CD").alias("CLM_PAYE_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_PRCS_CTL_AGNT_PFX_CD").alias("CLM_PRCS_CTL_AGNT_PFX_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_SVC_DEFN_PFX_CD").alias("CLM_SVC_DEFN_PFX_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_SVC_PROV_SPEC_CD").alias("CLM_SVC_PROV_SPEC_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_SVC_PROV_TYP_CD").alias("CLM_SVC_PROV_TYP_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_STTUS_CD").alias("CLM_STTUS_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_SUBMT_BCBS_PLN_CD").alias("CLM_SUBMTTING_BCBS_PLN_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_SUB_BCBS_PLN_CD").alias("CLM_SUB_BCBS_PLN_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD_SK"),
    F.col("EYEMEDClmTrns.CLM_TYP_CD").alias("CLM_TYP_CD_SK"),
    F.col("EYEMEDClmTrns.ATCHMT_IN"),
    F.col("EYEMEDClmTrns.CLM_CLNCL_EDIT_CD").alias("CLNCL_EDIT_IN"),
    F.col("EYEMEDClmTrns.COBRA_CLM_IN"),
    F.col("EYEMEDClmTrns.FIRST_PASS_IN"),
    F.col("EYEMEDClmTrns.HOST_IN"),
    F.col("EYEMEDClmTrns.LTR_IN"),
    F.col("EYEMEDClmTrns.MCARE_ASG_IN"),
    F.col("EYEMEDClmTrns.NOTE_IN"),
    F.col("EYEMEDClmTrns.PCA_AUDIT_IN"),
    F.col("EYEMEDClmTrns.PCP_SUBMT_IN"),
    F.col("EYEMEDClmTrns.PROD_OOA_IN"),
    F.col("EYEMEDClmTrns.ACDNT_DT").alias("ACDNT_DT_SK"),
    F.col("EYEMEDClmTrns.INPT_DT").alias("INPT_DT_SK"),
    F.col("EYEMEDClmTrns.MBR_PLN_ELIG_DT").alias("MBR_PLN_ELIG_DT_SK"),
    F.col("EYEMEDClmTrns.NEXT_RVW_DT").alias("NEXT_RVW_DT_SK"),
    F.col("EYEMEDClmTrns.PD_DT").alias("PD_DT_SK"),
    F.col("EYEMEDClmTrns.PAYMT_DRAG_CYC_DT").alias("PAYMT_DRAG_CYC_DT_SK"),
    F.col("EYEMEDClmTrns.PRCS_DT").alias("PRCS_DT_SK"),
    F.col("EYEMEDClmTrns.RCVD_DT").alias("RCVD_DT_SK"),
    F.col("EYEMEDClmTrns.SVC_STRT_DT").alias("SVC_STRT_DT_SK"),
    F.col("EYEMEDClmTrns.SVC_END_DT").alias("SVC_END_DT_SK"),
    F.col("EYEMEDClmTrns.SMLR_ILNS_DT").alias("SMLR_ILNS_DT_SK"),
    F.col("EYEMEDClmTrns.STTUS_DT").alias("STTUS_DT_SK"),
    F.col("EYEMEDClmTrns.WORK_UNABLE_BEG_DT").alias("WORK_UNABLE_BEG_DT_SK"),
    F.col("EYEMEDClmTrns.WORK_UNABLE_END_DT").alias("WORK_UNABLE_END_DT_SK"),
    F.col("EYEMEDClmTrns.ACDNT_AMT"),
    F.col("EYEMEDClmTrns.ACTL_PD_AMT"),
    F.col("EYEMEDClmTrns.ALLOW_AMT").alias("ALW_AMT"),
    F.col("EYEMEDClmTrns.DSALW_AMT"),
    F.col("EYEMEDClmTrns.COINS_AMT"),
    F.col("EYEMEDClmTrns.CNSD_CHRG_AMT"),
    F.col("EYEMEDClmTrns.COPAY_AMT"),
    F.col("EYEMEDClmTrns.CHRG_AMT"),
    F.col("EYEMEDClmTrns.DEDCT_AMT"),
    F.col("EYEMEDClmTrns.PAYBL_AMT"),
    F.col("EYEMEDClmTrns.CLM_CT"),
    F.col("EYEMEDClmTrns.MBR_AGE"),
    F.col("EYEMEDClmTrns.ADJ_FROM_CLM_ID"),
    F.col("EYEMEDClmTrns.ADJ_TO_CLM_ID"),
    F.col("EYEMEDClmTrns.DOC_TX_ID"),
    F.col("EYEMEDClmTrns.MCAID_RESUB_NO"),
    F.col("EYEMEDClmTrns.MCARE_ID"),
    F.col("EYEMEDClmTrns.MBR_SFX_NO"),
    F.col("EYEMEDClmTrns.PATN_ACCT_NO"),
    F.col("EYEMEDClmTrns.PAYMT_REF_ID"),
    F.col("EYEMEDClmTrns.PROV_AGMNT_ID"),
    F.col("EYEMEDClmTrns.RFRNG_PROV_TX"),
    F.col("EYEMEDClmTrns.SUB_ID"),
    F.col("EYEMEDClmTrns.PRPR_ENTITY"),
    F.col("EYEMEDClmTrns.PCA_TYP_CD_SK"),
    F.col("EYEMEDClmTrns.REL_PCA_CLM_SK"),
    F.col("EYEMEDClmTrns.REL_BASE_CLM_SK"),
    F.col("EYEMEDClmTrns.REMIT_SUPRSION_AMT"),
    F.col("EYEMEDClmTrns.MCAID_STTUS_ID"),
    F.col("EYEMEDClmTrns.PATN_PD_AMT"),
    F.col("EYEMEDClmTrns.CLM_SUBMT_ICD_VRSN_CD").alias("CLM_SUBMT_ICD_VRSN_CD_SK"),
    F.col("EYEMEDClmTrns.PRPR_ENTITY").alias("PRPR_ENTITY"),
    F.col("EYEMEDClmTrns.CLCL_MICRO_ID"),
    F.col("EYEMEDClmTrns.CLM_UPDT_SW"),
    F.col("EYEMEDClmTrns.TXNMY_CD").alias("CLM_TXNMY_CD")
)

df_Snapshot = df_Transform

df_Pkey = df_Snapshot.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"),18," ").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("ADJ_FROM_CLM_SK"),12," ").alias("ADJ_FROM_CLM"),
    F.rpad(F.col("ADJ_TO_CLM_SK"),12," ").alias("ADJ_TO_CLM"),
    F.rpad(F.col("ALPHA_PFX_SK"),3," ").alias("ALPHA_PFX_CD"),
    F.rpad(F.col("CLM_EOB_EXCD_SK"),3," ").alias("CLM_EOB_EXCD"),
    F.rpad(F.col("CLS_SK"),4," ").alias("CLS"),
    F.rpad(F.col("CLS_PLN_SK"),8," ").alias("CLS_PLN"),
    F.rpad(F.col("EXPRNC_CAT_SK"),4," ").alias("EXPRNC_CAT"),
    F.rpad(F.col("FNCL_LOB_SK"),4," ").alias("FNCL_LOB_NO"),
    F.rpad(F.col("GRP_SK"),8," ").alias("GRP"),
    F.col("MBR_SK").alias("MBR_CK"),
    F.rpad(F.col("NTWK_SK"),12," ").alias("NTWK"),
    F.rpad(F.col("PROD_SK"),8," ").alias("PROD"),
    F.rpad(F.col("SUBGRP_SK"),4," ").alias("SUBGRP"),
    F.col("SUB_SK").alias("SUB_CK"),
    F.rpad(F.col("CLM_ACDNT_CD_SK"),10," ").alias("CLM_ACDNT_CD"),
    F.rpad(F.col("CLM_ACDNT_ST_CD_SK"),2," ").alias("CLM_ACDNT_ST_CD"),
    F.rpad(F.col("CLM_ACTV_BCBS_PLN_CD_SK"),10," ").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    F.rpad(F.col("CLM_AGMNT_SRC_CD_SK"),10," ").alias("CLM_AGMNT_SRC_CD"),
    F.rpad(F.col("CLM_BTCH_ACTN_CD_SK"),1," ").alias("CLM_BTCH_ACTN_CD"),
    F.rpad(F.col("CLM_CAP_CD_SK"),10," ").alias("CLM_CAP_CD"),
    F.rpad(F.col("CLM_CAT_CD_SK"),10," ").alias("CLM_CAT_CD"),
    F.rpad(F.col("CLM_CHK_CYC_OVRD_CD_SK"),1," ").alias("CLM_CHK_CYC_OVRD_CD"),
    F.rpad(F.col("CLM_COB_CD_SK"),1," ").alias("CLM_COB_CD"),
    F.col("CLM_FINL_DISP_CD_SK").alias("FINL_DISP_CD"),
    F.rpad(F.col("CLM_INPT_METH_CD_SK"),1," ").alias("CLM_INPT_METH_CD"),
    F.rpad(F.col("CLM_INPT_SRC_CD_SK"),10," ").alias("CLM_INPT_SRC_CD"),
    F.rpad(F.col("CLM_INTER_PLN_PGM_CD_SK"),10," ").alias("CLM_IPP_CD"),
    F.rpad(F.col("CLM_NTWK_STTUS_CD_SK"),2," ").alias("CLM_NTWK_STTUS_CD"),
    F.rpad(F.col("CLM_NONPAR_PROV_PFX_CD_SK"),4," ").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.rpad(F.col("CLM_OTHR_BNF_CD_SK"),1," ").alias("CLM_OTHER_BNF_CD"),
    F.rpad(F.col("CLM_PAYE_CD_SK"),1," ").alias("CLM_PAYE_CD"),
    F.rpad(F.col("CLM_PRCS_CTL_AGNT_PFX_CD_SK"),4," ").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.rpad(F.col("CLM_SVC_DEFN_PFX_CD_SK"),4," ").alias("CLM_SVC_DEFN_PFX_CD"),
    F.rpad(F.col("CLM_SVC_PROV_SPEC_CD_SK"),10," ").alias("CLM_SVC_PROV_SPEC_CD"),
    F.rpad(F.col("CLM_SVC_PROV_TYP_CD_SK"),10," ").alias("CLM_SVC_PROV_TYP_CD"),
    F.rpad(F.col("CLM_STTUS_CD_SK"),2," ").alias("CLM_STTUS_CD"),
    F.rpad(F.col("CLM_SUBMTTING_BCBS_PLN_CD_SK"),10," ").alias("CLM_SUBMT_BCBS_PLN_CD"),
    F.rpad(F.col("CLM_SUB_BCBS_PLN_CD_SK"),10," ").alias("CLM_SUB_BCBS_PLN_CD"),
    F.rpad(F.col("CLM_SUBTYP_CD_SK"),10," ").alias("CLM_SUBTYP_CD"),
    F.rpad(F.col("CLM_TYP_CD_SK"),1," ").alias("CLM_TYP_CD"),
    F.col("ATCHMT_IN"),
    F.rpad(F.col("CLNCL_EDIT_IN"),1," ").alias("CLM_CLNCL_EDIT_CD"),
    F.rpad(F.col("COBRA_CLM_IN"),1," ").alias("COBRA_CLM_IN"),
    F.rpad(F.col("FIRST_PASS_IN"),1," ").alias("FIRST_PASS_IN"),
    F.rpad(F.col("HOST_IN"),1," ").alias("HOST_IN"),
    F.rpad(F.col("LTR_IN"),1," ").alias("LTR_IN"),
    F.rpad(F.col("MCARE_ASG_IN"),1," ").alias("MCARE_ASG_IN"),
    F.rpad(F.col("NOTE_IN"),1," ").alias("NOTE_IN"),
    F.rpad(F.col("PCA_AUDIT_IN"),1," ").alias("PCA_AUDIT_IN"),
    F.rpad(F.col("PCP_SUBMT_IN"),1," ").alias("PCP_SUBMT_IN"),
    F.rpad(F.col("PROD_OOA_IN"),1," ").alias("PROD_OOA_IN"),
    F.rpad(F.col("ACDNT_DT_SK"),10," ").alias("ACDNT_DT"),
    F.rpad(F.col("INPT_DT_SK"),10," ").alias("INPT_DT"),
    F.rpad(F.col("MBR_PLN_ELIG_DT_SK"),10," ").alias("MBR_PLN_ELIG_DT"),
    F.rpad(F.col("NEXT_RVW_DT_SK"),10," ").alias("NEXT_RVW_DT"),
    F.rpad(F.col("PD_DT_SK"),10," ").alias("PD_DT"),
    F.rpad(F.col("PAYMT_DRAG_CYC_DT_SK"),10," ").alias("PAYMT_DRAG_CYC_DT"),
    F.rpad(F.col("PRCS_DT_SK"),10," ").alias("PRCS_DT"),
    F.rpad(F.col("RCVD_DT_SK"),10," ").alias("RCVD_DT"),
    F.rpad(F.col("SVC_STRT_DT_SK"),10," ").alias("SVC_STRT_DT"),
    F.rpad(F.col("SVC_END_DT_SK"),10," ").alias("SVC_END_DT"),
    F.rpad(F.col("SMLR_ILNS_DT_SK"),10," ").alias("SMLR_ILNS_DT"),
    F.rpad(F.col("STTUS_DT_SK"),10," ").alias("STTUS_DT"),
    F.rpad(F.col("WORK_UNABLE_BEG_DT_SK"),10," ").alias("WORK_UNABLE_BEG_DT"),
    F.rpad(F.col("WORK_UNABLE_END_DT_SK"),10," ").alias("WORK_UNABLE_END_DT"),
    F.col("ACDNT_AMT"),
    F.col("ACTL_PD_AMT"),
    F.col("ALW_AMT").alias("ALLOW_AMT"),
    F.col("DSALW_AMT"),
    F.col("COINS_AMT"),
    F.col("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT"),
    F.col("CHRG_AMT"),
    F.col("DEDCT_AMT"),
    F.col("PAYBL_AMT"),
    F.col("CLM_CT"),
    F.col("MBR_AGE"),
    F.col("ADJ_FROM_CLM_ID"),
    F.col("ADJ_TO_CLM_ID"),
    F.rpad(F.col("DOC_TX_ID"),18," ").alias("DOC_TX_ID"),
    F.rpad(F.col("MCAID_RESUB_NO"),15," ").alias("MCAID_RESUB_NO"),
    F.rpad(F.col("MCARE_ID"),12," ").alias("MCARE_ID"),
    F.rpad(F.col("MBR_SFX_NO"),2," ").alias("MBR_SFX_NO"),
    F.col("PATN_ACCT_NO"),
    F.rpad(F.col("PAYMT_REF_ID"),16," ").alias("PAYMT_REF_ID"),
    F.rpad(F.col("PROV_AGMNT_ID"),12," ").alias("PROV_AGMNT_ID"),
    F.col("RFRNG_PROV_TX"),
    F.rpad(F.col("SUB_ID"),14," ").alias("SUB_ID"),
    F.rpad(F.col("PRPR_ENTITY"),1," ").alias("PRPR_ENTITY"),
    F.rpad(F.col("PCA_TYP_CD_SK"),18," ").alias("PCA_TYP_CD"),
    F.col("REL_PCA_CLM_SK").alias("REL_PCA_CLM_ID"),
    F.col("CLCL_MICRO_ID"),
    F.rpad(F.col("CLM_UPDT_SW"),1," ").alias("CLM_UPDT_SW"),
    F.col("REMIT_SUPRSION_AMT"),
    F.col("MCAID_STTUS_ID"),
    F.col("PATN_PD_AMT"),
    F.col("CLM_SUBMT_ICD_VRSN_CD_SK").alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.col("CLM_TXNMY_CD"),
    F.rpad(F.lit("N"),1," ").alias("BILL_PAYMT_EXCL_IN")
)

df_SnapshotOut = df_Snapshot.select(
    F.rpad(F.col("CLM_ID"),12," ").alias("CLM_ID"),
    F.col("MBR_SK").alias("MBR_CK"),
    F.rpad(F.col("GRP_SK"),8," ").alias("GRP"),
    F.col("SVC_STRT_DT_SK").alias("SVC_STRT_DT"),
    F.col("CHRG_AMT"),
    F.col("PAYBL_AMT"),
    F.rpad(F.col("EXPRNC_CAT_SK"),4," ").alias("EXPRNC_CAT"),
    F.rpad(F.col("FNCL_LOB_SK"),4," ").alias("FNCL_LOB_NO"),
    F.col("CLM_CT"),
    F.rpad(F.col("PCA_TYP_CD_SK"),18," ").alias("PCA_TYP_CD"),
    F.rpad(F.col("CLM_STTUS_CD_SK"),2," ").alias("CLM_STTUS_CD"),
    F.rpad(F.col("CLM_CAT_CD_SK"),10," ").alias("CLM_CAT_CD")
)

params_ClmPK = {
    "CurrRunCycle": RunCycle
}
df_Key = ClmPK(df_Pkey, params_ClmPK)

final_df_Key = df_Key.select(
    [F.col(c) for c in df_Key.columns]
)

write_files(
    final_df_Key,
    f"{adls_path}/key/EyeMedClmExtr.EyeMedClm.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Transformer = df_SnapshotOut

df_Transformer_enriched = df_Transformer.withColumn(
    "ExpCatCdSk",
    GetFkeyExprncCat(F.lit("FACETS"),F.lit(0),F.col("EXPRNC_CAT"),F.lit("N"))
).withColumn(
    "GrpSk",
    GetFkeyGrp(F.lit("FACETS"),F.lit(0),F.col("GRP"),F.lit("N"))
).withColumn(
    "MbrSk",
    GetFkeyMbr(F.lit("FACETS"),F.lit(0),F.col("MBR_CK"),F.lit("N"))
).withColumn(
    "FnclLobSk",
    GetFkeyFnclLob(F.lit("PSI"),F.lit(0),F.col("FNCL_LOB_NO"),F.lit("N"))
).withColumn(
    "PcaTypCdSk",
    GetFkeyCodes(F.lit("FACETS"),F.lit(0),F.lit("PERSONAL CARE ACCOUNT PROCESSING"),F.col("PCA_TYP_CD"),F.lit("N"))
).withColumn(
    "ClmSttusCdSk",
    GetFkeyCodes(F.col("SrcSysCd"),F.lit(0),F.lit("CLAIM STATUS"),F.col("CLM_STTUS_CD"),F.lit("X"))
).withColumn(
    "ClmCatCdSk",
    GetFkeyCodes(F.col("SrcSysCd"),F.lit(0),F.lit("CLAIM CATEGORY"),F.col("CLM_CAT_CD"),F.lit("X"))
)

df_RowCount = df_Transformer_enriched.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("ClmSttusCdSk").alias("CLM_STTUS_CD_SK"),
    F.col("ClmCatCdSk").alias("CLM_CAT_CD_SK"),
    F.col("ExpCatCdSk").alias("EXPRNC_CAT_SK"),
    F.col("FnclLobSk").alias("FNCL_LOB_SK"),
    F.col("GrpSk").alias("GRP_SK"),
    F.col("MbrSk").alias("MBR_SK"),
    F.rpad(F.col("SVC_STRT_DT"),10," ").alias("SVC_STRT_DT_SK"),
    F.col("CHRG_AMT"),
    F.col("PAYBL_AMT"),
    F.col("CLM_CT"),
    F.col("PcaTypCdSk").alias("PCA_TYP_CD_SK")
)

final_df_RowCount = df_RowCount.select(
    [F.col(c) for c in df_RowCount.columns]
)

write_files(
    final_df_RowCount,
    f"{adls_path}/load/B_CLM.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)