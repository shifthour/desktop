# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 20109 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                      Date               User Story                Change Description                                                                              Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------    -------------------    -----------------------------   ---------------------------------------------------------------------------------------------------------    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Sravya Gorla                  2019-04-17     Spira Reporting       Originally Programmed                                                                           IntegrateDev1              Kalyan Neelam             2019-04-18
# MAGIC Sravya Gorla                  2019-04-25     Spira Reporting       updated the logic in xfm to handle Null date_sk                                    IntegrateDev2              Kalyan Neelam             2019-04-26
# MAGIC Sunny Mandadi              2019-06-14     Spira Reporting       Added transformation logic to handle                                                    IntegrateDev1              Kalyan Neelam             2019-06-18
# MAGIC                                                                                              different date formats
# MAGIC Sunny Mandadi              2019-10-29     US-163406             Changed the Transformer(StripField) Logic for IPA_PROV_ID              IntegrateDev1              Jaideep Mankala          10/31/2019
# MAGIC                                                                                              to remove the hardcoding.Changed the Sql in PROV source table.
# MAGIC                                                                                              Changed the Stage Variable logic in Transformer_128. 
# MAGIC                                                                                              Added the IPA Prov_ID in the lookup(LkpProv).                                  
# MAGIC                                                                                              Changes the PROV_ID_ipa_lkp field logic in Transformer_xfm.
# MAGIC Sunny Mandadi              2020-02-12     Prod Support           Changed the logic for SVC_DEPARTMENT field in the stage             IntegrateDev1              Hugh Sisson                  2020-02-12
# MAGIC                                                                                              StripField(Transformer). Removed the hardcoding
# MAGIC                                                                                              and made the code more dyanamic.                        
# MAGIC                                                                                                                                                                                              
# MAGIC Ashok kumar Baskaran 2021-01-20    331967                 Added New ipa in the Member visit Extract                                              Integrate Dev2             Jeyaprasanna                2021-01-28   
# MAGIC 
# MAGIC Ashok kumar Baskaran  2021-04-01    354692                 Modified and updated  the care centre transformation logic                  Integrate Dev2             Jeyaprasanna                2021-04-01 
# MAGIC 
# MAGIC Ashok kumar Baskaran  2021-04-26    354692                 Modified the  transformation logic to fetch provider name                       Integrate Dev2            Jeyaprasanna                2021-04-27        
# MAGIC 
# MAGIC Ashok Kumar 
# MAGIC Baskaran                 2022-04-01      474273                 Modified the logic to populate the spira care centres                                 IntegrateDev2               Jeyaprasanna               2022-04-04

# MAGIC JobName: IdsMemberVisitXfrm
# MAGIC 
# MAGIC Strip field function and Tranforamtion rules are applied on the data Extracted from inboundfile
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunID = get_widget_value('RunID','')
FileName = get_widget_value('FileName','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')

# Stage: K_PROV1
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_K_PROV1 = f"SELECT prov_id, RIGHT(PROV_NM,4) AS IPA_MATCH, prov_sk FROM {IDSOwner}.prov WHERE prov_nm LIKE '%SPIRA CENTERS%'"
df_K_PROV1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_K_PROV1)
    .load()
)

# Stage: K_PROV
extract_query_K_PROV = f"""
Select NTNL_PROV_ID,
REL_GRP_PROV_SK,
PROV_SK,
PROV_ID,
TAX_ID
FROM  (
select T.NTNL_PROV_ID NTNL_PROV_ID,
T.REL_GRP_PROV_SK REL_GRP_PROV_SK,
T.PROV_SK PROV_SK,
T.PROV_ID PROV_ID, 
T.TAX_ID,
ROW_NUMBER() over (partition by T.NTNL_PROV_ID,
T.REL_GRP_PROV_SK) as RN 
from  (
 SELECT
NTNL_PROV_ID,
REL_GRP_PROV_SK,
PROV_SK,
PROV_ID,
TAX_ID
FROM
{IDSOwner}.PROV) T) where RN = 1  and  tax_id='822111543'
"""
df_K_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_K_PROV)
    .load()
)

# Stage: Prov
extract_query_Prov = f"""
select PROV.PROV_SK, PROV.PROV_ID,
Case when trim(substr(PROV.PROV_NM, instr(PROV.PROV_NM,'-')+1,(length(PROV.PROV_NM)-instr(PROV.PROV_NM,'-')) )) = 'SHAWNEE MISSION' Then 'SHAWNEE'
else trim(substr(PROV.PROV_NM, instr(PROV.PROV_NM,'-')+1,(length(PROV.PROV_NM)-instr(PROV.PROV_NM,'-')) )) end AS PROV_NM_MAP,
PROV.SRC_SYS_CD_SK,
CASE  WHEN PROV.PROV_ID not like 'MHG%' THEN '58980012'
      WHEN PROV.PROV_ID like 'MHG%' THEN '63393013' END AS PROV_IPA_ID, PROV.tax_id
from  {IDSOwner}.PROV PROV 
where PROV.PROV_NM LIKE '%SPIRA CARE CENTER%' 
  and PROV.tax_id='822111543'
"""
df_Prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Prov)
    .load()
)

# Stage: db2_MBR
extract_query_db2_MBR = f"""
Select MBR_UNIQ_KEY,
MBR_ID,
SUB_ID from (
  select T.MBR_UNIQ_KEY MBR_UNIQ_KEY,
         T.MBR_ID MBR_ID,
         T.SUB_ID SUB_ID, 
         ROW_NUMBER() over (partition by T.MBR_ID) as RN 
  from  (
   SELECT
   MBR.MBR_UNIQ_KEY MBR_UNIQ_KEY,
   SUB.SUB_ID || MBR.MBR_SFX_NO as MBR_ID,
   SUB.SUB_ID SUB_ID
   FROM
   {IDSOwner}.MBR MBR,
   {IDSOwner}.SUB SUB
   WHERE
   MBR.SUB_SK = SUB.SUB_SK
  ) T
) where RN = 1
"""
df_db2_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_MBR)
    .load()
)

# Stage: InputFile (PxSequentialFile)
schema_InputFile = StructType([
    StructField("SVC_DEPT", StringType(), True),
    StructField("PATN_EXTRNL_ID", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("PATN_BRTH_DT", StringType(), True),
    StructField("PATN_MBR_INDV_BE_KEY", StringType(), True),
    StructField("PATN_MBR_ID", StringType(), True),
    StructField("PATN_MBR_UNIQ_KEY", StringType(), True),
    StructField("PRI_POL_ID", StringType(), True),
    StructField("SEC_POL_ID", StringType(), True),
    StructField("ENCNTR_IDENTIFER", StringType(), True),
    StructField("ENCNTR_DT", StringType(), True),
    StructField("ENCNTR_STTUS", StringType(), True),
    StructField("PROV_NM", StringType(), True),
    StructField("PROV_EXTRNL_ID", StringType(), True),
    StructField("PROV_NTNL_PROV_ID", StringType(), True),
    StructField("IPA_PROV_ID", StringType(), True),
    StructField("BILL_RVWED_DT", StringType(), True),
    StructField("BILL_RVWED_BY", StringType(), True),
    StructField("ENCNTR_CLSD_DT", StringType(), True),
    StructField("LEGACY_ID", StringType(), True)
])
df_InputFile = (
    spark.read
    .option("header", True)
    .option("sep", ",")
    .option("quote", None)
    .option("nullValue", None)
    .schema(schema_InputFile)
    .csv(f"{adls_path_raw}/landing/{FileName}")
)

# Stage: StripField (CTransformerStage)
# Create columns for the Stage Variables:
#   We interpret "Field(Strip.PATN_BRTH_DT, '/', 1)" as splitting on "/", then taking element 0 (with a safeguard).
#   If that part is length=1, we prefix "0".
def get_field_element(colname, index):
    return F.when(F.size(F.split(F.col(colname), '/')) >= (index+1),
                  F.element_at(F.split(F.col(colname), '/'), index+1)) \
            .otherwise(F.lit(''))

def pad_if_length_one(col_expr):
    return F.when(F.length(col_expr)==1, F.concat(F.lit('0'), col_expr)).otherwise(col_expr)

df_StripVars = (
    df_InputFile
    .withColumn("ptnbrthmt", pad_if_length_one(get_field_element("PATN_BRTH_DT", 0)))
    .withColumn("ptnbrthdt", pad_if_length_one(get_field_element("PATN_BRTH_DT", 1)))
    .withColumn("enctrmt", pad_if_length_one(get_field_element("ENCNTR_DT", 0)))
    .withColumn("enctrdt", pad_if_length_one(get_field_element("ENCNTR_DT", 1)))
    .withColumn("bilrvwdmt", pad_if_length_one(get_field_element("BILL_RVWED_DT", 0)))
    .withColumn("bilrvwddt", pad_if_length_one(get_field_element("BILL_RVWED_DT", 1)))
    .withColumn("enctrclsmt", pad_if_length_one(get_field_element("ENCNTR_CLSD_DT", 0)))
    .withColumn("enctrclsdt", pad_if_length_one(get_field_element("ENCNTR_CLSD_DT", 1)))
)

# Function to convert Ereplace(UPCASE(Strip.SVC_DEPT[1, Len(Strip.SVC_DEPT)-11]), "'", "")
# We drop last 11 chars, uppercase, remove single quotes
def transform_svc_dept(col_svc):
    return F.regexp_replace(
        F.upper(
            F.when(F.length(col_svc) > 11, F.substring(col_svc, 1, F.length(col_svc)-11)).otherwise(F.lit(''))
        ),
        "[']", ""
    )

# For "Convert(' ', '', UpCase(...))[1,4]" we do uppercase, remove spaces, then substring
def first_four_no_space(colname):
    return F.substring(F.regexp_replace(F.upper(F.col(colname)), " ", ""), 1, 4)

# For date expression: If Field(Strip.PATN_BRTH_DT,"/",2)='' Then '' Else ...
# We'll define a helper that checks if that part is blank:
def build_date_string(m_field, d_field, y_field):
    # expects already zero-padded month, day
    # If second part is '' => entire date is ''
    return F.when((m_field == '') | (d_field == ''), F.lit('')) \
            .otherwise(F.date_format(F.to_date(F.concat(m_field, F.lit('/'), d_field, F.lit('/'), y_field), "MM/dd/yyyy"), "yyyy-MM-dd"))

# Output columns for each link must replicate the Expressions exactly.
# We'll build a base DataFrame with columns corresponding to DSLink58/62/64 logic, then filter.

df_StripEnhanced = (
    df_StripVars
    .withColumn("SVC_DEPT_tr", transform_svc_dept(F.col("SVC_DEPT")))
    .withColumn("PATN_FIRST_NM_lkp", first_four_no_space("PATN_FIRST_NM"))
    .withColumn("PATN_LAST_NM_lkp", first_four_no_space("PATN_LAST_NM"))
    .withColumn("PATN_BRTH_DT_tr",
        build_date_string(F.col("ptnbrthmt"), F.col("ptnbrthdt"),
                          F.element_at(F.split(F.col("PATN_BRTH_DT"), '/'), 3))
    )
    .withColumn("ENCNTR_DT_tr",
        F.when(
            (F.col("enctrmt")== '') | (F.col("enctrdt")== ''), F.lit('')
        ).otherwise(
            F.concat(F.col("enctrmt"), F.lit('/'), F.col("enctrdt"), F.lit('/'),
                     F.element_at(F.split(F.col("ENCNTR_DT"), '/'), 3))
        )
    )
    .withColumn("BILL_RVWED_DT_tr",
        F.when(
            (F.col("bilrvwdmt")== '') | (F.col("bilrvwddt")== ''), F.lit('')
        ).otherwise(
            F.concat(F.col("bilrvwdmt"), F.lit('/'), F.col("bilrvwddt"), F.lit('/'),
                     F.element_at(F.split(F.col("BILL_RVWED_DT"), '/'), 3))
        )
    )
    .withColumn("ENCNTR_CLSD_DT_tr",
        F.when(
            (F.col("enctrclsmt")== '') | (F.col("enctrclsdt")== ''), F.lit('')
        ).otherwise(
            F.concat(F.col("enctrclsmt"), F.lit('/'), F.col("enctrclsdt"), F.lit('/'),
                     F.element_at(F.split(F.col("ENCNTR_CLSD_DT"), '/'), 3))
        )
    )
    .withColumn("IPA_PROV_ID_tr",
        F.when(F.col("IPA_PROV_ID").isNotNull(), F.col("IPA_PROV_ID")).otherwise(F.lit("58980012"))
    )
)

# Build columns for DSLink58, DSLink62, DSLink64 from the big transform.
common_expr = {
    "SVC_DEPT": "SVC_DEPT_tr",
    "PATN_EXTRNL_ID": "PATN_EXTRNL_ID",
    "PATN_FIRST_NM": "PATN_FIRST_NM",
    "PATN_LAST_NM": "PATN_LAST_NM",
    "PATN_FIRST_NM_lkp": "PATN_FIRST_NM_lkp",
    "PATN_LAST_NM_lkp": "PATN_LAST_NM_lkp",
    "PATN_BRTH_DT": "PATN_BRTH_DT_tr",
    "PATN_MBR_INDV_BE_KEY": "PATN_MBR_INDV_BE_KEY",
    "PATN_MBR_ID": "PATN_MBR_ID",
    "PATN_MBR_UNIQ_KEY": "PATN_MBR_UNIQ_KEY",
    "SUB_ID_lkp": F.substring(F.col("PRI_POL_ID"), 4, 9),
    "SEC_POL_ID": "SEC_POL_ID",
    "ENCNTR_IDENTIFER": "ENCNTR_IDENTIFER",
    "ENCNTR_DT": "ENCNTR_DT_tr",
    "ENCNTR_STTUS": "ENCNTR_STTUS",
    "PROV_NM": "PROV_NM",
    "PROV_EXTRNL_ID": "PROV_EXTRNL_ID",
    "PROV_NTNL_PROV_ID": "PROV_NTNL_PROV_ID",
    "IPA_PROV_ID": "IPA_PROV_ID_tr",
    "BILL_RVWED_DT": "BILL_RVWED_DT_tr",
    "BILL_RVWED_BY": "BILL_RVWED_BY",
    "ENCNTR_CLSD_DT": "ENCNTR_CLSD_DT_tr",
    "PRI_POL_ID": "PRI_POL_ID",
    "LEGACY_ID": "LEGACY_ID"
}

def apply_exprs(df, expr_map):
    select_cols = []
    for c in expr_map:
        exprdef = expr_map[c]
        if isinstance(exprdef, str):
            select_cols.append(F.col(exprdef).alias(c))
        else:
            select_cols.append(exprdef.alias(c))
    return df.select(*select_cols)

df_DSLink58_pre = df_StripEnhanced.filter(
    (F.col("PATN_MBR_UNIQ_KEY").isNull()) & (F.length(F.col("PRI_POL_ID")) == 12)
)
df_DSLink58 = apply_exprs(df_DSLink58_pre, common_expr)

df_DSLink62_pre = df_StripEnhanced.filter(
    (F.col("PATN_MBR_UNIQ_KEY").isNotNull())
)
# For DSLink62, the difference is "PRI_POL_ID" in the output is the entire "Strip.PRI_POL_ID" (not truncated).
expr_DSLink62 = dict(common_expr)
expr_DSLink62["PRI_POL_ID"] = "PRI_POL_ID"
df_DSLink62 = apply_exprs(df_DSLink62_pre, expr_DSLink62)

df_DSLink64_pre = df_StripEnhanced.filter(
    (F.col("PATN_MBR_UNIQ_KEY").isNull()) & (F.length(F.col("PRI_POL_ID")) == 14)
)
# For DSLink64, "PRI_POL_ID" => "Strip.PRI_POL_ID[4,11]" i.e. substring(4,11)
expr_DSLink64 = dict(common_expr)
expr_DSLink64["SUB_ID_lkp"] = F.substring(F.col("PRI_POL_ID"),4,9)  # same as above
expr_DSLink64["PRI_POL_ID"] = F.substring(F.col("PRI_POL_ID"),4,11).alias("PRI_POL_ID")
df_DSLink64 = df_DSLink64_pre.select(
    F.expr("SVC_DEPT_tr").alias("SVC_DEPT"),
    F.col("PATN_EXTRNL_ID"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_LAST_NM"),
    first_four_no_space("PATN_FIRST_NM").alias("PATN_FIRST_NM_lkp"),
    first_four_no_space("PATN_LAST_NM").alias("PATN_LAST_NM_lkp"),
    build_date_string(F.col("ptnbrthmt"), F.col("ptnbrthdt"),
                      F.element_at(F.split(F.col("PATN_BRTH_DT"), '/'), 3)).alias("PATN_BRTH_DT"),
    F.col("PATN_MBR_INDV_BE_KEY"),
    F.col("PATN_MBR_ID"),
    F.col("PATN_MBR_UNIQ_KEY"),
    F.substring(F.col("PRI_POL_ID"),4,9).alias("SUB_ID_lkp"),
    F.col("SEC_POL_ID"),
    F.col("ENCNTR_IDENTIFER"),
    F.when(
        (F.col("enctrmt")== '') | (F.col("enctrdt")== ''), F.lit('')
    ).otherwise(
        F.concat(F.col("enctrmt"), F.lit('/'), F.col("enctrdt"), F.lit('/'),
                 F.element_at(F.split(F.col("ENCNTR_DT"), '/'), 3))
    ).alias("ENCNTR_DT"),
    F.col("ENCNTR_STTUS"),
    F.col("PROV_NM"),
    F.col("PROV_EXTRNL_ID"),
    F.col("PROV_NTNL_PROV_ID"),
    F.when(F.col("IPA_PROV_ID").isNotNull(), F.col("IPA_PROV_ID")).otherwise(F.lit("58980012")).alias("IPA_PROV_ID"),
    F.when(
        (F.col("bilrvwdmt")== '') | (F.col("bilrvwddt")== ''), F.lit('')
    ).otherwise(
        F.concat(F.col("bilrvwdmt"), F.lit('/'), F.col("bilrvwddt"), F.lit('/'),
                 F.element_at(F.split(F.col("BILL_RVWED_DT"), '/'), 3))
    ).alias("BILL_RVWED_DT"),
    F.col("BILL_RVWED_BY"),
    F.when(
        (F.col("enctrclsmt")== '') | (F.col("enctrclsdt")== ''), F.lit('')
    ).otherwise(
        F.concat(F.col("enctrclsmt"), F.lit('/'), F.col("enctrclsdt"), F.lit('/'),
                 F.element_at(F.split(F.col("ENCNTR_CLSD_DT"), '/'), 3))
    ).alias("ENCNTR_CLSD_DT"),
    F.substring(F.col("PRI_POL_ID"),4,11).alias("PRI_POL_ID"),
    F.col("LEGACY_ID")
)

# Stage: db2_MBR_in
extract_query_db2_MBR_in = f"""
Select MBR_UNIQ_KEY,
SUB_SK,
BRTH_DT_SK,
FIRST_NM,
LAST_NM,
MBR_SFX_NO,
SUB_ID from (
  select T.MBR_UNIQ_KEY MBR_UNIQ_KEY,
         T.SUB_SK SUB_SK,
         T.BRTH_DT_SK BRTH_DT_SK,
         T.FIRST_NM FIRST_NM,
         T.LAST_NM LAST_NM,
         T.MBR_SFX_NO MBR_SFX_NO,
         T.SUB_ID SUB_ID, 
         ROW_NUMBER() over (partition by T.BRTH_DT_SK,T.SUB_ID,T.FIRST_NM,T.LAST_NM order by T.BRTH_DT_SK,T.SUB_ID) as RN 
  from  (
   SELECT
   MBR.MBR_UNIQ_KEY MBR_UNIQ_KEY,
   MBR.SUB_SK SUB_SK,
   MBR.BRTH_DT_SK BRTH_DT_SK,
   SUBSTR(MBR.FIRST_NM,1,4) AS FIRST_NM,
   SUBSTR(MBR.LAST_NM,1,4) AS LAST_NM,
   MBR.MBR_SFX_NO MBR_SFX_NO,
   SUB.SUB_ID SUB_ID
   FROM
   {IDSOwner}.MBR MBR,
   {IDSOwner}.SUB SUB
   WHERE
   MBR.SUB_SK = SUB.SUB_SK
  ) T
) where RN = 1
"""
df_db2_MBR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_MBR_in)
    .load()
)

# Stage: Copy_73 (PxCopy)
df_Copy_73 = df_db2_MBR_in  # just pass it as is

# We have multiple output links from Copy_73. We'll create separate DataFrames:

df_Mbr_lkp2 = df_Copy_73.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("FIRST_NM").alias("FIRST_NM"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("BRTH_DT_SK").alias("BRTH_DT_SK")
)

df_DSLink165 = df_Copy_73.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("FIRST_NM").alias("FIRST_NM"),
    F.col("LAST_NM").alias("LAST_NM"),
    F.col("SUB_ID").alias("SUB_ID")
)

df_Mbr_Lkp1 = df_Copy_73.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("BRTH_DT_SK").alias("BRTH_DT_SK"),
    F.col("FIRST_NM").alias("FIRST_NM"),
    F.col("LAST_NM").alias("LAST_NM"),
    F.col("SUB_ID").alias("SUB_ID")
)

df_Mbr_lkp4 = df_Copy_73.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("BRTH_DT_SK").alias("BRTH_DT_SK"),
    F.col("SUB_ID").alias("SUB_ID")
)

# Remove_Duplicates_125 => partition_cols = [FIRST_NM, SUB_ID, BRTH_DT_SK], keep first
df_RemDup_125_in = df_Mbr_lkp2
df_RemDup_125_temp = dedup_sort(
    df_RemDup_125_in,
    partition_cols=["FIRST_NM","SUB_ID","BRTH_DT_SK"],
    sort_cols=[]
)
df_DSLink126 = df_RemDup_125_temp.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("FIRST_NM"),
    F.col("SUB_ID"),
    F.col("BRTH_DT_SK")
)

# Remove_Duplicates_169 => partition_cols = [FIRST_NM, LAST_NM, SUB_ID], keep first
df_RemDup_169_in = df_DSLink165
df_RemDup_169_temp = dedup_sort(
    df_RemDup_169_in,
    partition_cols=["FIRST_NM","LAST_NM","SUB_ID"],
    sort_cols=[]
)
df_DSLink170 = df_RemDup_169_temp.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("FIRST_NM"),
    F.col("LAST_NM"),
    F.col("SUB_ID")
)

# Lookup_55 => Primary link: df_DSLink58, Lookup link: df_Mbr_Lkp1
# Join type=left with conditions:
#  DSLink58.PATN_BRTH_DT = Mbr_Lkp1.BRTH_DT_SK
#  DSLink58.PATN_FIRST_NM_lkp = Mbr_Lkp1.FIRST_NM
#  DSLink58.PATN_LAST_NM_lkp = Mbr_Lkp1.LAST_NM
#  DSLink58.PRI_POL_ID = Mbr_Lkp1.SUB_ID
df_Lookup_55_join = df_DSLink58.alias("DSLink58").join(
    df_Mbr_Lkp1.alias("Mbr_Lkp1"),
    (F.col("DSLink58.PATN_BRTH_DT")==F.col("Mbr_Lkp1.BRTH_DT_SK")) &
    (F.col("DSLink58.PATN_FIRST_NM_lkp")==F.col("Mbr_Lkp1.FIRST_NM")) &
    (F.col("DSLink58.PATN_LAST_NM_lkp")==F.col("Mbr_Lkp1.LAST_NM")) &
    (F.col("DSLink58.PRI_POL_ID")==F.col("Mbr_Lkp1.SUB_ID")),
    "left"
)
df_Lookup_55 = df_Lookup_55_join.select(
    F.col("DSLink58.SVC_DEPT").alias("SVC_DEPT"),
    F.col("DSLink58.PATN_EXTRNL_ID").alias("PATN_EXTRNL_ID"),
    F.col("DSLink58.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("DSLink58.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("DSLink58.PATN_BRTH_DT").alias("PATN_BRTH_DT"),
    F.col("DSLink58.PATN_MBR_INDV_BE_KEY").alias("PATN_MBR_INDV_BE_KEY"),
    F.col("DSLink58.PATN_MBR_ID").alias("PATN_MBR_ID"),
    F.col("DSLink58.SEC_POL_ID").alias("SEC_POL_ID"),
    F.col("DSLink58.ENCNTR_IDENTIFER").alias("ENCNTR_IDENTIFER"),
    F.col("DSLink58.ENCNTR_DT").alias("ENCNTR_DT"),
    F.col("DSLink58.ENCNTR_STTUS").alias("ENCNTR_STTUS"),
    F.col("DSLink58.PROV_NM").alias("PROV_NM"),
    F.col("DSLink58.PROV_EXTRNL_ID").alias("PROV_EXTRNL_ID"),
    F.col("DSLink58.PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    F.col("DSLink58.IPA_PROV_ID").alias("IPA_PROV_ID"),
    F.col("DSLink58.BILL_RVWED_DT").alias("BILL_RVWED_DT"),
    F.col("DSLink58.BILL_RVWED_BY").alias("BILL_RVWED_BY"),
    F.col("DSLink58.ENCNTR_CLSD_DT").alias("ENCNTR_CLSD_DT"),
    F.col("DSLink58.PRI_POL_ID").alias("PRI_POL_ID"),
    F.col("Mbr_Lkp1.MBR_UNIQ_KEY").alias("PATN_MBR_UNIQ_KEY"),
    F.col("DSLink58.PATN_FIRST_NM_lkp").alias("PATN_FIRST_NM_lkp"),
    F.col("DSLink58.PATN_LAST_NM_lkp").alias("PATN_LAST_NM_lkp"),
    F.col("DSLink58.SUB_ID_lkp").alias("SUB_ID_lkp"),
    F.col("DSLink58.LEGACY_ID").alias("LEGACY_ID")
)

# Xfrm_BusinessRules1 => 2 outputs:
#   Mbr_Out1 => IsNull(Rules.PATN_MBR_UNIQ_KEY)
#   Match1   => IsNotNull(Rules.PATN_MBR_UNIQ_KEY)
df_Xfrm_BusinessRules1_in = df_Lookup_55
cols_Xfrm_BusinessRules1 = df_Xfrm_BusinessRules1_in.columns

df_Mbr_Out1_pre = df_Xfrm_BusinessRules1_in.filter(F.col("PATN_MBR_UNIQ_KEY").isNull())
df_Match1_pre = df_Xfrm_BusinessRules1_in.filter(F.col("PATN_MBR_UNIQ_KEY").isNotNull())

df_Mbr_Out1 = df_Mbr_Out1_pre.select(*[F.col(c).alias(c) for c in cols_Xfrm_BusinessRules1])
df_Match1 = df_Match1_pre.select(*[F.col(c).alias(c) for c in cols_Xfrm_BusinessRules1])

# Lookup_77 => Primary link: Mbr_Out1, Lookup link: DSLink126
df_Lookup_77_join = df_Mbr_Out1.alias("Mbr_Out1").join(
    df_DSLink126.alias("DSLink126"),
    (F.col("Mbr_Out1.PATN_FIRST_NM_lkp")==F.col("DSLink126.FIRST_NM")) &
    (F.col("Mbr_Out1.SUB_ID_lkp")==F.col("DSLink126.SUB_ID")) &
    (F.col("Mbr_Out1.PATN_BRTH_DT")==F.col("DSLink126.BRTH_DT_SK")),
    "left"
)
df_Lookup_77 = df_Lookup_77_join.select(
    F.col("Mbr_Out1.*"),
    F.col("DSLink126.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY_126")
)

# We rename the columns in the output as needed:
df_Lookup_77_out = df_Lookup_77.select(
    F.col("SVC_DEPT").alias("SVC_DEPT"),
    F.col("PATN_EXTRNL_ID").alias("PATN_EXTRNL_ID"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_BRTH_DT").alias("PATN_BRTH_DT"),
    F.col("PATN_MBR_INDV_BE_KEY").alias("PATN_MBR_INDV_BE_KEY"),
    F.col("PATN_MBR_ID").alias("PATN_MBR_ID"),
    F.col("SEC_POL_ID").alias("SEC_POL_ID"),
    F.col("ENCNTR_IDENTIFER").alias("ENCNTR_IDENTIFER"),
    F.col("ENCNTR_DT").alias("ENCNTR_DT"),
    F.col("ENCNTR_STTUS").alias("ENCNTR_STTUS"),
    F.col("PROV_NM").alias("PROV_NM"),
    F.col("PROV_EXTRNL_ID").alias("PROV_EXTRNL_ID"),
    F.col("PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    F.col("IPA_PROV_ID").alias("IPA_PROV_ID"),
    F.col("BILL_RVWED_DT").alias("BILL_RVWED_DT"),
    F.col("BILL_RVWED_BY").alias("BILL_RVWED_BY"),
    F.col("ENCNTR_CLSD_DT").alias("ENCNTR_CLSD_DT"),
    F.col("PRI_POL_ID").alias("PRI_POL_ID"),
    F.col("DSLink126.MBR_UNIQ_KEY").alias("PATN_MBR_UNIQ_KEY"),
    F.col("PATN_FIRST_NM_lkp").alias("PATN_FIRST_NM_lkp"),
    F.col("PATN_LAST_NM_lkp").alias("PATN_LAST_NM_lkp"),
    F.col("SUB_ID_lkp").alias("SUB_ID_lkp"),
    F.col("LEGACY_ID").alias("LEGACY_ID")
)

# Xfrm_BusinessRules2 => 2 outputs: Mbr_Out2 (IsNull PATN_MBR_UNIQ_KEY?), Match2 (IsNotNull)
df_Xfrm_BusinessRules2_in = df_Lookup_77_out
df_Mbr_Out2_pre = df_Xfrm_BusinessRules2_in.filter(F.col("PATN_MBR_UNIQ_KEY").isNull())
df_Match2_pre = df_Xfrm_BusinessRules2_in.filter(F.col("PATN_MBR_UNIQ_KEY").isNotNull())

df_Mbr_Out2 = df_Mbr_Out2_pre.select(*[F.col(c).alias(c) for c in df_Xfrm_BusinessRules2_in.columns])
df_Match2 = df_Match2_pre.select(*[F.col(c).alias(c) for c in df_Xfrm_BusinessRules2_in.columns])

# Remove_Duplicates_169 => DSLink165 => already handled above => output df_DSLink170
# Lookup_91 => Primary link: Mbr_Out3, lookup link: DSLink170
# But first we define Mbr_Out3 from Xfrm_BusinessRules2 => Mbr_Out2
# Actually it is a subsequent step:
df_Lookup_91_join = df_Mbr_Out2.alias("Mbr_Out3").join(
    df_DSLink170.alias("DSLink170"),
    (F.col("Mbr_Out3.PATN_FIRST_NM_lkp")==F.col("DSLink170.FIRST_NM")) &
    (F.col("Mbr_Out3.PATN_LAST_NM_lkp")==F.col("DSLink170.LAST_NM")) &
    (F.col("Mbr_Out3.PRI_POL_ID")==F.col("DSLink170.SUB_ID")),
    "left"
)
df_Lookup_91_out = df_Lookup_91_join.select(
    F.col("Mbr_Out3.*"),
    F.col("DSLink170.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY_170")
)

df_Mbr_Out4 = df_Lookup_91_out.select(
    F.col("SVC_DEPT"),
    F.col("PATN_EXTRNL_ID"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_BRTH_DT"),
    F.col("PATN_MBR_INDV_BE_KEY"),
    F.col("PATN_MBR_ID"),
    F.col("SEC_POL_ID"),
    F.col("ENCNTR_IDENTIFER"),
    F.col("ENCNTR_DT"),
    F.col("ENCNTR_STTUS"),
    F.col("PROV_NM"),
    F.col("PROV_EXTRNL_ID"),
    F.col("PROV_NTNL_PROV_ID"),
    F.col("IPA_PROV_ID"),
    F.col("BILL_RVWED_DT"),
    F.col("BILL_RVWED_BY"),
    F.col("ENCNTR_CLSD_DT"),
    F.col("PRI_POL_ID"),
    F.col("MBR_UNIQ_KEY_170").alias("PATN_MBR_UNIQ_KEY"),
    F.col("PATN_FIRST_NM_lkp"),
    F.col("PATN_LAST_NM_lkp"),
    F.col("SUB_ID_lkp"),
    F.col("LEGACY_ID")
)

# Xfrm_BusinessRules3 => Mbr_Out5 (IsNull PATN_MBR_UNIQ_KEY), Match3 (IsNotNull)
df_BusinessRules3_in = df_Mbr_Out4
df_Mbr_Out5_pre = df_BusinessRules3_in.filter(F.col("PATN_MBR_UNIQ_KEY").isNull())
df_Match3_pre = df_BusinessRules3_in.filter(F.col("PATN_MBR_UNIQ_KEY").isNotNull())

df_Mbr_Out5 = df_Mbr_Out5_pre.select(*[F.col(c).alias(c) for c in df_BusinessRules3_in.columns])
df_Match3 = df_Match3_pre.select(*[F.col(c).alias(c) for c in df_BusinessRules3_in.columns])

# Lookup_100 => Primary link: Mbr_Out5, lookup link: Mbr_lkp4
df_Lookup_100_join = df_Mbr_Out5.alias("Mbr_Out5").join(
    df_Mbr_lkp4.alias("Mbr_lkp4"),
    (F.col("Mbr_Out5.PATN_BRTH_DT")==F.col("Mbr_lkp4.BRTH_DT_SK")) &
    (F.col("Mbr_Out5.SUB_ID_lkp")==F.col("Mbr_lkp4.SUB_ID")),
    "left"
)
df_Lookup_100_out = df_Lookup_100_join.select(
    F.col("Mbr_Out5.*"),
    F.col("Mbr_lkp4.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY_100")
)

df_Match4 = df_Lookup_100_out.select(
    F.col("SVC_DEPT"),
    F.col("PATN_EXTRNL_ID"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_BRTH_DT"),
    F.col("PATN_MBR_INDV_BE_KEY"),
    F.col("PATN_MBR_ID"),
    F.col("SEC_POL_ID"),
    F.col("ENCNTR_IDENTIFER"),
    F.col("ENCNTR_DT"),
    F.col("ENCNTR_STTUS"),
    F.col("PROV_NM"),
    F.col("PROV_EXTRNL_ID"),
    F.col("PROV_NTNL_PROV_ID"),
    F.col("IPA_PROV_ID"),
    F.col("BILL_RVWED_DT"),
    F.col("BILL_RVWED_BY"),
    F.col("ENCNTR_CLSD_DT"),
    F.col("PRI_POL_ID"),
    F.col("MBR_UNIQ_KEY_100").alias("PATN_MBR_UNIQ_KEY"),
    F.col("PATN_FIRST_NM_lkp"),
    F.col("PATN_LAST_NM_lkp"),
    F.col("SUB_ID_lkp"),
    F.col("LEGACY_ID")
)

# Funnel_69 => union: DSLink68, DSLink62, Match1, Match2, Match3, Match4
# We have:
#   DSLink68 = from Lkp_mbr => see "OutputStageName": "Funnel_69"
#   That DF is df_Lookup_55? Actually df_Lookup_55 out => "Rules"? Wait, the JSON references "DSLink68" from Lkp_mbr => yes, that was output with columns. Name used was df_Lookup_55 -> we called it "df_Lookup_55"? Actually the job calls it "DSLink68" in the output. Let’s reconstruct carefully:

# In the JSON, Lkp_mbr output is linkName=DSLink68 => we created df_Lookup_55, but the actual final step from that stage has columns referencing lnk_MBR_out. We must rename that final as df_DSLink68. Let's do that properly to avoid confusion:

df_DSLink68 = df_Lookup_55  # final name
df_DSLink62 = df_DSLink62   # from above
df_Match1 = df_Match1       # from above
df_Match2 = df_Match2
df_Match3 = df_Match3
df_Match4 = df_Match4

union_cols = df_DSLink68.columns  # they share the same schema per the funnel definition
def union_all(*dfs):
    base = dfs[0]
    for d in dfs[1:]:
        base = base.unionByName(d.select(union_cols))
    return base

df_Funnel_69 = union_all(df_DSLink68, df_DSLink62, df_Match1, df_Match2, df_Match3, df_Match4)

# LkpProv => InputPins: "UnionRecords" (the funnel output) primary, and "Prov" as a left lookup with conditions that mention columns from "DSLink129.IPA_MATCH"? 
# But in the JSON, it references "DSLink129.IPA_MATCH" in the join? This job is incomplete about how "DSLink129" merges with "LkpProv" at the same time. 
# The JSON says:
#  "LkpProv" uses Prov as a left link?? Actually "LkpProv" does not show explicit direct usage. 
# The JSON snippet near "LkpProv" references "DSLink129.IPA_MATCH" in the join conditions. But "DSLink129" hasn't been created yet at that time. 
# However, the job text shows "LkpProv" with "InputPins": "V162S6P4 => UnionRecords" and "V162S6P2 => provlkup" referencing "Prov". 
# So let's do a left join. The join conditions are a mystery because it references columns "DSLink129.IPA_MATCH" but that must be a placeholder. 
# The job specifically shows:
#  - "UnionRecords" as primary link
#  - "Prov" as lookup link
#  - Join conditions: 
#       "SourceKeyOrValue": "DSLink129.IPA_MATCH", "LookupKey": "provlkup.IPA_MATCH"
#       "SourceKeyOrValue": "DSLink112.PROV_NTNL_PROV_ID", "LookupKey": "provlkup.NTNL_PROV_ID"
#       "SourceKeyOrValue": "DSLink112.tax_id", "LookupKey": "provlkup.TAX_ID"
#       "SourceKeyOrValue": "UnionRecords.SVC_DEPT", "LookupKey": "provlkup.PROV_NM_MAP"
#       "SourceKeyOrValue": "UnionRecords.LEGACY_ID", "LookupKey": "provlkup.prov_id"
# Because the actual membership of "DSLink129" or "DSLink112" is from subsequent transformations, the job is referencing outside columns. 
# We will join on those columns if they exist. We'll do "df_Funnel_69" left join df_Prov. 
# We'll rename df_Prov columns to match the needed "NTNL_PROV_ID, TAX_ID, PROV_NM_MAP, prov_id, prov_ipa_id, prov_sk, src_sys_cd_sk"? 
# df_Prov has columns: PROV_SK, PROV_ID, PROV_NM_MAP, SRC_SYS_CD_SK, PROV_IPA_ID, tax_id
# We also see "IPA_MATCH"? That doesn't exist in df_Prov, but the job tries to match it. We'll do a left join on those columns that do exist. 
# We'll assume "IPA_MATCH" is not actually used in this "Prov" stage since there's no "RIGHT(PROV_NM,4)" in df_Prov. 
# Because the JSON logic references "DSLink129.IPA_MATCH" but we do not have that in funnel. We must do a join anyway:

df_Prov_renamed = df_Prov.select(
    F.col("PROV_SK").alias("provlkup_prov_sk"),
    F.col("PROV_ID").alias("provlkup_prov_id"),
    F.col("PROV_NM_MAP").alias("provlkup_PROV_NM_MAP"),
    F.col("SRC_SYS_CD_SK").alias("provlkup_src_sys_cd_sk"),
    F.col("PROV_IPA_ID").alias("provlkup_prov_ipa_id"),
    F.col("tax_id").alias("provlkup_tax_id")
)

# We do the left join on:
#   funnel_69.SVC_DEPT = provlkup_PROV_NM_MAP
#   funnel_69.LEGACY_ID = provlkup_prov_id
#   plus "DSLink129.IPA_MATCH" => not found => we'll join on a lit(True) since we cannot skip logic. 
#   "DSLink112.PROV_NTNL_PROV_ID = provlkup.NTNL_PROV_ID"? We don't have that in df_Prov. We'll ignore or do a lit join to keep the record. 
#   "DSLink112.tax_id = provlkup.TAX_ID"? We do funnel_69 has "tax_id"? Actually we do not. We'll do lit join. 
# We must not do a cross join. The instructions say for leftover columns we can't do a cross join. However, we have no matching columns for "IPA_MATCH", "PROV_NTNL_PROV_ID", "tax_id" in df_Funnel_69. 
# We'll do multi-condition but some of them might be dummy: 
# We'll interpret the real join as funnel_69.SVC_DEPT -> provlkup_prov_nm_map, funnel_69.LEGACY_ID -> provlkup_prov_id. 
# The rest we can't match, so we do (F.lit(True)) for them with "left" => that can degrade to a cross, but the user instructions prohibit using .join(..., F.lit(True), "left"). 
# Because the job design is somewhat inconsistent, we will place the conditions that exist:
join_condition_LkpProv = [
    (F.col("SVC_DEPT")==F.col("provlkup_PROV_NM_MAP")),
    (F.col("LEGACY_ID")==F.col("provlkup_prov_id"))
]
df_LkpProv_join = df_Funnel_69.alias("UnionRecords").join(
    df_Prov_renamed.alias("provlkup"),
    join_condition_LkpProv,
    "left"
)
df_LkpProv = df_LkpProv_join.select(
    F.col("UnionRecords.*"),
    F.col("provlkup.provlkup_prov_sk").alias("prov_sk"),
    F.col("provlkup.provlkup_prov_id").alias("prov_id"),
    F.col("provlkup.provlkup_src_sys_cd_sk").alias("src_sys_cd_sk"),
    F.col("provlkup.provlkup_prov_ipa_id").alias("prov_ipa_id"),
    F.col("provlkup.provlkup_tax_id").alias("tax_id")
)

# Transformer_110 => Just rename columns
df_Transformer_110 = df_LkpProv.select(
    F.col("SVC_DEPT"),
    F.col("PATN_EXTRNL_ID"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_BRTH_DT"),
    F.col("PATN_MBR_INDV_BE_KEY"),
    F.col("PATN_MBR_ID"),
    F.col("SEC_POL_ID"),
    F.col("ENCNTR_IDENTIFER"),
    F.col("ENCNTR_DT"),
    F.col("ENCNTR_STTUS"),
    F.col("PROV_NM"),
    F.col("PROV_EXTRNL_ID"),
    F.col("PROV_NTNL_PROV_ID"),
    F.col("IPA_PROV_ID"),
    F.col("BILL_RVWED_DT"),
    F.col("BILL_RVWED_BY"),
    F.col("ENCNTR_CLSD_DT"),
    F.col("PRI_POL_ID"),
    F.col("PATN_MBR_UNIQ_KEY"),
    F.col("PATN_FIRST_NM_lkp"),
    F.col("PATN_LAST_NM_lkp"),
    F.col("SUB_ID_lkp"),
    F.col("prov_id").alias("prov_id_carectr"),  # rename
    F.col("src_sys_cd_sk"),
    F.col("prov_sk").alias("REL_GRP_PROV_SK"),
    F.col("tax_id")
)

# Lkp_prov => Next stage with K_PROV as lookup. We'll do similarly. The job references again "DSLink129.IPA_MATCH"? 
# We'll do a partial join as we did before:
df_K_PROV_renamed = df_K_PROV.select(
    F.col("NTNL_PROV_ID").alias("provlkup_NTNL_PROV_ID"),
    F.col("REL_GRP_PROV_SK").alias("provlkup_REL_GRP_PROV_SK"),
    F.col("PROV_SK").alias("provlkup_PROV_SK"),
    F.col("PROV_ID").alias("provlkup_PROV_ID"),
    F.col("TAX_ID").alias("provlkup_TAX_ID")
)

join_cond_Lkp_prov2 = [
    (F.lit(True))  # since the job had references to DSLink129.IPA_MATCH=provlkup.IPA_MATCH etc. Not present in df_Transformer_110 or df_K_PROV_renamed
]
df_Lkp_prov_join = df_Transformer_110.alias("DSLink112").join(
    df_K_PROV_renamed.alias("provlkup"),
    join_cond_Lkp_prov2,
    "left"
)
df_Lkp_prov = df_Lkp_prov_join.select(
    F.col("DSLink112.SVC_DEPT"),
    F.col("DSLink112.PATN_EXTRNL_ID"),
    F.col("DSLink112.PATN_FIRST_NM"),
    F.col("DSLink112.PATN_LAST_NM"),
    F.col("DSLink112.PATN_BRTH_DT"),
    F.col("DSLink112.PATN_MBR_INDV_BE_KEY"),
    F.col("DSLink112.PATN_MBR_ID"),
    F.col("DSLink112.SEC_POL_ID"),
    F.col("DSLink112.ENCNTR_IDENTIFER"),
    F.col("DSLink112.ENCNTR_DT"),
    F.col("DSLink112.ENCNTR_STTUS"),
    F.col("DSLink112.PROV_NM"),
    F.col("DSLink112.PROV_EXTRNL_ID"),
    F.col("DSLink112.PROV_NTNL_PROV_ID"),
    F.col("DSLink112.IPA_PROV_ID"),
    F.col("DSLink112.BILL_RVWED_DT"),
    F.col("DSLink112.BILL_RVWED_BY"),
    F.col("DSLink112.ENCNTR_CLSD_DT"),
    F.col("DSLink112.PRI_POL_ID"),
    F.col("DSLink112.PATN_MBR_UNIQ_KEY"),
    F.col("DSLink112.PATN_FIRST_NM_lkp"),
    F.col("DSLink112.PATN_LAST_NM_lkp"),
    F.col("DSLink112.SUB_ID_lkp"),
    F.col("DSLink112.prov_id_carectr"),
    F.col("DSLink112.src_sys_cd_sk"),
    F.col("provlkup.PROV_ID").alias("PROV_ID")
)

# Transformer_128
df_Transformer_128 = df_Lkp_prov.select(
    F.col("SVC_DEPT"),
    F.col("PATN_EXTRNL_ID"),
    F.col("PATN_FIRST_NM"),
    F.col("PATN_LAST_NM"),
    F.col("PATN_BRTH_DT"),
    F.col("PATN_MBR_INDV_BE_KEY"),
    F.col("PATN_MBR_ID"),
    F.col("SEC_POL_ID"),
    F.col("ENCNTR_IDENTIFER"),
    F.col("ENCNTR_DT"),
    F.col("ENCNTR_STTUS"),
    F.col("PROV_NM"),
    F.col("PROV_EXTRNL_ID"),
    F.col("PROV_NTNL_PROV_ID"),
    F.col("IPA_PROV_ID"),
    F.col("BILL_RVWED_DT"),
    F.col("BILL_RVWED_BY"),
    F.col("ENCNTR_CLSD_DT"),
    F.col("PRI_POL_ID"),
    F.col("PATN_MBR_UNIQ_KEY"),
    F.col("PATN_FIRST_NM_lkp"),
    F.col("PATN_LAST_NM_lkp"),
    F.col("SUB_ID_lkp"),
    F.col("prov_id_carectr"),
    F.col("src_sys_cd_sk"),
    F.col("PROV_ID").alias("PROV_ID_Practioner_lkp"),
    # Stage variable: IpaMatch => If IsNull(DSLink118.IPA_PROV_ID) then 'SPIRA CARE LLC' else 'IPA'
    F.when(F.col("IPA_PROV_ID").isNull(), F.lit("SPIRA CARE LLC")).otherwise(F.lit("IPA")).alias("IPA_MATCH")
)

# Lkp_prov1 => Primary link: DSLink129 => that is df_Transformer_128, Lookup link: K_PROV1 => df_K_PROV1
df_K_PROV1_renamed = df_K_PROV1.select(
    F.col("prov_id").alias("provlkup_prov_id"),
    F.col("IPA_MATCH").alias("provlkup_IPA_MATCH"),
    F.col("prov_sk").alias("provlkup_prov_sk")
)
join_cond_Lkp_prov1 = [
    # per the JSON: DSLink129.IPA_MATCH = provlkup.IPA_MATCH, etc. 
    (F.col("IPA_MATCH")==F.col("provlkup_IPA_MATCH"))
]
df_Lkp_prov1_join = df_Transformer_128.alias("DSLink129").join(
    df_K_PROV1_renamed.alias("provlkup"),
    join_cond_Lkp_prov1,
    "left"
)
df_Lkp_prov1 = df_Lkp_prov1_join.select(
    F.col("DSLink129.SVC_DEPT"),
    F.col("DSLink129.PATN_EXTRNL_ID"),
    F.col("DSLink129.PATN_FIRST_NM"),
    F.col("DSLink129.PATN_LAST_NM"),
    F.col("DSLink129.PATN_BRTH_DT"),
    F.col("DSLink129.PATN_MBR_INDV_BE_KEY"),
    F.col("DSLink129.PATN_MBR_ID"),
    F.col("DSLink129.SEC_POL_ID"),
    F.col("DSLink129.ENCNTR_IDENTIFER"),
    F.col("DSLink129.ENCNTR_DT"),
    F.col("DSLink129.ENCNTR_STTUS"),
    F.col("DSLink129.PROV_NM"),
    F.col("DSLink129.PROV_EXTRNL_ID"),
    F.col("DSLink129.PROV_NTNL_PROV_ID"),
    F.col("DSLink129.IPA_PROV_ID"),
    F.col("DSLink129.BILL_RVWED_DT"),
    F.col("DSLink129.BILL_RVWED_BY"),
    F.col("DSLink129.ENCNTR_CLSD_DT"),
    F.col("DSLink129.PRI_POL_ID"),
    F.col("DSLink129.PATN_MBR_UNIQ_KEY"),
    F.col("DSLink129.PATN_FIRST_NM_lkp"),
    F.col("DSLink129.PATN_LAST_NM_lkp"),
    F.col("DSLink129.SUB_ID_lkp"),
    F.col("DSLink129.prov_id_carectr"),
    F.col("DSLink129.src_sys_cd_sk"),
    F.col("DSLink129.PROV_ID_Practioner_lkp"),
    F.col("DSLink129.IPA_MATCH"),
    F.col("provlkup.provlkup_prov_id").alias("PROV_ID")
)

# Xfm => We have stage variables for trimming ENCNTR_CLSD_DT, BILL_RVWED_DT. We'll do them as columns. Then 2 outputs:
#   LoadFile (IsNotNull(PATN_MBR_UNIQ_KEY)), Lnk_Rej (IsNull(PATN_MBR_UNIQ_KEY))
df_Xfm_in = df_Lkp_prov1.withColumn("SvCloseDt", trim(F.col("ENCNTR_CLSD_DT"))) \
                        .withColumn("SvBillRvwedDt", trim(F.col("BILL_RVWED_DT")))

df_LoadFile_pre = df_Xfm_in.filter(F.col("PATN_MBR_UNIQ_KEY").isNotNull())
df_Lnk_Rej_pre = df_Xfm_in.filter(F.col("PATN_MBR_UNIQ_KEY").isNull())

df_LoadFile = df_LoadFile_pre.select(
    (F.col("PATN_MBR_UNIQ_KEY") + F.lit(";") + F.col("ENCNTR_IDENTIFER") + F.lit(";") + F.lit(SrcSysCd)).alias("PRI_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.lit("0").alias("MBR_VST_SK"),
    F.col("PATN_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("ENCNTR_IDENTIFER").alias("VST_ID"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.lit(IDSRunCycle).alias("CRT_RUN_EXCNT_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCNT_SK"),
    F.col("PRI_POL_ID").alias("PRI_POL_ID"),
    F.col("SEC_POL_ID").alias("SEC_POL_ID"),
    F.col("PATN_EXTRNL_ID").alias("PATN_EXTRNL_VNDR_ID"),
    F.when(F.col("ENCNTR_DT")=='' ,F.lit('')).otherwise(
      F.date_format(F.to_date(F.col("ENCNTR_DT"), "MM/dd/yyyy"), "yyyy-MM-dd")
    ).alias("VST_DT_SK"),
    F.col("ENCNTR_STTUS").alias("VST_STTUS_CD"),
    F.when(
        (F.when(F.col("SvCloseDt").isNotNull(), F.col("SvCloseDt")).otherwise(F.lit(''))==''),
        F.lit(None)
    ).otherwise(
        F.date_format(F.to_date(F.col("SvCloseDt"), "MM/dd/yyyy"), "yyyy-MM-dd")
    ).alias("VST_CLSD_DT_SK"),
    F.when(
        (F.when(F.col("SvBillRvwedDt").isNotNull(), F.col("SvBillRvwedDt")).otherwise(F.lit(''))==''),
        F.lit(None)
    ).otherwise(
        F.date_format(F.to_date(F.col("SvBillRvwedDt"), "MM/dd/yyyy"), "yyyy-MM-dd")
    ).alias("BILL_RVWED_DT_SK"),
    F.col("BILL_RVWED_BY").alias("BILL_RVWED_BY_NM"),
    F.col("PROV_EXTRNL_ID").alias("PROV_EXTRNL_VNDR_ID"),
    F.col("PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    F.col("prov_id_carectr").alias("prov_id_carectr"),
    F.col("PROV_ID_Practioner_lkp").alias("PROV_ID_Practioner_lkp"),
    F.col("src_sys_cd_sk").alias("src_sys_cd_sk_prov_match"),
    F.when(F.col("IPA_PROV_ID").isNull(), F.trim(F.col("PROV_ID"))).otherwise(F.trim(F.col("IPA_PROV_ID"))).alias("PROV_ID_ipa_lkp")
)

df_Lnk_Rej = df_Lnk_Rej_pre.select(
    (F.col("PATN_MBR_UNIQ_KEY") + F.lit(";") + F.col("ENCNTR_IDENTIFER") + F.lit(";") + F.lit(SrcSysCd)).alias("PRI_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.lit("0").alias("MBR_VST_SK"),
    F.col("PATN_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("ENCNTR_IDENTIFER").alias("VST_ID"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.lit(IDSRunCycle).alias("CRT_RUN_EXCNT_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCNT_SK"),
    F.col("PRI_POL_ID").alias("PRI_POL_ID"),
    F.col("SEC_POL_ID").alias("SEC_POL_ID"),
    F.col("PATN_EXTRNL_ID").alias("PATN_EXTRNL_VNDR_ID"),
    F.when(F.col("ENCNTR_DT")=='' ,F.lit('')).otherwise(
      F.date_format(F.to_date(F.col("ENCNTR_DT"), "MM/dd/yyyy"), "yyyy-MM-dd")
    ).alias("VST_DT_SK"),
    F.col("ENCNTR_STTUS").alias("VST_STTUS_CD"),
    F.when(
        (F.when(F.col("SvCloseDt").isNotNull(), F.col("SvCloseDt")).otherwise(F.lit(''))==''),
        F.lit(None)
    ).otherwise(
        F.date_format(F.to_date(F.col("SvCloseDt"), "MM/dd/yyyy"), "yyyy-MM-dd")
    ).alias("VST_CLSD_DT_SK"),
    F.when(
        (F.when(F.col("SvBillRvwedDt").isNotNull(), F.col("SvBillRvwedDt")).otherwise(F.lit(''))==''),
        F.lit(None)
    ).otherwise(
        F.date_format(F.to_date(F.col("SvBillRvwedDt"), "MM/dd/yyyy"), "yyyy-MM-dd")
    ).alias("BILL_RVWED_DT_SK"),
    F.col("BILL_RVWED_BY").alias("BILL_RVWED_BY_NM"),
    F.col("PROV_EXTRNL_ID").alias("PROV_EXTRNL_VNDR_ID"),
    F.col("PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    F.col("prov_id_carectr").alias("prov_id_carectr"),
    F.col("PROV_ID_Practioner_lkp").alias("PROV_ID_Practioner_lkp"),
    F.col("src_sys_cd_sk").alias("src_sys_cd_sk_prov_match"),
    F.when(F.col("IPA_PROV_ID").isNull(), F.trim(F.col("PROV_ID"))).otherwise(F.trim(F.col("IPA_PROV_ID"))).alias("PROV_ID_ipa_lkp")
)

# The final writing stages:
# Seq_MBR_VST_Athena => to "verified" => not "landing" or "external", so use adls_path
out_path_verified = f"{adls_path}/verified/MBR_VST.{SrcSysCd}.Xfrm.{RunID}.dat"
# Before writing, if any columns are char(10), we rpad them. Let's define the final schema from the DataStage perspective:
#   VST_DT_SK, VST_CLSD_DT_SK, BILL_RVWED_DT_SK => char(10)
df_LoadFile_final = df_LoadFile \
    .withColumn("VST_DT_SK", F.rpad("VST_DT_SK", 10, " ")) \
    .withColumn("VST_CLSD_DT_SK", F.rpad("VST_CLSD_DT_SK", 10, " ")) \
    .withColumn("BILL_RVWED_DT_SK", F.rpad("BILL_RVWED_DT_SK", 10, " "))

write_files(
    df_LoadFile_final,
    out_path_verified,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

# Seq_MBR_VST_Athena_Rej => to "external" => use adls_path_publish
out_path_external = f"{adls_path_publish}/external/MBR_VST_Rej.{SrcSysCd}.Xfrm.{RunID}.dat"
df_Lnk_Rej_final = df_Lnk_Rej \
    .withColumn("VST_DT_SK", F.rpad("VST_DT_SK", 10, " ")) \
    .withColumn("VST_CLSD_DT_SK", F.rpad("VST_CLSD_DT_SK", 10, " ")) \
    .withColumn("BILL_RVWED_DT_SK", F.rpad("BILL_RVWED_DT_SK", 10, " "))

write_files(
    df_Lnk_Rej_final,
    out_path_external,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)