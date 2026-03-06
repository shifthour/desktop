# Databricks notebook source
# MAGIC %md
# MAGIC """
# MAGIC Â© Copyright 2020 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : CLAIMS_OPTUMRX_MEDD_DLY_WRHS_000
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     Med-D OptumRx Drug Claim Landing Extract. Looks up against the IDS MBR,SUB and GRP  table to get the right member unique key for a Member for a Claim
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                         Date                 Project/Altiris #                           Change Description                                                                                            Development Project\(9)Code Reviewer          Date Reviewed       
# MAGIC ------------------                      --------------------     ------------------------                            -----------------------------------------------------------------------                                                   --------------------------------       -------------------------------   ----------------------------      
# MAGIC Rekha Radhakrishna      07/15/2020      6131- PBM Replacement            Initial Programming                                                                                              IntegrateDev2\(9)\(9)Sravya Gorla\(9)2020/12/09
# MAGIC 
# MAGIC Rekha Radhakrishna     2020-02-05        PBM PhaseII                        Added LOB indicator to Program Info written to Audit File and DATESBM                 IntegrateDev2\(9)\(9)Abhiram Dasarathy\(9)2021-02-09   
# MAGIC Peter Gichiri                       2021-04-10        US -345228                           Mapped FMSTIER to MCARE_D_COV_DRUG                                                          IntegrateDev2\(9)\(9)Jaideep Mankala\(9)04/12/2021     
# MAGIC Geetanjali Rajendran    2021-06-03         PBM PhaseII                        Added TIER_ID, DRUG_TYPE_CD and CLIENTDEF2 to the                                        IntegrateDev2\(9)\(9)Abhiram Dasarathy\(9)2021-06-23
# MAGIC                                                                                                                               DrugClmPrice Landing file and modified DRUG_TYP derivation in
# MAGIC                                                                                                                               PaidReversal Landing file
# MAGIC Tamanna kumari             2023-11-07        US -600307                      MA-Ingest Plan Drug Status (PLANDRUGST) from v8.0E  MA Daily Claims 
# MAGIC                                                                                                             File into IDS. Added  PLN_DRUG_STTUS_CD  to the DrugClmPrice Landing file         IntegrateDevB\(9)\(9)Jeyaprasanna     2024-01-01
# MAGIC 
# MAGIC Ashok kumar B            2024-02-01        US 608682                        Added  PLAN_TYPE  to the DrugClmPrice Landing file                                                    IntegrateDev2                        Jeyaprasanna     2024-03-14

# MAGIC OptumRX Daily Claim File Pre-Processing.
# MAGIC 
# MAGIC Takes Optum Daily Claim File and breaks it down into files for Claims, Denied Rejected and Spread Pricing.
# MAGIC Member Not Found Error file is not created becase decision is not made on where to load it finally
# MAGIC 
# MAGIC All claims will be loaded , even when member not found  with MBR_UNIQ_KEY = 0
# MAGIC Append header record to audit file.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Utility_DS_Integrate
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, length, row_number, sum as sum_, max as max_, rpad, regexp_replace, concat
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunDate = get_widget_value('RunDate','')
MbrTermDate = get_widget_value('MbrTermDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

query_IDS_MBR = f"""SELECT 
SUB.SUB_ID as MEMBERID,
GRP_ID,
MBR_UNIQ_KEY,
SUB_ID,
MBR_SFX_NO
FROM
{IDSOwner}.MBR MBR,
{IDSOwner}.SUB SUB,
{IDSOwner}.GRP GRP
WHERE
MBR.SUB_SK = SUB.SUB_SK
AND SUB.GRP_SK = GRP.GRP_SK
AND MBR.TERM_DT_SK >= '{MbrTermDate}'
AND MBR.MBR_SFX_NO = '00'
"""

df_IDS_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_IDS_MBR)
    .load()
)

df_IDS_MBR_dedup = dedup_sort(
    df_IDS_MBR,
    partition_cols=["MEMBER_ID", "GRP_ID"],
    sort_cols=[("MEMBER_ID","A"),("GRP_ID","A")]
)

df_mbr_lkup = df_IDS_MBR_dedup


df_Optum = (
    spark.read
    .format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .load(f"{adls_path_raw}/landing/" + InFile)
)

# Select columns, applying rpad to char/varchar columns
df_Optum = df_Optum.select(
    col("_c0").alias("RXCLMNBR"),
    col("_c1").alias("CLMSEQNBR"),
    rpad(col("_c2"), 1, " ").alias("CLAIMSTS"),
    rpad(col("_c3"), 9, " ").alias("CARRIERID"),
    rpad(col("_c4"), 9, " ").alias("SCARRIERID"),
    col("_c5").alias("CARRPROC"),
    rpad(col("_c6"), 3, " ").alias("CLNTID"),
    rpad(col("_c7"), 3, " ").alias("CLNTSGMNT"),
    rpad(col("_c8"), 2, " ").alias("CLNTREGION"),
    rpad(col("_c9"), 15, " ").alias("ACCOUNTID"),
    rpad(col("_c10"), 10, " ").alias("ACCTBENCDE"),
    rpad(col("_c11"), 2, " ").alias("VERSIONNBR"),
    rpad(col("_c12"), 15, " ").alias("GROUPID"),
    rpad(col("_c13"), 10, " ").alias("GROUPPLAN"),
    rpad(col("_c14"), 10, " ").alias("GRPCLIBENF"),
    rpad(col("_c15"), 4, " ").alias("GROUPSIC"),
    rpad(col("_c16"), 1, " ").alias("CLMRESPSTS"),
    rpad(col("_c17"), 20, " ").alias("MEMBERID"),
    rpad(col("_c18"), 25, " ").alias("MBRLSTNME"),
    rpad(col("_c19"), 15, " ").alias("MBRFSTNME"),
    rpad(col("_c20"), 1, " ").alias("MBRMDINIT"),
    rpad(col("_c21"), 3, " ").alias("MBRPRSNCDE"),
    rpad(col("_c22"), 1, " ").alias("MBRRELCDE"),
    rpad(col("_c23"), 1, " ").alias("MBRSEX"),
    col("_c24").alias("MBRBIRTH"),
    col("_c25").alias("MBRAGE"),
    rpad(col("_c26"), 15, " ").alias("MBRZIP"),
    rpad(col("_c27"), 9, " ").alias("SOCSECNBR"),
    rpad(col("_c28"), 18, " ").alias("DURKEY"),
    rpad(col("_c29"), 1, " ").alias("DURFLAG"),
    rpad(col("_c30"), 20, " ").alias("MBRFAMLYID"),
    rpad(col("_c31"), 1, " ").alias("MBRFAMLIND"),
    rpad(col("_c32"), 1, " ").alias("MBRFAMLTYP"),
    rpad(col("_c33"), 2, " ").alias("COBIND"),
    rpad(col("_c34"), 10, " ").alias("MBRPLAN"),
    rpad(col("_c35"), 6, " ").alias("MBRPRODCDE"),
    rpad(col("_c36"), 6, " ").alias("MBRRIDERCD"),
    rpad(col("_c37"), 10, " ").alias("CARENETID"),
    rpad(col("_c38"), 10, " ").alias("CAREQUALID"),
    rpad(col("_c39"), 10, " ").alias("CAREFACID"),
    rpad(col("_c40"), 25, " ").alias("CAREFACNAM"),
    rpad(col("_c41"), 15, " ").alias("MBRPCPHYS"),
    rpad(col("_c42"), 15, " ").alias("PPRSFSTNME"),
    rpad(col("_c43"), 25, " ").alias("PPRSLSTNME"),
    rpad(col("_c44"), 1, " ").alias("PPRSMDINIT"),
    rpad(col("_c45"), 6, " ").alias("PPRSSPCCDE"),
    rpad(col("_c46"), 3, " ").alias("PPRSTATE"),
    rpad(col("_c47"), 1, " ").alias("MBRALTINFL"),
    rpad(col("_c48"), 10, " ").alias("MBRALTINCD"),
    rpad(col("_c49"), 20, " ").alias("MBRALTINID"),
    col("_c50").alias("MBRMEDDTE"),
    rpad(col("_c51"), 1, " ").alias("MBRMEDTYPE"),
    rpad(col("_c52"), 11, " ").alias("MBRHICCDE"),
    rpad(col("_c53"), 20, " ").alias("CARDHOLDER"),
    rpad(col("_c54"), 15, " ").alias("PATLASTNME"),
    rpad(col("_c55"), 12, " ").alias("PATFRSTNME"),
    rpad(col("_c56"), 3, " ").alias("PERSONCDE"),
    col("_c57").alias("RELATIONCD"),
    col("_c58").alias("SEXCODE"),
    col("_c59").alias("BIRTHDTE"),
    rpad(col("_c60"), 1, " ").alias("ELIGCLARIF"),
    rpad(col("_c61"), 2, " ").alias("CUSTLOC"),
    rpad(col("_c62"), 2, " ").alias("SBMPLSRVCE"),
    rpad(col("_c63"), 2, " ").alias("SBMPATRESD"),
    rpad(col("_c64"), 15, " ").alias("PRMCAREPRV"),
    rpad(col("_c65"), 2, " ").alias("PRMCAREPRQ"),
    rpad(col("_c66"), 10, " ").alias("FACILITYID"),
    rpad(col("_c67"), 2, " ").alias("OTHCOVERAG"),
    rpad(col("_c68"), 6, " ").alias("BINNUMBER"),
    rpad(col("_c69"), 10, " ").alias("PROCESSOR"),
    rpad(col("_c70"), 15, " ").alias("GROUPNBR"),
    rpad(col("_c71"), 2, " ").alias("TRANSCDE"),
    col("_c72").alias("DATESBM"),
    col("_c73").alias("TIMESBM"),
    col("_c74").alias("CHGDATE"),
    col("_c75").alias("CHGTIME"),
    col("_c76").alias("ORGPDSBMDT"),
    col("_c77").alias("RVDATESBM"),
    col("_c78").alias("CLMCOUNTER"),
    col("_c79").alias("GENERICCTR"),
    col("_c80").alias("FORMLRYCTR"),
    rpad(col("_c81"), 12, " ").alias("RXNUMBER"),
    rpad(col("_c82"), 1, " ").alias("RXNUMBERQL"),
    rpad(col("_c83"), 2, " ").alias("REFILL"),
    rpad(col("_c84"), 1, " ").alias("DISPSTATUS"),
    col("_c85").alias("DTEFILLED"),
    rpad(col("_c86"), 1, " ").alias("COMPOUNDCD"),
    rpad(col("_c87"), 2, " ").alias("SBMCMPDTYP"),
    rpad(col("_c88"), 2, " ").alias("PRODTYPCDE"),
    rpad(col("_c89"), 20, " ").alias("PRODUCTID"),
    col("_c90").alias("PRODUCTKEY"),
    col("_c91").alias("METRICQTY"),
    col("_c92").alias("DECIMALQTY"),
    col("_c93").alias("DAYSSUPPLY"),
    rpad(col("_c94"), 1, " ").alias("PSC"),
    col("_c95").alias("WRITTENDTE"),
    col("_c96").alias("NBRFLSAUTH"),
    rpad(col("_c97"), 1, " ").alias("ORIGINCDE"),
    rpad(col("_c98"), 2, " ").alias("SBMCLARCD1"),
    rpad(col("_c99"), 2, " ").alias("SBMCLARCD2"),
    rpad(col("_c100"), 2, " ").alias("SBMCLARCD3"),
    rpad(col("_c101"), 11, " ").alias("PAMCNBR"),
    rpad(col("_c102"), 2, " ").alias("PAMCCDE"),
    rpad(col("_c103"), 11, " ").alias("PRAUTHNBR"),
    rpad(col("_c104"), 2, " ").alias("PRAUTHRSN"),
    col("_c105").alias("PRAUTHFDTE"),
    col("_c106").alias("PRAUTHTDTE"),
    rpad(col("_c107"), 30, " ").alias("LABELNAME"),
    rpad(col("_c108"), 70, " ").alias("PRODNAME"),
    rpad(col("_c109"), 5, " ").alias("DRUGMFGRID"),
    rpad(col("_c110"), 10, " ").alias("DRUGMFGR"),
    rpad(col("_c111"), 14, " ").alias("GPINUMBER"),
    rpad(col("_c112"), 60, " ").alias("GENERICNME"),
    rpad(col("_c113"), 2, " ").alias("PRDPACUOM"),
    col("_c114").alias("PRDPACSIZE"),
    col("_c115").alias("DDID"),
    col("_c116").alias("GCN"),
    col("_c117").alias("GCNSEQ"),
    col("_c118").alias("KDC"),
    rpad(col("_c119"), 8, " ").alias("AHFS"),
    rpad(col("_c120"), 1, " ").alias("DRUGDEACOD"),
    rpad(col("_c121"), 1, " ").alias("RXOTCIND"),
    rpad(col("_c122"), 1, " ").alias("MULTSRCCDE"),
    rpad(col("_c123"), 1, " ").alias("GENINDOVER"),
    rpad(col("_c124"), 1, " ").alias("PRDREIMIND"),
    rpad(col("_c125"), 1, " ").alias("BRNDTRDNME"),
    rpad(col("_c126"), 2, " ").alias("FDATHERAEQ"),
    col("_c127").alias("METRICSTRG"),
    rpad(col("_c128"), 10, " ").alias("DRGSTRGUOM"),
    rpad(col("_c129"), 2, " ").alias("ADMINROUTE"),
    rpad(col("_c130"), 11, " ").alias("ADMINRTESN"),
    rpad(col("_c131"), 4, " ").alias("DOSAGEFORM"),
    rpad(col("_c132"), 1, " ").alias("NDA"),
    rpad(col("_c133"), 1, " ").alias("ANDA"),
    rpad(col("_c134"), 1, " ").alias("ANDAOR"),
    rpad(col("_c135"), 10, " ").alias("RXNORMCODE"),
    rpad(col("_c136"), 1, " ").alias("MNTDRUGCDE"),
    rpad(col("_c137"), 1, " ").alias("MNTSOURCE"),
    rpad(col("_c138"), 1, " ").alias("MNTCARPROR"),
    rpad(col("_c139"), 1, " ").alias("MNTGPILIST"),
    rpad(col("_c140"), 10, " ").alias("CPQSPCPRG"),
    rpad(col("_c141"), 1, " ").alias("CPQSPCPGIN"),
    rpad(col("_c142"), 10, " ").alias("CPQSPCSCHD"),
    rpad(col("_c143"), 1, " ").alias("THRDPARTYX"),
    rpad(col("_c144"), 1, " ").alias("DRGUNITDOS"),
    rpad(col("_c145"), 1, " ").alias("SBMUNITDOS"),
    rpad(col("_c146"), 1, " ").alias("ALTPRODTYP"),
    rpad(col("_c147"), 13, " ").alias("ALTPRODCDE"),
    rpad(col("_c148"), 6, " ").alias("SRXNETWRK"),
    rpad(col("_c149"), 2, " ").alias("SRXNETTYPE"),
    rpad(col("_c150"), 6, " ").alias("RXNETWORK"),
    rpad(col("_c151"), 25, " ").alias("RXNETWRKNM"),
    rpad(col("_c152"), 9, " ").alias("RXNETCARR"),
    rpad(col("_c153"), 1, " ").alias("RXNETWRKQL"),
    rpad(col("_c154"), 1, " ").alias("RXNETPRCQL"),
    rpad(col("_c155"), 10, " ").alias("REGIONCDE"),
    rpad(col("_c156"), 10, " ").alias("PHRAFFIL"),
    rpad(col("_c157"), 3, " ").alias("NETPRIOR"),
    rpad(col("_c158"), 2, " ").alias("NETTYPE"),
    col("_c159").alias("NETSEQ"),
    rpad(col("_c160"), 10, " ").alias("PAYCNTR"),
    rpad(col("_c161"), 10, " ").alias("PHRNDCLST"),
    rpad(col("_c162"), 10, " ").alias("PHRGPILST"),
    rpad(col("_c163"), 15, " ").alias("SRVPROVID"),
    rpad(col("_c164"), 2, " ").alias("SRVPROVIDQ"),
    rpad(col("_c165"), 10, " ").alias("NPIPROV"),
    rpad(col("_c166"), 12, " ").alias("PRVNCPDPID"),
    rpad(col("_c167"), 15, " ").alias("SBMSRVPRID"),
    rpad(col("_c168"), 2, " ").alias("SBMSRVPRQL"),
    rpad(col("_c169"), 55, " ").alias("SRVPROVNME"),
    rpad(col("_c170"), 1, " ").alias("PROVLOCKQL"),
    rpad(col("_c171"), 15, " ").alias("PROVLOCKID"),
    rpad(col("_c172"), 15, " ").alias("STORENBR"),
    rpad(col("_c173"), 6, " ").alias("AFFILIATIN"),
    rpad(col("_c174"), 12, " ").alias("PAYEEID"),
    rpad(col("_c175"), 3, " ").alias("DISPRCLASS"),
    rpad(col("_c176"), 3, " ").alias("DISPROTHER"),
    rpad(col("_c177"), 10, " ").alias("PHARMZIP"),
    rpad(col("_c178"), 6, " ").alias("PRESNETWID"),
    rpad(col("_c179"), 15, " ").alias("PRESCRIBER"),
    rpad(col("_c180"), 2, " ").alias("PRESCRIDQL"),
    rpad(col("_c181"), 10, " ").alias("NPIPRESCR"),
    rpad(col("_c182"), 15, " ").alias("PRESCDEAID"),
    rpad(col("_c183"), 25, " ").alias("PRESLSTNME"),
    rpad(col("_c184"), 15, " ").alias("PRESFSTNME"),
    rpad(col("_c185"), 1, " ").alias("PRESMDINIT"),
    rpad(col("_c186"), 6, " ").alias("PRESSPCCDE"),
    rpad(col("_c187"), 3, " ").alias("PRESSPCCDQ"),
    rpad(col("_c188"), 3, " ").alias("PRSSTATE"),
    rpad(col("_c189"), 15, " ").alias("SBMRPHID"),
    rpad(col("_c190"), 2, " ").alias("SBMRPHIDQL"),
    rpad(col("_c191"), 10, " ").alias("FNLPLANCDE"),
    col("_c192").alias("FNLPLANDTE"),
    rpad(col("_c193"), 8, " ").alias("PLANTYPE"),
    rpad(col("_c194"), 10, " ").alias("PLANQUAL"),
    rpad(col("_c195"), 10, " ").alias("PLNNDCLIST"),
    rpad(col("_c196"), 10, " ").alias("LSTQUALNDC"),
    rpad(col("_c197"), 10, " ").alias("PLNGPILIST"),
    rpad(col("_c198"), 10, " ").alias("LSTQUALGPI"),
    rpad(col("_c199"), 10, " ").alias("PLNPNDCLST"),
    rpad(col("_c200"), 10, " ").alias("PLNPGPILST"),
    rpad(col("_c201"), 1, " ").alias("PLANDRUGST"),
    rpad(col("_c202"), 1, " ").alias("PLANFRMLRY"),
    rpad(col("_c203"), 10, " ").alias("PLNFNLPSCH"),
    rpad(col("_c204"), 13, " ").alias("PHRDCSCHID"),
    col("_c205").alias("PHRDCSCHSQ"),
    rpad(col("_c206"), 13, " ").alias("CLTDCSCHID"),
    col("_c207").alias("CLTDCSCHSQ"),
    col("_c208").alias("PHRDCCSCID"),
    col("_c209").alias("PHRDCCSCSQ"),
    rpad(col("_c210"), 14, " ").alias("CLTDCCSCID"),
    col("_c211").alias("CLTDCCSCSQ"),
    rpad(col("_c212"), 13, " ").alias("PHRPRTSCID"),
    rpad(col("_c213"), 13, " ").alias("CLTPRTSCID"),
    rpad(col("_c214"), 10, " ").alias("PHRRMSCHID"),
    rpad(col("_c215"), 10, " ").alias("CLTRMSCHID"),
    rpad(col("_c216"), 10, " ").alias("PRDPFLSTID"),
    rpad(col("_c217"), 10, " ").alias("PRFPRDSCID"),
    rpad(col("_c218"), 1, " ").alias("FORMULARY"),
    rpad(col("_c219"), 1, " ").alias("FORMLRFLAG"),
    col("_c220").alias("TIERVALUE"),
    rpad(col("_c221"), 1, " ").alias("CONTHERAPY"),
    rpad(col("_c222"), 20, " ").alias("CTSCHEDID"),
    rpad(col("_c223"), 10, " ").alias("PRBASISCHD"),
    rpad(col("_c224"), 10, " ").alias("REGDISOR"),
    rpad(col("_c225"), 13, " ").alias("DRUGSTSTBL"),
    rpad(col("_c226"), 10, " ").alias("TRANSBEN"),
    rpad(col("_c227"), 10, " ").alias("MESSAGECD1"),
    rpad(col("_c228"), 40, " ").alias("MESSAGE1"),
    rpad(col("_c229"), 10, " ").alias("MESSAGECD2"),
    rpad(col("_c230"), 40, " ").alias("MESSAGE2"),
    rpad(col("_c231"), 10, " ").alias("MESSAGECD3"),
    rpad(col("_c232"), 40, " ").alias("MESSAGE3"),
    col("_c233").alias("REJCNT"),
    rpad(col("_c234"), 3, " ").alias("REJCDE1"),
    rpad(col("_c235"), 3, " ").alias("REJCDE2"),
    rpad(col("_c236"), 3, " ").alias("REJCDE3"),
    rpad(col("_c237"), 8, " ").alias("RJCPLANID"),
    rpad(col("_c238"), 2, " ").alias("DURCONFLCT"),
    rpad(col("_c239"), 2, " ").alias("DURINTERVN"),
    rpad(col("_c240"), 2, " ").alias("DUROUTCOME"),
    rpad(col("_c241"), 2, " ").alias("LVLSERVICE"),
    rpad(col("_c242"), 2, " ").alias("PHARSRVTYP"),
    rpad(col("_c243"), 15, " ").alias("DIAGNOSIS"),
    rpad(col("_c244"), 2, " ").alias("DIAGNOSISQ"),
    rpad(col("_c245"), 2, " ").alias("RVDURCNFLC"),
    rpad(col("_c246"), 2, " ").alias("RVDURINTRV"),
    rpad(col("_c247"), 2, " ").alias("RVDUROUTCM"),
    rpad(col("_c248"), 2, " ").alias("RVLVLSERVC"),
    rpad(col("_c249"), 2, " ").alias("DRGCNFLCT1"),
    rpad(col("_c250"), 1, " ").alias("SEVERITY1"),
    rpad(col("_c251"), 1, " ").alias("OTHRPHARM1"),
    col("_c252").alias("DTEPRVFIL1"),
    col("_c253").alias("QTYPRVFIL1"),
    rpad(col("_c254"), 1, " ").alias("DATABASE1"),
    rpad(col("_c255"), 1, " ").alias("OTHRPRESC1"),
    rpad(col("_c256"), 30, " ").alias("FREETEXT1"),
    rpad(col("_c257"), 2, " ").alias("DRGCNFLCT2"),
    rpad(col("_c258"), 1, " ").alias("SEVERITY2"),
    rpad(col("_c259"), 1, " ").alias("OTHRPHARM2"),
    col("_c260").alias("DTEPRVFIL2"),
    col("_c261").alias("QTYPRVFIL2"),
    rpad(col("_c262"), 1, " ").alias("DATABASE2"),
    rpad(col("_c263"), 1, " ").alias("OTHRPRESC2"),
    rpad(col("_c264"), 30, " ").alias("FREETEXT2"),
    rpad(col("_c265"), 2, " ").alias("DRGCNFLCT3"),
    rpad(col("_c266"), 1, " ").alias("SEVERITY3"),
    rpad(col("_c267"), 1, " ").alias("OTHRPHARM3"),
    col("_c268").alias("DTEPRVFIL3"),
    col("_c269").alias("QTYPRVFIL3"),
    rpad(col("_c270"), 1, " ").alias("DATABASE3"),
    rpad(col("_c271"), 1, " ").alias("OTHRPRESC3"),
    col("_c272").alias("FREETEXT3"),
    rpad(col("_c273"), 2, " ").alias("FEETYPE"),
    col("_c274").alias("BENUNITCST"),
    col("_c275").alias("AWPUNITCST"),
    col("_c276").alias("WACUNITCST"),
    col("_c277").alias("GEAPUNTCST"),
    col("_c278").alias("CTYPEUCOST"),
    rpad(col("_c279"), 2, " ").alias("BASISCOST"),
    col("_c280").alias("PRICEQTY"),
    col("_c281").alias("PRODAYSSUP"),
    col("_c282").alias("PROQTY"),
    col("_c283").alias("RVINCNTVSB"),
    col("_c284").alias("SBMINGRCST"),
    col("_c285").alias("SBMDISPFEE"),
    col("_c286").alias("SBMPSLSTX"),
    col("_c287").alias("SBMFSLSTX"),
    col("_c288").alias("SBMSLSTX"),
    col("_c289").alias("SBMPATPAY"),
    col("_c290").alias("SBMAMTDUE"),
    col("_c291").alias("SBMINCENTV"),
    col("_c292").alias("SBMPROFFEE"),
    col("_c293").alias("SBMTOTHAMT"),
    col("_c294").alias("SBMOPAMTCT"),
    rpad(col("_c295"), 2, " ").alias("SBMOPAMTQL"),
    col("_c296").alias("USUALNCUST"),
    col("_c297").alias("DENIALDTE"),
    col("_c298").alias("OTHRPAYOR"),
    col("_c299").alias("SBMMDPDAMT"),
    col("_c300").alias("CALINGRCST"),
    col("_c301").alias("CALDISPFEE"),
    col("_c302").alias("CALPSTAX"),
    col("_c303").alias("CALFSTAX"),
    col("_c304").alias("CALSLSTAX"),
    col("_c305").alias("CALPATPAY"),
    col("_c306").alias("CALDUEAMT"),
    col("_c307").alias("CALWITHHLD"),
    col("_c308").alias("CALFCOPAY"),
    col("_c309").alias("CALPCOPAY"),
    col("_c310").alias("CALCOPAY"),
    col("_c311").alias("CALPRODSEL"),
    col("_c312").alias("CALATRTAX"),
    col("_c313").alias("CALEXCEBFT"),
    col("_c314").alias("CALINCENTV"),
    col("_c315").alias("CALATRDED"),
    col("_c316").alias("CALCOB"),
    col("_c317").alias("CALTOTHAMT"),
    col("_c318").alias("CALPROFFEE"),
    col("_c319").alias("CALOTHPAYA"),
    rpad(col("_c320"), 1, " ").alias("CALCOSTSRC"),
    col("_c321").alias("CALADMNFEE"),
    col("_c322").alias("CALPROCFEE"),
    col("_c323").alias("CALPATSTAX"),
    col("_c324").alias("CALPLNSTAX"),
    col("_c325").alias("CALPRVNSEL"),
    col("_c326").alias("CALPSCBRND"),
    col("_c327").alias("CALPSCNONP"),
    col("_c328").alias("CALPSCBRNP"),
    col("_c329").alias("CALCOVGAP"),
    col("_c330").alias("CALINGCSTC"),
    col("_c331").alias("CALDSPFEEC"),
    col("_c332").alias("PHRINGRCST"),
    col("_c333").alias("PHRDISPFEE"),
    col("_c334").alias("PHRPPSTAX"),
    col("_c335").alias("PHRFSTAX"),
    col("_c336").alias("PHRSLSTAX"),
    col("_c337").alias("PHRPATPAY"),
    col("_c338").alias("PHRDUEAMT"),
    col("_c339").alias("PHRWITHHLD"),
    rpad(col("_c340"), 10, " ").alias("PHRPPRCS"),
    rpad(col("_c341"), 10, " ").alias("PHRPRCST"),
    rpad(col("_c342"), 10, " ").alias("PHRPTPS"),
    rpad(col("_c343"), 13, " ").alias("PHRPTPST"),
    rpad(col("_c344"), 10, " ").alias("PHRCOPAYSC"),
    col("_c345").alias("PHRCOPAYSS"),
    col("_c346").alias("PHRFCOPAY"),
    col("_c347").alias("PHRPCOPAY"),
    col("_c348").alias("PHRCOPAY"),
    col("_c349").alias("PHRPRODSEL"),
    col("_c350").alias("PHRATRTAX"),
    col("_c351").alias("PHREXCEBFT"),
    col("_c352").alias("PHRINCENTV"),
    col("_c353").alias("PHRATRDED"),
    col("_c354").alias("PHRCOB"),
    col("_c355").alias("PHRTOTHAMT"),
    col("_c356").alias("PHRPROFFEE"),
    col("_c357").alias("PHROTHPAYA"),
    rpad(col("_c358"), 1, " ").alias("PHRCOSTSRC"),
    rpad(col("_c359"), 10, " ").alias("PHRCOSTTYP"),
    rpad(col("_c360"), 10, " ").alias("PHRPRCTYPE"),
    col("_c361").alias("PHRRATE"),
    col("_c362").alias("PHRPROCFEE"),
    col("_c363").alias("PHRPATSTAX"),
    col("_c364").alias("PHRPLNSTAX"),
    col("_c365").alias("PHRPRVNSEL"),
    col("_c366").alias("PHRPSCBRND"),
    col("_c367").alias("PHRPSCNONP"),
    col("_c368").alias("PHRPSCBRNP"),
    col("_c369").alias("PHRCOVGAP"),
    col("_c370").alias("PHRINGCSTC"),
    col("_c371").alias("PHRDSPFEEC"),
    col("_c372").alias("POSINGRCST"),
    col("_c373").alias("POSDISPFEE"),
    col("_c374").alias("POSPSLSTAX"),
    col("_c375").alias("POSFSLSTAX"),
    col("_c376").alias("POSSLSTAX"),
    col("_c377").alias("POSPATPAY"),
    col("_c378").alias("POSDUEAMT"),
    col("_c379").alias("POSWITHHLD"),
    col("_c380").alias("POSCOPAY"),
    col("_c381").alias("POSPRODSEL"),
    col("_c382").alias("POSATRTAX"),
    col("_c383").alias("POSEXCEBFT"),
    col("_c384").alias("POSINCENTV"),
    col("_c385").alias("POSATRDED"),
    col("_c386").alias("POSTOTHAMT"),
    col("_c387").alias("POSPROFFEE"),
    col("_c388").alias("POSOTHPAYA"),
    rpad(col("_c389"), 1, " ").alias("POSCOSTSRC"),
    col("_c390").alias("POSPROCFEE"),
    col("_c391").alias("POSPATSTAX"),
    col("_c392").alias("POSPLNSTAX"),
    col("_c393").alias("POSPRVNSEL"),
    col("_c394").alias("POSPSCBRND"),
    col("_c395").alias("POSPSCNONP"),
    col("_c396").alias("POSPSCBRNP"),
    col("_c397").alias("POSCOVGAP"),
    col("_c398").alias("POSINGCSTC"),
    col("_c399").alias("POSDSPFEEC"),
    rpad(col("_c400"), 1, " ").alias("CLIENTFLAG"),
    col("_c401").alias("CLTINGRCST"),
    col("_c402").alias("CLTDISPFEE"),
    col("_c403").alias("CLTSLSTAX"),
    col("_c404").alias("CLTPATPAY"),
    col("_c405").alias("CLTDUEAMT"),
    col("_c406").alias("CLTWITHHLD"),
    rpad(col("_c407"), 10, " ").alias("CLTPRCS"),
    rpad(col("_c408"), 10, " ").alias("CLTPRCST"),
    rpad(col("_c409"), 10, " ").alias("CLTPTPS"),
    rpad(col("_c410"), 13, " ").alias("CLTPTPST"),
    rpad(col("_c411"), 10, " ").alias("CLTCOPAYS"),
    col("_c412").alias("CLTCOPAYSS"),
    col("_c413").alias("CLTFCOPAY"),
    col("_c414").alias("CLTPCOPAY"),
    col("_c415").alias("CLTCOPAY"),
    col("_c416").alias("CLTPRODSEL"),
    col("_c417").alias("CLTPSTAX"),
    col("_c418").alias("CLTFSTAX"),
    col("_c419").alias("CLTATRTAX"),
    col("_c420").alias("CLTEXCEBFT"),
    col("_c421").alias("CLTINCENTV"),
    col("_c422").alias("CLTATRDED"),
    col("_c423").alias("CLTTOTHAMT"),
    col("_c424").alias("CLTPROFFEE"),
    col("_c425").alias("CLTCOB"),
    col("_c426").alias("CLTOTHPAYA"),
    rpad(col("_c427"), 1, " ").alias("CLTCOSTSRC"),
    rpad(col("_c428"), 10, " ").alias("CLTCOSTTYP"),
    rpad(col("_c429"), 10, " ").alias("CLTPRCTYPE"),
    col("_c430").alias("CLTRATE"),
    col("_c431").alias("CLTPRSCSTP"),
    rpad(col("_c432"), 18, " ").alias("CLTPRSCHNM"),
    col("_c433").alias("CLTPROCFEE"),
    col("_c434").alias("CLTPATSTAX"),
    col("_c435").alias("CLTPLNSTAX"),
    col("_c436").alias("CLTPRVNSEL"),
    col("_c437").alias("CLTPSCBRND"),
    col("_c438").alias("CLTPSCNONP"),
    col("_c439").alias("CLTPSCBRNP"),
    col("_c440").alias("CLTCOVGAP"),
    col("_c441").alias("CLTINGCSTC"),
    col("_c442").alias("CLTDSPFEEC"),
    rpad(col("_c443"), 2, " ").alias("RSPREIMBUR"),
    col("_c444").alias("RSPINGRCST"),
    col("_c445").alias("RSPDISPFEE"),
    col("_c446").alias("RSPPSLSTAX"),
    col("_c447").alias("RSPFSLSTAX"),
    col("_c448").alias("RSPSLSTAX"),
    col("_c449").alias("RSPPATPAY"),
    col("_c450").alias("RSPDUEAMT"),
    col("_c451").alias("RSPFCOPAY"),
    col("_c452").alias("RSPPCOPAY"),
    col("_c453").alias("RSPCOPAY"),
    col("_c454").alias("RSPPRODSEL"),
    col("_c455").alias("RSPATRTAX"),
    col("_c456").alias("RSPEXCEBFT"),
    col("_c457").alias("RSPINCENTV"),
    col("_c458").alias("RSPATRDED"),
    col("_c459").alias("RSPTOTHAMT"),
    col("_c460").alias("RSPPROFEE"),
    col("_c461").alias("RSPOTHPAYA"),
    col("_c462").alias("RSPACCUDED"),
    col("_c463").alias("RSPREMBFT"),
    col("_c464").alias("RSPREMDED"),
    col("_c465").alias("RSPPROCFEE"),
    col("_c466").alias("RSPPATSTAX"),
    col("_c467").alias("RSPPLNSTAX"),
    col("_c468").alias("RSPPRVNSEL"),
    col("_c469").alias("RSPPSCBRND"),
    col("_c470").alias("RSPPSCNONP"),
    col("_c471").alias("RSPPSCBRNP"),
    col("_c472").alias("RSPCOVGAP"),
    col("_c473").alias("RSPINGCSTC"),
    col("_c474").alias("RSPDSPFEEC"),
    rpad(col("_c475"), 8, " ").alias("RSPPLANID"),
    rpad(col("_c476"), 2, " ").alias("BENSTGQL1"),
    col("_c477").alias("BENSTGAMT1"),
    rpad(col("_c478"), 2, " ").alias("BENSTGQL2"),
    col("_c479").alias("BENSTGAMT2"),
    rpad(col("_c480"), 2, " ").alias("BENSTGQL3"),
    col("_c481").alias("BENSTGAMT3"),
    rpad(col("_c482"), 2, " ").alias("BENSTGQL4"),
    col("_c483").alias("BENSTGAMT4"),
    col("_c484").alias("ESTGENSAV"),
    col("_c485").alias("SPDACCTREM"),
    col("_c486").alias("HLTHPLNAMT"),
    col("_c487").alias("INDDEDPTD"),
    col("_c488").alias("INDDEDREM"),
    col("_c489").alias("FAMDEDPTD"),
    col("_c490").alias("FAMDEDREM"),
    rpad(col("_c491"), 10, " ").alias("DEDSCHED"),
    rpad(col("_c492"), 10, " ").alias("DEDACCC"),
    rpad(col("_c493"), 1, " ").alias("DEDFLAG"),
    col("_c494").alias("INDLBFTUT"),
    col("_c495").alias("INDLBFTPTD"),
    col("_c496").alias("INDLBFTREM"),
    col("_c497").alias("INDLBFTUT2"),
    col("_c498").alias("FAMLBFTPTD"),
    col("_c499").alias("FAMLBFTREM"),
    rpad(col("_c500"), 10, " ").alias("LFTBFTMSCH"),
    rpad(col("_c501"), 10, " ").alias("LFTBFTACCC"),
    rpad(col("_c502"), 1, " ").alias("LFTBFTFLAG"),
    col("_c503").alias("INDBFTUT"),
    col("_c504").alias("INDBMAXPTD"),
    col("_c505").alias("FAMBFTUT"),
    col("_c506").alias("FAMBMAXPTD"),
    col("_c507").alias("INDBMAXREM"),
    col("_c508").alias("FAMBMAXREM"),
    rpad(col("_c509"), 10, " ").alias("BFTMAXSCHD"),
    rpad(col("_c510"), 10, " ").alias("BFTMAXACCC"),
    rpad(col("_c511"), 1, " ").alias("BFTMAXFLAG"),
    col("_c512").alias("INDOOPPTD"),
    col("_c513").alias("FAMOOPPTD"),
    col("_c514").alias("INDOOPREM"),
    col("_c515").alias("FAMOOPREM"),
    rpad(col("_c516"), 10, " ").alias("OOPSCHED"),
    rpad(col("_c517"), 10, " ").alias("OOPACCC"),
    rpad(col("_c518"), 1, " ").alias("OOPFLAG"),
    col("_c519").alias("CONTRIBUT"),
    rpad(col("_c520"), 2, " ").alias("CONTBASIS"),
    rpad(col("_c521"), 10, " ").alias("CONTSCHED"),
    rpad(col("_c522"), 10, " ").alias("CONTACCCD"),
    rpad(col("_c523"), 1, " ").alias("CONTFLAG"),
    rpad(col("_c524"), 1, " ").alias("RXTFLAG"),
    rpad(col("_c525"), 1, " ").alias("REIMBURSMT"),
    rpad(col("_c526"), 1, " ").alias("CLMORIGIN"),
    rpad(col("_c527"), 1, " ").alias("HLDCLMFLAG"),
    col("_c528").alias("HLDCLMDAYS"),
    rpad(col("_c529"), 1, " ").alias("PARTDFLAG"),
    rpad(col("_c530"), 1, " ").alias("COBEXTFLG"),
    rpad(col("_c531"), 1, " ").alias("PAEXTFLG"),
    rpad(col("_c532"), 1, " ").alias("HSAEXTIND"),
    rpad(col("_c533"), 1, " ").alias("FFPMEDRMST"),
    rpad(col("_c534"), 1, " ").alias("FFPMEDPXST"),
    rpad(col("_c535"), 1, " ").alias("FFPMEDMSST"),
    rpad(col("_c536"), 25, " ").alias("INCIDENTID"),
    rpad(col("_c537"), 30, " ").alias("ETCNBR"),
    col("_c538").alias("DTEINJURY"),
    rpad(col("_c539"), 10, " ").alias("ADDUSER"),
    rpad(col("_c540"), 10, " ").alias("CHGUSER"),
    rpad(col("_c541"), 10, " ").alias("DMRUSERID"),
    rpad(col("_c542"), 10, " ").alias("PRAUSERID"),
    rpad(col("_c543"), 30, " ").alias("CLAIMREFID"),
    rpad(col("_c544"), 3, " ").alias("EOBDNOV"),
    rpad(col("_c545"), 3, " ").alias("EOBPDOV"),
    rpad(col("_c546"), 14, " ").alias("MANTRKNBR"),
    col("_c547").alias("MANRECVDTE"),
    rpad(col("_c548"), 2, " ").alias("PASAUTHTYP"),
    rpad(col("_c549"), 11, " ").alias("PASAUTHID"),
    rpad(col("_c550"), 1, " ").alias("PASREQTYPE"),
    col("_c551").alias("PASREQFROM"),
    col("_c552").alias("PASREQTHRU"),
    rpad(col("_c553"), 2, " ").alias("PASBASISRQ"),
    rpad(col("_c554"), 12, " ").alias("PASREPFN"),
    rpad(col("_c555"), 15, " ").alias("PASREPLN"),
    rpad(col("_c556"), 30, " ").alias("PASSTREET"),
    rpad(col("_c557"), 20, " ").alias("PASCITY"),
    rpad(col("_c558"), 2, " ").alias("PASSTATE"),
    rpad(col("_c559"), 15, " ").alias("PASZIP"),
    rpad(col("_c560"), 11, " ").alias("PASPANBR"),
    rpad(col("_c561"), 20, " ").alias("PASAUTHNBR"),
    col("_c562").alias("PASSDOCCT"),
    rpad(col("_c563"), 1, " ").alias("PAYERTYPE"),
    rpad(col("_c564"), 2, " ").alias("DELAYRSNCD"),
    rpad(col("_c565"), 2, " ").alias("MEDCDIND"),
    rpad(col("_c566"), 20, " ").alias("MEDCDID"),
    rpad(col("_c567"), 15, " ").alias("MEDCDAGNBR"),
    rpad(col("_c568"), 20, " ").alias("MEDCDTCN"),
    rpad(col("_c569"), 2, " ").alias("FMSTIER"),
    rpad(col("_c570"), 2, " ").alias("FMSSTATUS"),
    rpad(col("_c571"), 1, " ").alias("FMSDFLTIND"),
    rpad(col("_c572"), 10, " ").alias("FMSBENLST"),
    rpad(col("_c573"), 10, " ").alias("FMSLSTLVL3"),
    rpad(col("_c574"), 10, " ").alias("FMSLSTLVL2"),
    rpad(col("_c575"), 10, " ").alias("FMSLSTLVL1"),
    rpad(col("_c576"), 10, " ").alias("FMSRULESET"),
    rpad(col("_c577"), 10, " ").alias("FMSRULE"),
    rpad(col("_c578"), 2, " ").alias("FMSPROCCD"),
    rpad(col("_c579"), 10, " ").alias("CLIENTDEF1"),
    rpad(col("_c580"), 10, " ").alias("CLIENTDEF2"),
    rpad(col("_c581"), 10, " ").alias("CLIENTDEF3"),
    rpad(col("_c582"), 10, " ").alias("CLIENTDEF4"),
    rpad(col("_c583"), 10, " ").alias("CLIENTDEF5"),
    rpad(col("_c584"), 10, " ").alias("CCTRESERV1"),
    rpad(col("_c585"), 10, " ").alias("CCTRESERV2"),
    rpad(col("_c586"), 10, " ").alias("CCTRESERV3"),
    rpad(col("_c587"), 10, " ").alias("CCTRESERV4"),
    rpad(col("_c588"), 10, " ").alias("CCTRESERV5"),
    rpad(col("_c589"), 10, " ").alias("CCTRESERV6"),
    rpad(col("_c590"), 10, " ").alias("CCTRESERV7"),
    rpad(col("_c591"), 10, " ").alias("CCTRESERV8"),
    rpad(col("_c592"), 20, " ").alias("CCTRESERV9"),
    rpad(col("_c593"), 20, " ").alias("CCTRESRV10"),
    col("_c594").alias("OOPAPPLIED"),
    col("_c595").alias("CCTRESRV12"),
    col("_c596").alias("CCTRESRV13"),
    col("_c597").alias("CCTRESRV14"),
    rpad(col("_c598"), 10, " ").alias("USERFIELD"),
    col("_c599").alias("EXTRACTDTE"),
    rpad(col("_c600"), 21, " ").alias("BATCHCTRL"),
    col("_c601").alias("CLTTYPUCST"),
    rpad(col("_c602"), 3, " ").alias("REJCDE4"),
    rpad(col("_c603"), 3, " ").alias("REJCDE5"),
    rpad(col("_c604"), 3, " ").alias("REJCDE6"),
    rpad(col("_c605"), 10, " ").alias("MESSAGECD4"),
    rpad(col("_c606"), 40, " ").alias("MESSAGE4"),
    rpad(col("_c607"), 10, " ").alias("MESSAGECD5"),
    rpad(col("_c608"), 40, " ").alias("MESSAGE5"),
    rpad(col("_c609"), 10, " ").alias("MESSAGECD6"),
    rpad(col("_c610"), 40, " ").alias("MESSAGE6"),
    rpad(col("_c611"), 1, " ").alias("CLIENT2FLAG"),
    col("_c612").alias("CLT2INGRCST"),
    col("_c613").alias("CLT2DISPFEE"),
    col("_c614").alias("CLT2SLSTAX"),
    col("_c615").alias("CLT2DUEAMT"),
    col("_c616").alias("CLTPRSCSTP2"),
    col("_c617").alias("CLTPRSCHNM2"),
    rpad(col("_c618"), 10, " ").alias("CLT2PRCS"),
    rpad(col("_c619"), 10, " ").alias("CLT2PRCST"),
    col("_c620").alias("CLT2PSTAX"),
    col("_c621").alias("CLT2FSTAX"),
    col("_c622").alias("CLT2OTHAMT"),
    rpad(col("_c623"), 1, " ").alias("CLT2COSTSRC"),
    rpad(col("_c624"), 10, " ").alias("CLT2COSTTYP"),
    rpad(col("_c625"), 10, " ").alias("CLT2PRCTYPE"),
    col("_c626").alias("CLT2RATE"),
    rpad(col("_c627"), 14, " ").alias("CLT2DCCSCID"),
    col("_c628").alias("CLT2DCCSCSQ"),
    rpad(col("_c629"), 13, " ").alias("CLT2DCSCHID"),
    col("_c630").alias("CLT2DCSCHSQ"),
    col("_c631").alias("CALINCENTV2"),
    rpad(col("_c632"), 1, " ").alias("CLIENT3FLAG"),
    col("_c633").alias("CLT3INGRCST"),
    col("_c634").alias("CLT3DISPFEE"),
    col("_c635").alias("CLT3SLSTAX"),
    col("_c636").alias("CLT3DUEAMT"),
    col("_c637").alias("CLTPRSCSTP3"),
    col("_c638").alias("CLTPRSCHNM3"),
    rpad(col("_c639"), 10, " ").alias("CLT3PRCS"),
    rpad(col("_c640"), 10, " ").alias("CLT3PRCST"),
    col("_c641").alias("CLT3PSTAX"),
    col("_c642").alias("CLT3FSTAX"),
    col("_c643").alias("CLT3OTHAMT"),
    rpad(col("_c644"), 1, " ").alias("CLT3COSTSRC"),
    rpad(col("_c645"), 10, " ").alias("CLT3COSTTYP"),
    rpad(col("_c646"), 10, " ").alias("CLT3PRCTYPE"),
    col("_c647").alias("CLT3RATE"),
    rpad(col("_c648"), 14, " ").alias("CLT3DCCSCID"),
    col("_c649").alias("CLT3DCCSCSQ"),
    rpad(col("_c650"), 13, " ").alias("CLT3DCSCHID"),
    col("_c651").alias("CLT3DCSCHSQ"),
    col("_c652").alias("CALINCENTV3"),
    rpad(col("_c653"), 5, " ").alias("TBR_TBR_REJECT_REASON1"),
    rpad(col("_c654"), 5, " ").alias("TBR_TBR_REJECT_REASON2"),
    rpad(col("_c655"), 5, " ").alias("TBR_TBR_REJECT_REASON3"),
    rpad(col("_c656"), 13, " ").alias("SBM_QTY_INT_DIS"),
    rpad(col("_c657"), 3, " ").alias("SBM_DS_INT_DIS"),
    rpad(col("_c658"), 3, " ").alias("OTHR_AMT_CLMSBM_CT"),
    rpad(col("_c659"), 2, " ").alias("OTHR_AMT_CLMSBM_QLFR1"),
    rpad(col("_c660"), 11, " ").alias("OTHR_AMT_CLMSBM1"),
    rpad(col("_c661"), 2, " ").alias("OTHR_AMT_CLMSBM_QLFR2"),
    rpad(col("_c662"), 11, " ").alias("OTHR_AMT_CLMSBM2"),
    rpad(col("_c663"), 2, " ").alias("OTHR_AMT_CLMSBM_QLFR3"),
    rpad(col("_c664"), 11, " ").alias("OTHR_AMT_CLMSBM3"),
    rpad(col("_c665"), 2, " ").alias("OTHR_AMT_CLMSBM_QLFR4"),
    rpad(col("_c666"), 11, " ").alias("OTHR_AMT_CLMSBM4"),
    rpad(col("_c667"), 2, " ").alias("OTHR_AMT_CLMSBM_QLFR5"),
    rpad(col("_c668"), 11, " ").alias("OTHR_AMT_CLMSBM5"),
    rpad(col("_c669"), 2, " ").alias("RSN_SRVCDE1"),
    rpad(col("_c670"), 2, " ").alias("RSN_SRVCDE2"),
    rpad(col("_c671"), 2, " ").alias("RSN_SRVCDE3"),
    rpad(col("_c672"), 2, " ").alias("PROF_SRVCDE1"),
    rpad(col("_c673"), 2, " ").alias("PROF_SRVCDE2"),
    rpad(col("_c674"), 2, " ").alias("PROF_SRVCDE3"),
    rpad(col("_c675"), 2, " ").alias("RESULT_OF_SRV_CDE1"),
    rpad(col("_c676"), 2, " ").alias("RESULT_OF_SRV_CDE2"),
    rpad(col("_c677"), 2, " ").alias("RESULT_OF_SRV_CDE3"),
    rpad(col("_c678"), 12, " ").alias("SCH_RX_ID_NBR"),
    rpad(col("_c679"), 2, " ").alias("BASIS_CAL_DISPFEE"),
    rpad(col("_c680"), 2, " ").alias("BASIS_CAL_COPAY"),
    rpad(col("_c681"), 2, " ").alias("BASIS_CAL_FLT_TAX"),
    rpad(col("_c682"), 2, " ").alias("BASIS_CAL_PRCT_TAX"),
    rpad(col("_c683"), 2, " ").alias("BASIS_CAL_COINSUR"),
    rpad(col("_c684"), 1, " ").alias("PAID_MSG_CODE_COUNT"),
    rpad(col("_c685"), 3, " ").alias("PAID_MESSAGE_CODE_1"),
    rpad(col("_c686"), 3, " ").alias("PAID_MESSAGE_CODE_2"),
    rpad(col("_c687"), 3, " ").alias("PAID_MESSAGE_CODE_3"),
    rpad(col("_c688"), 3, " ").alias("PAID_MESSAGE_CODE_4"),
    rpad(col("_c689"), 3, " ").alias("PAID_MESSAGE_CODE_5"),
    rpad(col("_c690"), 26, " ").alias("FILLER_FOR_REJECT_FIELD_IND")
)

windowSpec = Window.orderBy(lit(1))

df_DataCleansing = (
    df_Optum
    .withColumn(
        "svDATESBM",
        when(
            (length(trim(col("DATESBM"))) == 0),
            lit("1753-01-01")
        ).otherwise(
            regexp_replace(trim(col("DATESBM")), "[\u000A\u000D\u0009]", "")
        )
    )
    .withColumn(
        "svDATESBM",
        when(
            length(col("svDATESBM")) > 0,
            when(
                length(col("svDATESBM")) == 8,
                concat(
                    col("svDATESBM").substr(1, 4), lit("-"),
                    col("svDATESBM").substr(5, 2), lit("-"),
                    col("svDATESBM").substr(7, 2)
                )
            ).otherwise(col("svDATESBM"))
        ).otherwise(lit("1753-01-01"))
    )
    .withColumn("svCLAIMSTS", trim(col("CLAIMSTS")))
    .withColumn("svACCOUNTID", when(length(trim(col("ACCOUNTID")))==0, lit("UNK")).otherwise(trim(col("ACCOUNTID"))))
    .withColumn("svGROUPID", when(length(trim(col("GROUPID")))==0, lit("UNK")).otherwise(trim(col("GROUPID"))))
    .withColumn(
        "svMEMBERID_Orig",
        when(
            (col("MEMBERID").isNull()) | (length(trim(col("MEMBERID"))) == 0),
            lit("UNK")
        ).otherwise(trim(col("MEMBERID")).upper())
    )
    .withColumn(
        "svMEMBERID",
        when(
            (col("MEMBERID").isNull()) | (length(trim(col("MEMBERID"))) == 0),
            lit("UNK")
        ).otherwise(trim(col("MEMBERID")).substr(4, 999999).upper())
    )
    .withColumn(
        "svMBRLSTNME",
        when(length(trim(col("MBRLSTNME")))==0, lit("UNK"))
        .otherwise(trim(col("MBRLSTNME")).upper())
    )
    .withColumn(
        "svMBRFSTNME",
        when(length(trim(col("MBRFSTNME")))==0, lit("UNK"))
        .otherwise(trim(col("MBRFSTNME")).upper())
    )
    .withColumn("svMBRRELCDE", trim(col("MBRRELCDE")))
    .withColumn("svMBRSEX", trim(col("MBRSEX")))
    .withColumn(
        "svMBRBIRTH_tmp",
        regexp_replace(trim(col("MBRBIRTH")), "[\u000A\u000D\u0009]", "")
    )
    .withColumn(
        "svMBRBIRTH_tmp",
        when(length(col("svMBRBIRTH_tmp"))==0, lit("1753-01-01"))
        .otherwise(col("svMBRBIRTH_tmp"))
    )
    .withColumn(
        "svMBRBIRTH",
        when(
            length(col("svMBRBIRTH_tmp")) == 8,
            concat(
                col("svMBRBIRTH_tmp").substr(1,4), lit("-"),
                col("svMBRBIRTH_tmp").substr(5,2), lit("-"),
                col("svMBRBIRTH_tmp").substr(7,2)
            )
        ).otherwise(col("svMBRBIRTH_tmp"))
    )
    .withColumn("svSOCSECNBR", trim(col("SOCSECNBR")))
    .withColumn("svCUSTLOC", trim(col("CUSTLOC")))
    .withColumn("svFACILITYID", trim(col("FACILITYID")))
    .withColumn("svOTHCOVERAG", trim(col("OTHCOVERAG")))
    .withColumn("svTIMESBM", col("TIMESBM"))
    .withColumn("svORGPDSBMDT", col("ORGPDSBMDT"))
    .withColumn("svRXNUMBER", trim(col("RXNUMBER")))
    .withColumn("svREFILL", trim(col("REFILL")))
    .withColumn("svDISPSTATUS", trim(col("DISPSTATUS")))
    .withColumn(
        "svDTEFILLED_tmp",
        regexp_replace(trim(col("DTEFILLED")), "[\u000A\u000D\u0009]", "")
    )
    .withColumn(
        "svDTEFILLED",
        when(
            length(col("svDTEFILLED_tmp")) == 8,
            concat(
                col("svDTEFILLED_tmp").substr(1,4), lit("-"),
                col("svDTEFILLED_tmp").substr(5,2), lit("-"),
                col("svDTEFILLED_tmp").substr(7,2)
            )
        ).otherwise(lit("1753-01-01"))
    )
    .withColumn("svCOMPOUNDCD", trim(col("COMPOUNDCD")))
    .withColumn("svPRODUCTID", trim(col("PRODUCTID")))
    .withColumn(
        "svMETRICQTY",
        when(col("METRICQTY").isNull(), lit(-999999)).otherwise(col("METRICQTY"))
    )
    .withColumn(
        "svDECIMALQTY",
        when(col("DECIMALQTY").isNull(), lit(-999999)).otherwise(col("DECIMALQTY"))
    )
    .withColumn(
        "svDAYSSUPPLY",
        when(col("DAYSSUPPLY").isNull(), lit(-999)).otherwise(trim(col("DAYSSUPPLY")))
    )
    .withColumn("svPSC", trim(col("PSC")))
    .withColumn(
        "svWRITTENDTE_tmp",
        regexp_replace(trim(col("WRITTENDTE")), "[\u000A\u000D\u0009]", "")
    )
    .withColumn(
        "svWRITTENDTE",
        when(
            length(col("svWRITTENDTE_tmp")) == 0,
            lit("1753-01-01")
        ).otherwise(
            when(
                length(col("svWRITTENDTE_tmp")) == 8,
                concat(
                    col("svWRITTENDTE_tmp").substr(1,4), lit("-"),
                    col("svWRITTENDTE_tmp").substr(5,2), lit("-"),
                    col("svWRITTENDTE_tmp").substr(7,2)
                )
            ).otherwise(lit("1753-01-01"))
        )
    )
    .withColumn("svNBRFLSAUTH", trim(col("NBRFLSAUTH")))
    .withColumn("svORIGINCDE", trim(col("ORIGINCDE")))
    .withColumn("svPRAUTHNBR", trim(col("PRAUTHNBR")))
    .withColumn("svLABELNAME", trim(col("LABELNAME")))
    .withColumn(
        "svGPINUMBER",
        when(
            (col("GPINUMBER").isNull()) | (length(trim(col("GPINUMBER"))) == 0),
            lit("NA")
        ).otherwise(trim(col("GPINUMBER")))
    )
    .withColumn("svMULTSRCCDE", col("MULTSRCCDE"))
    .withColumn("svGENINDOVER", col("GENINDOVER"))
    .withColumn("svCPQSPCPRG", col("CPQSPCPRG"))
    .withColumn("svCPQSPCPGIN", col("CPQSPCPGIN"))
    .withColumn("svSRXNETWRK", trim(col("SRXNETWRK")))
    .withColumn("svRXNETWORK", trim(col("RXNETWORK")))
    .withColumn("svRXNETWRKQL", col("RXNETWRKQL"))
    .withColumn("svSRVPROVID", trim(col("SRVPROVID")))
    .withColumn(
        "svSRVPROVIDQ",
        when(length(trim(col("SRVPROVIDQ")))==0, lit("00"))
        .otherwise(trim(col("SRVPROVIDQ")))
    )
    .withColumn(
        "svNPIPROV",
        when(
            (col("NPIPROV").isNull()) | (length(trim(col("NPIPROV"))) == 0),
            lit("NA")
        ).otherwise(trim(col("NPIPROV")))
    )
    .withColumn(
        "svPRVNCPDPID",
        when(
            (col("PRVNCPDPID").isNull()) | (length(trim(col("PRVNCPDPID"))) == 0),
            lit("NA")
        ).otherwise(trim(col("PRVNCPDPID")))
    )
    .withColumn("svSRVPROVNME", col("SRVPROVNME"))
    .withColumn("svDISPROTHER", trim(col("DISPROTHER")))
    .withColumn(
        "svPRESCRIBER",
        when(
            (col("PRESCRIBER").isNull()) | (length(trim(col("PRESCRIBER"))) == 0),
            lit("NA")
        ).otherwise(col("PRESCRIBER"))
    )
    .withColumn(
        "svPRESCRIDQL",
        when(length(trim(col("PRESCRIDQL")))==0, lit("00"))
        .otherwise(trim(col("PRESCRIDQL")))
    )
    .withColumn(
        "svNPIPRESCR",
        when(
            (col("NPIPRESCR").isNull()) | (length(trim(col("NPIPRESCR"))) == 0),
            lit("NA")
        ).otherwise(col("NPIPRESCR"))
    )
    .withColumn(
        "svPRESCDEAID",
        when(
            (col("PRESCDEAID").isNull()) | (length(trim(col("PRESCDEAID"))) == 0),
            lit("NA")
        ).otherwise(trim(col("PRESCDEAID")))
    )
    .withColumn(
        "svPRESLSTNME",
        when(length(trim(col("PRESLSTNME")))==0, lit("")).otherwise(trim(col("PRESLSTNME")))
    )
    .withColumn(
        "svPRESFSTNME",
        when(length(trim(col("PRESFSTNME")))==0, lit("")).otherwise(trim(col("PRESFSTNME")))
    )
    .withColumn(
        "svPRESSPCCDE",
        when(length(trim(col("PRESSPCCDE")))==0, lit("NA")).otherwise(trim(col("PRESSPCCDE")))
    )
    .withColumn("svPRSSTATE", col("PRSSTATE"))
    .withColumn("svFNLPLANCDE", col("FNLPLANCDE"))
    .withColumn("svFNLPLANDTE", col("FNLPLANDTE"))
    .withColumn("svPLANQUAL", trim(col("PLANQUAL")))
    .withColumn("svCOBIND", trim(col("COBIND")))
    .withColumn("svFORMLRFLAG", col("FORMLRFLAG"))
    .withColumn("svREJCDE1", col("REJCDE1"))
    .withColumn("svREJCDE2", col("REJCDE2"))
    .withColumn(
        "svAWPUNITCST",
        when(
            (col("AWPUNITCST").isNull()) | (length(trim(col("AWPUNITCST"))) == 0),
            lit("0.0")
        ).otherwise(col("AWPUNITCST"))
    )
    .withColumn("svWACUNITCST", col("WACUNITCST"))
    .withColumn("svCTYPEUCOST", col("CTYPEUCOST"))
    .withColumn("svBASISCOST", trim(col("BASISCOST")))
    .withColumn("svPROQTY", col("PROQTY"))
    .withColumn(
        "svSBMINGRCST",
        when(
            (col("SBMINGRCST").isNull()) | (length(trim(col("SBMINGRCST"))) == 0),
            lit(0)
        ).otherwise(col("SBMINGRCST"))
    )
    .withColumn(
        "svSBMDISPFEE",
        when(
            (col("SBMDISPFEE").isNull()) | (length(trim(col("SBMDISPFEE"))) == 0),
            lit(0)
        ).otherwise(col("SBMDISPFEE"))
    )
    .withColumn(
        "svSBMAMTDUE",
        when(
            (col("SBMAMTDUE").isNull()) | (length(trim(col("SBMAMTDUE"))) == 0),
            lit(0)
        ).otherwise(col("SBMAMTDUE"))
    )
    .withColumn(
        "svUSUALNCUST",
        when(
            (col("USUALNCUST").isNull()) | (length(trim(col("USUALNCUST"))) == 0),
            lit(0)
        ).otherwise(col("USUALNCUST"))
    )
    .withColumn(
        "svDENIALDTE_tmp",
        regexp_replace(trim(col("DENIALDTE")), "[\u000A\u000D\u0009]", "")
    )
    .withColumn(
        "svDENIALDTE",
        when(
            length(col("svDENIALDTE_tmp")) == 0,
            lit("1753-01-01")
        ).otherwise(
            when(
                length(col("svDENIALDTE_tmp")) == 8,
                concat(
                    col("svDENIALDTE_tmp").substr(1,4), lit("-"),
                    col("svDENIALDTE_tmp").substr(5,2), lit("-"),
                    col("svDENIALDTE_tmp").substr(7,2)
                )
            ).otherwise(lit("1753-01-01"))
        )
    )
    .withColumn(
        "svPHRDISPFEE",
        when(
            (col("PHRDISPFEE").isNull()) | (length(trim(col("PHRDISPFEE"))) == 0),
            lit(0.00)
        ).otherwise(col("PHRDISPFEE"))
    )
    .withColumn(
        "svCLTINGRCST",
        when(
            (col("CLTINGRCST").isNull()) | (length(trim(col("CLTINGRCST"))) == 0),
            lit(0.00)
        ).otherwise(col("CLTINGRCST"))
    )
    .withColumn(
        "svCLTDISPFEE",
        when(
            (col("CLTDISPFEE").isNull()) | (length(trim(col("CLTDISPFEE"))) == 0),
            lit(0.00)
        ).otherwise(col("CLTDISPFEE"))
    )
    .withColumn(
        "svCLTSLSTAX",
        when(
            (col("CLTSLSTAX").isNull()) | (length(trim(col("CLTSLSTAX"))) == 0),
            lit(0.00)
        ).otherwise(col("CLTSLSTAX"))
    )
    .withColumn(
        "svCLTPATPAY",
        when(
            (col("CLTPATPAY").isNull()) | (length(trim(col("CLTPATPAY"))) == 0),
            lit(0.00)
        ).otherwise(col("CLTPATPAY"))
    )
    .withColumn(
        "svCLTDUEAMT",
        when(
            (col("CLTDUEAMT").isNull()) | (length(trim(col("CLTDUEAMT"))) == 0),
            lit(0.00)
        ).otherwise(col("CLTDUEAMT"))
    )
    .withColumn(
        "svCLTFCOPAY",
        when(
            (col("CLTFCOPAY").isNull()) | (length(trim(col("CLTFCOPAY"))) == 0),
            lit(0.00)
        ).otherwise(col("CLTFCOPAY"))
    )
    .withColumn(
        "svCLTPCOPAY",
        when(
            (col("CLTPCOPAY").isNull()) | (length(trim(col("CLTPCOPAY"))) == 0),
            lit(0.00)
        ).otherwise(col("CLTPCOPAY"))
    )
    .withColumn(
        "svCLTCOPAY",
        when(
            (col("CLTCOPAY").isNull()) | (length(trim(col("CLTCOPAY"))) == 0),
            lit(0)
        ).otherwise(col("CLTCOPAY"))
    )
    .withColumn(
        "svCLTINCENTV",
        when(
            (col("CLTINCENTV").isNull()) | (length(trim(col("CLTINCENTV"))) == 0),
            lit(0)
        ).otherwise(col("CLTINCENTV"))
    )
    .withColumn(
        "svCLTATRDED",
        when(
            (col("CLTATRDED").isNull()) | (length(trim(col("CLTATRDED"))) == 0),
            lit(0)
        ).otherwise(col("CLTATRDED"))
    )
    .withColumn("svCLTTOTHAMT", col("CLTTOTHAMT"))
    .withColumn(
        "svCLTOTHPAYA",
        when(
            (col("CLTOTHPAYA").isNull()) | (length(trim(col("CLTOTHPAYA"))) == 0),
            lit(0)
        ).otherwise(col("CLTOTHPAYA"))
    )
    .withColumn("svCLTCOSTTYP", col("CLTCOSTTYP"))
    .withColumn("svCLTPRCTYPE", col("CLTPRCTYPE"))
    .withColumn("svCLTRATE", col("CLTRATE"))
    .withColumn("svCLTPSCBRND", col("CLTPSCBRND"))
    .withColumn("svREIMBURSMT", col("REIMBURSMT"))
    .withColumn("svCLMORIGIN", col("CLMORIGIN"))
    .withColumn("svCLTTYPUCST", col("CLTTYPUCST"))
    .withColumn(
        "svCLT2INGRCST",
        when(
            (col("CLT2INGRCST").isNull()) | (length(trim(col("CLT2INGRCST"))) == 0),
            lit(0.00)
        ).otherwise(col("CLT2INGRCST"))
    )
    .withColumn(
        "svCLT2DISPFEE",
        when(
            (col("CLT2DISPFEE").isNull()) | (length(trim(col("CLT2DISPFEE"))) == 0),
            lit(0.00)
        ).otherwise(col("CLT2DISPFEE"))
    )
    .withColumn(
        "svCLT2SLSTAX",
        when(
            (col("CLT2SLSTAX").isNull()) | (length(trim(col("CLT2SLSTAX"))) == 0),
            lit(0.00)
        ).otherwise(col("CLT2SLSTAX"))
    )
    .withColumn(
        "svCLT2DUEAMT",
        when(
            (col("CLT2DUEAMT").isNull()) | (length(trim(col("CLT2DUEAMT"))) == 0),
            lit(0.00)
        ).otherwise(col("CLT2DUEAMT"))
    )
    .withColumn(
        "svCLT2PSTAX",
        when(
            (col("CLT2PSTAX").isNull()) | (length(trim(col("CLT2PSTAX"))) == 0),
            lit(0.00)
        ).otherwise(col("CLT2PSTAX"))
    )
    .withColumn(
        "svCLT2FSTAX",
        when(
            (col("CLT2FSTAX").isNull()) | (length(trim(col("CLT2FSTAX"))) == 0),
            lit(0.00)
        ).otherwise(col("CLT2FSTAX"))
    )
    .withColumn("svCLT2COSTSRC", col("CLT2COSTSRC"))
    .withColumn("svCLT2COSTTYP", col("CLT2COSTTYP"))
    .withColumn("svCLT2PRCTYPE", col("CLT2PRCTYPE"))
    .withColumn("svCLT2RATE", col("CLT2RATE"))
    .withColumn(
        "svPPRSFSTNME",
        when(length(trim(col("PPRSFSTNME")))==0, lit("")).otherwise(trim(col("PPRSFSTNME")))
    )
    .withColumn(
        "svPPRSLSTNME",
        when(length(trim(col("PPRSLSTNME")))==0, lit("")).otherwise(trim(col("PPRSLSTNME")))
    )
    .withColumn(
        "svPPRSTATE",
        when(length(trim(col("PPRSTATE")))==0, lit("")).otherwise(trim(col("PPRSTATE")))
    )
    .withColumn(
        "svCLT2OTHAMT",
        when(
            (col("CLT2OTHAMT").isNull()) | (length(trim(col("CLT2OTHAMT"))) == 0),
            lit(0.00)
        ).otherwise(col("CLT2OTHAMT"))
    )
    .withColumn(
        "svCALINCENTV2",
        when(
            (col("CALINCENTV2").isNull()) | (length(trim(col("CALINCENTV2"))) == 0),
            lit(0.00)
        ).otherwise(col("CALINCENTV2"))
    )
    .withColumn(
        "svRXNETPRCQL",
        when(
            (col("RXNETPRCQL").isNull()) | (length(trim(col("RXNETPRCQL"))) == 0),
            lit("N")
        ).otherwise(
            when(trim(col("RXNETPRCQL")) == "Y", lit("Y")).otherwise(lit("N"))
        )
    )
    .withColumn(
        "svSBMPATRESD",
        when(
            (col("SBMPATRESD").isNull()) | (length(trim(col("SBMPATRESD"))) == 0),
            lit(0)
        ).otherwise(col("SBMPATRESD"))
    )
    .withColumn(
        "svCLIENTDEF1",
        when(
            (col("CLIENTDEF1").isNull()) | (length(trim(col("CLIENTDEF1"))) == 0),
            lit("N")
        ).otherwise(trim(col("CLIENTDEF1")))
    )
    .withColumn("svSBMPLSRVCE", col("SBMPLSRVCE"))
    .withColumn("svPRODTYPCDE", col("PRODTYPCDE"))
    .withColumn("svCONTHERAPY", col("CONTHERAPY"))
    .withColumn("svCTSCHEDID", col("CTSCHEDID"))
    .withColumn("svCLTPRODSEL", col("CLTPRODSEL"))
    .withColumn("svCLTATRTAX", col("CLTATRTAX"))
    .withColumn("svCLTPROCFEE", col("CLTPROCFEE"))
    .withColumn("svCLTPRVNSEL", col("CLTPRVNSEL"))
    .withColumn("svFMSTIER", trim(col("FMSTIER")))
    .withColumn("svCLNTDEF2", trim(col("CLIENTDEF2")))
    .withColumn(
        "svPLANDRUGST",
        when(
            (col("PLANDRUGST").isNull()) | (length(col("PLANDRUGST")) == 0),
            lit("UNK")
        ).otherwise(col("PLANDRUGST"))
    )
    .withColumn(
        "svPLANTYPE",
        when(
            (col("PLANTYPE").isNull()) | (length(col("PLANTYPE")) == 0),
            lit("UNK")
        ).otherwise(col("PLANTYPE"))
    )
    .withColumn("INROW_NUM", row_number().over(windowSpec))
)

df_inMbrMatch = df_DataCleansing.select(
    col("svCLAIMSTS").alias("CLAIMSTS"),
    col("svACCOUNTID").alias("ACCOUNTID"),
    col("svGROUPID").alias("GROUPID"),
    col("svMEMBERID_Orig").alias("MEMBERID_Orig"),
    col("svMEMBERID").alias("MEMBERID"),
    col("svMBRLSTNME").alias("MBRLSTNME"),
    col("svMBRFSTNME").alias("MBRFSTNME"),
    col("svMBRRELCDE").alias("MBRRELCDE"),
    col("svMBRSEX").alias("MBRSEX"),
    col("svMBRBIRTH").alias("MBRBIRTH"),
    col("svSOCSECNBR").alias("SOCSECNBR"),
    col("svCUSTLOC").alias("CUSTLOC"),
    col("svFACILITYID").alias("FACILITYID"),
    col("svOTHCOVERAG").alias("OTHCOVERAG"),
    col("svDATESBM").alias("DATESBM"),
    col("svTIMESBM").alias("TIMESBM"),
    col("svORGPDSBMDT").alias("ORGPDSBMDT"),
    col("svRXNUMBER").alias("RXNUMBER"),
    col("svREFILL").alias("REFILL"),
    col("svDISPSTATUS").alias("DISPSTATUS"),
    col("svDTEFILLED").alias("DTEFILLED"),
    col("svCOMPOUNDCD").alias("COMPOUNDCD"),
    col("svPRODUCTID").alias("PRODUCTID"),
    col("svMETRICQTY").alias("METRICQTY"),
    col("svDECIMALQTY").alias("DECIMALQTY"),
    col("svDAYSSUPPLY").alias("DAYSSUPPLY"),
    col("svPSC").alias("PSC"),
    col("svWRITTENDTE").alias("WRITTENDTE"),
    col("svNBRFLSAUTH").alias("NBRFLSAUTH"),
    col("svORIGINCDE").alias("ORIGINCDE"),
    col("svPRAUTHNBR").alias("PRAUTHNBR"),
    col("svLABELNAME").alias("LABELNAME"),
    col("svGPINUMBER").alias("GPINUMBER"),
    col("svMULTSRCCDE").alias("MULTSRCCDE"),
    col("svGENINDOVER").alias("GENINDOVER"),
    col("svCPQSPCPRG").alias("CPQSPCPRG"),
    col("svCPQSPCPGIN").alias("CPQSPCPGIN"),
    col("svSRXNETWRK").alias("SRXNETWRK"),
    col("svRXNETWORK").alias("RXNETWORK"),
    col("svRXNETWRKQL").alias("RXNETWRKQL"),
    col("svSRVPROVID").alias("SRVPROVID"),
    col("svSRVPROVIDQ").alias("SRVPROVIDQ"),
    col("svNPIPROV").alias("NPIPROV"),
    col("svPRVNCPDPID").alias("PRVNCPDPID"),
    col("svSRVPROVNME").alias("SRVPROVNME"),
    col("svDISPROTHER").alias("DISPROTHER"),
    col("svPRESCRIBER").alias("PRESCRIBER"),
    col("svPRESCRIDQL").alias("PRESCRIDQL"),
    col("svNPIPRESCR").alias("NPIPRESCR"),
    col("svPRESCDEAID").alias("PRESCDEAID"),
    col("svPRESLSTNME").alias("PRESLSTNME"),
    col("svPRESFSTNME").alias("PRESFSTNME"),
    col("svPRESSPCCDE").alias("PRESSPCCDE"),
    col("svPRSSTATE").alias("PRSSTATE"),
    col("svFNLPLANCDE").alias("FNLPLANCDE"),
    col("svFNLPLANDTE").alias("FNLPLANDTE"),
    col("svPLANQUAL").alias("PLANQUAL"),
    col("svCOBIND").alias("COBIND"),
    col("svFORMLRFLAG").alias("FORMLRFLAG"),
    col("svREJCDE1").alias("REJCDE1"),
    col("svREJCDE2").alias("REJCDE2"),
    col("svAWPUNITCST").alias("AWPUNITCST"),
    col("svWACUNITCST").alias("WACUNITCST"),
    col("svCTYPEUCOST").alias("CTYPEUCOST"),
    col("svBASISCOST").alias("BASISCOST"),
    col("svPROQTY").alias("PROQTY"),
    col("svSBMINGRCST").alias("SBMINGRCST"),
    col("svSBMDISPFEE").alias("SBMDISPFEE"),
    col("svSBMAMTDUE").alias("SBMAMTDUE"),
    col("svUSUALNCUST").alias("USUALNCUST"),
    col("svDENIALDTE").alias("DENIALDTE"),
    col("svPHRDISPFEE").alias("PHRDISPFEE"),
    col("svCLTINGRCST").alias("CLTINGRCST"),
    col("svCLTDISPFEE").alias("CLTDISPFEE"),
    col("svCLTSLSTAX").alias("CLTSLSTAX"),
    col("svCLTPATPAY").alias("CLTPATPAY"),
    col("svCLTDUEAMT").alias("CLTDUEAMT"),
    col("svCLTFCOPAY").alias("CLTFCOPAY"),
    col("svCLTPCOPAY").alias("CLTPCOPAY"),
    col("svCLTCOPAY").alias("CLTCOPAY"),
    col("svCLTINCENTV").alias("CLTINCENTV"),
    col("svCLTATRDED").alias("CLTATRDED"),
    col("svCLTTOTHAMT").alias("CLTTOTHAMT"),
    col("svCLTOTHPAYA").alias("CLTOTHPAYA"),
    col("svCLTCOSTTYP").alias("CLTCOSTTYP"),
    col("svCLTPRCTYPE").alias("CLTPRCTYPE"),
    col("svCLTRATE").alias("CLTRATE"),
    col("svCLTPSCBRND").alias("CLTPSCBRND"),
    col("svREIMBURSMT").alias("REIMBURSMT"),
    col("svCLMORIGIN").alias("CLMORIGIN"),
    col("svCLTTYPUCST").alias("CLTTYPUCST"),
    col("svCLT2INGRCST").alias("CLT2INGRCST"),
    col("svCLT2DISPFEE").alias("CLT2DISPFEE"),
    col("svCLT2SLSTAX").alias("CLT2SLSTAX"),
    col("svCLT2DUEAMT").alias("CLT2DUEAMT"),
    col("svCLT2PSTAX").alias("CLT2PSTAX"),
    col("svCLT2FSTAX").alias("CLT2FSTAX"),
    col("svCLT2COSTSRC").alias("CLT2COSTSRC"),
    col("svCLT2COSTTYP").alias("CLT2COSTTYP"),
    col("svCLT2PRCTYPE").alias("CLT2PRCTYPE"),
    col("svCLT2RATE").alias("CLT2RATE"),
    col("INROW_NUM"),
    col("svPPRSFSTNME").alias("PPRSFSTNME"),
    col("svPPRSLSTNME").alias("PPRSLSTNME"),
    col("svPPRSTATE").alias("PPRSTATE"),
    col("svCLT2OTHAMT").alias("CLT2OTHAMT"),
    col("svCALINCENTV2").alias("CALINCENTV2"),
    col("svRXNETPRCQL").alias("RXNETPRCQL"),
    col("svSBMPATRESD").alias("SBMPATRESD"),
    col("svCLIENTDEF1").alias("CLIENTDEF1"),
    col("svSBMPLSRVCE").alias("SBMPLSRVCE"),
    col("svPRODTYPCDE").alias("PRODTYPCDE"),
    col("svCONTHERAPY").alias("CONTHERAPY"),
    col("svCTSCHEDID").alias("CTSCHEDID"),
    col("svCLTPRODSEL").alias("CLTPRODSEL"),
    col("svCLTATRTAX").alias("CLTATRTAX"),
    col("svCLTPROCFEE").alias("CLTPROCFEE"),
    col("svCLTPRVNSEL").alias("CLTPRVNSEL"),
    col("svFMSTIER").alias("FMSTIER"),
    col("svCLNTDEF2").alias("CLNTDEF2"),
    col("svPLANDRUGST").alias("PLANDRUGST"),
    col("svPLANTYPE").alias("PLANTYPE")
)

df_aggregate = df_DataCleansing.select(
    col("INROW_NUM").alias("INROW_NUM"),
    trim(col("CLAIMSTS")).alias("CLAIMSTS"),
    col("CLT2DUEAMT").alias("AMT_BILL"),
    col("svDATESBM").alias("DATESBM")
)

df_CntByAdjDtRecType = (
    df_aggregate
    .groupBy("CLAIMSTS","DATESBM")
    .agg(
        max_("INROW_NUM").alias("RCRD_CNT"),
        sum_("AMT_BILL").alias("TOT_AMT")
    )
)

df_mbr_lkup_alias = df_mbr_lkup.alias("MBR_lkup")

df_inMbrMatch_alias = df_inMbrMatch.alias("inMbrMatch")

df_Member_Lkp_joined = df_inMbrMatch_alias.join(
    df_mbr_lkup_alias,
    [
        df_inMbrMatch_alias["MEMBERID"] == df_mbr_lkup_alias["MEMBER_ID"],
        df_inMbrMatch_alias["GROUPID"] == df_mbr_lkup_alias["GRP_ID"]
    ],
    how="left"
)

df_Member_Lkp = df_Member_Lkp_joined.select(
    col("inMbrMatch.RXCLMNBR").alias("RXCLMNBR"),
    col("inMbrMatch.CLMSEQNBR").alias("CLMSEQNBR"),
    col("inMbrMatch.CLAIMSTS").alias("CLAIMSTS"),
    rpad(col("inMbrMatch.CARID"),9," ").alias("CARID"),
    rpad(col("inMbrMatch.ACCOUNTID"),15," ").alias("ACCOUNTID"),
    rpad(col("inMbrMatch.ACCOUNTID"),15," ").alias("GROUPID"),
    col("inMbrMatch.MEMBERID_Orig").alias("MEMBERID_Orig"),
    rpad(trim(col("inMbrMatch.MEMBERID")),20," ").alias("MEMBERID"),
    rpad(col("inMbrMatch.MBRLSTNME"),25," ").alias("MBRLSTNME"),
    rpad(col("inMbrMatch.MBRFSTNME"),15," ").alias("MBRFSTNME"),
    rpad(col("inMbrMatch.MBRRELCDE"),1," ").alias("MBRRELCDE"),
    rpad(col("inMbrMatch.MBRSEX"),1," ").alias("MBRSEX"),
    col("inMbrMatch.MBRBIRTH").alias("MBRBIRTH"),
    rpad(col("inMbrMatch.SOCSECNBR"),9," ").alias("SOCSECNBR"),
    rpad(col("inMbrMatch.CUSTLOC"),2," ").alias("CUSTLOC"),
    rpad(col("inMbrMatch.FACILITYID"),10," ").alias("FACILITYID"),
    rpad(col("inMbrMatch.OTHCOVERAG"),2," ").alias("OTHCOVERAG"),
    rpad(col("inMbrMatch.DATESBM"),8," ").alias("DATESBM"),
    col("inMbrMatch.TIMESBM").alias("TIMESBM"),
    col("inMbrMatch.ORGPDSBMDT").alias("ORGPDSBMDT"),
    rpad(col("inMbrMatch.RXNUMBER"),12," ").alias("RXNUMBER"),
    rpad(col("inMbrMatch.REFILL"),2," ").alias("REFILL"),
    rpad(col("inMbrMatch.DISPSTATUS"),1," ").alias("DISPSTATUS"),
    col("inMbrMatch.DTEFILLED").alias("DTEFILLED"),
    rpad(col("inMbrMatch.COMPOUNDCD"),1," ").alias("COMPOUNDCD"),
    rpad(col("inMbrMatch.PRODUCTID"),20," ").alias("PRODUCTID"),
    col("inMbrMatch.METRICQTY").alias("METRICQTY"),
    col("inMbrMatch.DECIMALQTY").alias("DECIMALQTY"),
    col("inMbrMatch.DAYSSUPPLY").alias("DAYSSUPPLY"),
    rpad(col("inMbrMatch.PSC"),1," ").alias("PSC"),
    col("inMbrMatch.WRITTENDTE").alias("WRITTENDTE"),
    col("inMbrMatch.NBRFLSAUTH").alias("NBRFLSAUTH"),
    rpad(col("inMbrMatch.ORIGINCDE"),1," ").alias("ORIGINCDE"),
    rpad(col("inMbrMatch.PRAUTHNBR"),11," ").alias("PRAUTHNBR"),
    rpad(col("inMbrMatch.LABELNAME"),30," ").alias("LABELNAME"),
    rpad(col("inMbrMatch.GPINUMBER"),14," ").alias("GPINUMBER"),
    rpad(col("inMbrMatch.MULTSRCCDE"),1," ").alias("MULTSRCCDE"),
    rpad(col("inMbrMatch.GENINDOVER"),1," ").alias("GENINDOVER"),
    rpad(col("inMbrMatch.CPQSPCPRG"),10," ").alias("CPQSPCPRG"),
    rpad(col("inMbrMatch.CPQSPCPGIN"),1," ").alias("CPQSPCPGIN"),
    rpad(col("inMbrMatch.SRXNETWRK"),6," ").alias("SRXNETWRK"),
    rpad(col("inMbrMatch.RXNETWORK"),6," ").alias("RXNETWORK"),
    rpad(col("inMbrMatch.RXNETWRKQL"),1," ").alias("RXNETWRKQL"),
    rpad(col("inMbrMatch.SRVPROVID"),15," ").alias("SRVPROVID"),
    rpad(col("inMbrMatch.SRVPROVIDQ"),2," ").alias("SRVPROVIDQ"),
    rpad(col("inMbrMatch.NPIPROV"),10," ").alias("NPIPROV"),
    rpad(col("inMbrMatch.PRVNCPDPID"),12," ").alias("PRVNCPDPID"),
    rpad(col("inMbrMatch.SRVPROVNME"),55," ").alias("SRVPROVNME"),
    rpad(col("inMbrMatch.DISPROTHER"),3," ").alias("DISPROTHER"),
    rpad(col("inMbrMatch.PRESCRIBER"),15," ").alias("PRESCRIBER"),
    rpad(col("inMbrMatch.PRESCRIDQL"),2," ").alias("PRESCRIDQL"),
    rpad(col("inMbrMatch.NPIPRESCR"),10," ").alias("NPIPRESCR"),
    rpad(col("inMbrMatch.PRESCDEAID"),15," ").alias("PRESCDEAID"),
    rpad(col("inMbrMatch.PRESLSTNME"),25," ").alias("PRESLSTNME"),
    rpad(col("inMbrMatch.PRESFSTNME"),15," ").alias("PRESFSTNME"),
    rpad(col("inMbrMatch.PRESSPCCDE"),6," ").alias("PRESSPCCDE"),
    rpad(col("inMbrMatch.PRSSTATE"),3," ").alias("PRSSTATE"),
    rpad(col("inMbrMatch.FNLPLANCDE"),10," ").alias("FNLPLANCDE"),
    col("inMbrMatch.FNLPLANDTE").alias("FNLPLANDTE"),
    rpad(col("inMbrMatch.PLANQUAL"),10," ").alias("PLANQUAL"),
    rpad(col("inMbrMatch.COBIND"),2," ").alias("COBIND"),
    rpad(col("inMbrMatch.FORMLRFLAG"),1," ").alias("FORMLRFLAG"),
    rpad(col("inMbrMatch.REJCDE1"),3," ").alias("REJCDE1"),
    rpad(col("inMbrMatch.REJCDE2"),3," ").alias("REJCDE2"),
    col("inMbrMatch.AWPUNITCST").alias("AWPUNITCST"),
    col("inMbrMatch.WACUNITCST").alias("WACUNITCST"),
    col("inMbrMatch.CTYPEUCOST").alias("CTYPEUCOST"),
    rpad(col("inMbrMatch.BASISCOST"),2," ").alias("BASISCOST"),
    col("inMbrMatch.PROQTY").alias("PROQTY"),
    col("inMbrMatch.SBMINGRCST").alias("SBMINGRCST"),
    col("inMbrMatch.SBMDISPFEE").alias("SBMDISPFEE"),
    col("inMbrMatch.SBMAMTDUE").alias("SBMAMTDUE"),
    col("inMbrMatch.USUALNCUST").alias("USUALNCUST"),
    col("inMbrMatch.DENIALDTE").alias("DENIALDTE"),
    col("inMbrMatch.PHRDISPFEE").alias("PHRDISPFEE"),
    col("inMbrMatch.CLTINGRCST").alias("CLTINGRCST"),
    col("inMbrMatch.CLTDISPFEE").alias("CLTDISPFEE"),
    col("inMbrMatch.CLTSLSTAX").alias("CLTSLSTAX"),
    col("inMbrMatch.CLTPATPAY").alias("CLTPATPAY"),
    col("inMbrMatch.CLTDUEAMT").alias("CLTDUEAMT"),
    col("inMbrMatch.CLTFCOPAY").alias("CLTFCOPAY"),
    col("inMbrMatch.CLTPCOPAY").alias("CLTPCOPAY"),
    col("inMbrMatch.CLTCOPAY").alias("CLTCOPAY"),
    col("inMbrMatch.CLTINCENTV").alias("CLTINCENTV"),
    col("inMbrMatch.CLTATRDED").alias("CLTATRDED"),
    col("inMbrMatch.CLTTOTHAMT").alias("CLTTOTHAMT"),
    col("inMbrMatch.CLTOTHPAYA").alias("CLTOTHPAYA"),
    rpad(col("inMbrMatch.CLTCOSTTYP"),10," ").alias("CLTCOSTTYP"),
    rpad(col("inMbrMatch.CLTPRCTYPE"),10," ").alias("CLTPRCTYPE"),
    col("inMbrMatch.CLTRATE").alias("CLTRATE"),
    col("inMbrMatch.CLTPSCBRND").alias("CLTPSCBRND"),
    rpad(col("inMbrMatch.REIMBURSMT"),1," ").alias("REIMBURSMT"),
    rpad(col("inMbrMatch.CLMORIGIN"),1," ").alias("CLMORIGIN"),
    col("inMbrMatch.CLTTYPUCST").alias("CLTTYPUCST"),
    col("inMbrMatch.CLT2INGRCST").alias("CLT2INGRCST"),
    col("inMbrMatch.CLT2DISPFEE").alias("CLT2DISPFEE"),
    col("inMbrMatch.CLT2SLSTAX").alias("CLT2SLSTAX"),
    col("inMbrMatch.CLT2DUEAMT").alias("CLT2DUEAMT"),
    col("inMbrMatch.CLT2PSTAX").alias("CLT2PSTAX"),
    col("inMbrMatch.CLT2FSTAX").alias("CLT2FSTAX"),
    rpad(col("inMbrMatch.CLT2COSTSRC"),1," ").alias("CLT2COSTSRC"),
    rpad(col("inMbrMatch.CLT2COSTTYP"),10," ").alias("CLT2COSTTYP"),
    rpad(col("inMbrMatch.CLT2PRCTYPE"),10," ").alias("CLT2PRCTYPE"),
    col("inMbrMatch.CLT2RATE").alias("CLT2RATE"),
    when(
        col("inMbrMatch.CLAIMSTS")=="X",
        concat(col("inMbrMatch.RXCLMNBR"), col("inMbrMatch.CLMSEQNBR"), lit("R"))
    ).otherwise(
        concat(col("inMbrMatch.RXCLMNBR"), col("inMbrMatch.CLMSEQNBR"))
    ).alias("CLM_ID"),
    when(
        col("MBR_lkup.MBR_UNIQ_KEY").isNull(),
        lit(0)
    ).otherwise(col("MBR_lkup.MBR_UNIQ_KEY")).alias("ORIG_MBR"),
    when(
        col("inMbrMatch.ACCOUNTID").isNull(),
        lit("UNK")
    ).otherwise(col("MBR_lkup.GRP_ID")).alias("GRP_ID"),
    rpad(col("inMbrMatch.PPRSFSTNME"),15," ").alias("PPRSFSTNME"),
    rpad(col("inMbrMatch.PPRSLSTNME"),25," ").alias("PPRSLSTNME"),
    rpad(col("inMbrMatch.PPRSTATE"),3," ").alias("PPRSTATE"),
    col("inMbrMatch.CLT2OTHAMT").alias("CLT2OTHAMT"),
    col("inMbrMatch.CALINCENTV2").alias("CALINCENTV2"),
    rpad(col("inMbrMatch.RXNETPRCQL"),1," ").alias("RXNETPRCQL"),
    rpad(col("inMbrMatch.SBMPATRESD"),2," ").alias("SBMPATRESD"),
    rpad(col("inMbrMatch.CLIENTDEF1"),1," ").alias("CLIENTDEF1"),
    rpad(col("inMbrMatch.SBMPLSRVCE"),2," ").alias("SBMPLSRVCE"),
    rpad(col("inMbrMatch.PRODTYPCDE"),2," ").alias("PRODTYPCDE"),
    rpad(col("inMbrMatch.CONTHERAPY"),1," ").alias("CONTHERAPY"),
    rpad(col("inMbrMatch.CTSCHEDID"),20," ").alias("CTSCHEDID"),
    col("inMbrMatch.CLTPRODSEL").alias("CLTPRODSEL"),
    col("inMbrMatch.CLTATRTAX").alias("CLTATRTAX"),
    col("inMbrMatch.CLTPROCFEE").alias("CLTPROCFEE"),
    col("inMbrMatch.CLTPRVNSEL").alias("CLTPRVNSEL"),
    col("inMbrMatch.FMSTIER").alias("FMSTIER"),
    rpad(col("inMbrMatch.CLNTDEF2"),10," ").alias("CLNTDEF2"),
    rpad(col("inMbrMatch.PLANDRUGST"),1," ").alias("PLANDRUGST"),
    rpad(col("inMbrMatch.PLANTYPE"),8," ").alias("PLANTYPE")
)

df_PrepAuditFile = df_CntByAdjDtRecType.select(
    rpad(lit(RunDate),10," ").alias("Run_Date"),
    rpad(lit("  "),2," ").alias("Filler1"),
    rpad(lit("DAILY"),9," ").alias("Frequency"),
    rpad(lit("*  "),3," ").alias("Filler2"),
    rpad(concat(lit("ORX-"),lit("MA-"),col("CLAIMSTS")),9," ").alias("Program"),
    rpad(col("DATESBM"),11," ").alias("File_date"),
    rpad(col("RCRD_CNT").cast("string"),8," ").alias("Row_Count"),
    rpad(col("TOT_AMT").cast("string"),12," ").alias("Pd_amount")
)

write_files(
    df_PrepAuditFile,
    f"{adls_path_raw}/landing/Drug_Header_Record.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

import pyspark.sql.types as T
from pyspark.sql.functions import col, lit, when, upper, trim, rpad, expr

IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
RunID = get_widget_value("RunID", "")
SrcSysCd = get_widget_value("SrcSysCd", "")
SrcSysCdSK = get_widget_value("SrcSysCdSK", "")
RunDate = get_widget_value("RunDate", "")
InFile = get_widget_value("InFile", "")
MbrTermDate = get_widget_value("MbrTermDate", "")

df_Extract = df_Member_Lkp

df_Extract = (
    df_Extract
    .withColumn("svServicingProviderIDisNABP",
        when(trim(col("SRVPROVIDQ")) == '07', lit("Y")).otherwise(lit("N")))
    .withColumn("svServicingProviderIDisNPI",
        when((trim(col("SRVPROVIDQ")) == '01') & (ProviderNPIValidator(col("SRVPROVID")) == 'Y'), lit("Y")).otherwise(lit("N")))
    .withColumn("svServicingProviderNCPDP",
        when(trim(col("PRVNCPDPID")) > lit("0"), lit("Y")).otherwise(lit("N")))
    .withColumn("svNPIProvhasNPI",
        when(ProviderNPIValidator(col("NPIPROV")) == 'Y', lit("Y")).otherwise(lit("N")))
    .withColumn("svPrescribingIDisDEA",
        when((trim(col("PRESCRIDQL")) == '12') & (ProviderDEAValidator(col("PRESCRIBER")) == 'Y'), lit("Y")).otherwise(lit("N")))
    .withColumn("svPrescribingIDisNPI",
        when((trim(col("PRESCRIDQL")) == '01') & (ProviderNPIValidator(col("PRESCRIBER")) == 'Y'), lit("Y")).otherwise(lit("N")))
    .withColumn("svPrescribingDEAIDisDEA",
        when(ProviderDEAValidator(col("PRESCDEAID")) == 'Y', lit("Y")).otherwise(lit("N")))
    .withColumn("svNPIPresciberHasNPI",
        when(ProviderNPIValidator(col("NPIPRESCR")) == 'Y', lit("Y")).otherwise(lit("N")))
    .withColumn("svDEAsMatchTest",
        when(
            (col("svPrescribingIDisDEA") == 'Y') &
            (col("svPrescribingDEAIDisDEA") == 'Y') &
            (trim(col("PRESCRIBER")) == trim(col("PRESCDEAID"))),
            lit("Y")
        ).otherwise(lit("N")))
    .withColumn("svServiceIDToUse",
        when(col("svServicingProviderIDisNABP") == 'Y', col("SRVPROVID"))
         .when(col("svServicingProviderNCPDP") == 'Y', col("PRVNCPDPID"))
         .otherwise(lit("NA")))
    .withColumn("svServiceNPIToUse",
        when(col("svServicingProviderIDisNPI") == 'Y', trim(col("SRVPROVID")))
         .when(col("svNPIProvhasNPI") == 'Y', trim(col("NPIPROV")))
         .otherwise(lit("NA")))
    .withColumn("svPrescriberDEAtoUse",
        when(col("svPrescribingIDisDEA") == 'Y', trim(col("PRESCRIBER")))
         .when(col("svPrescribingDEAIDisDEA") == 'Y', trim(col("PRESCDEAID")))
         .otherwise(lit("NA")))
    .withColumn("svPrescriberNPItoUse",
        when(col("svPrescribingIDisNPI") == 'Y', trim(col("PRESCRIBER")))
         .when(col("svNPIPresciberHasNPI") == 'Y', trim(col("NPIPRESCR")))
         .otherwise(lit("NA")))
    .withColumn("svSellClientIngredientCosts",
        when(col("CLTINGRCST").isNull(), lit("0.00")).otherwise(col("CLTINGRCST")))
    .withColumn("svSellClientDispenseFee",
        when(col("CLTDISPFEE").isNull(), lit("0.00")).otherwise(col("CLTDISPFEE")))
    .withColumn("svSellClientSalesTax",
        when(col("CLTSLSTAX").isNull(), lit("0.00")).otherwise(col("CLTSLSTAX")))
    .withColumn("svSellClientIncentive",
        when(col("CLTINCENTV").isNull(), lit("0.00")).otherwise(col("CLTINCENTV")))
    .withColumn("svSellClientOtherAmt",
        when(col("CLTTOTHAMT").isNull(), lit("0.00")).otherwise(col("CLTTOTHAMT")))
    .withColumn("svSellClientOtherPayAmt",
        when(col("CLTOTHPAYA").isNull(), lit("0.00")).otherwise(col("CLTOTHPAYA")))
    .withColumn("svSellClientPatientPaid",
        when(col("CLTPATPAY").isNull(), lit("0.00")).otherwise(col("CLTPATPAY")))
    .withColumn("svSellClientDueAmount",
        when(col("CLTDUEAMT").isNull(), lit("0.00")).otherwise(col("CLTDUEAMT")))
    .withColumn("svSellDeriveActualPaidPerFields",
        (col("svSellClientIngredientCosts").cast("double") +
         col("svSellClientDispenseFee").cast("double") +
         col("svSellClientSalesTax").cast("double") +
         col("svSellClientOtherAmt").cast("double") +
         col("svSellClientIncentive").cast("double")) -
        (col("svSellClientOtherPayAmt").cast("double") +
         col("svSellClientPatientPaid").cast("double"))
    )
    .withColumn("svDoesActualPaidEqClientDueAmt",
        when(col("svSellDeriveActualPaidPerFields") == col("svSellClientDueAmount").cast("double"), lit("Y")).otherwise(lit("N")))
    .withColumn("svBuyClientIngredientCosts",
        when(col("CLT2INGRCST").isNull(), lit("0.00")).otherwise(col("CLT2INGRCST")))
    .withColumn("svBuyClientDispenseFee",
        when(col("CLT2DISPFEE").isNull(), lit("0.00")).otherwise(col("CLT2DISPFEE")))
    .withColumn("svBuyClientPSTax",
        when(col("CLT2PSTAX").isNull(), lit("0.00")).otherwise(col("CLT2PSTAX")))
    .withColumn("svBuyClientFSTax",
        when(col("CLT2FSTAX").isNull(), lit("0.00")).otherwise(col("CLT2FSTAX")))
    .withColumn("svBuyClientIncentive",
        when(col("CALINCENTV2").isNull(), lit("0.00")).otherwise(col("CALINCENTV2")))
    .withColumn("svBuyClientOtherAmt",
        when(col("CLT2OTHAMT").isNull(), lit("0.00")).otherwise(col("CLT2OTHAMT")))
    .withColumn("svBuyClientDueAmount",
        when(col("CLT2DUEAMT").isNull(), lit("0.00")).otherwise(col("CLT2DUEAMT")))
    .withColumn("svBuyDeriveActualPaidPerFields",
        (col("svBuyClientIngredientCosts").cast("double") +
         col("svBuyClientDispenseFee").cast("double") +
         col("svBuyClientPSTax").cast("double") +
         col("svBuyClientFSTax").cast("double") +
         col("svBuyClientIncentive").cast("double") +
         col("svBuyClientOtherAmt").cast("double")) -
        (col("svSellClientOtherPayAmt").cast("double") +
         col("svSellClientPatientPaid").cast("double")))
    .withColumn("svDoesBuyActualPaidEqClientDueAmt",
        when(col("svBuyDeriveActualPaidPerFields") == col("svBuyClientDueAmount").cast("double"), lit("Y")).otherwise(lit("N")))
)

df_Landing_File_Paid_Reversal = (
    df_Extract
    .filter((col("CLAIMSTS") == 'P') | (col("CLAIMSTS") == 'X'))
    .select(
       rpad(lit("OPTUMRX"), 7, " ").alias("SRC_SYS_CD"),
       rpad(col("DATESBM"), 10, " ").alias("FILE_RCVD_DT"),
       lit(None).alias("RCRD_ID"),
       lit(None).alias("PRCSR_NO"),
       lit(None).alias("BTCH_NO"),
       col("svServiceIDToUse").alias("PDX_NO"),
       col("RXNUMBER").alias("RX_NO"),
       rpad(col("DTEFILLED"), 10, " ").alias("FILL_DT"),
       rpad(col("PRODUCTID"), 20, " ").alias("NDC"),
       col("LABELNAME").alias("DRUG_DESC"),
       col("REFILL").alias("NEW_OR_RFL_CD"),
       col("DECIMALQTY").alias("METRIC_QTY"),
       col("DAYSSUPPLY").alias("DAYS_SUPL"),
       rpad(lit(None), 2, " ").alias("BSS_OF_CST_DTRM"),
       col("CLTINGRCST").alias("INGR_CST_AMT"),
       col("CLTDISPFEE").alias("DISPNS_FEE_AMT"),
       col("CLTFCOPAY").alias("COPAY_AMT"),
       col("CLTSLSTAX").alias("SLS_TAX_AMT"),
       col("CLTDUEAMT").alias("BILL_AMT"),
       col("MBRFSTNME").alias("PATN_FIRST_NM"),
       col("MBRLSTNME").alias("PATN_LAST_NM"),
       rpad(col("MBRBIRTH"), 10, " ").alias("BRTH_DT"),
       when(upper(trim(col("MBRSEX"))) == 'M', lit("1"))
        .when(upper(trim(col("MBRSEX"))) == 'F', lit("2"))
        .otherwise(lit("0")).alias("SEX_CD"),
       when((col("MEMBERID").isNull()) | (length(col("MEMBERID")) == 0), lit("UNK"))
        .otherwise(trim(col("MEMBERID"))).alias("CARDHLDR_ID_NO"),
       col("MBRRELCDE").alias("RELSHP_CD"),
       col("ACCOUNTID").alias("GRP_NO"),
       lit(None).alias("HOME_PLN"),
       lit(None).alias("HOST_PLN"),
       col("svPrescriberDEAtoUse").alias("PRSCRBR_ID"),
       lit(None).alias("DIAG_CD"),
       lit(None).alias("CARDHLDR_FIRST_NM"),
       lit(None).alias("CARDHLDR_LAST_NM"),
       rpad(col("PRAUTHNBR"), 12, " ").alias("PRAUTH_NO"),
       lit(None).alias("PA_MC_SC_NO"),
       col("CUSTLOC").alias("CUST_LOC"),
       lit(None).alias("RESUB_CYC_CT"),
       rpad(lit(None), 10, " ").alias("RX_DT"),
       when((col("PSC").isNull()) | (length(trim(col("PSC"))) == 0), lit("UNK"))
        .otherwise(col("PSC")).alias("DISPENSE_AS_WRTN_PROD_SEL_CD"),
       lit("NA").alias("PRSN_CD"),
       col("OTHCOVERAG").alias("OTHR_COV_CD"),
       lit(None).alias("ELIG_CLRFCTN_CD"),
       col("COMPOUNDCD").alias("CMPND_CD"),
       col("NBRFLSAUTH").alias("NO_OF_RFLS_AUTH"),
       lit(None).alias("LVL_OF_SVC"),
       col("ORIGINCDE").alias("RX_ORIG_CD"),
       lit(None).alias("RX_DENIAL_CLRFCTN"),
       lit(None).alias("PRI_PRSCRBR"),
       lit(None).alias("CLNC_ID_NO"),
       when(
         ((col("GENINDOVER").isNull()) | (length(trim(col("GENINDOVER"))) == 0)) &
         ((trim(col("MULTSRCCDE")) == 'M') | (trim(col("MULTSRCCDE")) == 'N')),
         lit("1")
       ).when(
         ((col("GENINDOVER").isNull()) | (length(trim(col("GENINDOVER"))) == 0)) &
         (trim(col("MULTSRCCDE")) == 'Y'),
         lit("3")
       ).when(
         ((col("GENINDOVER").isNull()) | (length(trim(col("GENINDOVER"))) == 0)) &
         (trim(col("MULTSRCCDE")) == 'O'),
         lit("5")
       ).when(
         (trim(col("GENINDOVER")) == 'M') | (trim(col("GENINDOVER")) == 'N'),
         lit("1")
       ).when(
         trim(col("GENINDOVER")) == 'Y', lit("3")
       ).when(
         trim(col("GENINDOVER")) == 'O', lit("5")
       ).otherwise(lit("0")).alias("DRUG_TYP"),
       when((length(col("PRESLSTNME")) == 0) & (length(col("PRESFSTNME")) == 0), lit("UNK"))
        .when((length(col("PRESFSTNME")) != 0) & (length(col("PRESLSTNME")) == 0), col("PRESFSTNME"))
        .when((length(col("PRESFSTNME")) == 0) & (length(col("PRESLSTNME")) != 0), col("PRESLSTNME"))
        .otherwise(expr("PRESFSTNME || ' ' || PRESLSTNME")).alias("PRSCRBR_LAST_NM"),
       lit("0.00").alias("POSTAGE_AMT"),
       lit(None).alias("UNIT_DOSE_IN"),
       col("CLTOTHPAYA").alias("OTHR_PAYOR_AMT"),
       lit(None).alias("BSS_OF_DAYS_SUPL_DTRM"),
       when(col("COMPOUNDCD") == lit("2"),
            expr("(USUALNCUST + CLTDISPFEE + CLTSLSTAX)"))
        .otherwise(expr("((AWPUNITCST * DECIMALQTY) + CLTDISPFEE + CLTSLSTAX)"))
        .alias("FULL_AVG_WHLSL_PRICE"),
       rpad(lit(None), 1, " ").alias("EXPNSN_AREA"),
       col("CARID").alias("MSTR_CAR"),
       lit(None).alias("SUBCAR"),
       when(col("CLAIMSTS") == 'X', lit("R")).otherwise(lit("P")).alias("CLM_TYP"),
       lit(None).alias("SUBGRP"),
       rpad(lit(None), 1, " ").alias("PLN_DSGNR"),
       rpad(col("DATESBM"), 10, " ").alias("ADJDCT_DT"),
       lit("0.00").alias("ADMIN_FEE_AMT"),
       lit("0.00").alias("CAP_AMT"),
       col("SBMINGRCST").alias("INGR_CST_SUB_AMT"),
       lit("0.00").alias("MBR_NON_COPAY_AMT"),
       rpad(lit(None), 2, " ").alias("MBR_PAY_CD"),
       col("CLTINCENTV").alias("INCNTV_FEE_AMT"),
       lit("0.00").alias("CLM_ADJ_AMT"),
       rpad(lit(None), 2, " ").alias("CLM_ADJ_CD"),
       rpad(col("FORMLRFLAG"), 1, " ").alias("FRMLRY_FLAG"),
       lit(None).alias("GNRC_CLS_NO"),
       lit(None).alias("THRPTC_CLS_AHFS"),
       rpad(col("DISPROTHER"), 3, " ").alias("PDX_TYP"),
       when((col("BASISCOST").isNull()) | (length(trim(col("BASISCOST"))) == 0), lit("NA"))
        .otherwise(col("BASISCOST")).alias("BILL_BSS_CD"),
       col("USUALNCUST").alias("USL_AND_CUST_CHRG_AMT"),
       rpad(lit("1753-01-01"), 10, " ").alias("PD_DT"),
       lit(None).alias("BNF_CD"),
       lit(None).alias("DRUG_STRG"),
       col("ORIG_MBR").alias("ORIG_MBR"),
       rpad(lit(None), 10, " ").alias("INJRY_DT"),
       lit("0.00").alias("FEE_AMT"),
       col("RXCLMNBR").alias("REF_NO"),
       lit(None).alias("CLNT_CUST_ID"),
       lit(None).alias("PLN_TYP"),
       lit(None).alias("ADJDCT_REF_NO"),
       when((col("CLTPSCBRND").isNull()) | (length(trim(col("CLTPSCBRND"))) == 0), lit("0.0"))
        .otherwise(col("CLTPSCBRND")).alias("ANCLRY_AMT"),
       expr(
         "FMT(FACILITYID, '10\" \"R') || "
         "FMT(PRESCRIDQL, '2\" \"R') || "
         "FMT(PRESCDEAID, '15\" \"R') || "
         "FMT(SBMDISPFEE, '11\" \"R') || "
         "FMT(SRXNETWRK, '6\" \"R') || "
         "FMT(RXNETWORK, '6\" \"R') || "
         "FMT(PLANQUAL, '10\" \"R') || "
         "FMT(COBIND, '2\" \"R') || "
         "FMT(RXNETPRCQL, '1\" \"R') || "
         "FMT(CLIENTDEF1, '1\" \"R') || "
         "FMT(SBMPLSRVCE, '2\" \"R')"
       ).alias("CLNT_GNRL_PRPS_AREA"),
       when((col("DISPSTATUS").isNull()) | (length(trim(col("DISPSTATUS"))) == 0), lit("BLANK"))
        .otherwise(col("DISPSTATUS")).alias("PRTL_FILL_STTUS_CD"),
       rpad(lit("1753-01-01"), 10, " ").alias("BILL_DT"),
       lit(None).alias("FSA_VNDR_CD"),
       rpad(lit(None), 1, " ").alias("PICA_DRUG_CD"),
       when((col("CLTPATPAY").isNull()) | (length(trim(col("CLTPATPAY"))) == 0), lit("0.0"))
        .otherwise(col("CLTPATPAY")).alias("CLM_AMT"),
       lit("0.00").alias("DSALW_AMT"),
       rpad(lit(None), 1, " ").alias("FED_DRUG_CLS_CD"),
       col("CLTATRDED").alias("DEDCT_AMT"),
       rpad(lit(None), 1, " ").alias("BNF_COPAY_100"),
       rpad(lit(None), 1, " ").alias("CLM_PRCS_TYP"),
       lit(None).alias("INDEM_HIER_TIER_NO"),
       rpad(col("FMSTIER"), 1, " ").alias("MCARE_D_COV_DRUG"),
       rpad(lit(None), 1, " ").alias("RETRO_LICS_CD"),
       lit("0.00").alias("RETRO_LICS_AMT"),
       lit("0.00").alias("LICS_SBSDY_AMT"),
       rpad(lit(None), 1, " ").alias("MCARE_B_DRUG"),
       rpad(lit(None), 1, " ").alias("MCARE_B_CLM"),
       rpad(col("PRESCRIDQL"), 2, " ").alias("PRSCRBR_QLFR"),
       col("svPrescriberNPItoUse").alias("PRSCRBR_NTNL_PROV_ID"),
       rpad(col("SRVPROVIDQ"), 2, " ").alias("PDX_QLFR"),
       col("svServiceNPIToUse").alias("PDX_NTNL_PROV_ID"),
       lit("0.00").alias("HLTH_RMBRMT_ARGMT_APLD_AMT"),
       lit(None).alias("THER_CLS"),
       lit(None).alias("HIC_NO"),
       rpad(lit(None), 1, " ").alias("HLTH_RMBRMT_ARGMT_FLAG"),
       lit(None).alias("DOSE_CD"),
       rpad(lit(None), 1, " ").alias("LOW_INCM"),
       rpad(lit(None), 2, " ").alias("RTE_OF_ADMIN"),
       lit(None).alias("DEA_SCHD"),
       lit(None).alias("COPAY_BNF_OPT"),
       when((col("GPINUMBER").isNull()) | (length(trim(col("GPINUMBER"))) == 0), lit("NA"))
        .otherwise(col("GPINUMBER")).alias("GNRC_PROD_IN"),
       when((col("PRESSPCCDE").isNull()) | (length(trim(col("PRESSPCCDE"))) == 0), lit("NA"))
        .otherwise(col("PRESSPCCDE")).alias("PRSCRBR_SPEC"),
       lit(None).alias("VAL_CD"),
       lit(None).alias("PRI_CARE_PDX"),
       rpad(lit(None), 1, " ").alias("OFC_OF_INSPECTOR_GNRL"),
       lit(None).alias("PATN_SSN"),
       lit(None).alias("CARDHLDR_SSN"),
       lit(None).alias("CARDHLDR_BRTH_DT"),
       lit(None).alias("CARDHLDR_ADDR"),
       lit(None).alias("CARDHLDR_CITY"),
       lit(None).alias("CHADHLDR_ST"),
       lit(None).alias("CARDHLDR_ZIP_CD"),
       lit("0.00").alias("PSL_FMLY_MET_AMT"),
       lit("0.00").alias("PSL_MBR_MET_AMT"),
       lit("0.00").alias("PSL_FMLY_AMT"),
       lit("0.00").alias("DEDCT_FMLY_MET_AMT"),
       lit("0.00").alias("DEDCT_FMLY_AMT"),
       lit("0.00").alias("MOPS_FMLY_AMT"),
       lit("0.00").alias("MOPS_FMLY_MET_AMT"),
       lit("0.00").alias("MOPS_MBR_MET_AMT"),
       lit("0.00").alias("DEDCT_MBR_MET_AMT"),
       lit("0.00").alias("PSL_APLD_AMT"),
       lit("0.00").alias("MOPS_APLD_AMT"),
       when(
         ((col("REIMBURSMT") == 'M') & (col("SBMPATRESD") == '92')) |
         ((col("REIMBURSMT") == 'M') & (col("SBMPATRESD") == '93')) |
         ((col("REIMBURSMT") == 'M') & (col("SBMPATRESD") == '98')),
         lit("N")
       ).otherwise(lit("Y")).alias("PAR_PDX_IN"),
       col("CLTPCOPAY").alias("COPAY_PCT_AMT"),
       col("CLTFCOPAY").alias("COPAY_FLAT_AMT"),
       rpad(col("CLMORIGIN"), 1, " ").alias("CLM_TRNSMSN_METH"),
       lit(None).alias("RX_NO_2012"),
       col("CLM_ID").alias("CLM_ID"),
       col("CLAIMSTS").alias("CLM_STTUS_CD"),
       lit("NA").alias("ADJ_FROM_CLM_ID"),
       lit("NA").alias("ADJ_TO_CLM_ID"),
       col("PRODTYPCDE").alias("SUBMT_PROD_ID_QLFR"),
       col("CONTHERAPY").alias("CNTNGNT_THER_FLAG"),
       when((col("CTSCHEDID").isNull()) | (length(trim(col("CTSCHEDID"))) == 0),
            lit("NA")).otherwise(col("CLTPRODSEL")).alias("CNTNGNT_THER_SCHD"),
       when((col("CLTPRODSEL").isNull()) | (length(trim(col("CLTPRODSEL"))) == 0), lit("0.00"))
        .otherwise(col("CLTPRODSEL")).alias("CLNT_PATN_PAY_ATRBD_PROD_AMT"),
       when((col("CLTATRTAX").isNull()) | (length(trim(col("CLTATRTAX"))) == 0), lit("0.00"))
        .otherwise(col("CLTATRTAX")).alias("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"),
       when((col("CLTPROCFEE").isNull()) | (length(trim(col("CLTPROCFEE"))) == 0), lit("0.00"))
        .otherwise(col("CLTPROCFEE")).alias("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"),
       when((col("CLTPRVNSEL").isNull()) | (length(trim(col("CLTPRVNSEL"))) == 0), lit("0.00"))
        .otherwise(col("CLTPRVNSEL")).alias("CLNT_PATN_PAY_ATRBD_NTWK_AMT"),
       lit("MCARE").alias("LOB_IN")
    )
)

df_Landing_File_DeniedClaims = (
    df_Extract
    .filter(col("CLAIMSTS") == 'R')
    .select(
       when((col("MEMBERID").isNull()) | (length(col("MEMBERID")) == 0), lit("UNK"))
        .otherwise(trim(col("MEMBERID_Orig"))).alias("MEMBERID"),
       rpad(col("ACCOUNTID"), 15, " ").alias("ACCOUNTID"),
       rpad(col("NPIPROV"), 10, " ").alias("NPIPROV"),
       rpad(col("DENIALDTE"), 8, " ").alias("DENIALDTE"),
       rpad(col("RXNUMBER"), 12, " ").alias("RXNUMBER"),
       rpad(col("DATESBM"), 8, " ").alias("DATESBM"),
       rpad(col("TIMESBM"), 8, " ").alias("TIMESBM"),
       rpad(lit("OPTUMRX"), 7, " ").alias("SRC_SYS_CD"),
       rpad(col("MBRSEX"), 1, " ").alias("MBRSEX"),
       rpad(col("MBRRELCDE"), 1, " ").alias("MBRRELCDE"),
       rpad(col("REJCDE1"), 3, " ").alias("REJCDE1"),
       rpad(col("REJCDE2"), 3, " ").alias("REJCDE2"),
       rpad(col("RXNETWRKQL"), 1, " ").alias("RXNETWRKQL"),
       rpad(col("MBRBIRTH"), 8, " ").alias("MBRBIRTH"),
       col("PHRDISPFEE").alias("PHRDISPFEE"),
       col("CLTDUEAMT").alias("CLTDUEAMT"),
       col("CLT2DUEAMT").alias("CLT2DUEAMT"),
       col("CLTPATPAY").alias("CLTPATPAY"),
       col("CLTINGRCST").alias("CLTINGRCST"),
       rpad(col("MBRFSTNME"), 15, " ").alias("MBRFSTNME"),
       rpad(col("MBRLSTNME"), 25, " ").alias("MBRLSTNME"),
       rpad(col("SOCSECNBR"), 9, " ").alias("SOCSECNBR"),
       rpad(col("SRVPROVNME"), 55, " ").alias("SRVPROVNME"),
       rpad(
         when((length(trim(col("PRESCRIBER"))) != 0) & (trim(col("PRESCRIDQL")) == '12'),
              col("PRESCRIBER"))
          .when(length(trim(col("PRESCDEAID"))) != 0, trim(col("PRESCDEAID")))
          .otherwise(lit("NA")),
         15, " "
       ).alias("PRESCDEAID"),
       rpad(col("NPIPRESCR"), 10, " ").alias("NPIPRESCR"),
       rpad(col("PRESFSTNME"), 15, " ").alias("PPRSFSTNME"),
       rpad(col("PRESLSTNME"), 25, " ").alias("PPRSLSTNME"),
       rpad(col("PRSSTATE"), 3, " ").alias("PPRSTATE"),
       rpad(col("LABELNAME"), 30, " ").alias("LABELNAME"),
       rpad(col("SRVPROVID"), 15, " ").alias("SRVPROVID"),
       rpad(col("CARID"), 9, " ").alias("CARID"),
       rpad(col("ACCOUNTID"), 15, " ").alias("GROUPID"),
       rpad(col("CLMORIGIN"), 1, " ").alias("CLMORIGIN"),
       col("RXCLMNBR").alias("RXCLMNBR"),
       rpad(col("PRODUCTID"), 20, " ").alias("PRODUCTID"),
       col("ORIG_MBR").alias("MBR_UNIQ_KEY"),
       col("GRP_ID").alias("GRP_ID"),
       lit("MCARE").alias("LOB_IN")
    )
)

df_W_DRUG_ENR = (
    df_Extract
    .filter((col("CLAIMSTS") == 'P') | (col("CLAIMSTS") == 'X'))
    .select(
       col("CLM_ID").alias("CLM_ID"),
       rpad(col("DTEFILLED"), 10, " ").alias("FILL_DT_SK"),
       col("ORIG_MBR").alias("MBR_UNIQ_KEY")
    )
)

df_W_DRUG_CLM_PCP = (
    df_Extract
    .filter((col("CLAIMSTS") == 'P') | (col("CLAIMSTS") == 'X'))
    .select(
       col("ORIG_MBR").alias("MBR_UNIQ_KEY"),
       rpad(col("DTEFILLED"), 10, " ").alias("FILL_DT_SK")
    )
)

df_Landing_File_MedDDrugClmPrice = (
    df_Extract
    .filter((col("CLAIMSTS") == 'P') | (col("CLAIMSTS") == 'X'))
    .select(
       rpad(lit("OPTUMRX"), 7, " ").alias("SRC_SYS_CD"),
       col("SrcSysCdSK").alias("SRC_SYS_CD_SK"),  # Expression says "SrcSysCdSK"
       col("CLM_ID").alias("CLM_ID"),
       col("RXCLMNBR").alias("RXCLMNBR"),
       col("CLMSEQNBR").alias("CLMSEQNBR"),
       rpad(col("CLAIMSTS"), 1, " ").alias("CLAIMSTS"),
       col("ORGPDSBMDT").alias("ORGPDSBMDT"),
       col("METRICQTY").alias("METRICQTY"),
       col("GPINUMBER").alias("GPINUMBER"),
       rpad(col("GENINDOVER"), 1, " ").alias("GENINDOVER"),
       rpad(col("CPQSPCPRG"), 10, " ").alias("CPQSPCPRG"),
       rpad(col("CPQSPCPGIN"), 1, " ").alias("CPQSPCPGIN"),
       rpad(col("FNLPLANCDE"), 10, " ").alias("FNLPLANCDE"),
       col("FNLPLANDTE").alias("FNLPLANDTE"),
       rpad(col("FORMLRFLAG"), 1, " ").alias("FORMLRFLAG"),
       col("AWPUNITCST").alias("AWPUNITCST"),
       col("WACUNITCST").alias("WACUNITCST"),
       col("CTYPEUCOST").alias("CTYPEUCOST"),
       col("PROQTY").alias("PROQTY"),
       rpad(col("CLTCOSTTYP"), 10, " ").alias("CLTCOSTTYP"),
       rpad(col("CLTPRCTYPE"), 10, " ").alias("CLTPRCTYPE"),
       col("CLTRATE").alias("CLTRATE"),
       col("CLTTYPUCST").alias("CLTTYPUCST"),
       col("CLT2INGRCST").alias("CLT2INGRCST"),
       col("CLT2DISPFEE").alias("CLT2DISPFEE"),
       rpad(col("CLT2COSTSRC"), 1, " ").alias("CLT2COSTSRC"),
       rpad(col("CLT2COSTTYP"), 10, " ").alias("CLT2COSTTYP"),
       rpad(col("CLT2PRCTYPE"), 10, " ").alias("CLT2PRCTYPE"),
       col("CLT2RATE").alias("CLT2RATE"),
       col("RXNETWRKQL").alias("RXNETWRKQL"),
       col("CLT2SLSTAX").alias("CLT2SLSTAX"),
       col("CLT2PSTAX").alias("CLT2PSTAX"),
       col("CLT2FSTAX").alias("CLT2FSTAX"),
       col("CLT2DUEAMT").alias("CLT2DUEAMT"),
       col("CLT2OTHAMT").alias("CLT2OTHAMT"),
       col("CLTINCENTV").alias("CLTINCENTV"),
       col("CALINCENTV2").alias("CALINCENTV2"),
       col("CLTOTHPAYA").alias("CLTOTHPAYA"),
       col("CLTTOTHAMT").alias("CLTTOTHAMT"),
       rpad(col("CLNTDEF2"), 10, " ").alias("CLNTDEF2"),
       col("FMSTIER").alias("TIER_ID"),
       rpad(col("MULTSRCCDE"), 1, " ").alias("DRUG_TYP_CD"),
       rpad(col("PLANDRUGST"), 1, " ").alias("PLANDRUGST"),
       rpad(col("PLANTYPE"), 8, " ").alias("PLANTYPE")
    )
)

df_SellPriceNotEqClntDueAmt = (
    df_Extract
    .filter(
        ((col("CLAIMSTS") == 'P') | (col("CLAIMSTS") == 'X')) &
        (col("svDoesActualPaidEqClientDueAmt") == 'N')
    )
    .select(
       rpad(lit("OPTUMRX"), 7, " ").alias("SRC_SYS_CD"),
       col("SrcSysCdSK").alias("SRC_SYS_CD_SK"),
       col("CLM_ID").alias("CLM_ID"),
       col("RXCLMNBR").alias("RXCLMNBR"),
       col("CLMSEQNBR").alias("CLMSEQNBR"),
       rpad(col("CLAIMSTS"), 1, " ").alias("CLAIMSTS"),
       col("ORGPDSBMDT").alias("ORGPDSBMDT"),
       col("METRICQTY").alias("METRICQTY"),
       rpad(col("GPINUMBER"), 14, " ").alias("GPINUMBER"),
       rpad(col("GENINDOVER"), 1, " ").alias("GENINDOVER"),
       rpad(col("CPQSPCPRG"), 10, " ").alias("CPQSPCPRG"),
       rpad(col("CPQSPCPGIN"), 1, " ").alias("CPQSPCPGIN"),
       rpad(col("FNLPLANCDE"), 10, " ").alias("FNLPLANCDE"),
       col("FNLPLANDTE").alias("FNLPLANDTE"),
       rpad(col("FORMLRFLAG"), 1, " ").alias("FORMLRFLAG"),
       col("AWPUNITCST").alias("AWPUNITCST"),
       col("WACUNITCST").alias("WACUNITCST"),
       col("CTYPEUCOST").alias("CTYPEUCOST"),
       col("PROQTY").alias("PROQTY"),
       rpad(col("CLTCOSTTYP"), 10, " ").alias("CLTCOSTTYP"),
       rpad(col("CLTPRCTYPE"), 10, " ").alias("CLTPRCTYPE"),
       col("CLTRATE").alias("CLTRATE"),
       col("CLTINGRCST").alias("CLTINGRCST"),
       col("CLTDISPFEE").alias("CLTDISPFEE"),
       col("CLTSLSTAX").alias("CLTSLSTAX"),
       col("CLTINCENTV").alias("CLTINCENTV"),
       col("CLTPATPAY").alias("CLTPATPAY"),
       col("CLTTOTHAMT").alias("CLTTOTHAMT"),
       col("CLTOTHPAYA").alias("CLTOTHPAYA"),
       col("CLT2PSTAX").alias("CLT2PSTAX"),
       col("CLT2FSTAX").alias("CLT2FSTAX"),
       col("CLTDUEAMT").alias("CLTDUEAMT"),
       col("CLT2INGRCST").alias("CLT2INGRCST"),
       col("CLT2DISPFEE").alias("CLT2DISPFEE"),
       col("CLT2SLSTAX").alias("CLT2SLSTAX"),
       col("CLT2OTHAMT").alias("CLT2OTHAMT"),
       col("CLT2DUEAMT").alias("CLT2DUEAMT"),
       rpad(col("CLT2COSTSRC"), 1, " ").alias("CLT2COSTSRC"),
       rpad(col("CLT2COSTTYP"), 10, " ").alias("CLT2COSTTYP"),
       rpad(col("CLT2PRCTYPE"), 10, " ").alias("CLT2PRCTYPE")
    )
)

df_BuyPriceNotEqClntDueAmt = (
    df_Extract
    .filter(
        ((col("CLAIMSTS") == 'P') | (col("CLAIMSTS") == 'X')) &
        (col("svDoesBuyActualPaidEqClientDueAmt") == 'N')
    )
    .select(
       rpad(lit("OPTUMRX"), 7, " ").alias("SRC_SYS_CD"),
       col("SrcSysCdSK").alias("SRC_SYS_CD_SK"),
       col("CLM_ID").alias("CLM_ID"),
       col("RXCLMNBR").alias("RXCLMNBR"),
       col("CLMSEQNBR").alias("CLMSEQNBR"),
       rpad(col("CLAIMSTS"), 1, " ").alias("CLAIMSTS"),
       col("ORGPDSBMDT").alias("ORGPDSBMDT"),
       col("METRICQTY").alias("METRICQTY"),
       rpad(col("GPINUMBER"), 14, " ").alias("GPINUMBER"),
       rpad(col("GENINDOVER"), 1, " ").alias("GENINDOVER"),
       rpad(col("CPQSPCPRG"), 10, " ").alias("CPQSPCPRG"),
       rpad(col("CPQSPCPGIN"), 1, " ").alias("CPQSPCPGIN"),
       rpad(col("FNLPLANCDE"), 10, " ").alias("FNLPLANCDE"),
       col("FNLPLANDTE").alias("FNLPLANDTE"),
       rpad(col("FORMLRFLAG"), 1, " ").alias("FORMLRFLAG"),
       col("AWPUNITCST").alias("AWPUNITCST"),
       col("WACUNITCST").alias("WACUNITCST"),
       col("CTYPEUCOST").alias("CTYPEUCOST"),
       col("PROQTY").alias("PROQTY"),
       rpad(col("CLTCOSTTYP"), 10, " ").alias("CLTCOSTTYP"),
       rpad(col("CLTPRCTYPE"), 10, " ").alias("CLTPRCTYPE"),
       col("CLTTYPUCST").alias("CLTTYPUCST"),
       col("RXNETWRKQL").alias("RXNETWRKQL"),
       col("CLTRATE").alias("CLTRATE"),
       col("CLT2RATE").alias("CLT2RATE"),
       col("CLTINGRCST").alias("CLTINGRCST"),
       col("CLTDISPFEE").alias("CLTDISPFEE"),
       col("CLTSLSTAX").alias("CLTSLSTAX"),
       col("CLTINCENTV").alias("CLTINCENTV"),
       col("CLTPATPAY").alias("CLTPATPAY"),
       col("CLTTOTHAMT").alias("CLTTOTHAMT"),
       col("CLTOTHPAYA").alias("CLTOTHPAYA"),
       col("CLT2PSTAX").alias("CLT2PSTAX"),
       col("CLT2FSTAX").alias("CLT2FSTAX"),
       col("CLTDUEAMT").alias("CLTDUEAMT"),
       col("CLT2INGRCST").alias("CLT2INGRCST"),
       col("CLT2DISPFEE").alias("CLT2DISPFEE"),
       col("CLT2SLSTAX").alias("CLT2SLSTAX"),
       col("CLT2OTHAMT").alias("CLT2OTHAMT"),
       col("CLT2DUEAMT").alias("CLT2DUEAMT"),
       rpad(col("CLT2COSTSRC"), 1, " ").alias("CLT2COSTSRC"),
       rpad(col("CLT2COSTTYP"), 10, " ").alias("CLT2COSTTYP"),
       rpad(col("CLT2PRCTYPE"), 10, " ").alias("CLT2PRCTYPE")
    )
)

write_files(
    df_Landing_File_Paid_Reversal,
    f"{adls_path}/verified/PDX_CLM_STD_INPT_Land_{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_Landing_File_DeniedClaims,
    f"{adls_path}/verified/{SrcSysCd}_DeniedClaims_Landing.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

write_files(
    df_W_DRUG_CLM_PCP,
    f"{adls_path}/load/W_DRUG_CLM_PCP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_W_DRUG_ENR,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

write_files(
    df_Landing_File_MedDDrugClmPrice,
    f"{adls_path}/verified/DrugClmPrice_Land_{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_SellPriceNotEqClntDueAmt,
    f"{adls_path}/verified/Optum_SellPriceValidation_{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_BuyPriceNotEqClntDueAmt,
    f"{adls_path}/verified/Optum_BuyPriceValidation_{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)