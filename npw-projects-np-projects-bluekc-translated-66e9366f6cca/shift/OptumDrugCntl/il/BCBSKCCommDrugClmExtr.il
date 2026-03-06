model:
  config:
    origin: datastage
  jobs:
    - name: BCBSKCCommDrugClmExtr
      origin: datastage
      type: dataflow
      folder: "claim/DrugClaim"
      runAfterSubrOnJobSuccess: enabled
      description: "BCBSKCCommon Drug Claim Extract job"
      fullDescription: "COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\n\nCALLED BY : BCBSKCCommDrugClmExtrLoadSeq\n\nDESCRIPTION:  \n   \n Reads the Medtrak drug file from the extract program and adds business rules and reformats to the drug claim common format. Then runs shared container DrugClmPkey\n    \n\nMODIFICATIONS:\nDeveloper                                        Date                       Change Description                                                   Project #             Development Project          Code Reviewer               Date Reviewed  \n=======================================================================================================================================================================\nKaushik Kapoor                                2018-01-17             Initial Programming                                                    5828                    IntegrateDev2                     Kalyan Neelam               2018-02-26 \n\nSaikiran Subbagari                        2019-01-15           Added the TXNMY_CD column                                      5887                    IntegrateDev1\n                                                                                                    in DrugClmPK Container      \n\nBhargava Rampilla                        2019-10-16              Modified feilds as per the \n                                                                             OptumRX transformation rules:                                              6131                    Integrate Dev1\n                                                                                                  GNRC_DRUG_IN\n                                                                                                  MAIL_ORDER_IN (Lkup added)\n                                                                                                  MAC_REDC_IN\n                                                                                                  NON_FRMLRY_DRUG_IN\n                                                                                                  RECON_DT_SK\n                                                                                                  INGR_SAV_AMT\n                                                                                                  MBR_DIFF_PD_AMT\n                                                                                                  PDX_NTWK_ID (Lkup added)\n                                                                                                  ADM_FEE_AMT\n                                                                                                  DRUG_CLM_BILL_ BSS_CD\n                                                                                                  AVG_WHLSL_PRIC E_AMT\n                                                                                                  UCR_AMT\n                                                                                                  INCNTV_FEE_AMT\n                                                                                                  SNGL_SRC_IN\n                                                                                                  \nSRINIVAS GARIMELLA              2019-10-16              Modified feilds as per the                                          6131                       Integrate Dev1         \n                                                                                                OptumRX transformation rules: \n                                                                                                PRESCRIBER_ID\n  \nSAndrew -                                  Corrected the process to load the Pharmacy's NPI only to the IDS.PROV table and not to the IDS.PROV_DEA table which should only contain the Prescriber information\n                                                  On the PRCT sql, corrected the proecess to not select the min(PROV_DEA_SK) when the CMN_PRCT_SK is either 0 or 1.   \n                                                  Added PrescriberID to IDS ProvDEA  table to check if DEA number exists.    The process was assuming that PRESCRBR_ID would only contain NPI and was matching to the PRO_DEA.NPI fields.\n                                                  Added additional checks using the PRSCRBER_ID against both DEA and NPI and checking PRSCRBR_NPI to IDS.PROV_DEA's DEA and NPI.\n                                                  Reordered the rules when determining if the DEA or NPI is on the IDS.PROV_DEA table to pick the query with the best result set\n                                                 Added write to the P_CLM_PDX file, and only OPTUMRX source system will write records to this file.  File sent to Lexus Nexis for resolving unknown prescriber data\n                                                  For Optum, move as much information that is on the Prescriber Provider fields to the PROV_DEA record.\n                                                  Corrected hash file names in HASH.CLEAR to files in job                                                                                                   Kalyan Neelam                2019-11-21\n changed hash file script from                                          \n7;hf_bcbskccomm_drugclm_prct;hf_bcbskccomm_drugclm_provdea;hf_bcbskccomm_drg_clm_ndc_lkup;hf_bcbskccomm_drug_clm_dea;hf_bcbskccomm_drug_clm_deano;hf_bcbskccomm_pdx_no_pdx_ntwk_lkup;hf_bcbskccomm_drg_clm_mailorder_lkup\nto\n8;hf_bcbskccomm_drugclm_dea_only;hf_bcbskccomm_drugclm_prct;hf_bcbskccomm_drugclm_provdea;hf_bcbskccomm_drug_clm_dea;hf_bcbskccomm_drug_clm_deano;hf_bcbskccomm_pdx_no_pdx_ntwk_lkup;hf_bcbskccomm_drg_clm_mailorder_lkup;hf_bcbskccomm_drg_clm_ndc_lkup\n\n\n\nRekha Radhakrishna 2019-12-09         Added Src_sys_cd to the query WHERE clause in PDX_NTWK lookup           6131 PBM Replacement  IntegrateDev2  Kalyan Neelam 2019-12-09\n\nRekha Radhakrishna 2019-01-03     Defaulted DRUG_CLM_PRAUTH_CD to NA for 1                                                 6131 PBM Replacement  IntegrateDev2 Kalyan Neelam 2020-01-04\n\n\nRekha Radhakrishna 2020-01-28    Changed PVDR_NTWK Lookup to use PLANQUAL and SRXNETWK                    6131 PBM Replacement  IntegrateDev2 Kalyan Neelam  2020-01-29\n\nVelmani                      2020-03-31    Mapped speciality indicator field                                                                                 6131 PBM Replacement  \n\nRekha Radhakrishna    2020-07-15    Added 7 new fields to source seq file and Target drug_clm file                               PBM PhaseII       IntegrateDev2                Sravya Gorla     2020-12-10\nRekha Radhakrishna    2020-08-10    Modified Mapping logic for field MBR_DIFF_PD_AMT \n                                                             to BCBSKCComm.CLNT_PATN_PAY_ATRBD_PROD_AMT                               PBM PhaseII        IntegrateDev2                 Sravya Gorla     2020-12-10  \nRekha Radhakrishna    2020-08-13    Added LOB_IN field to source seq file                                                                    PBM PhaseII        IntegrateDev2                  Sravya Gorla     2020-12-10\nRekha Radhakrishna    2020-08-17    Added GNRC_PROD_IN and mapped to drug_clm                                               PBM PhaseII        IntegrateDev2                  Sravya Gorla     2020-12-10 \n\nRekha Radhakrishna 2020-09-22    6131 PBM Replacement   Changed PVDR_NTWK Lookup SQL to filter out records       IntegrateDev2          Jaideep Mankala     09/23/2020\n                                                                                                             that do not contain '=' in TRGT_CD_NM    \n\nGoutham K                  2020-11-12      US-283560                        added MEDIMPACT derivation for field                                  IntegrateDev2                 Reddy Sanam         2020-11-13\n                                                                                                       AVG_WHLSL_PRICE_AMT\n\nGoutham K                  2020-11-19      US-317229                        Added transformation for filed                                                  IntegrateDev2                 Kalyan Neelam                    2020-11-19\n                                                                                                       DRUG_CLM_MCPARTD_COVDRUG_CD\n\nPeter Gichiri                 2021-04-06      US-345228                 Added 'OPTUMRX' to the DRUG_CLM_MCPARTD_COVDRUG_CD    IntegrateDev2         Jaideep Mankala    04/12/2021\n                                                                                                                   derivation logic\n\nPeter Gichiri                 2021-04-26      US-373043                 Added REGEXP_REPLACE(DEA.DEA_NO,'[^a-zA-Z\\\\d]','') logic     IntegrateDev2\x09Abhiram Dasarathy\x092021-05-06\n                                                                                               to the  DEAonly SQL to prevent \"Missing or NULMissing\" error.\n                                                                                              This ensures that the job does not throw Phantom errors warnings"
      parameters:
        - name: CurrentDate
          type: string
          description: "Current Date"
        - name: SrcSysCd
          type: string
          description: "Source System Code"
          longDescription: OPTUMRX
        - name: SrcSysCdSk
          type: string
          description: "Source System Code Surrogate Key"
          longDescription: "-1951778164\n\n"
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: RunCycle
          type: integer
          description: "Run Cycle"
        - name: ProvRunCycle
          type: integer
          description: ProvRunCycle
        - name: RunID
          type: string
          description: "Run ID"
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
        - name: IDSDSN
          type: string
          description: "IDS ODBC DSN"
          defaultType: project
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
        - name: CactusAcct
          type: string
          description: "Cactus Account"
        - name: CactusPW
          type: encrypted
          description: "Cactus Password"
        - name: CactusOwner
          type: string
          description: "Cactus Table Owner"
          defaultType: project
        - name: CactusServer
          type: string
          description: "Cactus Server"
          defaultType: project
        - name: CactusDB
          type: string
          description: "Cactus Database"
          defaultType: project
      afterSubr:
        subrType: "DSU.HASH.CLEAR\\\\8;hf_bcbskccomm_drugclm_dea_only;hf_bcbskccomm_drugclm_prct;hf_bcbskccomm_drugclm_provdea;hf_bcbskccomm_drug_clm_dea;hf_bcbskccomm_drug_clm_deano;hf_bcbskccomm_pdx_no_pdx_ntwk_lkup;hf_bcbskccomm_drg_clm_mailorder_lkup;hf_bcbskccomm_drg_clm_ndc_lkup"
        subr: "8;hf_bcbskccomm_drugclm_dea_only;hf_bcbskccomm_drugclm_prct;hf_bcbskccomm_drugclm_provdea;hf_bcbskccomm_drug_clm_dea;hf_bcbskccomm_drug_clm_deano;hf_bcbskccomm_pdx_no_pdx_ntwk_lkup;hf_bcbskccomm_drg_clm_mailorder_lkup;hf_bcbskccomm_drg_clm_ndc_lkup"
      steps:
        - name: BCBSKCCommClmLand
          type: fileReader
          urls: 
            - "#$FilePath#/verified/PDX_CLM_STD_INPT_Land_#SrcSysCd#.dat.#RunID#"
          rejectMode: continue
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
            - name: FILE_RCVD_DT
              type: char
              width: 10
            - name: RCRD_ID
              type: decimal
              width: 2
            - name: PRCSR_NO
              type: varchar
              width: 10
            - name: BTCH_NO
              type: decimal
              width: 5
            - name: PDX_NO
              type: decimal
              width: 12
            - name: RX_NO
              type: decimal
              width: 9
            - name: FILL_DT
              type: char
              width: 10
            - name: NDC
              type: char
              width: 20
            - name: DRUG_DESC
              type: varchar
              width: 30
            - name: NEW_OR_RFL_CD
              type: int
              width: 4
            - name: METRIC_QTY
              type: decimal
              width: 10
              precision: 3
            - name: DAYS_SUPL
              type: decimal
              width: 3
            - name: BSS_OF_CST_DTRM
              type: char
              width: 2
            - name: INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BILL_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PATN_FIRST_NM
              type: varchar
              width: 35
            - name: PATN_LAST_NM
              type: varchar
              width: 75
            - name: BRTH_DT
              type: char
              width: 10
            - name: SEX_CD
              type: decimal
              width: 1
            - name: CARDHLDR_ID_NO
              type: varchar
              width: 20
            - name: RELSHP_CD
              type: decimal
              width: 1
            - name: GRP_NO
              type: varchar
              width: 20
            - name: HOME_PLN
              type: varchar
              width: 3
            - name: HOST_PLN
              type: varchar
              width: 3
            - name: PRSCRBR_ID
              type: varchar
              width: 20
            - name: DIAG_CD
              type: varchar
              width: 20
            - name: CARDHLDR_FIRST_NM
              type: varchar
              width: 35
            - name: CARDHLDR_LAST_NM
              type: varchar
              width: 75
            - name: PRAUTH_NO
              type: varchar
              width: 12
            - name: PA_MC_SC_NO
              type: varchar
              width: 7
            - name: CUST_LOC
              type: decimal
              width: 2
            - name: RESUB_CYC_CT
              type: decimal
              width: 2
            - name: RX_DT
              type: char
              width: 10
            - name: DISPENSE_AS_WRTN_PROD_SEL_CD
              type: char
              width: 1
            - name: PRSN_CD
              type: varchar
              width: 3
            - name: OTHR_COV_CD
              type: decimal
              width: 2
            - name: ELIG_CLRFCTN_CD
              type: decimal
              width: 1
            - name: CMPND_CD
              type: decimal
              width: 1
            - name: NO_OF_RFLS_AUTH
              type: decimal
              width: 2
            - name: LVL_OF_SVC
              type: decimal
              width: 2
            - name: RX_ORIG_CD
              type: decimal
              width: 1
            - name: RX_DENIAL_CLRFCTN
              type: decimal
              width: 2
            - name: PRI_PRSCRBR
              type: varchar
              width: 20
            - name: CLNC_ID_NO
              type: decimal
              width: 5
            - name: DRUG_TYP
              type: decimal
              width: 1
            - name: PRSCRBR_LAST_NM
              type: varchar
              width: 75
            - name: POSTAGE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: UNIT_DOSE_IN
              type: decimal
              width: 1
            - name: OTHR_PAYOR_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BSS_OF_DAYS_SUPL_DTRM
              type: decimal
              width: 1
            - name: FULL_AVG_WHLSL_PRICE
              type: decimal
              width: 13
              precision: 2
            - name: EXPNSN_AREA
              type: char
              width: 1
            - name: MSTR_CAR
              type: varchar
              width: 20
            - name: SUBCAR
              type: varchar
              width: 20
            - name: CLM_TYP
              type: char
              width: 1
            - name: SUBGRP
              type: varchar
              width: 20
            - name: PLN_DSGNR
              type: char
              width: 1
            - name: ADJDCT_DT
              type: char
              width: 10
            - name: ADMIN_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CAP_AMT
              type: decimal
              width: 13
              precision: 2
            - name: INGR_CST_SUB_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MBR_NON_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MBR_PAY_CD
              type: char
              width: 2
            - name: INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLM_ADJ_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLM_ADJ_CD
              type: char
              width: 2
            - name: FRMLRY_FLAG
              type: char
              width: 1
            - name: GNRC_CLS_NO
              type: varchar
              width: 20
            - name: THRPTC_CLS_AHFS
              type: varchar
              width: 20
            - name: PDX_TYP
              type: char
              width: 1
            - name: BILL_BSS_CD
              type: char
              width: 2
            - name: USL_AND_CUST_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PD_DT
              type: char
              width: 10
            - name: BNF_CD
              type: varchar
              width: 10
            - name: DRUG_STRG
              type: varchar
              width: 20
            - name: ORIG_MBR
              type: varchar
              width: 20
            - name: INJRY_DT
              type: char
              width: 10
            - name: FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: REF_NO
              type: varchar
              width: 20
            - name: CLNT_CUST_ID
              type: varchar
              width: 20
            - name: PLN_TYP
              type: varchar
              width: 20
            - name: ADJDCT_REF_NO
              type: decimal
              width: 9
            - name: ANCLRY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_GNRL_PRPS_AREA
              type: varchar
              width: 100
            - name: PRTL_FILL_STTUS_CD
              type: char
              width: 1
            - name: BILL_DT
              type: char
              width: 10
            - name: FSA_VNDR_CD
              type: varchar
              width: 20
            - name: PICA_DRUG_CD
              type: char
              width: 1
            - name: CLM_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
            - name: FED_DRUG_CLS_CD
              type: char
              width: 1
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BNF_COPAY_100
              type: char
              width: 1
            - name: CLM_PRCS_TYP
              type: char
              width: 1
            - name: INDEM_HIER_TIER_NO
              type: decimal
              width: 4
            - name: MCARE_D_COV_DRUG
              type: char
              width: 1
            - name: RETRO_LICS_CD
              type: char
              width: 1
            - name: RETRO_LICS_AMT
              type: decimal
              width: 13
              precision: 2
            - name: LICS_SBSDY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MCARE_B_DRUG
              type: char
              width: 1
            - name: MCARE_B_CLM
              type: char
              width: 1
            - name: PRSCRBR_QLFR
              type: char
              width: 2
            - name: PRSCRBR_NTNL_PROV_ID
              type: varchar
              width: 20
            - name: PDX_QLFR
              type: char
              width: 2
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
            - name: HLTH_RMBRMT_ARGMT_APLD_AMT
              type: decimal
              width: 11
            - name: THER_CLS
              type: decimal
              width: 6
            - name: HIC_NO
              type: varchar
              width: 20
            - name: HLTH_RMBRMT_ARGMT_FLAG
              type: char
              width: 1
            - name: DOSE_CD
              type: decimal
              width: 4
            - name: LOW_INCM
              type: char
              width: 1
            - name: RTE_OF_ADMIN
              type: char
              width: 2
            - name: DEA_SCHD
              type: decimal
              width: 1
            - name: COPAY_BNF_OPT
              type: decimal
              width: 10
            - name: GNRC_PROD_IN
              type: varchar
              width: 14
            - name: PRSCRBR_SPEC
              type: varchar
              width: 20
            - name: VAL_CD
              type: varchar
              width: 20
            - name: PRI_CARE_PDX
              type: varchar
              width: 20
            - name: OFC_OF_INSPECTOR_GNRL
              type: char
              width: 1
            - name: PATN_SSN
              type: varchar
              width: 11
            - name: CARDHLDR_SSN
              type: varchar
              width: 11
            - name: CARDHLDR_BRTH_DT
              type: varchar
              width: 10
            - name: CARDHLDR_ADDR
              type: varchar
              width: 40
            - name: CARDHLDR_CITY
              type: varchar
              width: 35
            - name: CHADHLDR_ST
              type: varchar
              width: 2
            - name: CARDHLDR_ZIP_CD
              type: varchar
              width: 5
            - name: PSL_FMLY_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PSL_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PSL_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DEDCT_FMLY_MET_AMT
              type: varchar
              width: 20
            - name: DEDCT_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_FMLY_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DEDCT_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PSL_APLD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_APLD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PAR_PDX_IN
              type: char
              width: 1
            - name: COPAY_PCT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: COPAY_FLAT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLM_TRNSMSN_METH
              type: char
              width: 1
            - name: RX_NO_2012
              type: decimal
              width: 12
            - name: CLM_ID
              type: varchar
              width: 20
            - name: CLM_STTUS_CD
              type: varchar
              width: 2
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
            - name: SUBMT_PROD_ID_QLFR
              type: varchar
              width: 2
            - name: CNTNGNT_THER_FLAG
              type: varchar
              width: 1
            - name: CNTNGNT_THER_SCHD
              type: varchar
              width: 20
            - name: CLNT_PATN_PAY_ATRBD_PROD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_PATN_PAY_ATRBD_NTWK_AMT
              type: decimal
              width: 13
              precision: 2
            - name: LOB_IN
              type: varchar
              width: 5
        - name: BCBSKCCommProvDea
          type: fileWriter
          urls: 
            - "#$FilePath#/key/BCBSKCCommDrugClmExtr_#SrcSysCd#.DrugProvDea.dat.#RunID#"
          source: ""
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: BCBSKCCommProvLoc
          type: fileWriter
          urls: 
            - "#$FilePath#/key/BCBSKCCommDrugClmExtr_#SrcSysCd#.ProvLoc.dat.#RunID#"
          source: ""
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: BCBSKCCommDrugClm
          type: fileWriter
          urls: 
            - "#$FilePath#/key/BCBSKCCommDrugClmExtr_#SrcSysCd#.DrugClmDrug.dat.#RunID#"
          source: ""
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: P_CLM_PDX
          type: fileWriter
          urls: 
            - "#$FilePath#/load/P_CLM_PDX.dat"
          source: "Snapshot.V0S135P7"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: BCBSKCCommNdc
          type: fileWriter
          urls: 
            - "#$FilePath#/key/BCBSKCCommDrugClmExtr_#SrcSysCd#.DrugNDC.dat.#RunID#"
          source: ""
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: BCBSKCCommProv
          type: fileWriter
          urls: 
            - "#$FilePath#/key/BCBSKCCommDrugClmExtr_#SrcSysCd#.DrugProv.dat.#RunID#"
          source: ""
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: B_DRUG_CLM
          type: fileWriter
          urls: 
            - "#$FilePath#/load/B_DRUG_CLM.#SrcSysCd#.dat.#RunID#"
          source: "Snapshot.V0S135P4"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: hf_bcbskccomm_drugclm_prct.hf_prct_facets_1
          type: map
          source: ""
          alias: PRCT
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PRCT.NTNL_PROV_ID"
            - name: SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "PRCT.SRC_CD"
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
              map: "PRCT.CMN_PRCT_SK"
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "PRCT.PROV_DEA_SK"
        - name: hf_bcbskccomm_drugclm_prct.hf_prct_vcac_1
          type: map
          source: ""
          alias: PRCT
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PRCT.NTNL_PROV_ID"
            - name: SRC_CD
              type: varchar
              width: 20
              map: "PRCT.SRC_CD"
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
              map: "PRCT.CMN_PRCT_SK"
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "PRCT.PROV_DEA_SK"
        - name: hf_bcbskccomm_drugclm_prct.hf_prct_vcac_2
          type: map
          source: ""
          alias: PRCT
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PRCT.NTNL_PROV_ID"
            - name: SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "PRCT.SRC_CD"
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
              map: "PRCT.CMN_PRCT_SK"
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "PRCT.PROV_DEA_SK"
        - name: hf_bcbskccomm_drugclm_prct.hf_prct_facets_2
          type: map
          source: ""
          alias: PRCT
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PRCT.NTNL_PROV_ID"
            - name: SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "PRCT.SRC_CD"
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
              map: "PRCT.CMN_PRCT_SK"
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "PRCT.PROV_DEA_SK"
        - name: hf_bcbskccomm_drugclm_provdea.hf_npi_min_dea_1
          type: map
          source: ""
          alias: NPIminDEA
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "NPIminDEA.NTNL_PROV_ID"
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "NPIminDEA.PROV_DEA_SK"
        - name: hf_bcbskccomm_drugclm_provdea.hf_npi_min_dea_2
          type: map
          source: ""
          alias: NPIminDEA
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "NPIminDEA.NTNL_PROV_ID"
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "NPIminDEA.PROV_DEA_SK"
        - name: hf_bcbskccomm_drugclm_prct_dea
          type: map
          source: ""
          alias: DEAonly
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "DEAonly.DEA_NO"
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
              map: "DEAonly.CMN_PRCT_SK"
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "DEAonly.PROV_DEA_SK"
        - name: hf_bcbskccomm_drug_clm_dea.hf_dea
          type: map
          source: "BCBSKCCommProvDeaSnapshot.lookupBCBSKCCommProvLocBusiness_RulesBCBSKCCommDrugClmP_CLM_PDXBCBSKCCommNdcBCBSKCCommProvB_DRUG_CLM"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: ".PROV_DEA_SK"
            - name: DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: ".DEA_NO"
        - name: hf_bcbskccomm_drug_clm_dea.dea_no
          type: map
          source: "BCBSKCCommProvDeaSnapshot.lookupBCBSKCCommProvLocBusiness_RulesBCBSKCCommDrugClmP_CLM_PDXBCBSKCCommNdcBCBSKCCommProvB_DRUG_CLM"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: ".DEA_NO"
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: ".PROV_DEA_SK"
        - name: hf_bcbskccomm_drg_clm_ndc_lkup
          type: map
          source: IDS_NDC
          alias: Ndc
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: NDC
              type: varchar
              width: 20
              nullable: N
              map: "Ndc.NDC"
            - name: NDC_DRUG_ABUSE_CTL_CD
              type: varchar
              width: 20
              nullable: N
              map: "Ndc.NDC_DRUG_ABUSE_CTL_CD"
            - name: DRUG_MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "Ndc.DRUG_MNTN_IN"
        - name: hf_bcbskccomm_pdx_no_pdx_ntwk_lkup
          type: map
          source: PDX_NTWK
          alias: Pdx_Ntwk
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Pdx_Ntwk.PROV_ID"
            - name: PLANQUAL
              type: varchar
              width: 10
              nullable: N
              map: "Pdx_Ntwk.PLANQUAL"
            - name: SRXNETWK
              type: varchar
              width: 6
              nullable: N
              map: "Pdx_Ntwk.SRXNETWK"
            - name: TRGT_CD_NM
              type: varchar
              width: 20
              nullable: N
              map: "Pdx_Ntwk.TRGT_CD_NM"
            - name: TRGT_CD
              type: varchar
              width: 20
              nullable: N
              map: "Pdx_Ntwk.TRGT_CD"
        - name: hf_bcbskccomm_drg_clm_mailorder_lkup
          type: map
          source: IDS_CD_MPPNG
          alias: MailOrder
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "MailOrder.SRC_CD"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "MailOrder.SRC_SYS_CD"
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "MailOrder.CD_MPPNG_SK"
        - name: IDS_PRCT_DEA
          type: dbReader
          originalDb: db2
        - name: IDS_DEA
          type: dbReader
          originalDb: db2
          query: "SELECT PROV_DEA_SK as PROV_DEA_SK,DEA_NO as DEA_NO FROM #$IDSOwner#.PROV_DEA WHERE PROV_DEA_SK <> 0 AND PROV_DEA_SK <> 1 "
          schema: 
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
            - name: DEA_NO
              type: varchar
              width: 20
              nullable: N
          keys:
            - PROV_DEA_SK
        - name: IDS_NDC
          type: dbReader
          originalDb: db2
          query: "SELECT NDC.NDC,MAP1.SRC_CD AS NDC_DRUG_ABUSE_CTL_CD,NDC.DRUG_MNTN_IN FROM #$IDSOwner#.NDC NDC, #$IDSOwner#.CD_MPPNG MAP1 WHERE NDC_DRUG_ABUSE_CTL_CD_SK = MAP1.CD_MPPNG_SK;"
          schema: 
            - name: NDC
              type: varchar
              width: 20
              nullable: N
            - name: NDC_DRUG_ABUSE_CTL_CD
              type: varchar
              width: 20
              nullable: N
            - name: DRUG_MNTN_IN
              type: char
              width: 1
              nullable: N
          keys:
            - NDC
        - name: PDX_NTWK
          type: dbReader
          originalDb: db2
          query: "Select NTWK.PROV_ID, \nSUBSTRING(MAP2.SRC_CD, 1,2)AS PLANQUAL,  \nTrim(SUBSTRING(MAP2.TRGT_CD_NM, POSITION( ' ' IN MAP2.TRGT_CD_NM ), POSITION('=' IN MAP2.TRGT_CD_NM)-3)) AS SRXNETWK,\nMAP2.TRGT_CD_NM,\nMAP2.TRGT_CD   \nfrom #$IDSOwner#.PDX_NTWK NTWK, #$IDSOwner#.CD_MPPNG MAP1, #$IDSOwner#.CD_MPPNG MAP2\nWHERE NTWK.SRC_SYS_CD_SK = MAP1.CD_MPPNG_SK\nAND MAP1.TRGT_CD = 'NABP'\nAND NTWK.PDX_NTWK_CD_SK = MAP2.CD_MPPNG_SK\nAND MAP2.TRGT_CD LIKE 'ORX%'\nAND LOCATE('=',MAP2.TRGT_CD_NM) <> 0"
          schema: 
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: PLANQUAL
              type: varchar
              width: 20
              nullable: N
            - name: SRXNETWK
              type: varchar
              width: 255
              nullable: N
            - name: TRGT_CD_NM
              type: varchar
              width: 255
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - PROV_ID
            - PLANQUAL
            - SRXNETWK
        - name: IDS_CD_MPPNG
          type: dbReader
          originalDb: db2
          query: "SELECT SRC_CD, SRC_SYS_CD, CD_MPPNG_SK FROM #$IDSOwner#.CD_MPPNG Where TRGT_CLCTN_CD = 'IDS'  And TRGT_CD_NM = 'MAIL ORDER PHARMACY' "
          schema: 
            - name: SRC_CD
              type: varchar
              width: 20
              nullable: N
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_CD
            - SRC_SYS_CD
        - name: Snapshot.lookup
          type: lookup
          stageVars:
            - name: NpiDeaNo
              type: decimal
              width: 0
              map: " If Len(Trim(dea_no.DEA_NO)) > 0 then Trim(dea_no.DEA_NO) else \"UNK\""
              initValue: "0"
            - name: FoundOptumRXPrescProvDeaNo
              type: decimal
              width: 0
              map: " If Transform.SRC_SYS_CD = 'OPTUMRX' THEN  If (IsNull(dea_no.PROV_DEA_SK) = @FALSE and dea_no.PROV_DEA_SK <> 0 and dea_no.PROV_DEA_SK <> 1) Then \"Y\" Else  If (IsNull(hf_dea.DEA_NO) = @FALSE and hf_dea.DEA_NO <> \"UNK\" AND hf_dea.DEA_NO <> \"NA\") Then \"Y\" Else \"N\" ELSE \"N\""
              initValue: "0"
          option:
            joinType: left
          sources: 
            - name: Business_Rules
              alias: Transform
              joinFilter: ""
              joinExpression: []
            - name: hf_bcbskccomm_drug_clm_dea.hf_dea
              alias: hf_dea
              joinFilter: ""
              joinExpression:
              - fromExpr: "hf_dea.PROV_DEA_SK"
                toExpr: "Transform.PROV_DEA_SK"
                caseInSensitive: true
            - name: hf_bcbskccomm_drug_clm_dea.dea_no
              alias: dea_no
              joinFilter: ""
              joinExpression:
              - fromExpr: "dea_no.DEA_NO"
                toExpr: "Trim(Transform.PRESCRIBER_ID)"
                caseInSensitive: true
          joinCondition: 
            - expression: "hf_dea.PROV_DEA_SK = Transform.PROV_DEA_SK"
              caseInSensitive: true
            - expression: "dea_no.DEA_NO = Trim(Transform.PRESCRIBER_ID)"
              caseInSensitive: true
          schema: 
            - name: Pkey_JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.JOB_EXCTN_RCRD_ERR_SK"
            - name: Pkey_INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.INSRT_UPDT_CD"
            - name: Pkey_DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.DISCARD_IN"
            - name: Pkey_PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.PASS_THRU_IN"
            - name: Pkey_FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Transform.FIRST_RECYC_DT"
            - name: Pkey_ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Transform.ERR_CT"
            - name: Pkey_RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Transform.RECYCLE_CT"
            - name: Pkey_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.SRC_SYS_CD"
            - name: Pkey_PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Transform.PRI_KEY_STRING"
            - name: Pkey_DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.DRUG_CLM_SK"
            - name: Pkey_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_ID"
            - name: Pkey_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.CRT_RUN_CYC_EXCTN_SK"
            - name: Pkey_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: Pkey_NDC
              type: varchar
              width: 20
              nullable: N
              map: "Transform.NDC"
            - name: Pkey_NDC_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.NDC_SK"
            - name: Pkey_PRSCRB_PROV_DEA
              type: varchar
              width: 20
              nullable: N
              map: " If Transform.SRC_SYS_CD = 'OPTUMRX' THEN  If (IsNull(dea_no.PROV_DEA_SK) = @FALSE and dea_no.PROV_DEA_SK <> 0 and dea_no.PROV_DEA_SK <> 1) Then Trim(Transform.PRESCRIBER_ID) Else  If (IsNull(hf_dea.DEA_NO) = @FALSE and hf_dea.DEA_NO <> \"UNK\" AND hf_dea.DEA_NO <> \"NA\") Then Trim(hf_dea.DEA_NO) else Trim(Transform.PRESCRIBER_ID) ELSE  If (IsNull(dea_no.PROV_DEA_SK) = @FALSE Or Len(Trim(dea_no.PROV_DEA_SK)) > 0) Then Trim(Transform.PRESCRIBER_ID) Else  If IsNull(hf_dea.DEA_NO) = @TRUE Then  If (IsNull(Transform.PRESCRIBER_ID) = @FALSE Or Len(Trim(Transform.PRESCRIBER_ID)) > 0) Then Trim(Transform.PRESCRIBER_ID) Else 'UNK' Else hf_dea.DEA_NO"
            - name: Pkey_DRUG_CLM_DAW_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.DRUG_CLM_DAW_CD"
            - name: Pkey_DRUG_CLM_LGL_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.DRUG_CLM_LGL_STTUS_CD"
            - name: Pkey_DRUG_CLM_TIER_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.DRUG_CLM_TIER_CD"
            - name: Pkey_DRUG_CLM_VNDR_STTUS_CD
              type: varchar
              width: 512
              nullable: N
              map: "Transform.DRUG_CLM_VNDR_STTUS_CD"
            - name: Pkey_CMPND_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.CMPND_IN"
            - name: Pkey_FRMLRY_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.FRMLRY_IN"
            - name: Pkey_GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.GNRC_DRUG_IN"
            - name: Pkey_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.MAIL_ORDER_IN"
            - name: Pkey_MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.MNTN_IN"
            - name: Pkey_MAC_REDC_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.MAC_REDC_IN"
            - name: Pkey_NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.NON_FRMLRY_DRUG_IN"
            - name: Pkey_SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.SNGL_SRC_IN"
            - name: Pkey_ADJ_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.ADJ_DT"
            - name: Pkey_FILL_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.FILL_DT"
            - name: Pkey_RECON_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.RECON_DT"
            - name: Pkey_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.DISPNS_FEE_AMT"
            - name: Pkey_HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.HLTH_PLN_EXCL_AMT"
            - name: Pkey_HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.HLTH_PLN_PD_AMT"
            - name: Pkey_INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.INGR_CST_ALW_AMT"
            - name: Pkey_INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.INGR_CST_CHRGD_AMT"
            - name: Pkey_INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.INGR_SAV_AMT"
            - name: Pkey_MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.MBR_DEDCT_EXCL_AMT"
            - name: Pkey_MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.MBR_DIFF_PD_AMT"
            - name: Pkey_MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.MBR_OOP_AMT"
            - name: Pkey_MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.MBR_OOP_EXCL_AMT"
            - name: Pkey_OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.OTHR_SAV_AMT"
            - name: Pkey_RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "Transform.RX_ALW_QTY"
            - name: Pkey_RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "Transform.RX_SUBMT_QTY"
            - name: Pkey_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.SLS_TAX_AMT"
            - name: Pkey_RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "Transform.RX_ALW_DAYS_SUPL_QTY"
            - name: Pkey_RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "Transform.RX_ORIG_DAYS_SUPL_QTY"
            - name: Pkey_PDX_NTWK_ID
              type: varchar
              width: 20
              map: "Transform.PDX_NTWK_ID"
            - name: Pkey_RX_NO
              type: varchar
              width: 20
              map: "Transform.RX_NO"
            - name: Pkey_RFL_NO
              type: varchar
              width: 20
              map: "Transform.RFL_NO"
            - name: Pkey_VNDR_CLM_NO
              type: varchar
              width: 20
              map: "Transform.VNDR_CLM_NO"
            - name: Pkey_VNDR_PREAUTH_ID
              type: varchar
              width: 20
              map: "Transform.VNDR_PREAUTH_ID"
            - name: Pkey_CLM_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_STTUS_CD"
            - name: Pkey_PROV_ID
              type: char
              width: 12
              nullable: N
              map: "Transform.PDX_PROV_ID"
            - name: Pkey_NDC_LABEL_NM
              type: varchar
              width: 35
              nullable: N
              map: "Transform.NDC_LABEL_NM"
            - name: Pkey_PRSCRB_NAME
              type: varchar
              width: 75
              nullable: N
              map: "Transform.PRSCRB_NAME"
            - name: Pkey_PHARMACY_NAME
              type: char
              width: 30
              nullable: N
              map: "Transform.PHARMACY_NAME"
            - name: Pkey_DRUG_CLM_BNF_FRMLRY_POL_CD
              type: char
              width: 1
              nullable: N
              map: "Transform.DRUG_CLM_BNF_FRMLRY_POL_CD"
            - name: Pkey_DRUG_CLM_BNF_RSTRCT_CD
              type: char
              width: 1
              nullable: N
              map: "Transform.DRUG_CLM_BNF_RSTRCT_CD"
            - name: Pkey_DRUG_CLM_MCPARTD_COVDRUG_CD
              type: char
              width: 1
              nullable: N
              map: "Transform.DRUG_CLM_MCPARTD_COVDRUG_CD"
            - name: Pkey_DRUG_CLM_PRAUTH_CD
              type: char
              width: 1
              nullable: N
              map: "Transform.DRUG_CLM_PRAUTH_CD"
            - name: Pkey_MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.MNDTRY_MAIL_ORDER_IN"
            - name: Pkey_ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.ADM_FEE_AMT"
            - name: Pkey_DRUG_CLM_BILL_BSS_CD
              type: char
              width: 2
              nullable: N
              map: "Transform.DRUG_CLM_BILL_BSS_CD"
            - name: Pkey_AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.AVG_WHLSL_PRICE_AMT"
            - name: Pkey_NTNL_PROV_ID
              type: char
              width: 10
              map: "Transform.NTNL_PROV_ID"
            - name: Pkey_DRUG_NM
              type: char
              width: 15
              nullable: N
              map: "'NA'"
            - name: Pkey_UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.UCR_AMT"
            - name: Pkey_PRTL_FILL_STTUS_CD
              type: char
              width: 1
              nullable: N
              map: "Transform.PRTL_FILL_STTUS_CD"
            - name: Pkey_PDX_TYP
              type: char
              width: 1
              nullable: N
              map: "Transform.PDX_TYP"
            - name: Pkey_INCNTV_FEE
              type: decimal
              width: 8
              precision: 2
              nullable: N
              map: "Transform.INCNTV_FEE_AMT"
            - name: Pkey_PRSCRBR_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Transform.PRSCRBR_NTNL_PROV_ID"
            - name: Pkey_SPEC_DRUG_IN
              type: char
              width: 1
              map: "Transform.SPEC_DRUG_IN"
            - name: Pkey_SUBMT_PROD_ID_QLFR
              type: varchar
              width: 2
              map: "Transform.SUBMT_PROD_ID_QLFR"
            - name: Pkey_CNTNGNT_THER_FLAG
              type: varchar
              width: 1
              map: "Transform.CNTNGNT_THER_FLAG"
            - name: Pkey_CNTNGNT_THER_SCHD
              type: varchar
              width: 20
              map: "Transform.CNTNGNT_THER_SCHD"
            - name: Pkey_CLNT_PATN_PAY_ATRBD_PROD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Transform.CLNT_PATN_PAY_ATRBD_PROD_AMT"
            - name: Pkey_CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Transform.CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"
            - name: Pkey_CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Transform.CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"
            - name: Pkey_CLNT_PATN_PAY_ATRBD_NTWK_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Transform.CLNT_PATN_PAY_ATRBD_NTWK_AMT"
            - name: Pkey_GNRC_PROD_IN
              type: varchar
              width: 14
              map: "Transform.GNRC_PROD_IN"
            - name: Snapshot_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSk
            - name: Snapshot_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_ID"
            - name: PclmPdx_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "GetFkeyClm(SrcSysCd, 1, Transform.CLM_ID, 'N')"
            - name: PclmPdx_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_ID"
            - name: PclmPdx_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "'OPTUMRX'"
            - name: PclmPdx_PRSCRB_PROV_DEA_ID
              type: varchar
              width: 20
              nullable: N
              map: " If (IsNull(dea_no.PROV_DEA_SK) = @FALSE and dea_no.PROV_DEA_SK <> 0 and dea_no.PROV_DEA_SK <> 1) Then Trim(Transform.PRESCRIBER_ID) Else  If (IsNull(hf_dea.DEA_NO) = @FALSE and hf_dea.DEA_NO <> \"UNK\" AND hf_dea.DEA_NO <> \"NA\") Then Trim(hf_dea.DEA_NO) else Trim(Transform.PRESCRIBER_ID)"
            - name: PclmPdx_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Trim(Transform.PRSCRBR_NTNL_PROV_ID)"
            - name: PclmPdx_MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: " If Transform.ORIG_MBR <> 0 then 1 else Transform.ORIG_MBR"
            - name: filter_PclmPdx
              type: smallint
              width: 2
              nullable: N
              map: "if (Transform.SRC_SYS_CD = \"OPTUMRX\" and ((FoundOptumRXPrescProvDeaNo = \"N\") Or ((FoundOptumRXPrescProvDeaNo = \"Y\" and (Len(Trim(Transform.PRSCRBR_NTNL_PROV_ID)) > 0 and (Trim(Transform.PRSCRBR_NTNL_PROV_ID) = 'NA' or Trim(Transform.PRSCRBR_NTNL_PROV_ID) = 'UNK')))))) then 1 else 0"
        - name: Snapshot.V0S135P7.filter
          type: filter
          source: Snapshot.lookup
          filter: "filter_PclmPdx = 1"
          schema: 
            - name: PclmPdx_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: PclmPdx_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: PclmPdx_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: PclmPdx_PRSCRB_PROV_DEA_ID
              type: varchar
              width: 20
              nullable: N
            - name: PclmPdx_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: PclmPdx_MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
        - name: Snapshot.V0S135P2
          type: map
          source: Snapshot.lookup
          alias: Pkey
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Pkey.Pkey_JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Pkey.Pkey_INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Pkey.Pkey_FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Pkey.Pkey_ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Pkey.Pkey_RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Pkey.Pkey_SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Pkey.Pkey_PRI_KEY_STRING"
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Pkey.Pkey_DRUG_CLM_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Pkey.Pkey_CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Pkey.Pkey_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Pkey.Pkey_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: NDC
              type: varchar
              width: 20
              nullable: N
              map: "Pkey.Pkey_NDC"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "Pkey.Pkey_NDC_SK"
            - name: PRSCRB_PROV_DEA
              type: varchar
              width: 20
              nullable: N
              map: "Pkey.Pkey_PRSCRB_PROV_DEA"
            - name: DRUG_CLM_DAW_CD
              type: varchar
              width: 20
              nullable: N
              map: "Pkey.Pkey_DRUG_CLM_DAW_CD"
            - name: DRUG_CLM_LGL_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Pkey.Pkey_DRUG_CLM_LGL_STTUS_CD"
            - name: DRUG_CLM_TIER_CD
              type: varchar
              width: 20
              nullable: N
              map: "Pkey.Pkey_DRUG_CLM_TIER_CD"
            - name: DRUG_CLM_VNDR_STTUS_CD
              type: varchar
              width: 512
              nullable: N
              map: "Pkey.Pkey_DRUG_CLM_VNDR_STTUS_CD"
            - name: CMPND_IN
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_CMPND_IN"
            - name: FRMLRY_IN
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_FRMLRY_IN"
            - name: GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_GNRC_DRUG_IN"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_MAIL_ORDER_IN"
            - name: MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_MNTN_IN"
            - name: MAC_REDC_IN
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_MAC_REDC_IN"
            - name: NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_NON_FRMLRY_DRUG_IN"
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_SNGL_SRC_IN"
            - name: ADJ_DT
              type: char
              width: 10
              nullable: N
              map: "Pkey.Pkey_ADJ_DT"
            - name: FILL_DT
              type: char
              width: 10
              nullable: N
              map: "Pkey.Pkey_FILL_DT"
            - name: RECON_DT
              type: char
              width: 10
              nullable: N
              map: "Pkey.Pkey_RECON_DT"
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Pkey.Pkey_DISPNS_FEE_AMT"
            - name: HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Pkey.Pkey_HLTH_PLN_EXCL_AMT"
            - name: HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Pkey.Pkey_HLTH_PLN_PD_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Pkey.Pkey_INGR_CST_ALW_AMT"
            - name: INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Pkey.Pkey_INGR_CST_CHRGD_AMT"
            - name: INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Pkey.Pkey_INGR_SAV_AMT"
            - name: MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Pkey.Pkey_MBR_DEDCT_EXCL_AMT"
            - name: MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Pkey.Pkey_MBR_DIFF_PD_AMT"
            - name: MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Pkey.Pkey_MBR_OOP_AMT"
            - name: MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Pkey.Pkey_MBR_OOP_EXCL_AMT"
            - name: OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Pkey.Pkey_OTHR_SAV_AMT"
            - name: RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "Pkey.Pkey_RX_ALW_QTY"
            - name: RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "Pkey.Pkey_RX_SUBMT_QTY"
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Pkey.Pkey_SLS_TAX_AMT"
            - name: RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "Pkey.Pkey_RX_ALW_DAYS_SUPL_QTY"
            - name: RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: "Pkey.Pkey_RX_ORIG_DAYS_SUPL_QTY"
            - name: PDX_NTWK_ID
              type: varchar
              width: 20
              map: "Pkey.Pkey_PDX_NTWK_ID"
            - name: RX_NO
              type: varchar
              width: 20
              map: "Pkey.Pkey_RX_NO"
            - name: RFL_NO
              type: varchar
              width: 20
              map: "Pkey.Pkey_RFL_NO"
            - name: VNDR_CLM_NO
              type: varchar
              width: 20
              map: "Pkey.Pkey_VNDR_CLM_NO"
            - name: VNDR_PREAUTH_ID
              type: varchar
              width: 20
              map: "Pkey.Pkey_VNDR_PREAUTH_ID"
            - name: CLM_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Pkey.Pkey_CLM_STTUS_CD"
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
              map: "Pkey.Pkey_PROV_ID"
            - name: NDC_LABEL_NM
              type: varchar
              width: 35
              nullable: N
              map: "Pkey.Pkey_NDC_LABEL_NM"
            - name: PRSCRB_NAME
              type: varchar
              width: 75
              nullable: N
              map: "Pkey.Pkey_PRSCRB_NAME"
            - name: PHARMACY_NAME
              type: char
              width: 30
              nullable: N
              map: "Pkey.Pkey_PHARMACY_NAME"
            - name: DRUG_CLM_BNF_FRMLRY_POL_CD
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_DRUG_CLM_BNF_FRMLRY_POL_CD"
            - name: DRUG_CLM_BNF_RSTRCT_CD
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_DRUG_CLM_BNF_RSTRCT_CD"
            - name: DRUG_CLM_MCPARTD_COVDRUG_CD
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_DRUG_CLM_MCPARTD_COVDRUG_CD"
            - name: DRUG_CLM_PRAUTH_CD
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_DRUG_CLM_PRAUTH_CD"
            - name: MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_MNDTRY_MAIL_ORDER_IN"
            - name: ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Pkey.Pkey_ADM_FEE_AMT"
            - name: DRUG_CLM_BILL_BSS_CD
              type: char
              width: 2
              nullable: N
              map: "Pkey.Pkey_DRUG_CLM_BILL_BSS_CD"
            - name: AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Pkey.Pkey_AVG_WHLSL_PRICE_AMT"
            - name: NTNL_PROV_ID
              type: char
              width: 10
              map: "Pkey.Pkey_NTNL_PROV_ID"
            - name: DRUG_NM
              type: char
              width: 15
              nullable: N
              map: "Pkey.Pkey_DRUG_NM"
            - name: UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Pkey.Pkey_UCR_AMT"
            - name: PRTL_FILL_STTUS_CD
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_PRTL_FILL_STTUS_CD"
            - name: PDX_TYP
              type: char
              width: 1
              nullable: N
              map: "Pkey.Pkey_PDX_TYP"
            - name: INCNTV_FEE
              type: decimal
              width: 8
              precision: 2
              nullable: N
              map: "Pkey.Pkey_INCNTV_FEE"
            - name: PRSCRBR_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Pkey.Pkey_PRSCRBR_NTNL_PROV_ID"
            - name: SPEC_DRUG_IN
              type: char
              width: 1
              map: "Pkey.Pkey_SPEC_DRUG_IN"
            - name: SUBMT_PROD_ID_QLFR
              type: varchar
              width: 2
              map: "Pkey.Pkey_SUBMT_PROD_ID_QLFR"
            - name: CNTNGNT_THER_FLAG
              type: varchar
              width: 1
              map: "Pkey.Pkey_CNTNGNT_THER_FLAG"
            - name: CNTNGNT_THER_SCHD
              type: varchar
              width: 20
              map: "Pkey.Pkey_CNTNGNT_THER_SCHD"
            - name: CLNT_PATN_PAY_ATRBD_PROD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Pkey.Pkey_CLNT_PATN_PAY_ATRBD_PROD_AMT"
            - name: CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Pkey.Pkey_CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"
            - name: CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Pkey.Pkey_CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"
            - name: CLNT_PATN_PAY_ATRBD_NTWK_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Pkey.Pkey_CLNT_PATN_PAY_ATRBD_NTWK_AMT"
            - name: GNRC_PROD_IN
              type: varchar
              width: 14
              map: "Pkey.Pkey_GNRC_PROD_IN"
        - name: Snapshot.V0S135P4
          type: map
          source: Snapshot.lookup
          alias: Snapshot
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Snapshot.Snapshot_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Snapshot.Snapshot_CLM_ID"
        - name: Snapshot.V0S135P7
          type: map
          source: Snapshot.V0S135P7.filter
          alias: PclmPdx
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "PclmPdx.PclmPdx_CLM_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "PclmPdx.PclmPdx_CLM_ID"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "PclmPdx.PclmPdx_SRC_SYS_CD"
            - name: PRSCRB_PROV_DEA_ID
              type: varchar
              width: 20
              nullable: N
              map: "PclmPdx.PclmPdx_PRSCRB_PROV_DEA_ID"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PclmPdx.PclmPdx_NTNL_PROV_ID"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "PclmPdx.PclmPdx_MBR_UNIQ_KEY"
        - name: Business_Rules
          type: lookup
          stageVars:
            - name: CmpndIn
              type: decimal
              width: 0
              map: " If BCBSKCComm.CMPND_CD = 0 then 'N' else  if BCBSKCComm.CMPND_CD = 1 then 'N' else  if BCBSKCComm.CMPND_CD = 2 then 'Y' else 'U'"
              initValue: "-1"
            - name: ClaimTier
              type: decimal
              width: 0
              map: " If BCBSKCComm.DRUG_TYP = 3 then 'TIER1' else  If BCBSKCComm.DRUG_TYP <> 3 And BCBSKCComm.FRMLRY_FLAG = 'Y' then 'TIER2' else  If BCBSKCComm.DRUG_TYP <> 3 And BCBSKCComm.FRMLRY_FLAG <> 'Y' then 'TIER3' else 'UNK'"
              initValue: "0"
            - name: LegalStatus
              type: decimal
              width: 0
              map: " If ISNULL(NdcLkup.NDC_DRUG_ABUSE_CTL_CD) = @TRUE then \"UNK\" else NdcLkup.NDC_DRUG_ABUSE_CTL_CD"
              initValue: "0"
            - name: SVPrtlFillSttusCd
              type: decimal
              width: 0
              map: " If (IsNull(BCBSKCComm.PRTL_FILL_STTUS_CD) = @TRUE or Len(Trim(BCBSKCComm.PRTL_FILL_STTUS_CD)) = 0) Then 'BLANK' Else BCBSKCComm.PRTL_FILL_STTUS_CD"
              initValue: "0"
          option:
            joinType: left
          sources: 
            - name: DataPrep
              alias: BCBSKCComm
              joinFilter: ""
              joinExpression: []
            - name: hf_bcbskccomm_drugclm_provdea.hf_npi_min_dea_1
              alias: hf_npi_min_dea_1
              joinFilter: ""
              joinExpression:
              - fromExpr: "hf_npi_min_dea_1.NTNL_PROV_ID"
                toExpr: "Trim(BCBSKCComm.PRSCRBR_ID)"
                caseInSensitive: true
            - name: hf_bcbskccomm_drugclm_prct.hf_prct_facets_1
              alias: hf_prct_facets_1
              joinFilter: "hf_prct_facets_1.SRC_CD = 'FACETS'"
              joinExpression:
              - fromExpr: "hf_prct_facets_1.NTNL_PROV_ID"
                toExpr: "Trim(BCBSKCComm.PRSCRBR_ID)"
                caseInSensitive: true
            - name: hf_bcbskccomm_drugclm_prct.hf_prct_vcac_1
              alias: hf_prct_vcac_1
              joinFilter: "hf_prct_vcac_1.SRC_CD = 'VCAC'"
              joinExpression:
              - fromExpr: "hf_prct_vcac_1.NTNL_PROV_ID"
                toExpr: "Trim(BCBSKCComm.PRSCRBR_ID)"
                caseInSensitive: true
            - name: hf_bcbskccomm_drg_clm_ndc_lkup
              alias: NdcLkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "NdcLkup.NDC"
                toExpr: "BCBSKCComm.NDC_DER"
                caseInSensitive: true
            - name: hf_bcbskccomm_pdx_no_pdx_ntwk_lkup
              alias: Pdxntwk_Lkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "Pdxntwk_Lkup.PROV_ID"
                toExpr: "Trim(BCBSKCComm.PDX_NO)"
                caseInSensitive: true
              - fromExpr: "Pdxntwk_Lkup.PLANQUAL"
                toExpr: "BCBSKCComm.PLANQUAL[1,2]"
                caseInSensitive: true
              - fromExpr: "Pdxntwk_Lkup.SRXNETWK"
                toExpr: "BCBSKCComm.SRXNETWK"
                caseInSensitive: true
            - name: hf_bcbskccomm_drg_clm_mailorder_lkup
              alias: MailOrderLkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "MailOrderLkup.SRC_CD"
                toExpr: "BCBSKCComm.PDX_TYP"
                caseInSensitive: true
              - fromExpr: "MailOrderLkup.SRC_SYS_CD"
                toExpr: "BCBSKCComm.SRC_SYS_CD"
                caseInSensitive: true
            - name: hf_bcbskccomm_drugclm_prct_dea
              alias: hf_dea
              joinFilter: ""
              joinExpression:
              - fromExpr: "hf_dea.DEA_NO"
                toExpr: "Trim(BCBSKCComm.PRSCRBR_ID)"
                caseInSensitive: true
            - name: hf_bcbskccomm_drugclm_prct.hf_prct_facets_2
              alias: hf_prct_facets_2
              joinFilter: "hf_prct_facets_2.SRC_CD = 'FACETS'"
              joinExpression:
              - fromExpr: "hf_prct_facets_2.NTNL_PROV_ID"
                toExpr: "Trim(BCBSKCComm.PRSCRBR_NTNL_PROV_ID)"
                caseInSensitive: true
            - name: hf_bcbskccomm_drugclm_prct.hf_prct_vcac_2
              alias: hf_prct_vcac_2
              joinFilter: "hf_prct_vcac_2.SRC_CD = 'VCAC'"
              joinExpression:
              - fromExpr: "hf_prct_vcac_2.NTNL_PROV_ID"
                toExpr: "Trim(BCBSKCComm.PRSCRBR_NTNL_PROV_ID)"
                caseInSensitive: true
            - name: hf_bcbskccomm_drugclm_provdea.hf_npi_min_dea_2
              alias: hf_npi_min_dea_2
              joinFilter: ""
              joinExpression:
              - fromExpr: "hf_npi_min_dea_2.NTNL_PROV_ID"
                toExpr: "Trim(BCBSKCComm.PRSCRBR_NTNL_PROV_ID)"
                caseInSensitive: true
          joinCondition: 
            - expression: "hf_npi_min_dea_1.NTNL_PROV_ID = Trim(BCBSKCComm.PRSCRBR_ID)"
              caseInSensitive: true
            - expression: "hf_prct_facets_1.NTNL_PROV_ID = Trim(BCBSKCComm.PRSCRBR_ID)"
              caseInSensitive: true
            - expression: hf_prct_facets_1.SRC_CD = 'FACETS'
            - expression: "hf_prct_vcac_1.NTNL_PROV_ID = Trim(BCBSKCComm.PRSCRBR_ID)"
              caseInSensitive: true
            - expression: hf_prct_vcac_1.SRC_CD = 'VCAC'
            - expression: "NdcLkup.NDC = BCBSKCComm.NDC_DER"
              caseInSensitive: true
            - expression: "Pdxntwk_Lkup.PROV_ID = Trim(BCBSKCComm.PDX_NO)"
              caseInSensitive: true
            - expression: "Pdxntwk_Lkup.PLANQUAL = BCBSKCComm.PLANQUAL[1,2]"
              caseInSensitive: true
            - expression: "Pdxntwk_Lkup.SRXNETWK = BCBSKCComm.SRXNETWK"
              caseInSensitive: true
            - expression: "MailOrderLkup.SRC_CD = BCBSKCComm.PDX_TYP"
              caseInSensitive: true
            - expression: "MailOrderLkup.SRC_SYS_CD = BCBSKCComm.SRC_SYS_CD"
              caseInSensitive: true
            - expression: "hf_dea.DEA_NO = Trim(BCBSKCComm.PRSCRBR_ID)"
              caseInSensitive: true
            - expression: "hf_prct_facets_2.NTNL_PROV_ID = Trim(BCBSKCComm.PRSCRBR_NTNL_PROV_ID)"
              caseInSensitive: true
            - expression: hf_prct_facets_2.SRC_CD = 'FACETS'
            - expression: "hf_prct_vcac_2.NTNL_PROV_ID = Trim(BCBSKCComm.PRSCRBR_NTNL_PROV_ID)"
              caseInSensitive: true
            - expression: hf_prct_vcac_2.SRC_CD = 'VCAC'
            - expression: "hf_npi_min_dea_2.NTNL_PROV_ID = Trim(BCBSKCComm.PRSCRBR_NTNL_PROV_ID)"
              caseInSensitive: true
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "\"I\""
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "\"N\""
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "\"Y\""
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: CurrentDate
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "0"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "0"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: SrcSysCd
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "SrcSysCd : \";\" : BCBSKCComm.CLM_ID"
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "BCBSKCComm.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NDC
              type: decimal
              width: 11
              nullable: N
              map: " If Trim(BCBSKCComm.CMPND_CD) = 2 then '99999999999' else  if (Isnull(BCBSKCComm.NDC) = @TRUE or Len(Trim(BCBSKCComm.NDC)) = 0) then BCBSKCComm.NDC else Trim(BCBSKCComm.NDC)"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: " If BCBSKCComm.SRC_SYS_CD = 'OPTUMRX' then  If Len(Trim(BCBSKCComm.PRSCRBR_ID)) <> 0 and Trim(BCBSKCComm.PRSCRBR_ID) <> \"NA\" and Trim(BCBSKCComm.PRSCRBR_ID) <> \"UNK\" then  If IsNull(hf_dea.PROV_DEA_SK) = @FALSE Then hf_dea.PROV_DEA_SK Else  If Len(Trim(BCBSKCComm.PRSCRBR_NTNL_PROV_ID)) <> 0 and Trim(BCBSKCComm.PRSCRBR_NTNL_PROV_ID) <> \"NA\" and Trim(BCBSKCComm.PRSCRBR_NTNL_PROV_ID) <> \"UNK\" then  If IsNull(hf_prct_facets_2.PROV_DEA_SK) = @FALSE Then hf_prct_facets_2.PROV_DEA_SK Else  If IsNull(hf_prct_vcac_2.PROV_DEA_SK) = @FALSE Then hf_prct_vcac_2.PROV_DEA_SK Else  If IsNull(hf_npi_min_dea_2.PROV_DEA_SK) = @FALSE Then hf_npi_min_dea_2.PROV_DEA_SK Else 0 ELSE 0 ELSE  If Len(Trim(BCBSKCComm.PRSCRBR_NTNL_PROV_ID)) <> 0 and Trim(BCBSKCComm.PRSCRBR_NTNL_PROV_ID) <> \"NA\" and Trim(BCBSKCComm.PRSCRBR_NTNL_PROV_ID) <> \"UNK\" then  If IsNull(hf_prct_facets_2.PROV_DEA_SK) = @FALSE Then hf_prct_facets_2.PROV_DEA_SK else  If IsNull(hf_prct_vcac_2.PROV_DEA_SK) = @FALSE Then hf_prct_vcac_2.PROV_DEA_SK Else  If IsNull(hf_npi_min_dea_2.PROV_DEA_SK) = @FALSE Then hf_npi_min_dea_2.PROV_DEA_SK Else 0 ELSE 0 ELSE  If IsNull(hf_prct_facets_1.PROV_DEA_SK) = @FALSE Then hf_prct_facets_1.PROV_DEA_SK Else  If IsNull(hf_prct_vcac_1.PROV_DEA_SK) = @FALSE Then hf_prct_vcac_1.PROV_DEA_SK Else  If IsNull(hf_npi_min_dea_1.PROV_DEA_SK) = @FALSE Then hf_npi_min_dea_1.PROV_DEA_SK Else 0"
            - name: DRUG_CLM_DAW_CD
              type: varchar
              width: 20
              nullable: N
              map: "BCBSKCComm.DISPENSE_AS_WRTN_PROD_SEL_CD"
            - name: DRUG_CLM_LGL_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: LegalStatus
            - name: DRUG_CLM_TIER_CD
              type: varchar
              width: 20
              nullable: N
              map: ClaimTier
            - name: DRUG_CLM_VNDR_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: CMPND_IN
              type: char
              width: 1
              nullable: N
              map: CmpndIn
            - name: FRMLRY_IN
              type: char
              width: 1
              nullable: N
              map: "BCBSKCComm.FRMLRY_FLAG"
            - name: GNRC_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: " If (Trim(BCBSKCComm.DRUG_TYP) = '' or ISNULL(BCBSKCComm.DRUG_TYP) = @True) then 'U' else  If BCBSKCComm.SRC_SYS_CD <> 'OPTUMRX' and (BCBSKCComm.DRUG_TYP = 2 Or BCBSKCComm.DRUG_TYP = 3) then 'Y' else  If BCBSKCComm.SRC_SYS_CD = 'OPTUMRX' and BCBSKCComm.DRUG_TYP = 3 then 'Y' else 'N'"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: " If IsNull(MailOrderLkup.CD_MPPNG_SK) = @TRUE Then 'N' Else 'Y'"
            - name: MNTN_IN
              type: char
              width: 1
              nullable: N
              map: " If ISNULL(NdcLkup.DRUG_MNTN_IN) = @TRUE then 'U' else NdcLkup.DRUG_MNTN_IN"
            - name: MAC_REDC_IN
              type: char
              width: 1
              nullable: N
              map: " IF (BCBSKCComm.SRC_SYS_CD = 'OPTUMRX') AND Trim(BCBSKCComm.BILL_BSS_CD) = '06' THEN \"Y\" else  IF TRIM(BCBSKCComm.SRC_SYS_CD <> \"OPTUMRX\") AND Trim(BCBSKCComm.BILL_BSS_CD) = '09' THEN \"Y\" else \"N\""
            - name: NON_FRMLRY_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: " If BCBSKCComm.FRMLRY_FLAG = 'Y' then 'N' else 'Y'"
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: " If IsNull(Trim(BCBSKCComm.DRUG_TYP)) = @TRUE Then 'N' Else  If BCBSKCComm.DRUG_TYP = 1 then 'Y' else 'N'"
            - name: ADJ_DT
              type: char
              width: 10
              nullable: N
              map: " If Isnull(BCBSKCComm.ADJDCT_DT) = @TRUE OR len(Trim(BCBSKCComm.ADJDCT_DT)) = 0 then '1753-01-01' else trim(BCBSKCComm.ADJDCT_DT)"
            - name: FILL_DT
              type: char
              width: 10
              nullable: N
              map: " If IsNull(BCBSKCComm.FILL_DT) = @TRUE Or Len(BCBSKCComm.FILL_DT) = 0 then '1753-01-01' else BCBSKCComm.FILL_DT"
            - name: RECON_DT
              type: char
              width: 10
              nullable: N
              map: " If Isnull(BCBSKCComm.ADJDCT_DT) = @TRUE OR len(Trim(BCBSKCComm.ADJDCT_DT)) = 0 then '1753-01-01' else trim(BCBSKCComm.ADJDCT_DT)"
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If Isnull(BCBSKCComm.DISPNS_FEE_AMT) = @TRUE then 0.00 Else BCBSKCComm.DISPNS_FEE_AMT"
            - name: HLTH_PLN_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: HLTH_PLN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If Isnull(BCBSKCComm.INGR_CST_AMT) = @TRUE then 0.00 else BCBSKCComm.INGR_CST_AMT"
            - name: INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If Isnull(BCBSKCComm.INGR_CST_AMT) = @TRUE then 0.00 else BCBSKCComm.INGR_CST_AMT"
            - name: INGR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If (BCBSKCComm.SRC_SYS_CD = 'OPTUMRX') Then  If BCBSKCComm.CMPND_CD <> 2 Then ((BCBSKCComm.INGR_CST_SUB_AMT + Trim(BCBSKCComm.CLNT_GNRL_PRPS_AREA[28, 11])) - (BCBSKCComm.INGR_CST_AMT + BCBSKCComm.DISPNS_FEE_AMT)) Else 0.00 Else 0.00"
            - name: MBR_DEDCT_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If (BCBSKCComm.SRC_SYS_CD = 'OPTUMRX') THEN (BCBSKCComm.CLNT_PATN_PAY_ATRBD_PROD_AMT) ELSE 0.00"
            - name: MBR_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: MBR_OOP_EXCL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: OTHR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: " If Isnull(BCBSKCComm.METRIC_QTY) = @TRUE then 0.000 else BCBSKCComm.METRIC_QTY"
            - name: RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: " If Isnull(BCBSKCComm.METRIC_QTY) = @TRUE then 0.000 else BCBSKCComm.METRIC_QTY"
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If Isnull(BCBSKCComm.SLS_TAX_AMT) = @TRUE then 0.00 else BCBSKCComm.SLS_TAX_AMT"
            - name: RX_ALW_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: " If Isnull(BCBSKCComm.DAYS_SUPL) = @TRUE then 0 else BCBSKCComm.DAYS_SUPL"
            - name: RX_ORIG_DAYS_SUPL_QTY
              type: int
              width: 4
              nullable: N
              map: " If Isnull(BCBSKCComm.DAYS_SUPL) = @TRUE then 0 else BCBSKCComm.DAYS_SUPL"
            - name: PDX_NTWK_ID
              type: varchar
              width: 20
              map: " If BCBSKCComm.SRC_SYS_CD = 'OPTUMRX' Then  If IsNull(Pdxntwk_Lkup.TRGT_CD) = @FALSE then TRIM(Pdxntwk_Lkup.TRGT_CD) else 'NA' Else 'NA'"
            - name: RX_NO
              type: decimal
              width: 9
              map: " If Isnull(BCBSKCComm.RX_NO) = @TRUE or Len(Trim(BCBSKCComm.RX_NO)) = 0 then @NULL else Trim(BCBSKCComm.RX_NO)"
            - name: RFL_NO
              type: varchar
              width: 20
              map: " If Isnull(BCBSKCComm.NEW_OR_RFL_CD) = @TRUE or Len(Trim(BCBSKCComm.NEW_OR_RFL_CD)) = 0 then @NULL else Trim(BCBSKCComm.NEW_OR_RFL_CD)"
            - name: VNDR_CLM_NO
              type: varchar
              width: 20
              map: "BCBSKCComm.REF_NO"
            - name: VNDR_PREAUTH_ID
              type: varchar
              width: 20
              map: " If (IsNull(BCBSKCComm.PRAUTH_NO) = @TRUE Or Len(Trim(BCBSKCComm.PRAUTH_NO)) = 0) Then @NULL Else BCBSKCComm.PRAUTH_NO"
            - name: CLM_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "BCBSKCComm.CLM_STTUS_CD"
            - name: PDX_PROV_ID
              type: char
              width: 12
              nullable: N
              map: "TRIM(BCBSKCComm.PDX_NO)"
            - name: NDC_LABEL_NM
              type: varchar
              width: 35
              nullable: N
              map: "'NA'"
            - name: PRSCRB_NAME
              type: varchar
              width: 75
              nullable: N
              map: " If Len(Trim(BCBSKCComm.PRSCRBR_LAST_NM)) = 0 then \"NA\" Else Trim(BCBSKCComm.PRSCRBR_LAST_NM)"
            - name: PHARMACY_NAME
              type: char
              width: 30
              nullable: N
              map: "@NULL"
            - name: DRUG_CLM_BNF_FRMLRY_POL_CD
              type: char
              width: 1
              nullable: N
              map: "'NA'"
            - name: DRUG_CLM_BNF_RSTRCT_CD
              type: char
              width: 1
              nullable: N
              map: "'NA'"
            - name: DRUG_CLM_MCPARTD_COVDRUG_CD
              type: char
              width: 1
              nullable: N
              map: " If (BCBSKCComm.SRC_SYS_CD = 'MEDIMPACT') Or (BCBSKCComm.SRC_SYS_CD = 'OPTUMRX') Then BCBSKCComm.MCARE_D_COV_DRUG Else 'NA'"
            - name: DRUG_CLM_PRAUTH_CD
              type: char
              width: 1
              nullable: N
              map: "'NA'"
            - name: MNDTRY_MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: ADM_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If Isnull(BCBSKCComm.ADMIN_FEE_AMT) = @TRUE OR len(Trim(BCBSKCComm.ADMIN_FEE_AMT)) = 0 then 0.00 else BCBSKCComm.ADMIN_FEE_AMT"
            - name: DRUG_CLM_BILL_BSS_CD
              type: char
              width: 2
              nullable: N
              map: " If (BCBSKCComm.SRC_SYS_CD = 'OPTUMRX') Then BCBSKCComm.BILL_BSS_CD Else 'NA'"
            - name: AVG_WHLSL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If BCBSKCComm.SRC_SYS_CD = 'OPTUMRX' THEN ( If Isnull(BCBSKCComm.FULL_AVG_WHLSL_PRICE) = @TRUE or Trim(BCBSKCComm.FULL_AVG_WHLSL_PRICE) = '' Then 0.00 Else  If (BCBSKCComm.CLM_TYP = 'R' and BCBSKCComm.FULL_AVG_WHLSL_PRICE > 0) Then (-1 * BCBSKCComm.FULL_AVG_WHLSL_PRICE) Else BCBSKCComm.FULL_AVG_WHLSL_PRICE) ELSE  If BCBSKCComm.SRC_SYS_CD = 'MEDIMPACT' Then BCBSKCComm.FULL_AVG_WHLSL_PRICE Else 0.00"
            - name: NTNL_PROV_ID
              type: char
              width: 10
              map: "BCBSKCComm.PDX_NTNL_PROV_ID"
            - name: PRESCRIBER_ID
              type: char
              width: 15
              nullable: N
              map: " IF (IF ISNULL((TRIM(BCBSKCComm.PRSCRBR_ID))) THEN \"\" ELSE (TRIM(BCBSKCComm.PRSCRBR_ID))) = \"\" THEN \"NA\" ELSE TRIM(BCBSKCComm.PRSCRBR_ID)"
            - name: UCR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If (BCBSKCComm.SRC_SYS_CD = 'OPTUMRX') AND len(Trim(BCBSKCComm.USL_AND_CUST_CHRG_AMT)) = 0 THEN 0.00 Else BCBSKCComm.USL_AND_CUST_CHRG_AMT"
            - name: INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If (Isnull(BCBSKCComm.INCNTV_FEE_AMT) = @TRUE or len(Trim(BCBSKCComm.INCNTV_FEE_AMT))) = 0 then 0.00 else BCBSKCComm.INCNTV_FEE_AMT"
            - name: ORIG_MBR
              type: varchar
              width: 20
              map: "BCBSKCComm.ORIG_MBR"
            - name: PDX_TYP
              type: char
              width: 1
              nullable: N
              map: " If (BCBSKCComm.SRC_SYS_CD = 'OPTUMRX') Then BCBSKCComm.PDX_TYP Else 'NA'"
            - name: PRTL_FILL_STTUS_CD
              type: char
              width: 1
              nullable: N
              map: " If (BCBSKCComm.SRC_SYS_CD = 'OPTUMRX') Then SVPrtlFillSttusCd Else 'NA'"
            - name: PRSCRBR_QLFR
              type: char
              width: 2
              map: "BCBSKCComm.PRSCRBR_QLFR"
            - name: PRSCRBR_NTNL_PROV_ID
              type: varchar
              width: 20
              map: "BCBSKCComm.PRSCRBR_NTNL_PROV_ID"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              map: "BCBSKCComm.PDX_NTNL_PROV_ID"
            - name: SPEC_DRUG_IN
              type: char
              width: 1
              map: "BCBSKCComm.SPEC_DRUG_IN"
            - name: SUBMT_PROD_ID_QLFR
              type: varchar
              width: 2
              map: "BCBSKCComm.SUBMT_PROD_ID_QLFR"
            - name: CNTNGNT_THER_FLAG
              type: varchar
              width: 1
              map: "BCBSKCComm.CNTNGNT_THER_FLAG"
            - name: CNTNGNT_THER_SCHD
              type: varchar
              width: 20
              map: "BCBSKCComm.CNTNGNT_THER_SCHD"
            - name: CLNT_PATN_PAY_ATRBD_PROD_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (BCBSKCComm.SRC_SYS_CD = 'OPTUMRX') THEN (BCBSKCComm.CLNT_PATN_PAY_ATRBD_PROD_AMT) ELSE 0.00"
            - name: CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (BCBSKCComm.SRC_SYS_CD = 'OPTUMRX') THEN (BCBSKCComm.CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT) ELSE 0.00"
            - name: CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (BCBSKCComm.SRC_SYS_CD = 'OPTUMRX') THEN (BCBSKCComm.CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT) ELSE 0.00"
            - name: CLNT_PATN_PAY_ATRBD_NTWK_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If (BCBSKCComm.SRC_SYS_CD = 'OPTUMRX') THEN (BCBSKCComm.CLNT_PATN_PAY_ATRBD_NTWK_AMT) ELSE 0.00"
            - name: GNRC_PROD_IN
              type: varchar
              width: 14
              map: " If (BCBSKCComm.SRC_SYS_CD = 'OPTUMRX') then BCBSKCComm.GNRC_PROD_IN else 'NA'"
        - name: Transformer_230.V0S230P2
          type: map
          source: Transformer_230.V0S230P2.filter
          alias: PROV_DEA
          schema: 
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "PROV_DEA.PROV_DEA_SK"
            - name: DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "PROV_DEA.DEA_NO"
        - name: Transformer_230.V0S230P2.filter
          type: filter
          source: IDS_DEA
          filter: "(PROV_DEA_SK <> 0 And PROV_DEA_SK <> 1)"
          schema: 
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
            - name: DEA_NO
              type: varchar
              width: 20
              nullable: N
          keys:
            - PROV_DEA_SK
        - name: Transformer_230.V0S230P3
          type: map
          source: Transformer_230.V0S230P3.filter
          alias: PROV_DEA
          schema: 
            - name: DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "PROV_DEA.DEA_NO"
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "PROV_DEA.PROV_DEA_SK"
        - name: Transformer_230.V0S230P3.filter
          type: filter
          source: IDS_DEA
          filter: "Len(trim(DEA_NO)) > 1"
          schema: 
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
            - name: DEA_NO
              type: varchar
              width: 20
              nullable: N
          keys:
            - PROV_DEA_SK
        - name: DataPrep
          type: map
          source: BCBSKCCommClmLand
          alias: inDataPrep
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              map: "inDataPrep.SRC_SYS_CD"
            - name: FILE_RCVD_DT
              type: char
              width: 10
              map: "inDataPrep.FILE_RCVD_DT"
            - name: RCRD_ID
              type: decimal
              width: 2
              map: "inDataPrep.RCRD_ID"
            - name: PRCSR_NO
              type: varchar
              width: 10
              map: "inDataPrep.PRCSR_NO"
            - name: BTCH_NO
              type: decimal
              width: 5
              map: "inDataPrep.BTCH_NO"
            - name: PDX_NO
              type: varchar
              width: 20
              map: "inDataPrep.PDX_NO"
            - name: RX_NO
              type: decimal
              width: 9
              map: "inDataPrep.RX_NO"
            - name: FILL_DT
              type: char
              width: 10
              map: "inDataPrep.FILL_DT"
            - name: NDC
              type: char
              width: 20
              map: "inDataPrep.NDC"
            - name: DRUG_DESC
              type: varchar
              width: 30
              map: "inDataPrep.DRUG_DESC"
            - name: NEW_OR_RFL_CD
              type: int
              width: 4
              map: "inDataPrep.NEW_OR_RFL_CD"
            - name: METRIC_QTY
              type: decimal
              width: 10
              precision: 3
              map: "inDataPrep.METRIC_QTY"
            - name: DAYS_SUPL
              type: decimal
              width: 3
              map: "inDataPrep.DAYS_SUPL"
            - name: BSS_OF_CST_DTRM
              type: char
              width: 2
              map: "inDataPrep.BSS_OF_CST_DTRM"
            - name: INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.INGR_CST_AMT"
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.DISPNS_FEE_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.COPAY_AMT"
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.SLS_TAX_AMT"
            - name: BILL_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.BILL_AMT"
            - name: PATN_FIRST_NM
              type: varchar
              width: 35
              map: "inDataPrep.PATN_FIRST_NM"
            - name: PATN_LAST_NM
              type: varchar
              width: 75
              map: "inDataPrep.PATN_LAST_NM"
            - name: BRTH_DT
              type: char
              width: 10
              map: "inDataPrep.BRTH_DT"
            - name: SEX_CD
              type: decimal
              width: 1
              map: "inDataPrep.SEX_CD"
            - name: CARDHLDR_ID_NO
              type: varchar
              width: 20
              map: "inDataPrep.CARDHLDR_ID_NO"
            - name: RELSHP_CD
              type: decimal
              width: 1
              map: "inDataPrep.RELSHP_CD"
            - name: GRP_NO
              type: varchar
              width: 20
              map: "inDataPrep.GRP_NO"
            - name: HOME_PLN
              type: varchar
              width: 3
              map: "inDataPrep.HOME_PLN"
            - name: HOST_PLN
              type: varchar
              width: 3
              map: "inDataPrep.HOST_PLN"
            - name: PRSCRBR_ID
              type: varchar
              width: 20
              map: "inDataPrep.PRSCRBR_ID"
            - name: DIAG_CD
              type: varchar
              width: 20
              map: "inDataPrep.DIAG_CD"
            - name: CARDHLDR_FIRST_NM
              type: varchar
              width: 35
              map: "inDataPrep.CARDHLDR_FIRST_NM"
            - name: CARDHLDR_LAST_NM
              type: varchar
              width: 75
              map: "inDataPrep.CARDHLDR_LAST_NM"
            - name: PRAUTH_NO
              type: char
              width: 12
              map: "inDataPrep.PRAUTH_NO"
            - name: PA_MC_SC_NO
              type: varchar
              width: 7
              map: "inDataPrep.PA_MC_SC_NO"
            - name: CUST_LOC
              type: decimal
              width: 2
              map: "inDataPrep.CUST_LOC"
            - name: RESUB_CYC_CT
              type: decimal
              width: 2
              map: "inDataPrep.RESUB_CYC_CT"
            - name: RX_DT
              type: char
              width: 10
              map: "inDataPrep.RX_DT"
            - name: DISPENSE_AS_WRTN_PROD_SEL_CD
              type: char
              width: 1
              map: "inDataPrep.DISPENSE_AS_WRTN_PROD_SEL_CD"
            - name: PRSN_CD
              type: varchar
              width: 3
              map: "inDataPrep.PRSN_CD"
            - name: OTHR_COV_CD
              type: decimal
              width: 2
              map: "inDataPrep.OTHR_COV_CD"
            - name: ELIG_CLRFCTN_CD
              type: decimal
              width: 1
              map: "inDataPrep.ELIG_CLRFCTN_CD"
            - name: CMPND_CD
              type: decimal
              width: 1
              map: "inDataPrep.CMPND_CD"
            - name: NO_OF_RFLS_AUTH
              type: decimal
              width: 2
              map: "inDataPrep.NO_OF_RFLS_AUTH"
            - name: LVL_OF_SVC
              type: decimal
              width: 2
              map: "inDataPrep.LVL_OF_SVC"
            - name: RX_ORIG_CD
              type: decimal
              width: 1
              map: "inDataPrep.RX_ORIG_CD"
            - name: RX_DENIAL_CLRFCTN
              type: decimal
              width: 2
              map: "inDataPrep.RX_DENIAL_CLRFCTN"
            - name: PRI_PRSCRBR
              type: varchar
              width: 20
              map: "inDataPrep.PRI_PRSCRBR"
            - name: CLNC_ID_NO
              type: decimal
              width: 5
              map: "inDataPrep.CLNC_ID_NO"
            - name: DRUG_TYP
              type: decimal
              width: 1
              map: "inDataPrep.DRUG_TYP"
            - name: PRSCRBR_LAST_NM
              type: varchar
              width: 75
              map: "inDataPrep.PRSCRBR_LAST_NM"
            - name: POSTAGE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.POSTAGE_AMT"
            - name: UNIT_DOSE_IN
              type: decimal
              width: 1
              map: "inDataPrep.UNIT_DOSE_IN"
            - name: OTHR_PAYOR_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.OTHR_PAYOR_AMT"
            - name: BSS_OF_DAYS_SUPL_DTRM
              type: decimal
              width: 1
              map: "inDataPrep.BSS_OF_DAYS_SUPL_DTRM"
            - name: FULL_AVG_WHLSL_PRICE
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.FULL_AVG_WHLSL_PRICE"
            - name: EXPNSN_AREA
              type: char
              width: 1
              map: "inDataPrep.EXPNSN_AREA"
            - name: MSTR_CAR
              type: varchar
              width: 20
              map: "inDataPrep.MSTR_CAR"
            - name: SUBCAR
              type: varchar
              width: 20
              map: "inDataPrep.SUBCAR"
            - name: CLM_TYP
              type: char
              width: 1
              map: "inDataPrep.CLM_TYP"
            - name: SUBGRP
              type: varchar
              width: 20
              map: "inDataPrep.SUBGRP"
            - name: PLN_DSGNR
              type: char
              width: 1
              map: "inDataPrep.PLN_DSGNR"
            - name: ADJDCT_DT
              type: char
              width: 10
              map: "inDataPrep.ADJDCT_DT"
            - name: ADMIN_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.ADMIN_FEE_AMT"
            - name: CAP_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.CAP_AMT"
            - name: INGR_CST_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.INGR_CST_SUB_AMT"
            - name: MBR_NON_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.MBR_NON_COPAY_AMT"
            - name: MBR_PAY_CD
              type: char
              width: 2
              map: "inDataPrep.MBR_PAY_CD"
            - name: INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.INCNTV_FEE_AMT"
            - name: CLM_ADJ_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.CLM_ADJ_AMT"
            - name: CLM_ADJ_CD
              type: char
              width: 2
              map: "inDataPrep.CLM_ADJ_CD"
            - name: FRMLRY_FLAG
              type: char
              width: 1
              map: "inDataPrep.FRMLRY_FLAG"
            - name: GNRC_CLS_NO
              type: varchar
              width: 20
              map: "inDataPrep.GNRC_CLS_NO"
            - name: THRPTC_CLS_AHFS
              type: varchar
              width: 20
              map: "inDataPrep.THRPTC_CLS_AHFS"
            - name: PDX_TYP
              type: char
              width: 3
              map: "inDataPrep.PDX_TYP"
            - name: BILL_BSS_CD
              type: char
              width: 2
              map: "inDataPrep.BILL_BSS_CD"
            - name: USL_AND_CUST_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.USL_AND_CUST_CHRG_AMT"
            - name: PD_DT
              type: char
              width: 10
              map: "inDataPrep.PD_DT"
            - name: BNF_CD
              type: varchar
              width: 10
              map: "inDataPrep.BNF_CD"
            - name: DRUG_STRG
              type: varchar
              width: 20
              map: "inDataPrep.DRUG_STRG"
            - name: ORIG_MBR
              type: varchar
              width: 20
              map: "inDataPrep.ORIG_MBR"
            - name: INJRY_DT
              type: char
              width: 10
              map: "inDataPrep.INJRY_DT"
            - name: FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.FEE_AMT"
            - name: REF_NO
              type: varchar
              width: 20
              map: "inDataPrep.REF_NO"
            - name: CLNT_CUST_ID
              type: varchar
              width: 20
              map: "inDataPrep.CLNT_CUST_ID"
            - name: PLN_TYP
              type: varchar
              width: 20
              map: "inDataPrep.PLN_TYP"
            - name: ADJDCT_REF_NO
              type: decimal
              width: 9
              map: "inDataPrep.ADJDCT_REF_NO"
            - name: ANCLRY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.ANCLRY_AMT"
            - name: CLNT_GNRL_PRPS_AREA
              type: varchar
              width: 40
              map: "inDataPrep.CLNT_GNRL_PRPS_AREA"
            - name: PRTL_FILL_STTUS_CD
              type: char
              width: 1
              map: "inDataPrep.PRTL_FILL_STTUS_CD"
            - name: BILL_DT
              type: char
              width: 10
              map: "inDataPrep.BILL_DT"
            - name: FSA_VNDR_CD
              type: varchar
              width: 20
              map: "inDataPrep.FSA_VNDR_CD"
            - name: PICA_DRUG_CD
              type: char
              width: 1
              map: "inDataPrep.PICA_DRUG_CD"
            - name: CLM_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.CLM_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.DSALW_AMT"
            - name: FED_DRUG_CLS_CD
              type: char
              width: 1
              map: "inDataPrep.FED_DRUG_CLS_CD"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.DEDCT_AMT"
            - name: BNF_COPAY_100
              type: char
              width: 1
              map: "inDataPrep.BNF_COPAY_100"
            - name: CLM_PRCS_TYP
              type: char
              width: 1
              map: "inDataPrep.CLM_PRCS_TYP"
            - name: INDEM_HIER_TIER_NO
              type: decimal
              width: 4
              map: "inDataPrep.INDEM_HIER_TIER_NO"
            - name: MCARE_D_COV_DRUG
              type: char
              width: 1
              map: "inDataPrep.MCARE_D_COV_DRUG"
            - name: RETRO_LICS_CD
              type: char
              width: 1
              map: "inDataPrep.RETRO_LICS_CD"
            - name: RETRO_LICS_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.RETRO_LICS_AMT"
            - name: LICS_SBSDY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.LICS_SBSDY_AMT"
            - name: MCARE_B_DRUG
              type: char
              width: 1
              map: "inDataPrep.MCARE_B_DRUG"
            - name: MCARE_B_CLM
              type: char
              width: 1
              map: "inDataPrep.MCARE_B_CLM"
            - name: PRSCRBR_QLFR
              type: char
              width: 2
              map: "inDataPrep.PRSCRBR_QLFR"
            - name: PRSCRBR_NTNL_PROV_ID
              type: varchar
              width: 20
              map: "inDataPrep.PRSCRBR_NTNL_PROV_ID"
            - name: PDX_QLFR
              type: char
              width: 2
              map: "inDataPrep.PDX_QLFR"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              map: "inDataPrep.PDX_NTNL_PROV_ID"
            - name: HLTH_RMBRMT_ARGMT_APLD_AMT
              type: decimal
              width: 11
              map: "inDataPrep.HLTH_RMBRMT_ARGMT_APLD_AMT"
            - name: THER_CLS
              type: decimal
              width: 6
              map: "inDataPrep.THER_CLS"
            - name: HIC_NO
              type: varchar
              width: 20
              map: "inDataPrep.HIC_NO"
            - name: HLTH_RMBRMT_ARGMT_FLAG
              type: char
              width: 1
              map: "inDataPrep.HLTH_RMBRMT_ARGMT_FLAG"
            - name: DOSE_CD
              type: decimal
              width: 4
              map: "inDataPrep.DOSE_CD"
            - name: LOW_INCM
              type: char
              width: 1
              map: "inDataPrep.LOW_INCM"
            - name: RTE_OF_ADMIN
              type: char
              width: 2
              map: "inDataPrep.RTE_OF_ADMIN"
            - name: DEA_SCHD
              type: decimal
              width: 1
              map: "inDataPrep.DEA_SCHD"
            - name: COPAY_BNF_OPT
              type: decimal
              width: 10
              map: "inDataPrep.COPAY_BNF_OPT"
            - name: GNRC_PROD_IN
              type: varchar
              width: 14
              map: "inDataPrep.GNRC_PROD_IN"
            - name: PRSCRBR_SPEC
              type: varchar
              width: 20
              map: "inDataPrep.PRSCRBR_SPEC"
            - name: VAL_CD
              type: varchar
              width: 20
              map: "inDataPrep.VAL_CD"
            - name: PRI_CARE_PDX
              type: varchar
              width: 20
              map: "inDataPrep.PRI_CARE_PDX"
            - name: OFC_OF_INSPECTOR_GNRL
              type: char
              width: 1
              map: "inDataPrep.OFC_OF_INSPECTOR_GNRL"
            - name: PATN_SSN
              type: varchar
              width: 11
              map: "inDataPrep.PATN_SSN"
            - name: CARDHLDR_SSN
              type: varchar
              width: 11
              map: "inDataPrep.CARDHLDR_SSN"
            - name: CARDHLDR_BRTH_DT
              type: varchar
              width: 10
              map: "inDataPrep.CARDHLDR_BRTH_DT"
            - name: CARDHLDR_ADDR
              type: varchar
              width: 40
              map: "inDataPrep.CARDHLDR_ADDR"
            - name: CARDHLDR_CITY
              type: varchar
              width: 35
              map: "inDataPrep.CARDHLDR_CITY"
            - name: CHADHLDR_ST
              type: varchar
              width: 2
              map: "inDataPrep.CHADHLDR_ST"
            - name: CARDHLDR_ZIP_CD
              type: varchar
              width: 5
              map: "inDataPrep.CARDHLDR_ZIP_CD"
            - name: PSL_FMLY_MET_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.PSL_FMLY_MET_AMT"
            - name: PSL_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.PSL_MBR_MET_AMT"
            - name: PSL_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.PSL_FMLY_AMT"
            - name: DEDCT_FMLY_MET_AMT
              type: varchar
              width: 20
              map: "inDataPrep.DEDCT_FMLY_MET_AMT"
            - name: DEDCT_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.DEDCT_FMLY_AMT"
            - name: MOPS_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.MOPS_FMLY_AMT"
            - name: MOPS_FMLY_MET_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.MOPS_FMLY_MET_AMT"
            - name: MOPS_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.MOPS_MBR_MET_AMT"
            - name: DEDCT_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.DEDCT_MBR_MET_AMT"
            - name: PSL_APLD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.PSL_APLD_AMT"
            - name: MOPS_APLD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.MOPS_APLD_AMT"
            - name: PAR_PDX_IN
              type: char
              width: 1
              map: "inDataPrep.PAR_PDX_IN"
            - name: COPAY_PCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.COPAY_PCT_AMT"
            - name: COPAY_FLAT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.COPAY_FLAT_AMT"
            - name: CLM_TRNSMSN_METH
              type: char
              width: 1
              map: "inDataPrep.CLM_TRNSMSN_METH"
            - name: RX_NO_2012
              type: decimal
              width: 12
              map: "inDataPrep.RX_NO_2012"
            - name: CLM_ID
              type: varchar
              width: 20
              map: "inDataPrep.CLM_ID"
            - name: CLM_STTUS_CD
              type: varchar
              width: 2
              map: "inDataPrep.CLM_STTUS_CD"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              map: "inDataPrep.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              map: "inDataPrep.ADJ_TO_CLM_ID"
            - name: PLANQUAL
              type: varchar
              width: 10
              map: "Trim(inDataPrep.CLNT_GNRL_PRPS_AREA[51, 10])"
            - name: SRXNETWK
              type: varchar
              width: 6
              map: " If (IsNull(Trim(inDataPrep.CLNT_GNRL_PRPS_AREA[39, 6])) = @TRUE Or Len(Trim(inDataPrep.CLNT_GNRL_PRPS_AREA[39, 6])) = 0) Then Trim(inDataPrep.CLNT_GNRL_PRPS_AREA[45, 6]) Else Trim(inDataPrep.CLNT_GNRL_PRPS_AREA[39, 6])"
            - name: NDC_DER
              type: varchar
              width: 20
              map: " If Trim(inDataPrep.CMPND_CD) = 2 then '99999999999' else inDataPrep.NDC"
            - name: SPEC_DRUG_IN
              type: char
              width: 1
              map: " If inDataPrep.SRC_SYS_CD = \"OPTUMRX\" Then inDataPrep.CLNT_GNRL_PRPS_AREA[64, 1] Else \"N\""
            - name: SUBMT_PROD_ID_QLFR
              type: varchar
              width: 2
              map: "inDataPrep.SUBMT_PROD_ID_QLFR"
            - name: CNTNGNT_THER_FLAG
              type: varchar
              width: 1
              map: "inDataPrep.CNTNGNT_THER_FLAG"
            - name: CNTNGNT_THER_SCHD
              type: varchar
              width: 20
              map: "inDataPrep.CNTNGNT_THER_SCHD"
            - name: CLNT_PATN_PAY_ATRBD_PROD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.CLNT_PATN_PAY_ATRBD_PROD_AMT"
            - name: CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"
            - name: CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"
            - name: CLNT_PATN_PAY_ATRBD_NTWK_AMT
              type: decimal
              width: 13
              precision: 2
              map: "inDataPrep.CLNT_PATN_PAY_ATRBD_NTWK_AMT"
            - name: LOB_IN
              type: varchar
              width: 5
              map: "inDataPrep.LOB_IN"
        - name: DrugClmPK
          type: sharedContainer
          containerName: DrugClmPK
          parameters:
            - name: "FilePath"
              expression: "#$FilePath#"
            - name: "RunCycle"
              expression: "#RunCycle#"
            - name: "CactusServer"
              expression: "#$CactusServer#"
            - name: "CactusOwner"
              expression: "#$CactusOwner#"
            - name: "CactusAcct"
              expression: "#$CactusAcct#"
            - name: "CactusPW"
              expression: "#$CactusPW#"
            - name: "IDSDB"
              expression: "#$IDSDB#"
            - name: "IDSOwner"
              expression: "#$IDSOwner#"
            - name: "IDSAcct"
              expression: "#$IDSAcct#"
            - name: "IDSPW"
              expression: "#$IDSPW#"
            - name: "ProvRunCycle"
              expression: "#ProvRunCycle#"
          source: Snapshot.V0S135P2


