model:
  config:
    origin: datastage
  jobs:
    - name: BCBSKCCommClmLnPKExtr
      origin: datastage
      type: dataflow
      folder: "claim/ClaimLine/ClmLn/BCBSKCCommon"
      description: "BCBS Comman Claim Line SK Assignment"
      fullDescription: "© Copyright 2010 Blue Cross/Blue Shield of Kansas City\n\n\nCALLED BY:  \n               \n\nPROCESSING:   \n         *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  Adjusted to/from claims don't need to be in hash file. \n         *  The primary key hash file hf_clm is the output of this job and is used by the following tables for keying\n             CLM\n             DRUG_CLM\n             CLM_REMIT_HIST\n\n\nMODIFICATIONS:\nDeveloper                       Date                 Project/Altiris #\x09Change Description\x09\x09\x09\x09   Development Project\x09Code Reviewer\x09Date Reviewed       \n------------------                    --------------------     \x09------------------------\x09-----------------------------------------------------------------------\x09    --------------------------------\x09-------------------------------\x09----------------------------       \nKaushik Kapoor             2018-01-08              5828                  Original Programming                                                   IntegrateDev2                        Kalyan Neelam        2018-02-26 \nRekha Radhakrishna    2020-08-13              6131                 Modified common file layout to include new 8 fields      IntegrateDev2                     Sravya Gorla               2020-09-12\n                                                                                               ( field 151 - 158)"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: SrcSysCd
          type: string
          description: "Source System Code"
          defaultType: expr
          defaultValue: MEDTRAK
        - name: SrcSysCdSK
          type: integer
          description: "Source System SK"
        - name: CurrRunCycle
          type: integer
          description: "Current Run Cycle"
          defaultType: expr
          defaultValue: "100"
        - name: RunID
          type: integer
          description: "Run ID"
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
      afterSubr:
        subrType: "DSU.HASH.CLEAR\\\\2;hf_clm_ln_pk_lkup;hf_BCBSKCCommClmLnPkExtr"
        subr: "2;hf_clm_ln_pk_lkup;hf_BCBSKCCommClmLnPkExtr"
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
              type: decimal
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
              type: decimal
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
        - name: hf_clm_ln_pk_lkup
          type: dbWriter
          originalDb: hashedFile
          table: "/hf_clm_ln_pk_lkup"
          tableAction: append
          source: ClmLnLoadPK
            # Hashed File has no directory specified
        - name: hf_BCBSKCCommClmLnPkExtr
          type: map
          source: Collector
          alias: Transform
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_ID"
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
              map: "Transform.CLM_LN_SEQ_NO"
        - name: Trans1.V145S3P2
          type: map
          source: BCBSKCCommClmLand
          alias: BCBSKCComm
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSK
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "BCBSKCComm.CLM_ID"
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
              map: "1"
          keys:
            - SRC_SYS_CD_SK
            - CLM_ID
            - CLM_LN_SEQ_NO
        - name: Trans1.V145S3P3
          type: map
          source: Trans1.V145S3P3.filter
          alias: BCBSKCComm
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSK
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "BCBSKCComm.CLM_ID[1, len(trim(BCBSKCComm.CLM_ID)) - 1]"
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
              map: "1"
        - name: Trans1.V145S3P3.filter
          type: filter
          source: BCBSKCCommClmLand
          filter: "CLM_TYP[1, 1] = 'X' Or CLM_TYP[1, 1] = 'R'"
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
              type: decimal
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
              type: decimal
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
        - name: Collector
          type: unknown
          source: hf_clm_ln_pk_lkup
          alias: 
          origType: CCollector
        - name: ClmLnLoadPK
          type: sharedContainer
          containerName: ClmLnLoadPK
          parameters:
            - name: "FilePath"
              expression: "#$FilePath#"
            - name: "SrcSysCd"
              expression: "#SrcSysCd#"
            - name: "SrcSysCdSK"
              expression: "#SrcSysCdSK#"
            - name: "CurrRunCycle"
              expression: "#CurrRunCycle#"
            - name: "IDSDB"
              expression: "#$IDSDB#"
            - name: "IDSOwner"
              expression: "#$IDSOwner#"
            - name: "IDSAcct"
              expression: "#$IDSAcct#"
            - name: "IDSPW"
              expression: "#$IDSPW#"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_SEQ_NO
              type: int
              width: 4
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD
            - CLM_ID
            - CLM_LN_SEQ_NO
          source: hf_BCBSKCCommClmLnPkExtr


