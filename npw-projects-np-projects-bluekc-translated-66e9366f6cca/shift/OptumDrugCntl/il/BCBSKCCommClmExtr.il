model:
  config:
    origin: datastage
  jobs:
    - name: BCBSKCCommClmExtr
      origin: datastage
      type: dataflow
      folder: "claim/claim/BCBSKCCommon"
      runAfterSubrOnJobSuccess: enabled
      description: "BCBSKC Common Claim Extract"
      fullDescription: "COPYRIGHT 2017 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\nCALLED BY:  BCBSKCCommDrugExtrSeq\n\nDESCRIPTION:  Reads the PDX_CLM_STD_INPT_Land.dat file and puts the data into the claim common record format and takes it through primary key process using Shared Container ClmPkey\n    \nMODIFICATIONS:\n\nDeveloper                      Date                Project/Altiris #                  Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       \n----------------------------------    -------------------     -------------------------------------     ------------------------------------------------------------------------------------------------    --------------------------------       -------------------------------   ----------------------------       \nKaushik Kapoor             2017-12-01      5828                                  Initial Programming                                                                       IntegrateDev2                Kalyan Neelam          2018-02-26 \nMohan Karnati               2019-06-06      ADO-73034                       Adding CLM_TXNMY_CD filed in alpha_pfx stage and                IntegrateDev1\x09   Kalyan Neelam          2019-07-01\n                                                                                                        passing it till BCBSKCCommClmExtr stage\nSrinivas Garimella          2019-10-16      6131 PBM replacement     changed business rules for fields PD_DT,ALW_AMT,                 IntegrateDev1     \n                                                                                                        COINS_AMT,CNSD_CHRG_AMT,COPAY_AMT,CHRG_AMT,\nSrinivas Garimella          2019-10-26      6131 PBM replacement     changed business rules for fields                                                  IntegrateDev2\n                                                                                                        CLM_INPT_SRC_CD_SK. CLM_COB_CD\nSrinivas Garimella          2019-10-26      6131 PBM replacement     changed business rules for field                                                   IntegrateDev2                Kalyan Neelam          2019-11-20\n                                                                                                        ADJ_FROM_CLM_ID\nRekha Radhakrishna    2020-02-27      6131                                  Changed ALW_AMT calculation for OPTUMRX                         IntegrateDev2                 Hugh Sisson             2020-03-06\n\nRamu                            2020-03-13      6131 PBM replacement      Mapped 63rd char value of Client General Purpose Area  to      IntegrateDev2                 Kalyan Neelam         2020-04-09\n                                                                                                            coulmn to BILL_PAYMT_EXCL_IN\n\nGoutham Kalidindi        10/22/2020       US-283560          Added MEDIMPACT derivation for   ALLWD_AMT                                   IntegrateDev2               Kalyan Neelam         2020-11-10\n\n\nGoutham Kalidindi        11/11/2020       US-283560                           Changed MEDIMPACT derivation for   ALLWD_AMT                 IntegrateDev2            Kalyan Neelam      2020-11-12    \n                                                                                                           Added MEDIMPACT derivation for CNSD_CHRG_AMT\n                                                                                                           CHRG_AMT  & CLM_COB_CD\n\n\n\n                                                                                                                \nSri Nannapaneni           2020-07-15      6131 PBM replacement      Added 7 new fields to source seq file                                          IntegrateDev2               Sravya Gorla            2020-12-09             \nSri Nannapaneni           08/13/2020    6131- PBM Replacement    Added LOB_IN field to source seq file                                          IntegrateDev2                Sravya Gorla            2020-12-09  \n\nGoutham Kalidindi        01-20-2021       US-341803                      Column derivation change on PAYMT_REF_ID                             IntegrateDev2            Kalyan Neelam          2021-01-21\n                                                                                                      for MedImpact claims ONLY"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
        - name: IDSInstance
          type: string
          description: "IDS Instance"
          defaultType: project
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
        - name: RunCycle
          type: integer
          description: "current run cycle"
          defaultType: expr
          defaultValue: "1"
        - name: RunID
          type: string
          description: "Run ID"
          defaultType: expr
          defaultValue: "100"
        - name: CurrentDate
          type: string
          description: "Current Date"
        - name: SrcSysCd
          type: string
          description: "Source System Code"
        - name: SrcSysCdSk
          type: string
          description: "Source System Code Surrogate Key"
      afterSubr:
        subrType: "DSU.HASH.CLEAR\\\\3;hf_bcbskccommon_clm_mbr;hf_bcbskccommon_clm_sub_alpha_pfx;hf_bcbskccommon_clm_mbr_enr"
        subr: "3;hf_bcbskccommon_clm_mbr;hf_bcbskccommon_clm_sub_alpha_pfx;hf_bcbskccommon_clm_mbr_enr"
      steps:
        - name: BCBSCommClmLand
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
        - name: BCBSKCCommClmExtr
          type: fileWriter
          urls: 
            - "#$FilePath#/key/BCBSKCCommClmExtr_#SrcSysCd#.DrugClm.dat.#RunID#"
          source: ClmPK
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: B_CLM
          type: fileWriter
          urls: 
            - "#$FilePath#/load/B_CLM.#SrcSysCd#.dat.#RunID#"
          source: Transformer
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: Hash.mbr_lkup
          type: map
          source: alpha_pfxBusinessRulesBCBSKCCommClmExtrB_CLM
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: ".MBR_UNIQ_KEY"
            - name: SUB_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: ".SUB_UNIQ_KEY"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: ".MBR_SFX_NO"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: ".SUB_ID"
        - name: Hash.sub_alpha_pfx_lkup
          type: map
          source: alpha_pfxBusinessRulesBCBSKCCommClmExtrB_CLM
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: SUB_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: ".SUB_UNIQ_KEY"
            - name: ALPHA_PFX_CD
              type: varchar
              width: 20
              map: ".ALPHA_PFX_CD"
        - name: Hash.mbr_enr_lkup
          type: map
          source: alpha_pfxBusinessRulesBCBSKCCommClmExtrB_CLM
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: ".CLM_ID"
            - name: CLS_ID
              type: varchar
              width: 20
              nullable: N
              map: ".CLS_ID"
            - name: CLS_PLN_ID
              type: varchar
              width: 20
              nullable: N
              map: ".CLS_PLN_ID"
            - name: SUBGRP_ID
              type: varchar
              width: 20
              nullable: N
              map: ".SUBGRP_ID"
            - name: EXPRNC_CAT_CD
              type: varchar
              width: 20
              nullable: N
              map: ".EXPRNC_CAT_CD"
            - name: FNCL_LOB_CD
              type: varchar
              width: 20
              nullable: N
              map: ".FNCL_LOB_CD"
            - name: PROD_ID
              type: varchar
              width: 20
              nullable: N
              map: ".PROD_ID"
        - name: ids
          type: dbReader
          originalDb: db2
        - name: alpha_pfx
          type: lookup
          option:
            joinType: left
          sources: 
            - name: BusinessRules
              alias: BCBSKCCommClmTrns
              joinFilter: ""
              joinExpression: []
            - name: Hash.sub_alpha_pfx_lkup
              alias: sub_alpha_pfx_lkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "sub_alpha_pfx_lkup.SUB_UNIQ_KEY"
                toExpr: "BCBSKCCommClmTrns.SUB_CK"
                caseInSensitive: true
            - name: Hash.mbr_enr_lkup
              alias: mbr_enr_lkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "mbr_enr_lkup.CLM_ID"
                toExpr: "BCBSKCCommClmTrns.CLM_ID"
                caseInSensitive: true
          joinCondition: 
            - expression: "sub_alpha_pfx_lkup.SUB_UNIQ_KEY = BCBSKCCommClmTrns.SUB_CK"
              caseInSensitive: true
            - expression: "mbr_enr_lkup.CLM_ID = BCBSKCCommClmTrns.CLM_ID"
              caseInSensitive: true
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "BCBSKCCommClmTrns.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "BCBSKCCommClmTrns.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "BCBSKCCommClmTrns.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "BCBSKCCommClmTrns.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "BCBSKCCommClmTrns.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "BCBSKCCommClmTrns.PRI_KEY_STRING"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "BCBSKCCommClmTrns.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "BCBSKCCommClmTrns.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "BCBSKCCommClmTrns.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "BCBSKCCommClmTrns.ADJ_FROM_CLM"
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "BCBSKCCommClmTrns.ADJ_TO_CLM"
            - name: ALPHA_PFX_CD
              type: char
              width: 3
              map: " If IsNull(sub_alpha_pfx_lkup.SUB_UNIQ_KEY) = @TRUE then 'UNK' else sub_alpha_pfx_lkup.ALPHA_PFX_CD"
            - name: CLM_EOB_EXCD
              type: char
              width: 3
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_EOB_EXCD"
            - name: CLS
              type: char
              width: 4
              nullable: N
              map: " If IsNull(mbr_enr_lkup.CLM_ID) = @TRUE then 'UNK' else mbr_enr_lkup.CLS_ID"
            - name: CLS_PLN
              type: char
              width: 8
              nullable: N
              map: " If IsNull(mbr_enr_lkup.CLM_ID) = @TRUE then 'UNK' else mbr_enr_lkup.CLS_PLN_ID"
            - name: EXPRNC_CAT
              type: char
              width: 4
              nullable: N
              map: " If IsNull(mbr_enr_lkup.CLM_ID) = @TRUE then 'UNK' else mbr_enr_lkup.EXPRNC_CAT_CD"
            - name: FNCL_LOB_NO
              type: char
              width: 4
              map: " If IsNull(mbr_enr_lkup.CLM_ID) = @TRUE then 'UNK' else mbr_enr_lkup.FNCL_LOB_CD"
            - name: GRP
              type: char
              width: 8
              nullable: N
              map: "BCBSKCCommClmTrns.GRP"
            - name: MBR_CK
              type: int
              width: 4
              nullable: N
              map: "BCBSKCCommClmTrns.MBR_CK"
            - name: PROD
              type: char
              width: 8
              nullable: N
              map: " If IsNull(mbr_enr_lkup.CLM_ID) = @TRUE Then 'UNK' Else Trim(mbr_enr_lkup.PROD_ID)"
            - name: SUBGRP
              type: char
              width: 4
              nullable: N
              map: " If IsNull(mbr_enr_lkup.CLM_ID) = @TRUE then 'UNK' else mbr_enr_lkup.SUBGRP_ID"
            - name: SUB_CK
              type: int
              width: 4
              nullable: N
              map: "BCBSKCCommClmTrns.SUB_CK"
            - name: CLM_ACDNT_CD
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_ACDNT_CD"
            - name: CLM_ACDNT_ST_CD
              type: char
              width: 2
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_ACDNT_ST_CD"
            - name: CLM_ACTIVATING_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_ACTIVATING_BCBS_PLN_CD"
            - name: CLM_AGMNT_SRC_CD
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_AGMNT_SRC_CD"
            - name: CLM_BTCH_ACTN_CD
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_BTCH_ACTN_CD"
            - name: CLM_CAP_CD
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_CAP_CD"
            - name: CLM_CAT_CD
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_CAT_CD"
            - name: CLM_CHK_CYC_OVRD_CD
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_CHK_CYC_OVRD_CD"
            - name: CLM_COB_CD
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_COB_CD"
            - name: FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "BCBSKCCommClmTrns.FINL_DISP_CD"
            - name: CLM_INPT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_INPT_METH_CD"
            - name: CLM_INPT_SRC_CD
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_INPT_SRC_CD"
            - name: CLM_IPP_CD
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_IPP_CD"
            - name: CLM_NTWK_STTUS_CD
              type: char
              width: 2
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_NTWK_STTUS_CD"
            - name: CLM_NONPAR_PROV_PFX_CD
              type: char
              width: 4
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_NONPAR_PROV_PFX_CD"
            - name: CLM_OTHER_BNF_CD
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_OTHER_BNF_CD"
            - name: CLM_PAYE_CD
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_PAYE_CD"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD
              type: char
              width: 4
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_PRCS_CTL_AGNT_PFX_CD"
            - name: CLM_SVC_DEFN_PFX_CD
              type: char
              width: 4
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_SVC_DEFN_PFX_CD"
            - name: CLM_SVC_PROV_SPEC_CD
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_SVC_PROV_SPEC_CD"
            - name: CLM_SVC_PROV_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_SVC_PROV_TYP_CD"
            - name: CLM_STTUS_CD
              type: char
              width: 2
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_STTUS_CD"
            - name: CLM_SUBMT_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_SUBMT_BCBS_PLN_CD"
            - name: CLM_SUB_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_SUB_BCBS_PLN_CD"
            - name: CLM_SUBTYP_CD
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_SUBTYP_CD"
            - name: CLM_TYP_CD
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_TYP_CD"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.ATCHMT_IN"
            - name: CLM_CLNCL_EDIT_CD
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_CLNCL_EDIT_CD"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.PROD_OOA_IN"
            - name: ACDNT_DT
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.ACDNT_DT"
            - name: INPT_DT
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.INPT_DT"
            - name: MBR_PLN_ELIG_DT
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.MBR_PLN_ELIG_DT"
            - name: NEXT_RVW_DT
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.NEXT_RVW_DT"
            - name: PD_DT
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.PD_DT"
            - name: PAYMT_DRAG_CYC_DT
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.PAYMT_DRAG_CYC_DT"
            - name: PRCS_DT
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.PRCS_DT"
            - name: RCVD_DT
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.RCVD_DT"
            - name: SVC_STRT_DT
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.SVC_STRT_DT"
            - name: SVC_END_DT
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.SVC_END_DT"
            - name: SMLR_ILNS_DT
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.SMLR_ILNS_DT"
            - name: STTUS_DT
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.STTUS_DT"
            - name: WORK_UNABLE_BEG_DT
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.WORK_UNABLE_BEG_DT"
            - name: WORK_UNABLE_END_DT
              type: char
              width: 10
              nullable: N
              map: "BCBSKCCommClmTrns.WORK_UNABLE_END_DT"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCCommClmTrns.ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCCommClmTrns.ACTL_PD_AMT"
            - name: ALLOW_AMT
              type: decimal
              width: 19
              precision: 4
              nullable: N
              map: "BCBSKCCommClmTrns.ALLOW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCCommClmTrns.DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCCommClmTrns.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCCommClmTrns.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCCommClmTrns.COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCCommClmTrns.CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCCommClmTrns.DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCCommClmTrns.PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              nullable: N
              map: "BCBSKCCommClmTrns.MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "BCBSKCCommClmTrns.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "BCBSKCCommClmTrns.ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "BCBSKCCommClmTrns.DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "BCBSKCCommClmTrns.MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "BCBSKCCommClmTrns.MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "BCBSKCCommClmTrns.MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 20
              map: "BCBSKCCommClmTrns.PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "BCBSKCCommClmTrns.PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "BCBSKCCommClmTrns.PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "BCBSKCCommClmTrns.RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "BCBSKCCommClmTrns.SUB_ID"
            - name: PRPR_ENTITY
              type: char
              width: 1
              map: "@NULL"
            - name: PCA_TYP_CD
              type: char
              width: 18
              nullable: N
              map: "BCBSKCCommClmTrns.PCA_TYP_CD"
            - name: REL_PCA_CLM_ID
              type: varchar
              width: 18
              nullable: N
              map: "BCBSKCCommClmTrns.REL_PCA_CLM_ID"
            - name: CLCL_MICRO_ID
              type: varchar
              width: 18
              nullable: N
              map: "'NA'"
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCCommClmTrns.REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "BCBSKCCommClmTrns.MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCCommClmTrns.PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD
              type: varchar
              width: 20
              nullable: N
              map: "BCBSKCCommClmTrns.CLM_SUBMT_ICD_VRSN_CD"
            - name: NTWK
              type: char
              width: 12
              nullable: N
              map: "BCBSKCCommClmTrns.NTWK"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "' '"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "BCBSKCCommClmTrns.BILL_PAYMT_EXCL_IN"
        - name: BusinessRules
          type: lookup
          stageVars:
            - name: svReversalClaim
              type: decimal
              width: 0
              map: " If Trim(BCBSKCComm.CLM_TYP[1, 1]) = 'R' then @TRUE else @FALSE"
              initValue: "@FALSE"
            - name: svSubCk
              type: decimal
              width: 0
              map: " If IsNull(mbr_lkup.SUB_UNIQ_KEY) = @TRUE then 1 else mbr_lkup.SUB_UNIQ_KEY"
              initValue: "-1"
            - name: svMbrAge
              type: decimal
              width: 0
              map: "AGE(BCBSKCComm.BRTH_DT, BCBSKCComm.FILL_DT)"
              initValue: "-1"
            - name: svClmCnt
              type: decimal
              width: 0
              map: " If svReversalClaim then -1 else 1"
              initValue: "0"
            - name: svAllowAmt
              type: decimal
              width: 0
              map: " If Trim(BCBSKCComm.CLNT_GNRL_PRPS_AREA[61, 2]) <> '01' Then (((BCBSKCComm.SLS_TAX_AMT + BCBSKCComm.INGR_CST_AMT + BCBSKCComm.DISPNS_FEE_AMT)) - BCBSKCComm.OTHR_PAYOR_AMT) Else (BCBSKCComm.INGR_CST_AMT + BCBSKCComm.DISPNS_FEE_AMT + BCBSKCComm.SLS_TAX_AMT)"
              initValue: "0"
            - name: svMIAllowAmt
              type: decimal
              width: 0
              map: " If BCBSKCComm.OTHR_COV_CD = '2' Then (BCBSKCComm.INGR_CST_AMT + BCBSKCComm.DISPNS_FEE_AMT + BCBSKCComm.SLS_TAX_AMT) - BCBSKCComm.OTHR_PAYOR_AMT Else BCBSKCComm.INGR_CST_AMT + BCBSKCComm.DISPNS_FEE_AMT + BCBSKCComm.SLS_TAX_AMT"
              initValue: "0"
          option:
            joinType: left
          sources: 
            - name: BCBSCommClmLand
              alias: BCBSKCComm
              joinFilter: ""
              joinExpression: []
            - name: Hash.mbr_lkup
              alias: mbr_lkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "mbr_lkup.MBR_UNIQ_KEY"
                toExpr: "BCBSKCComm.ORIG_MBR"
                caseInSensitive: true
          joinCondition: 
            - expression: "mbr_lkup.MBR_UNIQ_KEY = BCBSKCComm.ORIG_MBR"
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
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSk
            - name: CLM_ID
              type: char
              width: 18
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
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "BCBSKCComm.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "BCBSKCComm.ADJ_TO_CLM_ID"
            - name: ALPHA_PFX_CD
              type: char
              width: 3
              map: "'NA'"
            - name: CLM_EOB_EXCD
              type: char
              width: 3
              nullable: N
              map: "'NA'"
            - name: CLS
              type: char
              width: 4
              nullable: N
              map: "'NA'"
            - name: CLS_PLN
              type: char
              width: 8
              nullable: N
              map: "'NA'"
            - name: EXPRNC_CAT
              type: char
              width: 4
              nullable: N
              map: "'NA'"
            - name: FNCL_LOB_NO
              type: char
              width: 4
              map: "'NA'"
            - name: GRP
              type: char
              width: 8
              nullable: N
              map: "BCBSKCComm.GRP_NO"
            - name: MBR_CK
              type: int
              width: 4
              nullable: N
              map: "BCBSKCComm.ORIG_MBR"
            - name: PROD
              type: char
              width: 8
              nullable: N
              map: "BCBSKCComm.ORIG_MBR"
            - name: SUBGRP
              type: char
              width: 4
              nullable: N
              map: "BCBSKCComm.ORIG_MBR"
            - name: SUB_CK
              type: int
              width: 4
              nullable: N
              map: svSubCk
            - name: CLM_ACDNT_CD
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: CLM_ACDNT_ST_CD
              type: char
              width: 2
              nullable: N
              map: "'NA'"
            - name: CLM_ACTIVATING_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: CLM_AGMNT_SRC_CD
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: CLM_BTCH_ACTN_CD
              type: char
              width: 1
              nullable: N
              map: "'NA'"
            - name: CLM_CAP_CD
              type: char
              width: 10
              nullable: N
              map: "'N'"
            - name: CLM_CAT_CD
              type: char
              width: 10
              nullable: N
              map: "'STD'"
            - name: CLM_CHK_CYC_OVRD_CD
              type: char
              width: 1
              nullable: N
              map: "'NA'"
            - name: CLM_COB_CD
              type: char
              width: 2
              nullable: N
              map: " If BCBSKCComm.SRC_SYS_CD = 'OPTUMRX' And BCBSKCComm.OTHR_COV_CD = '2' and BCBSKCComm.OTHR_PAYOR_AMT <> 0 Then '2' Else  If BCBSKCComm.SRC_SYS_CD = 'MEDIMPACT' And BCBSKCComm.OTHR_COV_CD = '2' and BCBSKCComm.OTHR_PAYOR_AMT <> 0 Then '2' Else 'NA'"
            - name: FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "'ACPTD'"
            - name: CLM_INPT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "'NA'"
            - name: CLM_INPT_SRC_CD
              type: char
              width: 10
              nullable: N
              map: " If BCBSKCComm.SRC_SYS_CD = 'OPTUMRX' And ISNULL(BCBSKCComm.CLM_TRNSMSN_METH) = @FALSE Then BCBSKCComm.CLM_TRNSMSN_METH Else 'NA'"
            - name: CLM_IPP_CD
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: CLM_NTWK_STTUS_CD
              type: char
              width: 2
              nullable: N
              map: " If ISNULL(BCBSKCComm.PAR_PDX_IN) = @TRUE Or Len(BCBSKCComm.PAR_PDX_IN) = 0 then 'UNK' else BCBSKCComm.PAR_PDX_IN"
            - name: CLM_NONPAR_PROV_PFX_CD
              type: char
              width: 4
              nullable: N
              map: "'NA'"
            - name: CLM_OTHER_BNF_CD
              type: char
              width: 1
              nullable: N
              map: "'NA'"
            - name: CLM_PAYE_CD
              type: char
              width: 1
              nullable: N
              map: "'NA'"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD
              type: char
              width: 4
              nullable: N
              map: "'NA'"
            - name: CLM_SVC_DEFN_PFX_CD
              type: char
              width: 4
              nullable: N
              map: "'NA'"
            - name: CLM_SVC_PROV_SPEC_CD
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: CLM_SVC_PROV_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "'0012'"
            - name: CLM_STTUS_CD
              type: char
              width: 2
              nullable: N
              map: " If IsNull(BCBSKCComm.CLM_STTUS_CD) = @TRUE or Len(Trim(BCBSKCComm.CLM_STTUS_CD)) = 0 then '0' else Trim(BCBSKCComm.CLM_STTUS_CD)"
            - name: CLM_SUBMT_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: CLM_SUB_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: CLM_SUBTYP_CD
              type: char
              width: 10
              nullable: N
              map: "'RX'"
            - name: CLM_TYP_CD
              type: char
              width: 1
              nullable: N
              map: "'MED'"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: CLM_CLNCL_EDIT_CD
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: ACDNT_DT
              type: char
              width: 10
              nullable: N
              map: "'1753-01-01'"
            - name: INPT_DT
              type: char
              width: 10
              nullable: N
              map: "'1753-01-01'"
            - name: MBR_PLN_ELIG_DT
              type: char
              width: 10
              nullable: N
              map: "'1753-01-01'"
            - name: NEXT_RVW_DT
              type: char
              width: 10
              nullable: N
              map: " If BCBSKCComm.SRC_SYS_CD = 'OPTUMRX' Then '2199-12-31' Else '1753-01-01'"
            - name: PD_DT
              type: char
              width: 10
              nullable: N
              map: " If IsNull(BCBSKCComm.PD_DT) = @TRUE or Len(Trim(BCBSKCComm.PD_DT)) = 0 then '1753-01-01' else Trim(BCBSKCComm.PD_DT)"
            - name: PAYMT_DRAG_CYC_DT
              type: char
              width: 10
              nullable: N
              map: "'1753-01-01'"
            - name: PRCS_DT
              type: char
              width: 10
              nullable: N
              map: " If IsNull(BCBSKCComm.PD_DT) = @TRUE or Len(Trim(BCBSKCComm.PD_DT)) = 0 then '1753-01-01' else Trim(BCBSKCComm.PD_DT)"
            - name: RCVD_DT
              type: char
              width: 10
              nullable: N
              map: " If IsNull(BCBSKCComm.FILE_RCVD_DT) = @TRUE or Len(Trim(BCBSKCComm.FILE_RCVD_DT)) = 0 then '1753-01-01' else Trim(BCBSKCComm.FILE_RCVD_DT)"
            - name: SVC_STRT_DT
              type: char
              width: 10
              nullable: N
              map: " If IsNull(BCBSKCComm.FILL_DT) = @TRUE or Len(Trim(BCBSKCComm.FILL_DT)) = 0 then '1753-01-01' else Trim(BCBSKCComm.FILL_DT)"
            - name: SVC_END_DT
              type: char
              width: 10
              nullable: N
              map: " If IsNull(BCBSKCComm.FILL_DT) = @TRUE or Len(Trim(BCBSKCComm.FILL_DT)) = 0 then '1753-01-01' else Trim(BCBSKCComm.FILL_DT)"
            - name: SMLR_ILNS_DT
              type: char
              width: 10
              nullable: N
              map: "'1753-01-01'"
            - name: STTUS_DT
              type: char
              width: 10
              nullable: N
              map: CurrentDate
            - name: WORK_UNABLE_BEG_DT
              type: char
              width: 10
              nullable: N
              map: "'1753-01-01'"
            - name: WORK_UNABLE_END_DT
              type: char
              width: 10
              nullable: N
              map: "'2199-12-31'"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCComm.BILL_AMT"
            - name: ALLOW_AMT
              type: decimal
              width: 19
              precision: 4
              nullable: N
              map: " If BCBSKCComm.SRC_SYS_CD = 'OPTUMRX' then svAllowAmt else  If BCBSKCComm.SRC_SYS_CD = 'MEDIMPACT' Then svMIAllowAmt Else 0.00"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If BCBSKCComm.SRC_SYS_CD = 'OPTUMRX' then BCBSKCComm.COPAY_PCT_AMT else 0.00"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If BCBSKCComm.SRC_SYS_CD = 'OPTUMRX' then BCBSKCComm.FULL_AVG_WHLSL_PRICE Else  If BCBSKCComm.SRC_SYS_CD = 'MEDIMPACT' Then BCBSKCComm.CLM_AMT Else 0.00"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(BCBSKCComm.COPAY_AMT) = @TRUE or Len(Trim(BCBSKCComm.COPAY_AMT)) = 0 then '0.0' else Trim(BCBSKCComm.COPAY_AMT)"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If BCBSKCComm.SRC_SYS_CD = 'OPTUMRX' then BCBSKCComm.FULL_AVG_WHLSL_PRICE Else  If BCBSKCComm.SRC_SYS_CD = 'MEDIMPACT' Then BCBSKCComm.CLM_AMT else 0.00"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(BCBSKCComm.DEDCT_AMT) = @TRUE or Len(Trim(BCBSKCComm.DEDCT_AMT)) = 0 then '0.0' else Trim(BCBSKCComm.DEDCT_AMT)"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(BCBSKCComm.BILL_AMT) = @TRUE or Len(Trim(BCBSKCComm.BILL_AMT)) = 0 then '0.0' else Trim(BCBSKCComm.BILL_AMT)"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: svClmCnt
            - name: MBR_AGE
              type: decimal
              width: 3
              nullable: N
              map: svMbrAge
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "BCBSKCComm.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "BCBSKCComm.ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "'NA'"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "@NULL"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "'NA'"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: " IF IsNull(mbr_lkup.MBR_UNIQ_KEY) = @FALSE THEN TRIM(mbr_lkup.MBR_SFX_NO) ELSE '0'"
            - name: PATN_ACCT_NO
              type: varchar
              width: 20
              map: "@NULL"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: " If BCBSKCComm.SRC_SYS_CD = 'MEDIMPACT' Then BCBSKCComm.ADJDCT_REF_NO Else 'NA'"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "'NA'"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "@NULL"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: " IF IsNull(mbr_lkup.SUB_ID) = @FALSE THEN TRIM(mbr_lkup.SUB_ID) ELSE '0'"
            - name: PCA_TYP_CD
              type: varchar
              width: 18
              nullable: N
              map: "'NA'"
            - name: REL_PCA_CLM_ID
              type: varchar
              width: 18
              nullable: N
              map: "'NA'"
            - name: REL_BASE_CLM_ID
              type: varchar
              width: 18
              nullable: N
              map: "'NA'"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: CLM_SUBMT_ICD_VRSN_CD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: NTWK
              type: char
              width: 12
              nullable: N
              map: "'NA'"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: " If (BCBSKCComm.SRC_SYS_CD = \"OPTUMRX\") Then BCBSKCComm.CLNT_GNRL_PRPS_AREA[63, 1] Else \"N\""
        - name: Snapshot.V0S185P2
          type: map
          source: alpha_pfx
          alias: Transform
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Transform.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Transform.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Transform.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Transform.PRI_KEY_STRING"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
              map: "Transform.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
              map: "Transform.ADJ_FROM_CLM"
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
              map: "Transform.ADJ_TO_CLM"
            - name: ALPHA_PFX_CD
              type: char
              width: 3
              map: "Transform.ALPHA_PFX_CD"
            - name: CLM_EOB_EXCD
              type: char
              width: 3
              nullable: N
              map: "Transform.CLM_EOB_EXCD"
            - name: CLS
              type: char
              width: 4
              nullable: N
              map: "Transform.CLS"
            - name: CLS_PLN
              type: char
              width: 8
              nullable: N
              map: "Transform.CLS_PLN"
            - name: EXPRNC_CAT
              type: char
              width: 4
              nullable: N
              map: "Transform.EXPRNC_CAT"
            - name: FNCL_LOB_NO
              type: char
              width: 4
              map: "Transform.FNCL_LOB_NO"
            - name: GRP
              type: char
              width: 8
              nullable: N
              map: "Transform.GRP"
            - name: MBR_CK
              type: int
              width: 4
              nullable: N
              map: "Transform.MBR_CK"
            - name: NTWK
              type: char
              width: 12
              nullable: N
              map: "Transform.NTWK"
            - name: PROD
              type: char
              width: 8
              nullable: N
              map: "Transform.PROD"
            - name: SUBGRP
              type: char
              width: 4
              nullable: N
              map: "Transform.SUBGRP"
            - name: SUB_CK
              type: int
              width: 4
              nullable: N
              map: "Transform.SUB_CK"
            - name: CLM_ACDNT_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.CLM_ACDNT_CD"
            - name: CLM_ACDNT_ST_CD
              type: char
              width: 2
              nullable: N
              map: "Transform.CLM_ACDNT_ST_CD"
            - name: CLM_ACTIVATING_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.CLM_ACTIVATING_BCBS_PLN_CD"
            - name: CLM_AGMNT_SRC_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.CLM_AGMNT_SRC_CD"
            - name: CLM_BTCH_ACTN_CD
              type: char
              width: 1
              nullable: N
              map: "Transform.CLM_BTCH_ACTN_CD"
            - name: CLM_CAP_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.CLM_CAP_CD"
            - name: CLM_CAT_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.CLM_CAT_CD"
            - name: CLM_CHK_CYC_OVRD_CD
              type: char
              width: 1
              nullable: N
              map: "Transform.CLM_CHK_CYC_OVRD_CD"
            - name: CLM_COB_CD
              type: char
              width: 1
              nullable: N
              map: "Transform.CLM_COB_CD"
            - name: FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.FINL_DISP_CD"
            - name: CLM_INPT_METH_CD
              type: char
              width: 1
              nullable: N
              map: "Transform.CLM_INPT_METH_CD"
            - name: CLM_INPT_SRC_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.CLM_INPT_SRC_CD"
            - name: CLM_IPP_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.CLM_IPP_CD"
            - name: CLM_NTWK_STTUS_CD
              type: char
              width: 2
              nullable: N
              map: "Transform.CLM_NTWK_STTUS_CD"
            - name: CLM_NONPAR_PROV_PFX_CD
              type: char
              width: 4
              nullable: N
              map: "Transform.CLM_NONPAR_PROV_PFX_CD"
            - name: CLM_OTHER_BNF_CD
              type: char
              width: 1
              nullable: N
              map: "Transform.CLM_OTHER_BNF_CD"
            - name: CLM_PAYE_CD
              type: char
              width: 1
              nullable: N
              map: "Transform.CLM_PAYE_CD"
            - name: CLM_PRCS_CTL_AGNT_PFX_CD
              type: char
              width: 4
              nullable: N
              map: "Transform.CLM_PRCS_CTL_AGNT_PFX_CD"
            - name: CLM_SVC_DEFN_PFX_CD
              type: char
              width: 4
              nullable: N
              map: "Transform.CLM_SVC_DEFN_PFX_CD"
            - name: CLM_SVC_PROV_SPEC_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.CLM_SVC_PROV_SPEC_CD"
            - name: CLM_SVC_PROV_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.CLM_SVC_PROV_TYP_CD"
            - name: CLM_STTUS_CD
              type: char
              width: 2
              nullable: N
              map: "Transform.CLM_STTUS_CD"
            - name: CLM_SUBMT_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.CLM_SUBMT_BCBS_PLN_CD"
            - name: CLM_SUB_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.CLM_SUB_BCBS_PLN_CD"
            - name: CLM_SUBTYP_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.CLM_SUBTYP_CD"
            - name: CLM_TYP_CD
              type: char
              width: 1
              nullable: N
              map: "Transform.CLM_TYP_CD"
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.ATCHMT_IN"
            - name: CLM_CLNCL_EDIT_CD
              type: char
              width: 1
              nullable: N
              map: "Transform.CLM_CLNCL_EDIT_CD"
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.COBRA_CLM_IN"
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.FIRST_PASS_IN"
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.HOST_IN"
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.LTR_IN"
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.MCARE_ASG_IN"
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.NOTE_IN"
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.PCA_AUDIT_IN"
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.PCP_SUBMT_IN"
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.PROD_OOA_IN"
            - name: ACDNT_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.ACDNT_DT"
            - name: INPT_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.INPT_DT"
            - name: MBR_PLN_ELIG_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.MBR_PLN_ELIG_DT"
            - name: NEXT_RVW_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.NEXT_RVW_DT"
            - name: PD_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.PD_DT"
            - name: PAYMT_DRAG_CYC_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.PAYMT_DRAG_CYC_DT"
            - name: PRCS_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.PRCS_DT"
            - name: RCVD_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.RCVD_DT"
            - name: SVC_STRT_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.SVC_STRT_DT"
            - name: SVC_END_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.SVC_END_DT"
            - name: SMLR_ILNS_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.SMLR_ILNS_DT"
            - name: STTUS_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.STTUS_DT"
            - name: WORK_UNABLE_BEG_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.WORK_UNABLE_BEG_DT"
            - name: WORK_UNABLE_END_DT
              type: char
              width: 10
              nullable: N
              map: "Transform.WORK_UNABLE_END_DT"
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.ACDNT_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.ACTL_PD_AMT"
            - name: ALLOW_AMT
              type: decimal
              width: 19
              precision: 4
              nullable: N
              map: "Transform.ALLOW_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.DSALW_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.COPAY_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.CHRG_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.DEDCT_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.PAYBL_AMT"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "Transform.CLM_CT"
            - name: MBR_AGE
              type: decimal
              width: 3
              nullable: N
              map: "Transform.MBR_AGE"
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Transform.ADJ_FROM_CLM_ID"
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Transform.ADJ_TO_CLM_ID"
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
              map: "Transform.DOC_TX_ID"
            - name: MCAID_RESUB_NO
              type: char
              width: 15
              map: "Transform.MCAID_RESUB_NO"
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
              map: "Transform.MCARE_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "Transform.MBR_SFX_NO"
            - name: PATN_ACCT_NO
              type: varchar
              width: 20
              map: "Transform.PATN_ACCT_NO"
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
              map: "Transform.PAYMT_REF_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "Transform.PROV_AGMNT_ID"
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "Transform.RFRNG_PROV_TX"
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
              map: "Transform.SUB_ID"
            - name: PRPR_ENTITY
              type: char
              width: 1
              map: "Transform.PRPR_ENTITY"
            - name: PCA_TYP_CD
              type: char
              width: 18
              nullable: N
              map: "Transform.PCA_TYP_CD"
            - name: REL_PCA_CLM_ID
              type: varchar
              width: 18
              nullable: N
              map: "Transform.REL_PCA_CLM_ID"
            - name: CLCL_MICRO_ID
              type: varchar
              width: 18
              nullable: N
              map: "Transform.CLCL_MICRO_ID"
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
              map: "Transform.CLM_UPDT_SW"
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.REMIT_SUPRSION_AMT"
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
              map: "Transform.MCAID_STTUS_ID"
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.PATN_PD_AMT"
            - name: CLM_SUBMT_ICD_VRSN_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_SUBMT_ICD_VRSN_CD"
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
              map: "Transform.CLM_TXNMY_CD"
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.BILL_PAYMT_EXCL_IN"
          keys:
            - CLM_SK
        - name: Snapshot.V0S185P3
          type: map
          source: alpha_pfx
          alias: Transform
          schema: 
            - name: CLM_ID
              type: char
              width: 12
              nullable: N
              map: "Transform.CLM_ID"
            - name: MBR_CK
              type: int
              width: 4
              nullable: N
              map: "Transform.MBR_CK"
            - name: GRP
              type: char
              width: 8
              nullable: N
              map: "Transform.GRP"
            - name: SVC_STRT_DT
              type: timestamp
              nullable: N
              map: "Transform.SVC_STRT_DT"
            - name: CHRG_AMT
              type: decimal
              width: 19
              precision: 4
              nullable: N
              map: "Transform.CHRG_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 19
              precision: 4
              nullable: N
              map: "Transform.PAYBL_AMT"
            - name: EXPRNC_CAT
              type: char
              width: 4
              nullable: N
              map: "Transform.EXPRNC_CAT"
            - name: FNCL_LOB_NO
              type: char
              width: 4
              map: "Transform.FNCL_LOB_NO"
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
              map: "Transform.CLM_CT"
            - name: PCA_TYP_CD
              type: char
              width: 18
              nullable: N
              map: "Transform.PCA_TYP_CD"
            - name: CLM_STTUS_CD
              type: char
              width: 2
              nullable: N
              map: "Transform.CLM_STTUS_CD"
            - name: CLM_CAT_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.CLM_CAT_CD"
          keys:
            - CLM_ID
        - name: Transformer
          type: map
          source: Snapshot.V0S185P3
          alias: Snapshot
          stageVars: 
            - name: ExpCatCdSk
              type: decimal
              width: 0
              map: "GetFkeyExprncCat('FACETS', 0, Snapshot.EXPRNC_CAT, 'N')"
              initValue: "0"
            - name: GrpSk
              type: decimal
              width: 0
              map: "GetFkeyGrp('FACETS', 0, Snapshot.GRP, 'N')"
              initValue: "0"
            - name: MbrSk
              type: decimal
              width: 0
              map: "GetFkeyMbr('FACETS', 0, Snapshot.MBR_CK, 'N')"
              initValue: "0"
            - name: FnclLobSk
              type: decimal
              width: 0
              map: "GetFkeyFnclLob(\"PSI\", 0, Snapshot.FNCL_LOB_NO, 'N')"
              initValue: "0"
            - name: PcaTypCdSk
              type: decimal
              width: 0
              map: "GetFkeyCodes('FACETS', 0, \"PERSONAL CARE ACCOUNT PROCESSING\", Snapshot.PCA_TYP_CD, 'N')"
              initValue: "0"
            - name: ClmSttusCdSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(SrcSysCd, 0, \"CLAIM STATUS\", Snapshot.CLM_STTUS_CD, 'N')"
              initValue: "0"
            - name: ClmCatCdSk
              type: decimal
              width: 0
              map: "GetFkeyCodes(SrcSysCd, 0, \"CLAIM CATEGORY\", Snapshot.CLM_CAT_CD, 'X')"
              initValue: "0"
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSk
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Snapshot.CLM_ID"
            - name: CLM_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmSttusCdSk
            - name: CLM_CAT_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmCatCdSk
            - name: EXPRNC_CAT_SK
              type: int
              width: 4
              nullable: N
              map: ExpCatCdSk
            - name: FNCL_LOB_SK
              type: int
              width: 4
              nullable: N
              map: FnclLobSk
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: GrpSk
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: MbrSk
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Snapshot.SVC_STRT_DT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Snapshot.CHRG_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Snapshot.PAYBL_AMT"
            - name: CLM_CT
              type: int
              width: 4
              nullable: N
              map: "Snapshot.CLM_CT"
            - name: PCA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: PcaTypCdSk
            - name: ExpCatCdSk
              type: varchar
              width: 0
              map: ExpCatCdSk
              dropStageVar: true
              initValue: "0"
            - name: GrpSk
              type: varchar
              width: 0
              map: GrpSk
              dropStageVar: true
              initValue: "0"
            - name: MbrSk
              type: varchar
              width: 0
              map: MbrSk
              dropStageVar: true
              initValue: "0"
            - name: FnclLobSk
              type: varchar
              width: 0
              map: FnclLobSk
              dropStageVar: true
              initValue: "0"
            - name: PcaTypCdSk
              type: varchar
              width: 0
              map: PcaTypCdSk
              dropStageVar: true
              initValue: "0"
            - name: ClmSttusCdSk
              type: varchar
              width: 0
              map: ClmSttusCdSk
              dropStageVar: true
              initValue: "0"
            - name: ClmCatCdSk
              type: varchar
              width: 0
              map: ClmCatCdSk
              dropStageVar: true
              initValue: "0"
        - name: ClmPK
          type: sharedContainer
          containerName: ClmPK
          parameters:
            - name: "CurrRunCycle"
              expression: "#RunCycle#"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: ADJ_FROM_CLM
              type: char
              width: 12
              nullable: N
            - name: ADJ_TO_CLM
              type: char
              width: 12
              nullable: N
            - name: ALPHA_PFX_CD
              type: char
              width: 3
            - name: CLM_EOB_EXCD
              type: char
              width: 3
              nullable: N
            - name: CLS
              type: char
              width: 4
              nullable: N
            - name: CLS_PLN
              type: char
              width: 8
              nullable: N
            - name: EXPRNC_CAT
              type: char
              width: 4
              nullable: N
            - name: FNCL_LOB_NO
              type: char
              width: 4
            - name: GRP
              type: char
              width: 8
              nullable: N
            - name: MBR_CK
              type: int
              width: 4
              nullable: N
            - name: NTWK
              type: char
              width: 12
              nullable: N
            - name: PROD
              type: char
              width: 8
              nullable: N
            - name: SUBGRP
              type: char
              width: 4
              nullable: N
            - name: SUB_CK
              type: int
              width: 4
              nullable: N
            - name: CLM_ACDNT_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_ACDNT_ST_CD
              type: char
              width: 2
              nullable: N
            - name: CLM_ACTIVATING_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_AGMNT_SRC_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_BTCH_ACTN_CD
              type: char
              width: 1
              nullable: N
            - name: CLM_CAP_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_CAT_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_CHK_CYC_OVRD_CD
              type: char
              width: 1
              nullable: N
            - name: CLM_COB_CD
              type: char
              width: 1
              nullable: N
            - name: FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_INPT_METH_CD
              type: char
              width: 1
              nullable: N
            - name: CLM_INPT_SRC_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_IPP_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_NTWK_STTUS_CD
              type: char
              width: 2
              nullable: N
            - name: CLM_NONPAR_PROV_PFX_CD
              type: char
              width: 4
              nullable: N
            - name: CLM_OTHER_BNF_CD
              type: char
              width: 1
              nullable: N
            - name: CLM_PAYE_CD
              type: char
              width: 1
              nullable: N
            - name: CLM_PRCS_CTL_AGNT_PFX_CD
              type: char
              width: 4
              nullable: N
            - name: CLM_SVC_DEFN_PFX_CD
              type: char
              width: 4
              nullable: N
            - name: CLM_SVC_PROV_SPEC_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_SVC_PROV_TYP_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_STTUS_CD
              type: char
              width: 2
              nullable: N
            - name: CLM_SUBMT_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_SUB_BCBS_PLN_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_SUBTYP_CD
              type: char
              width: 10
              nullable: N
            - name: CLM_TYP_CD
              type: char
              width: 1
              nullable: N
            - name: ATCHMT_IN
              type: char
              width: 1
              nullable: N
            - name: CLM_CLNCL_EDIT_CD
              type: char
              width: 1
              nullable: N
            - name: COBRA_CLM_IN
              type: char
              width: 1
              nullable: N
            - name: FIRST_PASS_IN
              type: char
              width: 1
              nullable: N
            - name: HOST_IN
              type: char
              width: 1
              nullable: N
            - name: LTR_IN
              type: char
              width: 1
              nullable: N
            - name: MCARE_ASG_IN
              type: char
              width: 1
              nullable: N
            - name: NOTE_IN
              type: char
              width: 1
              nullable: N
            - name: PCA_AUDIT_IN
              type: char
              width: 1
              nullable: N
            - name: PCP_SUBMT_IN
              type: char
              width: 1
              nullable: N
            - name: PROD_OOA_IN
              type: char
              width: 1
              nullable: N
            - name: ACDNT_DT
              type: char
              width: 10
              nullable: N
            - name: INPT_DT
              type: char
              width: 10
              nullable: N
            - name: MBR_PLN_ELIG_DT
              type: char
              width: 10
              nullable: N
            - name: NEXT_RVW_DT
              type: char
              width: 10
              nullable: N
            - name: PD_DT
              type: char
              width: 10
              nullable: N
            - name: PAYMT_DRAG_CYC_DT
              type: char
              width: 10
              nullable: N
            - name: PRCS_DT
              type: char
              width: 10
              nullable: N
            - name: RCVD_DT
              type: char
              width: 10
              nullable: N
            - name: SVC_STRT_DT
              type: char
              width: 10
              nullable: N
            - name: SVC_END_DT
              type: char
              width: 10
              nullable: N
            - name: SMLR_ILNS_DT
              type: char
              width: 10
              nullable: N
            - name: STTUS_DT
              type: char
              width: 10
              nullable: N
            - name: WORK_UNABLE_BEG_DT
              type: char
              width: 10
              nullable: N
            - name: WORK_UNABLE_END_DT
              type: char
              width: 10
              nullable: N
            - name: ACDNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALLOW_AMT
              type: decimal
              width: 19
              precision: 4
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLM_CT
              type: smallint
              width: 2
              nullable: N
            - name: MBR_AGE
              type: decimal
              width: 3
              nullable: N
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: DOC_TX_ID
              type: char
              width: 18
              nullable: N
            - name: MCAID_RESUB_NO
              type: char
              width: 15
            - name: MCARE_ID
              type: char
              width: 12
              nullable: N
            - name: MBR_SFX_NO
              type: char
              width: 2
            - name: PATN_ACCT_NO
              type: varchar
              width: 20
            - name: PAYMT_REF_ID
              type: char
              width: 16
              nullable: N
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
            - name: RFRNG_PROV_TX
              type: varchar
              width: 70
            - name: SUB_ID
              type: char
              width: 14
              nullable: N
            - name: PRPR_ENTITY
              type: char
              width: 1
            - name: PCA_TYP_CD
              type: char
              width: 18
              nullable: N
            - name: REL_PCA_CLM_ID
              type: varchar
              width: 18
              nullable: N
            - name: CLCL_MICRO_ID
              type: varchar
              width: 18
              nullable: N
            - name: CLM_UPDT_SW
              type: char
              width: 1
              nullable: N
            - name: REMIT_SUPRSION_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MCAID_STTUS_ID
              type: varchar
              width: 20
              nullable: N
            - name: PATN_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLM_SUBMT_ICD_VRSN_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_TXNMY_CD
              type: varchar
              width: 20
            - name: BILL_PAYMT_EXCL_IN
              type: char
              width: 1
              nullable: N
          source: Snapshot.V0S185P2


