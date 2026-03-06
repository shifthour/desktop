model:
  config:
    origin: datastage
  jobs:
    - name: BCBSKCCommClmRemitHistExtr
      origin: datastage
      type: dataflow
      folder: "claim/RemitHist"
      runAfterSubrOnJobSuccess: enabled
      description: "BCBSKCCOMMONClaim Remit History"
      fullDescription: "********************************************************************************************\nCOPYRIGHT 2008, 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\nCALLED BY:          OptumDrugExtrSeq\n\nDESCRIPTION:     Reads the Optum landing file created in  OptumClmLand  job and puts the data into the claim  remit history common record format and runs through primary key using shared container ClmRemiHistPkey\n\nMODIFICATIONS:\n\nDeveloper                    Date              Project/Altiris #   Change Description                                                                                      Development Project   Code Reviewer          Date Reviewed       \n---------------------------------   ------------------    ------------------------   ------------------------------------------------------------------------------------------------------------------   ---------------------------------   -------------------------------   ----------------------------       \nSrinivas Garimella      2019-10-15   #6131                         Original Programming                                                                                IntegrateDev2              Kalyan Neelam          2019-11-20\nSri Nannapaneni       2020-07-15      6131 PBM                Added 7 new fields to source seq file                                                       IntegrateDev2                 \nSri Nannapaneni        08/13/2020    6131- PBM Replacement    Added LOB_IN field to source seq file                                          IntegrateDev2                Sravya Gorla             2020-12-09"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: CurrentDate
          type: string
          description: "Current Date"
        - name: RunCycle
          type: integer
          description: "Current cycle"
          defaultType: expr
          defaultValue: "1"
        - name: RunID
          type: string
          description: "Run ID"
        - name: SrcSysCd
          type: string
          description: "Source System Code"
        - name: SrcSysCdSk
          type: string
          description: "Source System Code Surrogate Key"
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
        - name: BCBSKCCommClmRemitHistExtr
          type: fileWriter
          urls: 
            - "#$FilePath#/key/BCBSKCCommClmRemitHistExtr_#SrcSysCd#.DrugClmRemitHist.dat.#RunID#"
          source: ClmRemitHistPK
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: B_CLM_REMIT_HIST
          type: fileWriter
          urls: 
            - "#$FilePath#/load/B_CLM_REMIT_HIST.#SrcSysCd#.dat.#RunID#"
          source: "Snapshot.V0S61P4"
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: BusinessRules
          type: map
          source: BCBSCommClmLand
          alias: BCBSKCComm
          stageVars: 
            - name: ClmId
              type: decimal
              width: 0
              map: "BCBSKCComm.CLM_ID"
              initValue: "0"
            - name: PkString
              type: decimal
              width: 0
              map: "SrcSysCd : \";\" : ClmId"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "'Y'"
              initValue: "'Y'"
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
              map: "'I'"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "'Y'"
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
              map: PkString
            - name: CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: ClmId
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
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
              map: "\"N\""
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: "\"N\""
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCComm.BILL_AMT"
            - name: COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCComm.OTHR_PAYOR_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCComm.COPAY_PCT_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCComm.FULL_AVG_WHLSL_PRICE"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCComm.COPAY_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "BCBSKCComm.DEDCT_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If BCBSKCComm.SRC_SYS_CD = 'OPTUMRX' Then Trim(BCBSKCComm.CLM_AMT) Else BCBSKCComm.COPAY_AMT"
            - name: WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: PCA_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
              map: "\"N\""
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: ClmId
              type: varchar
              width: 0
              map: ClmId
              dropStageVar: true
              initValue: "0"
            - name: PkString
              type: varchar
              width: 0
              map: PkString
              dropStageVar: true
              initValue: "0"
            - name: PassThru
              type: varchar
              width: 0
              map: PassThru
              dropStageVar: true
              initValue: "'Y'"
        - name: Snapshot.V0S61P2
          type: map
          source: BusinessRules
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
            - name: CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.CLM_REMIT_HIST_SK"
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
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.CLM_SK"
            - name: CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.CALC_ACTL_PD_AMT_IN"
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.SUPRESS_EOB_IN"
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.SUPRESS_REMIT_IN"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.ACTL_PD_AMT"
            - name: COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.COB_PD_AMT"
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
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.DEDCT_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.DSALW_AMT"
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.ER_COPAY_AMT"
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.INTRST_AMT"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.NO_RESP_AMT"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.PATN_RESP_AMT"
            - name: WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.WRTOFF_AMT"
            - name: PCA_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.PCA_PD_AMT"
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.ALT_CHRG_IN"
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.ALT_CHRG_PROV_WRTOFF_AMT"
        - name: Snapshot.V0S61P4
          type: map
          source: BusinessRules
          alias: Transform
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
              map: "Transform.CLM_ID"
          keys:
            - SRC_SYS_CD_SK
            - CLM_ID
        - name: ClmRemitHistPK
          type: sharedContainer
          containerName: ClmRemitHistPK
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
            - name: CLM_REMIT_HIST_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: CALC_ACTL_PD_AMT_IN
              type: char
              width: 1
              nullable: N
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COB_PD_AMT
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
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PCA_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
          source: Snapshot.V0S61P2


