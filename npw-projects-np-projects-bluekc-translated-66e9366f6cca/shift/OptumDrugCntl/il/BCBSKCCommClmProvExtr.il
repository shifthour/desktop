model:
  config:
    origin: datastage
  jobs:
    - name: BCBSKCCommClmProvExtr
      origin: datastage
      type: dataflow
      folder: "claim/provider/claim"
      runAfterSubrOnJobSuccess: enabled
      description: "BCBSKCComm Claim Provider"
      fullDescription: "COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\nCALLED BY:  MedtrakDrugExtrSeq\n\n\nDESCRIPTION:     Reads the ESIDrugFile.dat created in  ESIClmLand  job and puts the data into the claim  provider common record format and runs through primary key using Shared container ClmProvPkey\n    \n\nMODIFICATIONS:\n\nDeveloper                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       \n------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       \nKaushik Kapoor       2018-01-22        5828                      Initial Programming                                                                      IntegrateDev2                 Kalyan Neelam          2018-02-26 \n\nSudhir Bomshetty    2018-04-13        5781 - HEDIS        Added the NTNL_PROV_ID                                                       IntegrateDev2                 Kalyan Neelam          2018-04-25\n\nPeter Gichiri -  2019-10 -17   Added the stage Variables svSvcNtnlProvId  and svPcpNtnlProvId for SVC and PCP to populate NTNL_PROV_ID ,  \n                                       For the PRSCRB mapped PROV.TAX_ID for the PROV_ID associated with the row, udated the logic for the PROV_ID \n                                      as per OptumRx requirements                                                                                                   IntegrateDev1\n\n\nRekha Radhakrishna 2019-11-04    6131- PBM Replacement modified  NTNL_PROV_ID logic modified for PRSCRB, SVC and PCP output links\n\n\nSharon Anderw           2019-11-04     OptumRX             modified all provider reference queries to provide the right keys for provider matching        Kalyan Neelam          2019-11-21\n                                                                                      exluded  providers with NTNL_PROV_ID UNK and NA when the NTNL_PROV_ID was the field matched to\n                                                                                      included new functions that check if the Prescriber ID and Prescriber NPI are valid DEA and NPI numbers\n                                                                                      consolidated the Servicing and Prescriber look ups to the same hash files since they are all derived on same IDS Provider result sets.\n                                                                                      constructed a new hierarchy of rules for determining the ServicingProviderID/NPI and PrescribingProviderID/NPI\nchanged\n8;hf_bcbskccomm_clm_mbr_pcp;hf_bcbskccomm_clm_hmo_mbr_pcp;hf_bcbskccomm_clm_provnpi_lkup;hf_bcbskccomm_clm_prov_lkup;hf_bcbskccomm_prscrb_prov_npi_lkup;hf_min_bcbskccomm_clm_prov_prscrb;hf_bcbskccomm_prov_dea_lkup;hf_bcbskccomm_provdea_npi_lkup\nto \n7:hf_bcbskccomm_clm_mbr_pcp;hf_bcbskccomm_clm_hmo_mbr_pcp;hf_bcbskccomm_prscrb_prov_dea1;hf_bcbskccomm_prscrb_prov_dea2;hf_bcbskccomm_clm_prov_prscrb3;hf_bcbskccomm_clm_prov_prscrb4;hf_bcbskccomm_clm_prov_prscrb5\n\nSri Nannapaneni           2020-07-15      6131 PBM replacement      Added 7 new fields to source seq file                                          IntegrateDev2                 \nSri Nannapaneni           08/13/2020    6131- PBM Replacement    Added LOB_IN field to source seq file                                          IntegrateDev2      Sravya Gorla         2020-12-09\n\nSudeep Reddy             2024-03-20       US608683           Replace MAX logic based on requiremnt in StageName - ProvExtr            IntegrateDev2       Jeyaprasanna      2024-03-24\n                                                                                          of Link Name - PidNPINPI4,PidDeaProvNbr2,PidDeanCmnPrctProv1"
      parameters:
        - name: SrcSysCd
          type: string
          description: "Source System Code"
        - name: SrcSysCdSk
          type: string
          description: "Source System Code Surrogate Key"
        - name: CurrentDate
          type: string
          description: "Current Date"
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: RunID
          type: string
          description: "Run ID"
        - name: RunCycle
          type: integer
          description: "Current cycle"
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSInstance
          type: string
          description: "IDS Instance"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
      afterSubr:
        subrType: "DSU.HASH.CLEAR\\\\7:hf_bcbskccomm_clm_mbr_pcp;hf_bcbskccomm_clm_hmo_mbr_pcp;hf_bcbskccomm_prscrb_prov_dea1;hf_bcbskccomm_prscrb_prov_dea2;hf_bcbskccomm_clm_prov_prscrb3;hf_bcbskccomm_clm_prov_prscrb4;hf_bcbskccomm_clm_prov_prscrb5"
        subr: "7:hf_bcbskccomm_clm_mbr_pcp;hf_bcbskccomm_clm_hmo_mbr_pcp;hf_bcbskccomm_prscrb_prov_dea1;hf_bcbskccomm_prscrb_prov_dea2;hf_bcbskccomm_clm_prov_prscrb3;hf_bcbskccomm_clm_prov_prscrb4;hf_bcbskccomm_clm_prov_prscrb5"
      steps:
        - name: PDX_CLM_STD_INPT_Land
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
        - name: BcbskccommClmProvExtr
          type: fileWriter
          urls: 
            - "#$FilePath#/key/BCBSKCCommClmProvExtr.DrugClmProv.dat.#SrcSysCd#.#RunID#"
          source: ClmProvPK
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: B_CLM_PROV
          type: fileWriter
          urls: 
            - "#$FilePath#/load/B_CLM_PROV.#SrcSysCd#.dat.#RunID#"
          source: Transformer
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: hf_bcbskccomm_prscrb_prov_dea2
          type: map
          source: ""
          alias: PidDeaProvNbr2
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "PidDeaProvNbr2.DEA_NO"
            - name: DEA_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidDeaProvNbr2.DEA_NTNL_PROV_ID"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidDeaProvNbr2.PROV_ID"
            - name: PROV_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidDeaProvNbr2.PROV_NTNL_PROV_ID"
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidDeaProvNbr2.TAX_ID"
        - name: hf_bcbskccomm_prscrb_prov_dea1
          type: map
          source: ""
          alias: PidDeaCmnPrctProv1
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "PidDeaCmnPrctProv1.DEA_NO"
            - name: DEA_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidDeaCmnPrctProv1.DEA_NTNL_PROV_ID"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidDeaCmnPrctProv1.PROV_ID"
            - name: PROV_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidDeaCmnPrctProv1.PROV_NTNL_PROV_ID"
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidDeaCmnPrctProv1.TAX_ID"
        - name: hf_bcbskccomm_clm_prov_prscrb3.PidNPIProvLkup3
          type: map
          source: ""
          alias: PidNPIProv3
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPIProv3.PROV_ID"
            - name: PROV_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPIProv3.PROV_NTNL_PROV_ID"
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPIProv3.TAX_ID"
        - name: hf_bcbskccomm_clm_prov_prscrb3.PnpiNPIProvLkup5
          type: map
          source: ""
          alias: PidNPIProv3
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPIProv3.PROV_ID"
            - name: PROV_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPIProv3.PROV_NTNL_PROV_ID"
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPIProv3.TAX_ID"
        - name: hf_bcbskccomm_clm_prov_prscrb3.SvcProvIdLkup1
          type: map
          source: ""
          alias: PidNPIProv3
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPIProv3.PROV_ID"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPIProv3.NTNL_PROV_ID"
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPIProv3.TAX_ID"
        - name: hf_bcbskccomm_clm_mbr_pcp
          type: map
          source: ""
          alias: MbrPcpEffDt
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "MbrPcpEffDt.MBR_UNIQ_KEY"
            - name: ARGUS_FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "MbrPcpEffDt.ARGUS_FILL_DT_SK"
            - name: EFF_DT_SK
              type: char
              width: 10
              nullable: N
              map: "MbrPcpEffDt.EFF_DT_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "MbrPcpEffDt.PROV_ID"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "MbrPcpEffDt.NTNL_PROV_ID"
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
              map: "MbrPcpEffDt.TAX_ID"
            - name: TRGT_CD
              type: varchar
              width: 20
              map: "MbrPcpEffDt.TRGT_CD"
        - name: hf_bcbskccomm_clm_hmo_mbr_pcp
          type: map
          source: Transform
          alias: PCP
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "PCP.MBR_UNIQ_KEY"
            - name: ARGUS_FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "PCP.ARGUS_FILL_DT_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PCP.PROV_ID"
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
              map: "PCP.TAX_ID"
            - name: TRGT_CD
              type: varchar
              width: 20
              map: "PCP.TRGT_CD"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              map: "PCP.NTNL_PROV_ID"
        - name: hf_bcbskccomm_clm_prov_prscrb4.PidNPINPILkup4
          type: map
          source: ""
          alias: PidNPINPI4
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: PROV_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPINPI4.PROV_NTNL_PROV_ID"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPINPI4.PROV_ID"
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPINPI4.TAX_ID"
        - name: hf_bcbskccomm_clm_prov_prscrb4.PnpiNPINPILkup6
          type: map
          source: ""
          alias: PidNPINPI4
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: PROV_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPINPI4.PROV_NTNL_PROV_ID"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPINPI4.PROV_ID"
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPINPI4.TAX_ID"
        - name: hf_bcbskccomm_clm_prov_prscrb4.SvcPdxNoToNPILkp3
          type: map
          source: ""
          alias: PidNPINPI4
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: PROV_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPINPI4.PROV_NTNL_PROV_ID"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPINPI4.PROV_ID"
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPINPI4.TAX_ID"
        - name: hf_bcbskccomm_clm_prov_prscrb4.SvcNtnlProvIdLkup2
          type: map
          source: ""
          alias: PidNPINPI4
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPINPI4.NTNL_PROV_ID"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPINPI4.PROV_ID"
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
              map: "PidNPINPI4.TAX_ID"
        - name: hf_bcbskccomm_clm_prov_prscrb5
          type: map
          source: ""
          alias: PrscrbDeaNPI5
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: DEA_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PrscrbDeaNPI5.DEA_NTNL_PROV_ID"
        - name: MbrHmoExtr
          type: dbReader
          originalDb: db2
        - name: ProvExtr
          type: dbReader
          originalDb: db2
        - name: BusinessRules.lookup
          type: lookup
          stageVars:
            - name: ClmId
              type: decimal
              width: 0
              map: "Extract.CLM_ID"
              initValue: "0"
            - name: PkString
              type: decimal
              width: 0
              map: "SrcSysCd : \";\" : ClmId"
              initValue: "0"
            - name: CurDateTime
              type: decimal
              width: 0
              map: CurrentDate
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "'Y'"
              initValue: "'Y'"
            - name: svSvcProvID
              type: decimal
              width: 0
              map: " if IsNull(SvcProvIdLkup1.PROV_ID) = @FALSE then Extract.PDX_NO else  if IsNull(SvcNtnlProvIdLkup2.PROV_ID) = @FALSE then Trim(SvcNtnlProvIdLkup2.PROV_ID) else  if IsNull(SvcPdxNoToNPILkp3.PROV_ID) = @FALSE then Trim(SvcPdxNoToNPILkp3.PROV_ID) else  if (Len(Trim(Extract.PDX_NO)) > 0 and Trim(Extract.PDX_NO) <> 'NA') THEN Trim(Extract.PDX_NO) else  if ProviderNPIValidator(Extract.PDX_NTNL_PROV_ID) = 'Y' then Extract.PDX_NTNL_PROV_ID else  If Len(Trim(Extract.PDX_NO)) > 0 THEN Trim(Extract.PDX_NO) else  if Len(Trim(Extract.PDX_NTNL_PROV_ID)) > 0 then Trim(Extract.PDX_NTNL_PROV_ID) else \"NA\""
              initValue: "\"NA\""
            - name: svSvcNtnlProvId
              type: decimal
              width: 0
              map: " If (ProviderNPIValidator(Extract.PDX_NTNL_PROV_ID) = 'Y') then Extract.PDX_NTNL_PROV_ID Else  If (IsNull(SvcProvIdLkup1.NTNL_PROV_ID) = @FALSE and ProviderNPIValidator(SvcProvIdLkup1.NTNL_PROV_ID) = 'Y') then SvcProvIdLkup1.NTNL_PROV_ID Else  If (ProviderNPIValidator(Extract.PDX_NO) = 'Y') then Extract.PDX_NO Else \"NA\""
              initValue: "\"NA\""
            - name: svPrscrbIDDEA
              type: decimal
              width: 0
              map: "ProviderDEAValidator(Trim(Extract.PRSCRBR_ID))"
              initValue: "0"
            - name: svPrscrbIDNPI
              type: decimal
              width: 0
              map: "ProviderNPIValidator(Trim(Extract.PRSCRBR_ID))"
              initValue: "0"
            - name: svPrscrbNtnlProvIDNPI
              type: decimal
              width: 0
              map: "ProviderNPIValidator(Trim(Extract.PRSCRBR_NTNL_PROV_ID))"
              initValue: "0"
            - name: svPrscrbrProviderIDtoUse
              type: decimal
              width: 0
              map: " If svPrscrbIDDEA = 'Y' and IsNull(PidDeaCmnPrctProvLkup1.DEA_NO) = @FALSE then Trim(PidDeaCmnPrctProvLkup1.PROV_ID) else  If svPrscrbIDDEA = 'Y' and IsNull(PidDeaProvNbrLkup2.DEA_NO) = @FALSE then Trim(PidDeaProvNbrLkup2.PROV_ID) else  If svPrscrbIDNPI = 'Y' and IsNull(PidNPIProvLkup3.PROV_ID) = @FALSE then PidNPIProvLkup3.PROV_ID Else  If svPrscrbIDNPI = 'Y' and IsNull(PidNPINPILkup4.PROV_NTNL_PROV_ID) = @FALSE then PidNPINPILkup4.PROV_ID Else  If svPrscrbNtnlProvIDNPI = 'Y' and IsNull(PnpiNPIProvLkup5.PROV_ID) = @FALSE then PnpiNPIProvLkup5.PROV_ID else  If svPrscrbNtnlProvIDNPI = 'Y' and IsNull(PnpiNPINPILkup6.PROV_NTNL_PROV_ID) = @FALSE then PnpiNPINPILkup6.PROV_ID else  If svPrscrbIDDEA = 'Y' and IsNull(PrscrbDeaNPILkup5.DEA_NTNL_PROV_ID) = @FALSE and ProviderNPIValidator(Trim(PrscrbDeaNPILkup5.DEA_NTNL_PROV_ID)) = 'Y' then Trim(PrscrbDeaNPILkup5.DEA_NTNL_PROV_ID) Else  If svPrscrbIDNPI = 'Y' then Extract.PRSCRBR_ID Else  If svPrscrbNtnlProvIDNPI = 'Y' then Extract.PRSCRBR_NTNL_PROV_ID else 'NA'"
              initValue: "0"
            - name: svPrscrbNtnlProvIDtoUse
              type: decimal
              width: 0
              map: " If svPrscrbNtnlProvIDNPI = 'Y' then Extract.PRSCRBR_NTNL_PROV_ID Else  If svPrscrbIDNPI = 'Y' then Extract.PRSCRBR_ID Else  If svPrscrbIDDEA = 'Y' and IsNull(PidDeaCmnPrctProvLkup1.DEA_NO) = @FALSE and ProviderNPIValidator(PidDeaCmnPrctProvLkup1.DEA_NTNL_PROV_ID) = 'Y' then PidDeaCmnPrctProvLkup1.DEA_NTNL_PROV_ID Else  If svPrscrbIDDEA = 'Y' and IsNull(PidDeaCmnPrctProvLkup1.DEA_NO) = @FALSE and ProviderNPIValidator(PidDeaCmnPrctProvLkup1.PROV_NTNL_PROV_ID) = 'Y' then PidDeaCmnPrctProvLkup1.PROV_NTNL_PROV_ID Else  If svPrscrbIDDEA = 'Y' and IsNull(PidDeaProvNbrLkup2.DEA_NO) = @FALSE and ProviderNPIValidator(PidDeaProvNbrLkup2.DEA_NTNL_PROV_ID) = 'Y' Then PidDeaProvNbrLkup2.DEA_NTNL_PROV_ID else 'NA'"
              initValue: "0"
            - name: svPrscrbTaxIDtoUse
              type: decimal
              width: 0
              map: " If svPrscrbIDDEA = 'Y' and IsNull(PidDeaCmnPrctProvLkup1.DEA_NO) = @FALSE then Trim(PidDeaCmnPrctProvLkup1.TAX_ID) else  If svPrscrbIDDEA = 'Y' and IsNull(PidDeaProvNbrLkup2.DEA_NO) = @FALSE then Trim(PidDeaProvNbrLkup2.TAX_ID) else  If svPrscrbIDNPI = 'Y' and IsNull(PidNPIProvLkup3.PROV_ID) = @FALSE then PidNPIProvLkup3.TAX_ID Else  If svPrscrbIDNPI = 'Y' and IsNull(PidNPINPILkup4.PROV_NTNL_PROV_ID) = @FALSE then PidNPINPILkup4.TAX_ID Else  If svPrscrbNtnlProvIDNPI = 'Y' and IsNull(PnpiNPIProvLkup5.PROV_ID) = @FALSE then PnpiNPIProvLkup5.TAX_ID else  If svPrscrbNtnlProvIDNPI = 'Y' and IsNull(PnpiNPINPILkup6.PROV_NTNL_PROV_ID) = @FALSE then PnpiNPINPILkup6.TAX_ID else 'NA'"
              initValue: "0"
          option:
            joinType: left
          sources: 
            - name: PDX_CLM_STD_INPT_Land
              alias: Extract
              joinFilter: ""
              joinExpression: []
            - name: hf_bcbskccomm_clm_hmo_mbr_pcp
              alias: PCPLkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "PCPLkup.MBR_UNIQ_KEY"
                toExpr: "Extract.ORIG_MBR"
                caseInSensitive: true
              - fromExpr: "PCPLkup.ARGUS_FILL_DT_SK"
                toExpr: "Extract.FILL_DT"
                caseInSensitive: true
            - name: hf_bcbskccomm_clm_prov_prscrb3.SvcProvIdLkup1
              alias: SvcProvIdLkup1
              joinFilter: ""
              joinExpression:
              - fromExpr: "SvcProvIdLkup1.PROV_ID"
                toExpr: "Extract.PDX_NO"
                caseInSensitive: true
            - name: hf_bcbskccomm_clm_prov_prscrb4.SvcNtnlProvIdLkup2
              alias: SvcNtnlProvIdLkup2
              joinFilter: ""
              joinExpression:
              - fromExpr: "SvcNtnlProvIdLkup2.NTNL_PROV_ID"
                toExpr: "Extract.PDX_NTNL_PROV_ID"
                caseInSensitive: true
            - name: hf_bcbskccomm_prscrb_prov_dea2
              alias: PidDeaProvNbrLkup2
              joinFilter: ""
              joinExpression:
              - fromExpr: "PidDeaProvNbrLkup2.DEA_NO"
                toExpr: "Extract.PRSCRBR_ID"
                caseInSensitive: true
            - name: hf_bcbskccomm_prscrb_prov_dea1
              alias: PidDeaCmnPrctProvLkup1
              joinFilter: ""
              joinExpression:
              - fromExpr: "PidDeaCmnPrctProvLkup1.DEA_NO"
                toExpr: "Extract.PRSCRBR_ID"
                caseInSensitive: true
            - name: hf_bcbskccomm_clm_prov_prscrb3.PidNPIProvLkup3
              alias: PidNPIProvLkup3
              joinFilter: ""
              joinExpression:
              - fromExpr: "PidNPIProvLkup3.PROV_ID"
                toExpr: "Extract.PRSCRBR_ID"
                caseInSensitive: true
            - name: hf_bcbskccomm_clm_prov_prscrb4.PidNPINPILkup4
              alias: PidNPINPILkup4
              joinFilter: ""
              joinExpression:
              - fromExpr: "PidNPINPILkup4.PROV_NTNL_PROV_ID"
                toExpr: "Extract.PRSCRBR_ID"
                caseInSensitive: true
            - name: hf_bcbskccomm_clm_prov_prscrb4.SvcPdxNoToNPILkp3
              alias: SvcPdxNoToNPILkp3
              joinFilter: ""
              joinExpression:
              - fromExpr: "SvcPdxNoToNPILkp3.PROV_NTNL_PROV_ID"
                toExpr: "Extract.PDX_NO"
                caseInSensitive: true
            - name: hf_bcbskccomm_clm_prov_prscrb5
              alias: PrscrbDeaNPILkup5
              joinFilter: ""
              joinExpression:
              - fromExpr: "PrscrbDeaNPILkup5.DEA_NTNL_PROV_ID"
                toExpr: "Extract.PRSCRBR_ID"
                caseInSensitive: true
            - name: hf_bcbskccomm_clm_prov_prscrb3.PnpiNPIProvLkup5
              alias: PnpiNPIProvLkup5
              joinFilter: ""
              joinExpression:
              - fromExpr: "PnpiNPIProvLkup5.PROV_ID"
                toExpr: "Extract.PRSCRBR_NTNL_PROV_ID"
                caseInSensitive: true
            - name: hf_bcbskccomm_clm_prov_prscrb4.PnpiNPINPILkup6
              alias: PnpiNPINPILkup6
              joinFilter: ""
              joinExpression:
              - fromExpr: "PnpiNPINPILkup6.PROV_NTNL_PROV_ID"
                toExpr: "Extract.PRSCRBR_NTNL_PROV_ID"
                caseInSensitive: true
          joinCondition: 
            - expression: "PCPLkup.MBR_UNIQ_KEY = Extract.ORIG_MBR"
              caseInSensitive: true
            - expression: "PCPLkup.ARGUS_FILL_DT_SK = Extract.FILL_DT"
              caseInSensitive: true
            - expression: "SvcProvIdLkup1.PROV_ID = Extract.PDX_NO"
              caseInSensitive: true
            - expression: "SvcNtnlProvIdLkup2.NTNL_PROV_ID = Extract.PDX_NTNL_PROV_ID"
              caseInSensitive: true
            - expression: "PidDeaProvNbrLkup2.DEA_NO = Extract.PRSCRBR_ID"
              caseInSensitive: true
            - expression: "PidDeaCmnPrctProvLkup1.DEA_NO = Extract.PRSCRBR_ID"
              caseInSensitive: true
            - expression: "PidNPIProvLkup3.PROV_ID = Extract.PRSCRBR_ID"
              caseInSensitive: true
            - expression: "PidNPINPILkup4.PROV_NTNL_PROV_ID = Extract.PRSCRBR_ID"
              caseInSensitive: true
            - expression: "SvcPdxNoToNPILkp3.PROV_NTNL_PROV_ID = Extract.PDX_NO"
              caseInSensitive: true
            - expression: "PrscrbDeaNPILkup5.DEA_NTNL_PROV_ID = Extract.PRSCRBR_ID"
              caseInSensitive: true
            - expression: "PnpiNPIProvLkup5.PROV_ID = Extract.PRSCRBR_NTNL_PROV_ID"
              caseInSensitive: true
            - expression: "PnpiNPINPILkup6.PROV_NTNL_PROV_ID = Extract.PRSCRBR_NTNL_PROV_ID"
              caseInSensitive: true
          schema: 
            - name: SVC_JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: SVC_INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "'I'"
            - name: SVC_DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: SVC_PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: PassThru
            - name: SVC_FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: CurDateTime
            - name: SVC_ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "0"
            - name: SVC_RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "0"
            - name: SVC_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: SrcSysCd
            - name: SVC_PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "PkString : \";\" : \"SVC\""
            - name: SVC_CLM_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: SVC_CLM_ID
              type: char
              width: 18
              nullable: N
              map: ClmId
            - name: SVC_CLM_PROV_ROLE_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "\"SVC\""
            - name: SVC_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: SVC_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              map: "0"
            - name: SVC_PROV_ID
              type: char
              width: 12
              nullable: N
              map: svSvcProvID
            - name: SVC_TAX_ID
              type: char
              width: 9
              map: "'NA'"
            - name: SVC_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: svSvcNtnlProvId
            - name: PCP_JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PCP_INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "'I'"
            - name: PCP_DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: PCP_PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: PassThru
            - name: PCP_FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: CurDateTime
            - name: PCP_ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "0"
            - name: PCP_RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "0"
            - name: PCP_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: SrcSysCd
            - name: PCP_PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "PkString : \";\" : \"PCP\""
            - name: PCP_CLM_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PCP_CLM_ID
              type: char
              width: 18
              nullable: N
              map: ClmId
            - name: PCP_CLM_PROV_ROLE_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "\"PCP\""
            - name: PCP_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PCP_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              map: "0"
            - name: PCP_PROV_ID
              type: char
              width: 12
              nullable: N
              map: " If IsNull(Trim(PCPLkup.PROV_ID)) = @TRUE Then \"NA\" Else Trim(PCPLkup.PROV_ID)"
            - name: PCP_TAX_ID
              type: char
              width: 9
              map: " If IsNull(Trim(PCPLkup.TAX_ID)) = @TRUE Then \"NA\" Else Trim(PCPLkup.TAX_ID)"
            - name: PCP_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: " If (IsNull(PCPLkup.NTNL_PROV_ID) = @FALSE and ProviderNPIValidator(PCPLkup.NTNL_PROV_ID) = 'Y') then PCPLkup.NTNL_PROV_ID else \"NA\""
            - name: PRSCRB_JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PRSCRB_INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "'I'"
            - name: PRSCRB_DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: PRSCRB_PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: PassThru
            - name: PRSCRB_FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: CurDateTime
            - name: PRSCRB_ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "0"
            - name: PRSCRB_RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "0"
            - name: PRSCRB_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: SrcSysCd
            - name: PRSCRB_PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "PkString : \";\" : \"PRSCRB\""
            - name: PRSCRB_CLM_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PRSCRB_CLM_ID
              type: char
              width: 18
              nullable: N
              map: ClmId
            - name: PRSCRB_CLM_PROV_ROLE_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "\"PRSCRB\""
            - name: PRSCRB_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PRSCRB_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              map: "0"
            - name: PRSCRB_PROV_ID
              type: char
              width: 12
              nullable: N
              map: svPrscrbrProviderIDtoUse
            - name: PRSCRB_TAX_ID
              type: char
              width: 9
              map: svPrscrbTaxIDtoUse
            - name: PRSCRB_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: svPrscrbNtnlProvIDtoUse
            - name: filter_PCP
              type: smallint
              width: 2
              nullable: N
              map: "if (Not(IsNull(PCPLkup.MBR_UNIQ_KEY)) And Len(Trim(PCPLkup.MBR_UNIQ_KEY)) > 0) then 1 else 0"
        - name: BusinessRules.V0S20P12.filter
          type: filter
          source: BusinessRules.lookup
          filter: "filter_PCP = 1"
          schema: 
            - name: PCP_JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
            - name: PCP_INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
            - name: PCP_DISCARD_IN
              type: char
              width: 1
              nullable: N
            - name: PCP_PASS_THRU_IN
              type: char
              width: 1
              nullable: N
            - name: PCP_FIRST_RECYC_DT
              type: timestamp
              nullable: N
            - name: PCP_ERR_CT
              type: smallint
              width: 2
              nullable: N
            - name: PCP_RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
            - name: PCP_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: PCP_PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
            - name: PCP_CLM_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PCP_CLM_ID
              type: char
              width: 18
              nullable: N
            - name: PCP_CLM_PROV_ROLE_TYP_CD
              type: char
              width: 10
              nullable: N
            - name: PCP_CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: PCP_LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
            - name: PCP_PROV_ID
              type: char
              width: 12
              nullable: N
            - name: PCP_TAX_ID
              type: char
              width: 9
            - name: PCP_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
        - name: BusinessRules.V0S20P3
          type: map
          source: BusinessRules.lookup
          alias: SVC
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "SVC.SVC_JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "SVC.SVC_INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "SVC.SVC_DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "SVC.SVC_PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "SVC.SVC_FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "SVC.SVC_ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "SVC.SVC_RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "SVC.SVC_SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "SVC.SVC_PRI_KEY_STRING"
            - name: CLM_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "SVC.SVC_CLM_PROV_SK"
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
              map: "SVC.SVC_CLM_ID"
            - name: CLM_PROV_ROLE_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "SVC.SVC_CLM_PROV_ROLE_TYP_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "SVC.SVC_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              map: "SVC.SVC_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
              map: "SVC.SVC_PROV_ID"
            - name: TAX_ID
              type: char
              width: 9
              map: "SVC.SVC_TAX_ID"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "SVC.SVC_NTNL_PROV_ID"
        - name: BusinessRules.V0S20P12
          type: map
          source: BusinessRules.V0S20P12.filter
          alias: PCP
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "PCP.PCP_JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "PCP.PCP_INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "PCP.PCP_DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "PCP.PCP_PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "PCP.PCP_FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "PCP.PCP_ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "PCP.PCP_RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "PCP.PCP_SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "PCP.PCP_PRI_KEY_STRING"
            - name: CLM_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "PCP.PCP_CLM_PROV_SK"
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
              map: "PCP.PCP_CLM_ID"
            - name: CLM_PROV_ROLE_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "PCP.PCP_CLM_PROV_ROLE_TYP_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "PCP.PCP_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              map: "PCP.PCP_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
              map: "PCP.PCP_PROV_ID"
            - name: TAX_ID
              type: char
              width: 9
              map: "PCP.PCP_TAX_ID"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PCP.PCP_NTNL_PROV_ID"
        - name: BusinessRules.V0S20P17
          type: map
          source: BusinessRules.lookup
          alias: PRSCRB
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "PRSCRB.PRSCRB_JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "PRSCRB.PRSCRB_INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "PRSCRB.PRSCRB_DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "PRSCRB.PRSCRB_PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "PRSCRB.PRSCRB_FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "PRSCRB.PRSCRB_ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "PRSCRB.PRSCRB_RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "PRSCRB.PRSCRB_SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "PRSCRB.PRSCRB_PRI_KEY_STRING"
            - name: CLM_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "PRSCRB.PRSCRB_CLM_PROV_SK"
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
              map: "PRSCRB.PRSCRB_CLM_ID"
            - name: CLM_PROV_ROLE_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "PRSCRB.PRSCRB_CLM_PROV_ROLE_TYP_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "PRSCRB.PRSCRB_CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              map: "PRSCRB.PRSCRB_LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
              map: "PRSCRB.PRSCRB_PROV_ID"
            - name: TAX_ID
              type: char
              width: 9
              map: "PRSCRB.PRSCRB_TAX_ID"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "PRSCRB.PRSCRB_NTNL_PROV_ID"
        - name: Transform
          type: lookup
          option:
            joinType: left
          sources: 
            - name: 
              alias: HmoExtr
              joinFilter: ""
              joinExpression: []
            - name: hf_bcbskccomm_clm_mbr_pcp
              alias: MbrPcpLkup
              joinFilter: ""
              joinExpression:
              - fromExpr: "MbrPcpLkup.MBR_UNIQ_KEY"
                toExpr: "HmoExtr.MBR_UNIQ_KEY"
                caseInSensitive: true
              - fromExpr: "MbrPcpLkup.ARGUS_FILL_DT_SK"
                toExpr: "HmoExtr.ARGUS_FILL_DT_SK"
                caseInSensitive: true
              - fromExpr: "MbrPcpLkup.EFF_DT_SK"
                toExpr: "HmoExtr.EFF_DT_SK"
                caseInSensitive: true
          joinCondition: 
            - expression: "MbrPcpLkup.MBR_UNIQ_KEY = HmoExtr.MBR_UNIQ_KEY"
              caseInSensitive: true
            - expression: "MbrPcpLkup.ARGUS_FILL_DT_SK = HmoExtr.ARGUS_FILL_DT_SK"
              caseInSensitive: true
            - expression: "MbrPcpLkup.EFF_DT_SK = HmoExtr.EFF_DT_SK"
              caseInSensitive: true
          schema: 
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "HmoExtr.MBR_UNIQ_KEY"
            - name: ARGUS_FILL_DT_SK
              type: char
              width: 10
              nullable: N
              map: "HmoExtr.ARGUS_FILL_DT_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(MbrPcpLkup.PROV_ID) = @TRUE or len(MbrPcpLkup.PROV_ID) = 0 then 'UNK' else MbrPcpLkup.PROV_ID"
            - name: TAX_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(MbrPcpLkup.TAX_ID) = @TRUE or len(MbrPcpLkup.TAX_ID) = 0 then 'NA' else MbrPcpLkup.TAX_ID"
            - name: TRGT_CD
              type: varchar
              width: 20
              map: " If IsNull(MbrPcpLkup.TRGT_CD) = @TRUE or len(MbrPcpLkup.TRGT_CD) = 0 or MbrPcpLkup.TRGT_CD <> 'PRI' then 'UNK' else 'PCP'"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "MbrPcpLkup.NTNL_PROV_ID"
        - name: Snapshot.V0S139P3
          type: map
          source: Link_Collector
          alias: Trans
          schema: 
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
              map: "Trans.CLM_ID"
            - name: CLM_PROV_ROLE_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "Trans.CLM_PROV_ROLE_TYP_CD"
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
              map: "Trans.PROV_ID"
          keys:
            - CLM_ID
            - CLM_PROV_ROLE_TYP_CD
        - name: Snapshot.V0S139P4
          type: map
          source: Link_Collector
          alias: Trans
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
              map: "Trans.CLM_ID"
            - name: CLM_PROV_ROLE_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Trans.CLM_PROV_ROLE_TYP_CD"
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Trans.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Trans.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Trans.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Trans.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Trans.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Trans.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Trans.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Trans.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Trans.PRI_KEY_STRING"
            - name: CLM_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "Trans.CLM_PROV_SK"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Trans.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              map: "Trans.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Trans.PROV_ID"
            - name: TAX_ID
              type: char
              width: 9
              map: "Trans.TAX_ID"
            - name: SVC_FCLTY_LOC_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(Trans.NTNL_PROV_ID) then 'NA' else Trans.NTNL_PROV_ID"
          keys:
            - SRC_SYS_CD_SK
            - CLM_ID
            - CLM_PROV_ROLE_TYP_CD
        - name: Snapshot.V0S139P6
          type: map
          source: Link_Collector
          alias: Trans
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
              map: "Trans.CLM_ID"
            - name: CLM_PROV_ROLE_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Trans.CLM_PROV_ROLE_TYP_CD"
          keys:
            - SRC_SYS_CD_SK
            - CLM_ID
            - CLM_PROV_ROLE_TYP_CD
        - name: Transformer
          type: map
          source: Snapshot.V0S139P3
          alias: SnapShot
          stageVars: 
            - name: svClmProvRole
              type: decimal
              width: 0
              map: "GetFkeyCodes(SrcSysCd, 0, \"CLAIM PROVIDER ROLE TYPE\", SnapShot.CLM_PROV_ROLE_TYP_CD, 'X')"
              initValue: "0"
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSk
            - name: CLM_ID
              type: char
              width: 18
              nullable: N
              map: "SnapShot.CLM_ID"
            - name: CLM_PROV_ROLE_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: svClmProvRole
            - name: PROV_ID
              type: char
              width: 12
              nullable: N
              map: "SnapShot.PROV_ID"
            - name: svClmProvRole
              type: varchar
              width: 0
              map: svClmProvRole
              dropStageVar: true
              initValue: "0"
        - name: Link_Collector
          type: unknown
          source: BusinessRules.lookupBcbskccommClmProvExtrTransformB_CLM_PROV
          alias: 
          origType: CCollector
        - name: ClmProvPK
          type: sharedContainer
          containerName: ClmProvPK
          parameters:
            - name: "FilePath"
              expression: "#$FilePath#"
            - name: "CurrRunCycle"
              expression: "#RunCycle#"
            - name: "SrcSysCd"
              expression: "#SrcSysCd#"
            - name: "IDSDB"
              expression: "#$IDSDB#"
            - name: "IDSOwner"
              expression: "#$IDSOwner#"
            - name: "IDSAcct"
              expression: "#$IDSAcct#"
            - name: "IDSPW"
              expression: "#$IDSPW#"
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
            - name: CLM_PROV_SK
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
            - name: CLM_PROV_ROLE_TYP_CD
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
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: TAX_ID
              type: char
              width: 9
              nullable: N
            - name: SVC_FCLTY_LOC_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
          source: BusinessRules.lookupBcbskccommClmProvExtrTransformB_CLM_PROV


