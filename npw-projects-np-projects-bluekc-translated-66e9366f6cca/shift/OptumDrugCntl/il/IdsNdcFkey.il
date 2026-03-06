model:
  config:
    origin: datastage
  jobs:
    - name: IdsNdcFkey
      origin: datastage
      type: dataflow
      folder: "codes/ndc/ndc"
      runAfterSubrOnJobSuccess: enabled
      description: "IDS NDC  Data Foreign Keying.  "
      fullDescription: "* VC LOGS *\n^1_1 01/28/08 08:28:47 Batch  14638_30532 INIT bckcetl ids20 dsadm dsadm\n^1_1 12/26/07 11:16:35 Batch  14605_40601 INIT bckcetl ids20 dsadm dsadm\n^1_1 11/21/07 14:41:36 Batch  14570_52900 INIT bckcetl ids20 dsadm dsadm\n^1_1 08/21/07 12:23:43 Batch  14478_44630 INIT bckcetl ids20 dsadm dsadm\n^1_4 04/25/07 15:48:06 Batch  14360_56891 PROMOTE bckcetl ids20 dsadm bls for sa\n^1_4 04/25/07 15:45:35 Batch  14360_56742 PROMOTE bckcetl ids20 dsadm bls for sa\n^1_4 04/25/07 15:34:30 Batch  14360_56074 INIT bckcett testIDS30 dsadm bls for s andrew\n^1_2 04/11/07 11:23:01 Batch  14346_40989 PROMOTE bckcett testIDS30 u03651 steph for sharon\n^1_2 04/11/07 11:18:19 Batch  14346_40703 INIT bckcett devlIDS30 u03651 steffy\n^1_1 03/30/07 12:49:49 Batch  14334_46195 PROMOTE bckcett devlIDS30 u10157 sa\n^1_1 03/30/07 12:44:38 Batch  14334_45880 INIT bckcetl ids20 dcg01 sa\n^1_3 03/21/07 15:26:34 Batch  14325_55600 INIT bckcetl ids20 dsadm dsadm\n^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm\n^1_3 06/05/06 15:07:57 Batch  14036_54483 PROMOTE bckcetl ids20 dsadm Gina\n^1_3 06/05/06 14:57:55 Batch  14036_53885 INIT bckcett testIDS30 dsadm J. Mahaffey for B. Leland\n^1_11 12/13/05 13:55:16 Batch  13862_50122 INIT bckcett devlIDS30 u08717 Brent\n^1_6 12/05/05 10:59:01 Batch  13854_39546 INIT bckcett devlIDS30 dsadm Gina Parr\n^1_10 11/30/05 16:50:19 Batch  13849_60624 INIT bckcett devlIDS30 u05779 bj\n\n\nCOPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \nCOPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\n\nJOB NAME:     IdsNdcFkey\n\nDESCRIPTION:     Takes the sorted file and applies the Foreign key.  \n      \n\nINPUTS:  File from IdsNdcPkey\n\x09\n  \nHASH FILES:     hf_recycle - contains records that had errors on them for recycle.  Recycle is not used in this program - file is completely updated from Facets each time.\n\n\nTRANSFORMS:  GetFkeyCodes for diagnosis category code\n                                 \n                           Unknown and Not Applicable rows are created - all rows are sent through collector to combine into output file\n                           \n\nPROCESSING:  This job would be run whenever there is an update to the NDC table/file.  \n\nOUTPUTS:  NDC.#Source#.dat\n\nMODIFICATIONS:\n            Steph Goddard  08/2004  -   Originally Programmed\n            BJ Luce             06/15/2005 - changed for rx deductibles \n            BJ Luce             10/31/2005 - add new fields\n                                                            use FDB for source system for code mapping lookups\n                                                            use UWS for source system for code mapping lookup for DSM drug type code\n           BJ Luce             04/2006         use environment variables, use Source in output\n           Bhoomi              03/29/2007    Added new field to the table called NDC_RTE_TYP_CD_SK by directly mapping to GCRT2, which is brought from RROUTED3_ROUTE_DESC and looking up at NDC ROUTE TYPE domain                                                             from code mapping table in foreign key process."
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: InFile
          type: string
          description: InFile
          defaultType: expr
          defaultValue: "IdsNewNdcExtr.TMP"
        - name: Logging
          type: string
          description: Logging
          defaultType: expr
          defaultValue: N
        - name: Source
          type: string
          description: source
      steps:
        - name: NdcCrf
          type: fileReader
          urls: 
            - "#$FilePath#/key/#InFile#"
          rejectMode: continue
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
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
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
            - name: NDC
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
            - name: AHFS_TCC_CD
              type: char
              width: 10
              nullable: N
            - name: DOSE_FORM
              type: char
              width: 2
              nullable: N
            - name: TCC_CD
              type: char
              width: 10
              nullable: N
            - name: DSM_DRUG_TYP_CD
              type: char
              width: 10
              nullable: N
            - name: NDC_DRUG_ABUSE_CTL_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_DRUG_CLS_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_DRUG_FORM_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_FMT_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_GNRC_MNFCTR_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_GNRC_NMD_DRUG_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_GNRC_PRICE_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_GNRC_PRICE_SPREAD_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_ORANGE_BOOK_CD
              type: char
              width: 1
              nullable: N
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
            - name: DESI_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: DRUG_MNTN_IN
              type: char
              width: 1
              nullable: N
            - name: INNVTR_IN
              type: char
              width: 1
              nullable: N
            - name: INSTUT_PROD_IN
              type: char
              width: 1
              nullable: N
            - name: PRIV_LBLR_IN
              type: char
              width: 1
              nullable: N
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
            - name: UNIT_DOSE_IN
              type: char
              width: 1
              nullable: N
            - name: UNIT_OF_USE_IN
              type: char
              width: 1
              nullable: N
            - name: AVG_WHLSL_PRICE_CHG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: GNRC_PRICE_IN_CHG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: OBSLT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SRC_NDC_CRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SRC_NDC_UPDT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: BRND_NM
              type: varchar
              width: 255
            - name: CORE_NINE_NO
              type: varchar
              width: 20
            - name: DRUG_LABEL_NM
              type: varchar
              width: 35
            - name: DRUG_STRG_DESC
              type: varchar
              width: 255
            - name: GCN_CD_TX
              type: varchar
              width: 20
            - name: GNRC_NM_SH_DESC
              type: varchar
              width: 255
            - name: LBLR_NM
              type: varchar
              width: 75
            - name: LBLR_NO
              type: varchar
              width: 20
            - name: NEEDLE_GAUGE_VAL
              type: varchar
              width: 20
            - name: NEEDLE_LGTH_VAL
              type: varchar
              width: 20
            - name: PCKG_DESC
              type: varchar
              width: 255
            - name: PCKG_SIZE_EQVLNT_NO
              type: varchar
              width: 20
            - name: PCKG_SIZE_NO
              type: varchar
              width: 20
            - name: PROD_NO
              type: varchar
              width: 20
            - name: SYRNG_CPCT_VAL
              type: varchar
              width: 20
            - name: NDC_RTE_TYP_CD
              type: char
              width: 10
              nullable: N
          keys:
            - NDC_SK
        - name: NdcOut
          type: fileWriter
          urls: 
            - "#$FilePath#/load/NDC.#Source#.dat"
          source: NDCData
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: RecycleNdc
          type: dbWriter
          originalDb: hashedFile
          table: "/hf_recycle"
          tableAction: append
          source: "PurgeTrn.V0S20P4"
            # Hashed File has no directory specified
        - name: PurgeTrn.V0S20P3.map
          type: map
          source: NdcCrf
          alias: NdcCrfIn
          stageVars: 
            - name: NdcAHFSClass
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyAhfsTcc(NdcCrfIn.NDC_SK, NdcCrfIn.AHFS_TCC_CD, Logging)"
              initValue: "-1"
            - name: NdcTherCls
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyTCC(NdcCrfIn.NDC_SK, NdcCrfIn.TCC_CD, Logging)"
              initValue: "-1"
            - name: NdcDoseForm
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDoseForm(NdcCrfIn.NDC_SK, NdcCrfIn.DOSE_FORM, Logging)"
              initValue: "1"
            - name: DSMDrugType
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"UWS\", NdcCrfIn.NDC_SK, 'NDC DISEASE STATE MANAGEMENT DRUG TYPE', NdcCrfIn.DSM_DRUG_TYP_CD, Logging)"
              initValue: "-1"
            - name: DrugAbuseCtlCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC DRUG ABUSE CONTROL', NdcCrfIn.NDC_DRUG_ABUSE_CTL_CD, Logging)"
              initValue: "-1"
            - name: DrugClsCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC DRUG CLASS', NdcCrfIn.NDC_DRUG_CLS_CD, Logging)"
              initValue: "-1"
            - name: DrugFormCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC DRUG FORM', NdcCrfIn.NDC_DRUG_FORM_CD, Logging)"
              initValue: "-1"
            - name: FmtCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC FORMAT', NdcCrfIn.NDC_FMT_CD, Logging)"
              initValue: "-1"
            - name: GnrcMnfctrCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC MANUFACTURER', NdcCrfIn.NDC_GNRC_MNFCTR_CD, Logging)"
              initValue: "-1"
            - name: GnrcNmdDrugCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC NAME', NdcCrfIn.NDC_GNRC_NMD_DRUG_CD, Logging)"
              initValue: "-1"
            - name: GnrcPriceCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC PRICE', NdcCrfIn.NDC_GNRC_PRICE_CD, Logging)"
              initValue: "-1"
            - name: GnrcPriceSpreadCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC PRICE SPREAD', NdcCrfIn.NDC_GNRC_PRICE_SPREAD_CD, Logging)"
              initValue: "-1"
            - name: OrangeBookCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC ORANGE BOOK', NdcCrfIn.NDC_ORANGE_BOOK_CD, Logging)"
              initValue: "-1"
            - name: NdcRteTypCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC ROUTE TYPE', NdcCrfIn.NDC_RTE_TYP_CD, Logging)"
              initValue: "-1"
            - name: AvgWhlslPriceChgDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.AVG_WHLSL_PRICE_CHG_DT_SK, Logging)"
              initValue: "0"
            - name: GnrcPricInChgDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.GNRC_PRICE_IN_CHG_DT_SK, Logging)"
              initValue: "0"
            - name: ObsltDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.OBSLT_DT_SK, Logging)"
              initValue: "0"
            - name: SrcNdcCrtDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.SRC_NDC_CRT_DT_SK, Logging)"
              initValue: "0"
            - name: SrcNdcUpdtDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.SRC_NDC_UPDT_DT_SK, Logging)"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "NdcCrfIn.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(NdcCrfIn.NDC_SK)"
              initValue: "-1"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "NdcCrfIn.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "NdcCrfIn.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "NdcCrfIn.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "NdcCrfIn.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "NdcCrfIn.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "NdcCrfIn.PRI_KEY_STRING"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "NdcCrfIn.NDC_SK"
            - name: NDC
              type: varchar
              width: 20
              nullable: N
              map: "NdcCrfIn.NDC"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "NdcCrfIn.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "NdcCrfIn.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: AHFS_TCC_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.AHFS_TCC_CD"
            - name: DOSE_FORM
              type: char
              width: 2
              nullable: N
              map: "NdcCrfIn.DOSE_FORM"
            - name: TCC_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.TCC_CD"
            - name: DSM_DRUG_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.DSM_DRUG_TYP_CD"
            - name: NDC_DRUG_ABUSE_CTL_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_DRUG_ABUSE_CTL_CD"
            - name: NDC_DRUG_CLS_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_DRUG_CLS_CD"
            - name: NDC_DRUG_FORM_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_DRUG_FORM_CD"
            - name: NDC_FMT_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_FMT_CD"
            - name: NDC_GNRC_MNFCTR_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_MNFCTR_CD"
            - name: NDC_GNRC_NMD_DRUG_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_NMD_DRUG_CD"
            - name: NDC_GNRC_PRICE_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_PRICE_CD"
            - name: NDC_GNRC_PRICE_SPREAD_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_PRICE_SPREAD_CD"
            - name: NDC_ORANGE_BOOK_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_ORANGE_BOOK_CD"
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.CLM_TRANS_ADD_IN"
            - name: DESI_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.DESI_DRUG_IN"
            - name: DRUG_MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.DRUG_MNTN_IN"
            - name: INNVTR_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.INNVTR_IN"
            - name: INSTUT_PROD_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.INSTUT_PROD_IN"
            - name: PRIV_LBLR_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.PRIV_LBLR_IN"
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.SNGL_SRC_IN"
            - name: UNIT_DOSE_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.UNIT_DOSE_IN"
            - name: UNIT_OF_USE_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.UNIT_OF_USE_IN"
            - name: AVG_WHLSL_PRICE_CHG_DT_SK
              type: char
              width: 10
              nullable: N
              map: AvgWhlslPriceChgDt
            - name: GNRC_PRICE_IN_CHG_DT_SK
              type: char
              width: 10
              nullable: N
              map: GnrcPricInChgDt
            - name: OBSLT_DT_SK
              type: char
              width: 10
              nullable: N
              map: ObsltDt
            - name: SRC_NDC_CRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: SrcNdcCrtDt
            - name: SRC_NDC_UPDT_DT_SK
              type: char
              width: 10
              nullable: N
              map: SrcNdcUpdtDt
            - name: BRND_NM
              type: varchar
              width: 255
              map: "NdcCrfIn.BRND_NM"
            - name: CORE_NINE_NO
              type: varchar
              width: 20
              map: "NdcCrfIn.CORE_NINE_NO"
            - name: DRUG_LABEL_NM
              type: varchar
              width: 35
              map: "NdcCrfIn.DRUG_LABEL_NM"
            - name: DRUG_STRG_DESC
              type: varchar
              width: 255
              map: "NdcCrfIn.DRUG_STRG_DESC"
            - name: GCN_CD_TX
              type: varchar
              width: 20
              map: "NdcCrfIn.GCN_CD_TX"
            - name: GNRC_NM_SH_DESC
              type: varchar
              width: 255
              map: "NdcCrfIn.GNRC_NM_SH_DESC"
            - name: LBLR_NM
              type: varchar
              width: 75
              map: "NdcCrfIn.LBLR_NM"
            - name: LBLR_NO
              type: varchar
              width: 20
              map: "NdcCrfIn.LBLR_NO"
            - name: NEEDLE_GAUGE_VAL
              type: varchar
              width: 20
              map: "NdcCrfIn.NEEDLE_GAUGE_VAL"
            - name: NEEDLE_LGTH_VAL
              type: varchar
              width: 20
              map: "NdcCrfIn.NEEDLE_LGTH_VAL"
            - name: PCKG_DESC
              type: varchar
              width: 255
              map: "NdcCrfIn.PCKG_DESC"
            - name: PCKG_SIZE_EQVLNT_NO
              type: varchar
              width: 20
              map: "NdcCrfIn.PCKG_SIZE_EQVLNT_NO"
            - name: PCKG_SIZE_NO
              type: varchar
              width: 20
              map: "NdcCrfIn.PCKG_SIZE_NO"
            - name: PROD_NO
              type: varchar
              width: 20
              map: "NdcCrfIn.PROD_NO"
            - name: SYRNG_CPCT_VAL
              type: varchar
              width: 20
              map: "NdcCrfIn.SYRNG_CPCT_VAL"
            - name: NDC_RTE_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.NDC_RTE_TYP_CD"
            - name: AHFS_TCC_SK
              type: int
              width: 4
              nullable: N
              map: NdcAHFSClass
            - name: DOSE_FORM_SK
              type: int
              width: 4
              nullable: N
              map: NdcDoseForm
            - name: TCC_SK
              type: int
              width: 4
              nullable: N
              map: NdcTherCls
            - name: NDC_DSM_DRUG_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: DSMDrugType
            - name: NDC_DRUG_ABUSE_CTL_CD_SK
              type: int
              width: 4
              nullable: N
              map: DrugAbuseCtlCd
            - name: NDC_DRUG_CLS_CD_SK
              type: int
              width: 4
              nullable: N
              map: DrugClsCd
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
              nullable: N
              map: DrugFormCd
            - name: NDC_FMT_CD_SK
              type: int
              width: 4
              nullable: N
              map: FmtCd
            - name: NDC_GNRC_MNFCTR_CD_SK
              type: int
              width: 4
              nullable: N
              map: GnrcMnfctrCd
            - name: NDC_GNRC_NMD_DRUG_CD_SK
              type: int
              width: 4
              nullable: N
              map: GnrcNmdDrugCd
            - name: NDC_GNRC_PRICE_CD_SK
              type: int
              width: 4
              nullable: N
              map: GnrcPriceCd
            - name: NDC_GNRC_PRICE_SPREAD_CD_SK
              type: int
              width: 4
              nullable: N
              map: GnrcPriceSpreadCd
            - name: NDC_ORANGE_BOOK_CD_SK
              type: int
              width: 4
              nullable: N
              map: OrangeBookCd
            - name: NDC_RTE_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: NdcRteTypCd
            - name: NdcAHFSClass
              type: decimal
              width: 18
              precision: 4
              map: NdcAHFSClass
              initValue: "-1"
            - name: NdcTherCls
              type: decimal
              width: 18
              precision: 4
              map: NdcTherCls
              initValue: "-1"
            - name: NdcDoseForm
              type: decimal
              width: 18
              precision: 4
              map: NdcDoseForm
              initValue: "1"
            - name: DSMDrugType
              type: decimal
              width: 18
              precision: 4
              map: DSMDrugType
              initValue: "-1"
            - name: DrugAbuseCtlCd
              type: decimal
              width: 18
              precision: 4
              map: DrugAbuseCtlCd
              initValue: "-1"
            - name: DrugClsCd
              type: decimal
              width: 18
              precision: 4
              map: DrugClsCd
              initValue: "-1"
            - name: DrugFormCd
              type: decimal
              width: 18
              precision: 4
              map: DrugFormCd
              initValue: "-1"
            - name: FmtCd
              type: decimal
              width: 18
              precision: 4
              map: FmtCd
              initValue: "-1"
            - name: GnrcMnfctrCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcMnfctrCd
              initValue: "-1"
            - name: GnrcNmdDrugCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcNmdDrugCd
              initValue: "-1"
            - name: GnrcPriceCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcPriceCd
              initValue: "-1"
            - name: GnrcPriceSpreadCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcPriceSpreadCd
              initValue: "-1"
            - name: OrangeBookCd
              type: decimal
              width: 18
              precision: 4
              map: OrangeBookCd
              initValue: "-1"
            - name: NdcRteTypCd
              type: decimal
              width: 18
              precision: 4
              map: NdcRteTypCd
              initValue: "-1"
            - name: AvgWhlslPriceChgDt
              type: decimal
              width: 18
              precision: 4
              map: AvgWhlslPriceChgDt
              initValue: "0"
            - name: GnrcPricInChgDt
              type: decimal
              width: 18
              precision: 4
              map: GnrcPricInChgDt
              initValue: "0"
            - name: ObsltDt
              type: decimal
              width: 18
              precision: 4
              map: ObsltDt
              initValue: "0"
            - name: SrcNdcCrtDt
              type: decimal
              width: 18
              precision: 4
              map: SrcNdcCrtDt
              initValue: "0"
            - name: SrcNdcUpdtDt
              type: decimal
              width: 18
              precision: 4
              map: SrcNdcUpdtDt
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "-1"
        - name: PurgeTrn.V0S20P3
          type: filter
          source: PurgeTrn.V0S20P3.map
          filter: "ErrCount = 0 Or PassThru = 'Y'"
          schema: 
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
            - name: NDC
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
            - name: AHFS_TCC_SK
              type: int
              width: 4
              nullable: N
            - name: DOSE_FORM_SK
              type: int
              width: 4
              nullable: N
            - name: TCC_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_DSM_DRUG_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_DRUG_ABUSE_CTL_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_DRUG_CLS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_FMT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_GNRC_MNFCTR_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_GNRC_NMD_DRUG_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_GNRC_PRICE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_GNRC_PRICE_SPREAD_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_ORANGE_BOOK_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_RTE_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
            - name: DESI_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: DRUG_MNTN_IN
              type: char
              width: 1
              nullable: N
            - name: INNVTR_IN
              type: char
              width: 1
              nullable: N
            - name: INSTUT_PROD_IN
              type: char
              width: 1
              nullable: N
            - name: PRIV_LBLR_IN
              type: char
              width: 1
              nullable: N
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
            - name: UNIT_DOSE_IN
              type: char
              width: 1
              nullable: N
            - name: UNIT_OF_USE_IN
              type: char
              width: 1
              nullable: N
            - name: AVG_WHLSL_PRICE_CHG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: GNRC_PRICE_IN_CHG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: OBSLT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SRC_NDC_CRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SRC_NDC_UPDT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: BRND_NM
              type: varchar
              width: 255
            - name: CORE_NINE_NO
              type: varchar
              width: 20
            - name: DRUG_LABEL_NM
              type: varchar
              width: 35
            - name: DRUG_STRG_DESC
              type: varchar
              width: 255
            - name: GCN_CD_TX
              type: varchar
              width: 20
            - name: GNRC_NM_SH_DESC
              type: varchar
              width: 255
            - name: LBLR_NM
              type: varchar
              width: 75
            - name: LBLR_NO
              type: varchar
              width: 20
            - name: NEEDLE_GAUGE_VAL
              type: varchar
              width: 20
            - name: NEEDLE_LGTH_VAL
              type: varchar
              width: 20
            - name: PCKG_DESC
              type: varchar
              width: 255
            - name: PCKG_SIZE_EQVLNT_NO
              type: varchar
              width: 20
            - name: PCKG_SIZE_NO
              type: varchar
              width: 20
            - name: PROD_NO
              type: varchar
              width: 20
            - name: SYRNG_CPCT_VAL
              type: varchar
              width: 20
          keys:
            - NDC_SK
        - name: PurgeTrn.V0S20P4.map
          type: map
          source: NdcCrf
          alias: NdcCrfIn
          stageVars: 
            - name: NdcAHFSClass
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyAhfsTcc(NdcCrfIn.NDC_SK, NdcCrfIn.AHFS_TCC_CD, Logging)"
              initValue: "-1"
            - name: NdcTherCls
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyTCC(NdcCrfIn.NDC_SK, NdcCrfIn.TCC_CD, Logging)"
              initValue: "-1"
            - name: NdcDoseForm
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDoseForm(NdcCrfIn.NDC_SK, NdcCrfIn.DOSE_FORM, Logging)"
              initValue: "1"
            - name: DSMDrugType
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"UWS\", NdcCrfIn.NDC_SK, 'NDC DISEASE STATE MANAGEMENT DRUG TYPE', NdcCrfIn.DSM_DRUG_TYP_CD, Logging)"
              initValue: "-1"
            - name: DrugAbuseCtlCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC DRUG ABUSE CONTROL', NdcCrfIn.NDC_DRUG_ABUSE_CTL_CD, Logging)"
              initValue: "-1"
            - name: DrugClsCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC DRUG CLASS', NdcCrfIn.NDC_DRUG_CLS_CD, Logging)"
              initValue: "-1"
            - name: DrugFormCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC DRUG FORM', NdcCrfIn.NDC_DRUG_FORM_CD, Logging)"
              initValue: "-1"
            - name: FmtCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC FORMAT', NdcCrfIn.NDC_FMT_CD, Logging)"
              initValue: "-1"
            - name: GnrcMnfctrCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC MANUFACTURER', NdcCrfIn.NDC_GNRC_MNFCTR_CD, Logging)"
              initValue: "-1"
            - name: GnrcNmdDrugCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC NAME', NdcCrfIn.NDC_GNRC_NMD_DRUG_CD, Logging)"
              initValue: "-1"
            - name: GnrcPriceCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC PRICE', NdcCrfIn.NDC_GNRC_PRICE_CD, Logging)"
              initValue: "-1"
            - name: GnrcPriceSpreadCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC PRICE SPREAD', NdcCrfIn.NDC_GNRC_PRICE_SPREAD_CD, Logging)"
              initValue: "-1"
            - name: OrangeBookCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC ORANGE BOOK', NdcCrfIn.NDC_ORANGE_BOOK_CD, Logging)"
              initValue: "-1"
            - name: NdcRteTypCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC ROUTE TYPE', NdcCrfIn.NDC_RTE_TYP_CD, Logging)"
              initValue: "-1"
            - name: AvgWhlslPriceChgDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.AVG_WHLSL_PRICE_CHG_DT_SK, Logging)"
              initValue: "0"
            - name: GnrcPricInChgDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.GNRC_PRICE_IN_CHG_DT_SK, Logging)"
              initValue: "0"
            - name: ObsltDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.OBSLT_DT_SK, Logging)"
              initValue: "0"
            - name: SrcNdcCrtDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.SRC_NDC_CRT_DT_SK, Logging)"
              initValue: "0"
            - name: SrcNdcUpdtDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.SRC_NDC_UPDT_DT_SK, Logging)"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "NdcCrfIn.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(NdcCrfIn.NDC_SK)"
              initValue: "-1"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "GetRecycleKey(NdcCrfIn.NDC_SK)"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "NdcCrfIn.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "NdcCrfIn.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "NdcCrfIn.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "NdcCrfIn.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "NdcCrfIn.PRI_KEY_STRING"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "NdcCrfIn.NDC_SK"
            - name: NDC
              type: varchar
              width: 20
              nullable: N
              map: "NdcCrfIn.NDC"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "NdcCrfIn.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "NdcCrfIn.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: AHFS_TCC_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.AHFS_TCC_CD"
            - name: DOSE_FORM
              type: char
              width: 2
              nullable: N
              map: "NdcCrfIn.DOSE_FORM"
            - name: TCC_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.TCC_CD"
            - name: DSM_DRUG_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.DSM_DRUG_TYP_CD"
            - name: NDC_DRUG_ABUSE_CTL_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_DRUG_ABUSE_CTL_CD"
            - name: NDC_DRUG_CLS_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_DRUG_CLS_CD"
            - name: NDC_DRUG_FORM_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_DRUG_FORM_CD"
            - name: NDC_FMT_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_FMT_CD"
            - name: NDC_GNRC_MNFCTR_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_MNFCTR_CD"
            - name: NDC_GNRC_NMD_DRUG_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_NMD_DRUG_CD"
            - name: NDC_GNRC_PRICE_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_PRICE_CD"
            - name: NDC_GNRC_PRICE_SPREAD_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_PRICE_SPREAD_CD"
            - name: NDC_ORANGE_BOOK_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_ORANGE_BOOK_CD"
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.CLM_TRANS_ADD_IN"
            - name: DESI_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.DESI_DRUG_IN"
            - name: DRUG_MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.DRUG_MNTN_IN"
            - name: INNVTR_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.INNVTR_IN"
            - name: INSTUT_PROD_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.INSTUT_PROD_IN"
            - name: PRIV_LBLR_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.PRIV_LBLR_IN"
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.SNGL_SRC_IN"
            - name: UNIT_DOSE_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.UNIT_DOSE_IN"
            - name: UNIT_OF_USE_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.UNIT_OF_USE_IN"
            - name: AVG_WHLSL_PRICE_CHG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.AVG_WHLSL_PRICE_CHG_DT_SK"
            - name: GNRC_PRICE_IN_CHG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.GNRC_PRICE_IN_CHG_DT_SK"
            - name: OBSLT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.OBSLT_DT_SK"
            - name: SRC_NDC_CRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.SRC_NDC_CRT_DT_SK"
            - name: SRC_NDC_UPDT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.SRC_NDC_UPDT_DT_SK"
            - name: BRND_NM
              type: varchar
              width: 255
              map: "NdcCrfIn.BRND_NM"
            - name: CORE_NINE_NO
              type: varchar
              width: 20
              map: "NdcCrfIn.CORE_NINE_NO"
            - name: DRUG_LABEL_NM
              type: varchar
              width: 35
              map: "NdcCrfIn.DRUG_LABEL_NM"
            - name: DRUG_STRG_DESC
              type: varchar
              width: 255
              map: "NdcCrfIn.DRUG_STRG_DESC"
            - name: GCN_CD_TX
              type: varchar
              width: 20
              map: "NdcCrfIn.GCN_CD_TX"
            - name: GNRC_NM_SH_DESC
              type: varchar
              width: 255
              map: "NdcCrfIn.GNRC_NM_SH_DESC"
            - name: LBLR_NM
              type: varchar
              width: 75
              map: "NdcCrfIn.LBLR_NM"
            - name: LBLR_NO
              type: varchar
              width: 20
              map: "NdcCrfIn.LBLR_NO"
            - name: NEEDLE_GAUGE_VAL
              type: varchar
              width: 20
              map: "NdcCrfIn.NEEDLE_GAUGE_VAL"
            - name: NEEDLE_LGTH_VAL
              type: varchar
              width: 20
              map: "NdcCrfIn.NEEDLE_LGTH_VAL"
            - name: PCKG_DESC
              type: varchar
              width: 255
              map: "NdcCrfIn.PCKG_DESC"
            - name: PCKG_SIZE_EQVLNT_NO
              type: varchar
              width: 20
              map: "NdcCrfIn.PCKG_SIZE_EQVLNT_NO"
            - name: PCKG_SIZE_NO
              type: varchar
              width: 20
              map: "NdcCrfIn.PCKG_SIZE_NO"
            - name: PROD_NO
              type: varchar
              width: 20
              map: "NdcCrfIn.PROD_NO"
            - name: SYRNG_CPCT_VAL
              type: varchar
              width: 20
              map: "NdcCrfIn.SYRNG_CPCT_VAL"
            - name: NDC_RTE_TYP_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_RTE_TYP_CD"
            - name: NdcAHFSClass
              type: decimal
              width: 18
              precision: 4
              map: NdcAHFSClass
              initValue: "-1"
            - name: NdcTherCls
              type: decimal
              width: 18
              precision: 4
              map: NdcTherCls
              initValue: "-1"
            - name: NdcDoseForm
              type: decimal
              width: 18
              precision: 4
              map: NdcDoseForm
              initValue: "1"
            - name: DSMDrugType
              type: decimal
              width: 18
              precision: 4
              map: DSMDrugType
              initValue: "-1"
            - name: DrugAbuseCtlCd
              type: decimal
              width: 18
              precision: 4
              map: DrugAbuseCtlCd
              initValue: "-1"
            - name: DrugClsCd
              type: decimal
              width: 18
              precision: 4
              map: DrugClsCd
              initValue: "-1"
            - name: DrugFormCd
              type: decimal
              width: 18
              precision: 4
              map: DrugFormCd
              initValue: "-1"
            - name: FmtCd
              type: decimal
              width: 18
              precision: 4
              map: FmtCd
              initValue: "-1"
            - name: GnrcMnfctrCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcMnfctrCd
              initValue: "-1"
            - name: GnrcNmdDrugCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcNmdDrugCd
              initValue: "-1"
            - name: GnrcPriceCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcPriceCd
              initValue: "-1"
            - name: GnrcPriceSpreadCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcPriceSpreadCd
              initValue: "-1"
            - name: OrangeBookCd
              type: decimal
              width: 18
              precision: 4
              map: OrangeBookCd
              initValue: "-1"
            - name: NdcRteTypCd
              type: decimal
              width: 18
              precision: 4
              map: NdcRteTypCd
              initValue: "-1"
            - name: AvgWhlslPriceChgDt
              type: decimal
              width: 18
              precision: 4
              map: AvgWhlslPriceChgDt
              initValue: "0"
            - name: GnrcPricInChgDt
              type: decimal
              width: 18
              precision: 4
              map: GnrcPricInChgDt
              initValue: "0"
            - name: ObsltDt
              type: decimal
              width: 18
              precision: 4
              map: ObsltDt
              initValue: "0"
            - name: SrcNdcCrtDt
              type: decimal
              width: 18
              precision: 4
              map: SrcNdcCrtDt
              initValue: "0"
            - name: SrcNdcUpdtDt
              type: decimal
              width: 18
              precision: 4
              map: SrcNdcUpdtDt
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "-1"
        - name: PurgeTrn.V0S20P4
          type: filter
          source: PurgeTrn.V0S20P4.map
          filter: "ErrCount > 0"
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
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
            - name: NDC
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
            - name: AHFS_TCC_CD
              type: char
              width: 10
              nullable: N
            - name: DOSE_FORM
              type: char
              width: 2
              nullable: N
            - name: TCC_CD
              type: char
              width: 10
              nullable: N
            - name: DSM_DRUG_TYP_CD
              type: char
              width: 10
              nullable: N
            - name: NDC_DRUG_ABUSE_CTL_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_DRUG_CLS_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_DRUG_FORM_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_FMT_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_GNRC_MNFCTR_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_GNRC_NMD_DRUG_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_GNRC_PRICE_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_GNRC_PRICE_SPREAD_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_ORANGE_BOOK_CD
              type: char
              width: 1
              nullable: N
            - name: NDC_RTE_TYP_CD
              type: char
              width: 1
              nullable: N
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
            - name: DESI_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: DRUG_MNTN_IN
              type: char
              width: 1
              nullable: N
            - name: INNVTR_IN
              type: char
              width: 1
              nullable: N
            - name: INSTUT_PROD_IN
              type: char
              width: 1
              nullable: N
            - name: PRIV_LBLR_IN
              type: char
              width: 1
              nullable: N
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
            - name: UNIT_DOSE_IN
              type: char
              width: 1
              nullable: N
            - name: UNIT_OF_USE_IN
              type: char
              width: 1
              nullable: N
            - name: AVG_WHLSL_PRICE_CHG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: GNRC_PRICE_IN_CHG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: OBSLT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SRC_NDC_CRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SRC_NDC_UPDT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: BRND_NM
              type: varchar
              width: 255
            - name: CORE_NINE_NO
              type: varchar
              width: 20
            - name: DRUG_LABEL_NM
              type: varchar
              width: 35
            - name: DRUG_STRG_DESC
              type: varchar
              width: 255
            - name: GCN_CD_TX
              type: varchar
              width: 20
            - name: GNRC_NM_SH_DESC
              type: varchar
              width: 255
            - name: LBLR_NM
              type: varchar
              width: 75
            - name: LBLR_NO
              type: varchar
              width: 20
            - name: NEEDLE_GAUGE_VAL
              type: varchar
              width: 20
            - name: NEEDLE_LGTH_VAL
              type: varchar
              width: 20
            - name: PCKG_DESC
              type: varchar
              width: 255
            - name: PCKG_SIZE_EQVLNT_NO
              type: varchar
              width: 20
            - name: PCKG_SIZE_NO
              type: varchar
              width: 20
            - name: PROD_NO
              type: varchar
              width: 20
            - name: SYRNG_CPCT_VAL
              type: varchar
              width: 20
          keys:
            - NDC_SK
        - name: PurgeTrn.V0S20P5.map
          type: map
          source: NdcCrf
          alias: NdcCrfIn
          stageVars: 
            - name: NdcAHFSClass
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyAhfsTcc(NdcCrfIn.NDC_SK, NdcCrfIn.AHFS_TCC_CD, Logging)"
              initValue: "-1"
            - name: NdcTherCls
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyTCC(NdcCrfIn.NDC_SK, NdcCrfIn.TCC_CD, Logging)"
              initValue: "-1"
            - name: NdcDoseForm
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDoseForm(NdcCrfIn.NDC_SK, NdcCrfIn.DOSE_FORM, Logging)"
              initValue: "1"
            - name: DSMDrugType
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"UWS\", NdcCrfIn.NDC_SK, 'NDC DISEASE STATE MANAGEMENT DRUG TYPE', NdcCrfIn.DSM_DRUG_TYP_CD, Logging)"
              initValue: "-1"
            - name: DrugAbuseCtlCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC DRUG ABUSE CONTROL', NdcCrfIn.NDC_DRUG_ABUSE_CTL_CD, Logging)"
              initValue: "-1"
            - name: DrugClsCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC DRUG CLASS', NdcCrfIn.NDC_DRUG_CLS_CD, Logging)"
              initValue: "-1"
            - name: DrugFormCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC DRUG FORM', NdcCrfIn.NDC_DRUG_FORM_CD, Logging)"
              initValue: "-1"
            - name: FmtCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC FORMAT', NdcCrfIn.NDC_FMT_CD, Logging)"
              initValue: "-1"
            - name: GnrcMnfctrCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC MANUFACTURER', NdcCrfIn.NDC_GNRC_MNFCTR_CD, Logging)"
              initValue: "-1"
            - name: GnrcNmdDrugCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC NAME', NdcCrfIn.NDC_GNRC_NMD_DRUG_CD, Logging)"
              initValue: "-1"
            - name: GnrcPriceCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC PRICE', NdcCrfIn.NDC_GNRC_PRICE_CD, Logging)"
              initValue: "-1"
            - name: GnrcPriceSpreadCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC PRICE SPREAD', NdcCrfIn.NDC_GNRC_PRICE_SPREAD_CD, Logging)"
              initValue: "-1"
            - name: OrangeBookCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC ORANGE BOOK', NdcCrfIn.NDC_ORANGE_BOOK_CD, Logging)"
              initValue: "-1"
            - name: NdcRteTypCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC ROUTE TYPE', NdcCrfIn.NDC_RTE_TYP_CD, Logging)"
              initValue: "-1"
            - name: AvgWhlslPriceChgDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.AVG_WHLSL_PRICE_CHG_DT_SK, Logging)"
              initValue: "0"
            - name: GnrcPricInChgDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.GNRC_PRICE_IN_CHG_DT_SK, Logging)"
              initValue: "0"
            - name: ObsltDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.OBSLT_DT_SK, Logging)"
              initValue: "0"
            - name: SrcNdcCrtDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.SRC_NDC_CRT_DT_SK, Logging)"
              initValue: "0"
            - name: SrcNdcUpdtDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.SRC_NDC_UPDT_DT_SK, Logging)"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "NdcCrfIn.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(NdcCrfIn.NDC_SK)"
              initValue: "-1"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "NdcCrfIn.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "NdcCrfIn.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "NdcCrfIn.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "NdcCrfIn.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "NdcCrfIn.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "NdcCrfIn.PRI_KEY_STRING"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NDC
              type: varchar
              width: 20
              nullable: N
              map: "\"UNK\""
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
            - name: AHFS_TCC_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.AHFS_TCC_CD"
            - name: DOSE_FORM
              type: char
              width: 2
              nullable: N
              map: "NdcCrfIn.DOSE_FORM"
            - name: TCC_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.TCC_CD"
            - name: DSM_DRUG_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.DSM_DRUG_TYP_CD"
            - name: NDC_DRUG_ABUSE_CTL_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_DRUG_ABUSE_CTL_CD"
            - name: NDC_DRUG_CLS_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_DRUG_CLS_CD"
            - name: NDC_DRUG_FORM_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_DRUG_FORM_CD"
            - name: NDC_FMT_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_FMT_CD"
            - name: NDC_GNRC_MNFCTR_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_MNFCTR_CD"
            - name: NDC_GNRC_NMD_DRUG_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_NMD_DRUG_CD"
            - name: NDC_GNRC_PRICE_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_PRICE_CD"
            - name: NDC_GNRC_PRICE_SPREAD_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_PRICE_SPREAD_CD"
            - name: NDC_ORANGE_BOOK_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_ORANGE_BOOK_CD"
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DESI_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: DRUG_MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: INNVTR_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: INSTUT_PROD_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: PRIV_LBLR_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: UNIT_DOSE_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: UNIT_OF_USE_IN
              type: char
              width: 1
              nullable: N
              map: "'U'"
            - name: AVG_WHLSL_PRICE_CHG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "\"UNK\""
            - name: GNRC_PRICE_IN_CHG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "\"UNK\""
            - name: OBSLT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "\"UNK\""
            - name: SRC_NDC_CRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "\"UNK\""
            - name: SRC_NDC_UPDT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "\"UNK\""
            - name: BRND_NM
              type: varchar
              width: 255
              map: "'UNK'"
            - name: CORE_NINE_NO
              type: varchar
              width: 20
              map: "'UNK'"
            - name: DRUG_LABEL_NM
              type: varchar
              width: 35
              map: "'UNK'"
            - name: DRUG_STRG_DESC
              type: varchar
              width: 255
              map: "'UNK'"
            - name: GCN_CD_TX
              type: varchar
              width: 20
              map: "'UNK'"
            - name: GNRC_NM_SH_DESC
              type: varchar
              width: 255
              map: "'UNK'"
            - name: LBLR_NM
              type: varchar
              width: 75
              map: "'UNK'"
            - name: LBLR_NO
              type: varchar
              width: 20
              map: "'UNK'"
            - name: NEEDLE_GAUGE_VAL
              type: varchar
              width: 20
              map: "'UNK'"
            - name: NEEDLE_LGTH_VAL
              type: varchar
              width: 20
              map: "'UNK'"
            - name: PCKG_DESC
              type: varchar
              width: 255
              map: "'UNK'"
            - name: PCKG_SIZE_EQVLNT_NO
              type: varchar
              width: 20
              map: "'UNK'"
            - name: PCKG_SIZE_NO
              type: varchar
              width: 20
              map: "'UNK'"
            - name: PROD_NO
              type: varchar
              width: 20
              map: "'UNK'"
            - name: SYRNG_CPCT_VAL
              type: varchar
              width: 20
              map: "'UNK'"
            - name: NDC_RTE_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.NDC_RTE_TYP_CD"
            - name: AHFS_TCC_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: DOSE_FORM_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: TCC_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NDC_DSM_DRUG_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NDC_DRUG_ABUSE_CTL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NDC_DRUG_CLS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NDC_FMT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NDC_GNRC_MNFCTR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NDC_GNRC_NMD_DRUG_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NDC_GNRC_PRICE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NDC_GNRC_PRICE_SPREAD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NDC_ORANGE_BOOK_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NDC_RTE_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NdcAHFSClass
              type: decimal
              width: 18
              precision: 4
              map: NdcAHFSClass
              initValue: "-1"
            - name: NdcTherCls
              type: decimal
              width: 18
              precision: 4
              map: NdcTherCls
              initValue: "-1"
            - name: NdcDoseForm
              type: decimal
              width: 18
              precision: 4
              map: NdcDoseForm
              initValue: "1"
            - name: DSMDrugType
              type: decimal
              width: 18
              precision: 4
              map: DSMDrugType
              initValue: "-1"
            - name: DrugAbuseCtlCd
              type: decimal
              width: 18
              precision: 4
              map: DrugAbuseCtlCd
              initValue: "-1"
            - name: DrugClsCd
              type: decimal
              width: 18
              precision: 4
              map: DrugClsCd
              initValue: "-1"
            - name: DrugFormCd
              type: decimal
              width: 18
              precision: 4
              map: DrugFormCd
              initValue: "-1"
            - name: FmtCd
              type: decimal
              width: 18
              precision: 4
              map: FmtCd
              initValue: "-1"
            - name: GnrcMnfctrCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcMnfctrCd
              initValue: "-1"
            - name: GnrcNmdDrugCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcNmdDrugCd
              initValue: "-1"
            - name: GnrcPriceCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcPriceCd
              initValue: "-1"
            - name: GnrcPriceSpreadCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcPriceSpreadCd
              initValue: "-1"
            - name: OrangeBookCd
              type: decimal
              width: 18
              precision: 4
              map: OrangeBookCd
              initValue: "-1"
            - name: NdcRteTypCd
              type: decimal
              width: 18
              precision: 4
              map: NdcRteTypCd
              initValue: "-1"
            - name: AvgWhlslPriceChgDt
              type: decimal
              width: 18
              precision: 4
              map: AvgWhlslPriceChgDt
              initValue: "0"
            - name: GnrcPricInChgDt
              type: decimal
              width: 18
              precision: 4
              map: GnrcPricInChgDt
              initValue: "0"
            - name: ObsltDt
              type: decimal
              width: 18
              precision: 4
              map: ObsltDt
              initValue: "0"
            - name: SrcNdcCrtDt
              type: decimal
              width: 18
              precision: 4
              map: SrcNdcCrtDt
              initValue: "0"
            - name: SrcNdcUpdtDt
              type: decimal
              width: 18
              precision: 4
              map: SrcNdcUpdtDt
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "-1"
        - name: PurgeTrn.V0S20P5
          type: filter
          source: PurgeTrn.V0S20P5.map
          filter: "@INROWNUM = 1"
          schema: 
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
            - name: NDC
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
            - name: AHFS_TCC_SK
              type: int
              width: 4
              nullable: N
            - name: DOSE_FORM_SK
              type: int
              width: 4
              nullable: N
            - name: TCC_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_DSM_DRUG_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_DRUG_ABUSE_CTL_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_DRUG_CLS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_FMT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_GNRC_MNFCTR_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_GNRC_NMD_DRUG_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_GNRC_PRICE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_GNRC_PRICE_SPREAD_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_ORANGE_BOOK_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_RTE_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
            - name: DESI_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: DRUG_MNTN_IN
              type: char
              width: 1
              nullable: N
            - name: INNVTR_IN
              type: char
              width: 1
              nullable: N
            - name: INSTUT_PROD_IN
              type: char
              width: 1
              nullable: N
            - name: PRIV_LBLR_IN
              type: char
              width: 1
              nullable: N
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
            - name: UNIT_DOSE_IN
              type: char
              width: 1
              nullable: N
            - name: UNIT_OF_USE_IN
              type: char
              width: 1
              nullable: N
            - name: AVG_WHLSL_PRICE_CHG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: GNRC_PRICE_IN_CHG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: OBSLT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SRC_NDC_CRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SRC_NDC_UPDT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: BRND_NM
              type: varchar
              width: 255
            - name: CORE_NINE_NO
              type: varchar
              width: 20
            - name: DRUG_LABEL_NM
              type: varchar
              width: 35
            - name: DRUG_STRG_DESC
              type: varchar
              width: 255
            - name: GCN_CD_TX
              type: varchar
              width: 20
            - name: GNRC_NM_SH_DESC
              type: varchar
              width: 255
            - name: LBLR_NM
              type: varchar
              width: 75
            - name: LBLR_NO
              type: varchar
              width: 20
            - name: NEEDLE_GAUGE_VAL
              type: varchar
              width: 20
            - name: NEEDLE_LGTH_VAL
              type: varchar
              width: 20
            - name: PCKG_DESC
              type: varchar
              width: 255
            - name: PCKG_SIZE_EQVLNT_NO
              type: varchar
              width: 20
            - name: PCKG_SIZE_NO
              type: varchar
              width: 20
            - name: PROD_NO
              type: varchar
              width: 20
            - name: SYRNG_CPCT_VAL
              type: varchar
              width: 20
          keys:
            - NDC_SK
        - name: PurgeTrn.V0S20P6.map
          type: map
          source: NdcCrf
          alias: NdcCrfIn
          stageVars: 
            - name: NdcAHFSClass
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyAhfsTcc(NdcCrfIn.NDC_SK, NdcCrfIn.AHFS_TCC_CD, Logging)"
              initValue: "-1"
            - name: NdcTherCls
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyTCC(NdcCrfIn.NDC_SK, NdcCrfIn.TCC_CD, Logging)"
              initValue: "-1"
            - name: NdcDoseForm
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDoseForm(NdcCrfIn.NDC_SK, NdcCrfIn.DOSE_FORM, Logging)"
              initValue: "1"
            - name: DSMDrugType
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"UWS\", NdcCrfIn.NDC_SK, 'NDC DISEASE STATE MANAGEMENT DRUG TYPE', NdcCrfIn.DSM_DRUG_TYP_CD, Logging)"
              initValue: "-1"
            - name: DrugAbuseCtlCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC DRUG ABUSE CONTROL', NdcCrfIn.NDC_DRUG_ABUSE_CTL_CD, Logging)"
              initValue: "-1"
            - name: DrugClsCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC DRUG CLASS', NdcCrfIn.NDC_DRUG_CLS_CD, Logging)"
              initValue: "-1"
            - name: DrugFormCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC DRUG FORM', NdcCrfIn.NDC_DRUG_FORM_CD, Logging)"
              initValue: "-1"
            - name: FmtCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC FORMAT', NdcCrfIn.NDC_FMT_CD, Logging)"
              initValue: "-1"
            - name: GnrcMnfctrCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC MANUFACTURER', NdcCrfIn.NDC_GNRC_MNFCTR_CD, Logging)"
              initValue: "-1"
            - name: GnrcNmdDrugCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC NAME', NdcCrfIn.NDC_GNRC_NMD_DRUG_CD, Logging)"
              initValue: "-1"
            - name: GnrcPriceCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC PRICE', NdcCrfIn.NDC_GNRC_PRICE_CD, Logging)"
              initValue: "-1"
            - name: GnrcPriceSpreadCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC GENERIC PRICE SPREAD', NdcCrfIn.NDC_GNRC_PRICE_SPREAD_CD, Logging)"
              initValue: "-1"
            - name: OrangeBookCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC ORANGE BOOK', NdcCrfIn.NDC_ORANGE_BOOK_CD, Logging)"
              initValue: "-1"
            - name: NdcRteTypCd
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyCodes(\"FDB\", NdcCrfIn.NDC_SK, 'NDC ROUTE TYPE', NdcCrfIn.NDC_RTE_TYP_CD, Logging)"
              initValue: "-1"
            - name: AvgWhlslPriceChgDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.AVG_WHLSL_PRICE_CHG_DT_SK, Logging)"
              initValue: "0"
            - name: GnrcPricInChgDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.GNRC_PRICE_IN_CHG_DT_SK, Logging)"
              initValue: "0"
            - name: ObsltDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.OBSLT_DT_SK, Logging)"
              initValue: "0"
            - name: SrcNdcCrtDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.SRC_NDC_CRT_DT_SK, Logging)"
              initValue: "0"
            - name: SrcNdcUpdtDt
              type: decimal
              width: 18
              precision: 4
              map: "GetFkeyDate('IDS', NdcCrfIn.NDC_SK, NdcCrfIn.SRC_NDC_UPDT_DT_SK, Logging)"
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: "NdcCrfIn.PASS_THRU_IN"
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: "GetFkeyErrorCnt(NdcCrfIn.NDC_SK)"
              initValue: "-1"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "NdcCrfIn.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "NdcCrfIn.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "NdcCrfIn.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "NdcCrfIn.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "NdcCrfIn.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "NdcCrfIn.PRI_KEY_STRING"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NDC
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: AHFS_TCC_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.AHFS_TCC_CD"
            - name: DOSE_FORM
              type: char
              width: 2
              nullable: N
              map: "NdcCrfIn.DOSE_FORM"
            - name: TCC_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.TCC_CD"
            - name: DSM_DRUG_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.DSM_DRUG_TYP_CD"
            - name: NDC_DRUG_ABUSE_CTL_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_DRUG_ABUSE_CTL_CD"
            - name: NDC_DRUG_CLS_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_DRUG_CLS_CD"
            - name: NDC_DRUG_FORM_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_DRUG_FORM_CD"
            - name: NDC_FMT_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_FMT_CD"
            - name: NDC_GNRC_MNFCTR_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_MNFCTR_CD"
            - name: NDC_GNRC_NMD_DRUG_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_NMD_DRUG_CD"
            - name: NDC_GNRC_PRICE_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_PRICE_CD"
            - name: NDC_GNRC_PRICE_SPREAD_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_GNRC_PRICE_SPREAD_CD"
            - name: NDC_ORANGE_BOOK_CD
              type: char
              width: 1
              nullable: N
              map: "NdcCrfIn.NDC_ORANGE_BOOK_CD"
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DESI_DRUG_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: DRUG_MNTN_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: INNVTR_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: INSTUT_PROD_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: PRIV_LBLR_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: UNIT_DOSE_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: UNIT_OF_USE_IN
              type: char
              width: 1
              nullable: N
              map: "'X'"
            - name: AVG_WHLSL_PRICE_CHG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: GNRC_PRICE_IN_CHG_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: OBSLT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: SRC_NDC_CRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: SRC_NDC_UPDT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "'NA'"
            - name: BRND_NM
              type: varchar
              width: 255
              map: "'NA'"
            - name: CORE_NINE_NO
              type: varchar
              width: 20
              map: "'NA'"
            - name: DRUG_LABEL_NM
              type: varchar
              width: 35
              map: "'NA'"
            - name: DRUG_STRG_DESC
              type: varchar
              width: 255
              map: "'NA'"
            - name: GCN_CD_TX
              type: varchar
              width: 20
              map: "'NA'"
            - name: GNRC_NM_SH_DESC
              type: varchar
              width: 255
              map: "'NA'"
            - name: LBLR_NM
              type: varchar
              width: 75
              map: "'NA'"
            - name: LBLR_NO
              type: varchar
              width: 20
              map: "'NA'"
            - name: NEEDLE_GAUGE_VAL
              type: varchar
              width: 20
              map: "'NA'"
            - name: NEEDLE_LGTH_VAL
              type: varchar
              width: 20
              map: "'NA'"
            - name: PCKG_DESC
              type: varchar
              width: 255
              map: "'NA'"
            - name: PCKG_SIZE_EQVLNT_NO
              type: varchar
              width: 20
              map: "'NA'"
            - name: PCKG_SIZE_NO
              type: varchar
              width: 20
              map: "'NA'"
            - name: PROD_NO
              type: varchar
              width: 20
              map: "'NA'"
            - name: SYRNG_CPCT_VAL
              type: varchar
              width: 20
              map: "'NA'"
            - name: NDC_RTE_TYP_CD
              type: char
              width: 10
              nullable: N
              map: "NdcCrfIn.NDC_RTE_TYP_CD"
            - name: AHFS_TCC_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: DOSE_FORM_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: TCC_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NDC_DSM_DRUG_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NDC_DRUG_ABUSE_CTL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NDC_DRUG_CLS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NDC_FMT_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NDC_GNRC_MNFCTR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NDC_GNRC_NMD_DRUG_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NDC_GNRC_PRICE_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NDC_GNRC_PRICE_SPREAD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NDC_ORANGE_BOOK_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NDC_RTE_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: NdcAHFSClass
              type: decimal
              width: 18
              precision: 4
              map: NdcAHFSClass
              initValue: "-1"
            - name: NdcTherCls
              type: decimal
              width: 18
              precision: 4
              map: NdcTherCls
              initValue: "-1"
            - name: NdcDoseForm
              type: decimal
              width: 18
              precision: 4
              map: NdcDoseForm
              initValue: "1"
            - name: DSMDrugType
              type: decimal
              width: 18
              precision: 4
              map: DSMDrugType
              initValue: "-1"
            - name: DrugAbuseCtlCd
              type: decimal
              width: 18
              precision: 4
              map: DrugAbuseCtlCd
              initValue: "-1"
            - name: DrugClsCd
              type: decimal
              width: 18
              precision: 4
              map: DrugClsCd
              initValue: "-1"
            - name: DrugFormCd
              type: decimal
              width: 18
              precision: 4
              map: DrugFormCd
              initValue: "-1"
            - name: FmtCd
              type: decimal
              width: 18
              precision: 4
              map: FmtCd
              initValue: "-1"
            - name: GnrcMnfctrCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcMnfctrCd
              initValue: "-1"
            - name: GnrcNmdDrugCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcNmdDrugCd
              initValue: "-1"
            - name: GnrcPriceCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcPriceCd
              initValue: "-1"
            - name: GnrcPriceSpreadCd
              type: decimal
              width: 18
              precision: 4
              map: GnrcPriceSpreadCd
              initValue: "-1"
            - name: OrangeBookCd
              type: decimal
              width: 18
              precision: 4
              map: OrangeBookCd
              initValue: "-1"
            - name: NdcRteTypCd
              type: decimal
              width: 18
              precision: 4
              map: NdcRteTypCd
              initValue: "-1"
            - name: AvgWhlslPriceChgDt
              type: decimal
              width: 18
              precision: 4
              map: AvgWhlslPriceChgDt
              initValue: "0"
            - name: GnrcPricInChgDt
              type: decimal
              width: 18
              precision: 4
              map: GnrcPricInChgDt
              initValue: "0"
            - name: ObsltDt
              type: decimal
              width: 18
              precision: 4
              map: ObsltDt
              initValue: "0"
            - name: SrcNdcCrtDt
              type: decimal
              width: 18
              precision: 4
              map: SrcNdcCrtDt
              initValue: "0"
            - name: SrcNdcUpdtDt
              type: decimal
              width: 18
              precision: 4
              map: SrcNdcUpdtDt
              initValue: "0"
            - name: PassThru
              type: decimal
              width: 0
              map: PassThru
              initValue: "'Y'"
            - name: ErrCount
              type: decimal
              width: 0
              map: ErrCount
              initValue: "-1"
        - name: PurgeTrn.V0S20P6
          type: filter
          source: PurgeTrn.V0S20P6.map
          filter: "@INROWNUM = 1"
          schema: 
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
            - name: NDC
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
            - name: AHFS_TCC_SK
              type: int
              width: 4
              nullable: N
            - name: DOSE_FORM_SK
              type: int
              width: 4
              nullable: N
            - name: TCC_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_DSM_DRUG_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_DRUG_ABUSE_CTL_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_DRUG_FORM_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_DRUG_CLS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_FMT_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_GNRC_MNFCTR_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_GNRC_NMD_DRUG_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_GNRC_PRICE_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_GNRC_PRICE_SPREAD_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_ORANGE_BOOK_CD_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_RTE_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_TRANS_ADD_IN
              type: char
              width: 1
              nullable: N
            - name: DESI_DRUG_IN
              type: char
              width: 1
              nullable: N
            - name: DRUG_MNTN_IN
              type: char
              width: 1
              nullable: N
            - name: INNVTR_IN
              type: char
              width: 1
              nullable: N
            - name: INSTUT_PROD_IN
              type: char
              width: 1
              nullable: N
            - name: PRIV_LBLR_IN
              type: char
              width: 1
              nullable: N
            - name: SNGL_SRC_IN
              type: char
              width: 1
              nullable: N
            - name: UNIT_DOSE_IN
              type: char
              width: 1
              nullable: N
            - name: UNIT_OF_USE_IN
              type: char
              width: 1
              nullable: N
            - name: AVG_WHLSL_PRICE_CHG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: GNRC_PRICE_IN_CHG_DT_SK
              type: char
              width: 10
              nullable: N
            - name: OBSLT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SRC_NDC_CRT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: SRC_NDC_UPDT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: BRND_NM
              type: varchar
              width: 255
            - name: CORE_NINE_NO
              type: varchar
              width: 20
            - name: DRUG_LABEL_NM
              type: varchar
              width: 35
            - name: DRUG_STRG_DESC
              type: varchar
              width: 255
            - name: GCN_CD_TX
              type: varchar
              width: 20
            - name: GNRC_NM_SH_DESC
              type: varchar
              width: 255
            - name: LBLR_NM
              type: varchar
              width: 75
            - name: LBLR_NO
              type: varchar
              width: 20
            - name: NEEDLE_GAUGE_VAL
              type: varchar
              width: 20
            - name: NEEDLE_LGTH_VAL
              type: varchar
              width: 20
            - name: PCKG_DESC
              type: varchar
              width: 255
            - name: PCKG_SIZE_EQVLNT_NO
              type: varchar
              width: 20
            - name: PCKG_SIZE_NO
              type: varchar
              width: 20
            - name: PROD_NO
              type: varchar
              width: 20
            - name: SYRNG_CPCT_VAL
              type: varchar
              width: 20
          keys:
            - NDC_SK
        - name: NDCData
          type: unknown
          source: NdcOutRecycleNdc
          alias: 
          origType: CCollector


