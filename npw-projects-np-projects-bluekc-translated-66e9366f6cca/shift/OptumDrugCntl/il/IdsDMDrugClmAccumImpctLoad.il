model:
  config:
    origin: datastage
  jobs:
    - name: IdsDMDrugClmAccumImpctLoad
      origin: datastage
      type: dataflow
      folder: "WebDataMart/Claim/Claim"
      description: "Load extract from IDS table iinto DataMart table CLM_DM_DRUG_CLM_ACCUM_IMPCT"
      fullDescription: "© Copyright 2020 Blue Cross/Blue Shield of Kansas City\n\nCALLED BY :  OptumDrugLandSeq\n \nDESCRIPTION:  Load extract created in IdsDMDrugClmAccumImpctExtr into DataMart table CLM_DM_DRUG_CLM_ACCUM_IMPCT.  \n\nMODIFICATIONS:\n\nDeveloper                     Date               Project                 Change Description                                                                                                   Development Project    Code Reviewer               Date Reviewed\n--------------------------          --------------------    ----------------------      ------------------------------------------------------------------------------------------------------------------------------    ---------------------------------    ------------------------------------    ----------------------------              \nBill Schroeder              07/17/2023     US-586570          Original Programming                                                                                                 IntegrateDev2                Jeyaprasanna                 2023-08-15"
      parameters:
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: ClmMartServer
          type: string
          description: "Claim Mart Server"
          defaultType: project
        - name: ClmMartDB
          type: string
          description: "Claim Mart DB"
          defaultType: project
        - name: ClmMartOwner
          type: string
          description: "Claim Mart Owner"
          defaultType: project
        - name: ClmMartAcct
          type: string
          description: "Claim Mart Account"
        - name: ClmMartPW
          type: encrypted
          description: "Claim Mart Password"
        - name: ClmMartDSN
          type: string
          description: "Claim Mart ODBC DSN"
          defaultType: project
        - name: ClmMartArraySize
          type: string
          description: "DB Stage Array Size for Claim Mart"
          defaultType: project
        - name: ClmMartRecordCount
          type: string
          description: "DB Stage Record Count for Claim Mart"
          defaultType: project
      steps:
        - name: CLM_DM_DRUG_CLM_ACCUM_IMPCT
          type: fileReader
          urls: 
            - "#$FilePath#/load/CLM_DM_DRUG_CLM_ACCUM_IMPCT.dat"
          rejectMode: continue
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
            rowDelimiter: "\n"
            fixedLength: false
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: NCP_COUPON_TYP_CD
              type: varchar
              width: 20
              nullable: N
            - name: NCP_COUPON_TYP_NM
              type: varchar
              width: 255
              nullable: N
            - name: CCAA_APLD_IN
              type: char
              width: 1
              nullable: N
            - name: CCAA_COUPON_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: FMLY_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: FMLY_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INDV_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INDV_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INDV_APLD_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INDV_APLD_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: LAST_UPDT_RUN_CYC_NO
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD
            - CLM_ID
        - name: Seq_Rej_Drug_CLm_Accum_Impt
          type: fileWriter
          urls: 
            - "#$FilePath#/load/DRUG_CLM_ACCUM_IMPCT_Rej.dat"
          source: Odbc_CLM_DM_CLMout
          rejectLink: true
          rejectMode: continue
          option: 
            updateMode: append
            writeMode: SingleFile
          format: 
            header: false
            quotes: "^"
            fieldDelimiter: ","
            rowDelimiter: "\n"
            fixedLength: false
        - name: Odbc_CLM_DM_CLMout
          type: dbWriter
          originalDb: 
          origin: odbcConnector
          writeMode: "update then insert"
          updateSql: "UPDATE #$ClmMartOwner#.CLM_DM_DRUG_CLM_ACCUM_IMPCT \nSET \nSRC_SYS_CD = ORCHESTRATE.SRC_SYS_CD,\nCLM_ID = ORCHESTRATE.CLM_ID,\nNCP_COUPON_TYP_CD = ORCHESTRATE.NCP_COUPON_TYP_CD,\nNCP_COUPON_TYP_NM = ORCHESTRATE.NCP_COUPON_TYP_NM,\nCCAA_APLD_IN = ORCHESTRATE.CCAA_APLD_IN,\nCCAA_COUPON_AMT = ORCHESTRATE.CCAA_COUPON_AMT,\nFMLY_ACCUM_DEDCT_AMT = ORCHESTRATE.FMLY_ACCUM_DEDCT_AMT,\nFMLY_ACCUM_OOP_AMT = ORCHESTRATE.FMLY_ACCUM_OOP_AMT,\nINDV_ACCUM_DEDCT_AMT = ORCHESTRATE.INDV_ACCUM_DEDCT_AMT,\nINDV_ACCUM_OOP_AMT = ORCHESTRATE.INDV_ACCUM_OOP_AMT,\nINDV_APLD_DEDCT_AMT = ORCHESTRATE.INDV_APLD_DEDCT_AMT,\nINDV_APLD_OOP_AMT = ORCHESTRATE.INDV_APLD_OOP_AMT,\nLAST_UPDT_RUN_CYC_NO = ORCHESTRATE.LAST_UPDT_RUN_CYC_NO\nWHERE (SRC_SYS_CD = ORCHESTRATE.SRC_SYS_CD AND CLM_ID = ORCHESTRATE.CLM_ID);"
          insertSql: "INSERT INTO #$ClmMartOwner#.CLM_DM_DRUG_CLM_ACCUM_IMPCT \n(SRC_SYS_CD,\nCLM_ID,\nNCP_COUPON_TYP_CD,\nNCP_COUPON_TYP_NM,\nCCAA_APLD_IN,\nCCAA_COUPON_AMT,\nFMLY_ACCUM_DEDCT_AMT,\nFMLY_ACCUM_OOP_AMT,\nINDV_ACCUM_DEDCT_AMT,\nINDV_ACCUM_OOP_AMT,\nINDV_APLD_DEDCT_AMT,\nINDV_APLD_OOP_AMT,\nLAST_UPDT_RUN_CYC_NO) VALUES (ORCHESTRATE.SRC_SYS_CD,\n ORCHESTRATE.CLM_ID,\n ORCHESTRATE.NCP_COUPON_TYP_CD,\n ORCHESTRATE.NCP_COUPON_TYP_NM,\n ORCHESTRATE.CCAA_APLD_IN,\n ORCHESTRATE.CCAA_COUPON_AMT,\n ORCHESTRATE.FMLY_ACCUM_DEDCT_AMT,\n ORCHESTRATE.FMLY_ACCUM_OOP_AMT,\n ORCHESTRATE.INDV_ACCUM_DEDCT_AMT,\n ORCHESTRATE.INDV_ACCUM_OOP_AMT,\n ORCHESTRATE.INDV_APLD_DEDCT_AMT,\n ORCHESTRATE.INDV_APLD_OOP_AMT,\n  ORCHESTRATE.LAST_UPDT_RUN_CYC_NO\n );"
          source: Copy_222
          reject: 
            number: 0
            conditions: 
            dataElements: 
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: NCP_COUPON_TYP_CD
              type: varchar
              width: 20
              nullable: N
            - name: NCP_COUPON_TYP_NM
              type: varchar
              width: 255
              nullable: N
            - name: CCAA_APLD_IN
              type: char
              width: 1
              nullable: N
            - name: CCAA_COUPON_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: FMLY_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: FMLY_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INDV_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INDV_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INDV_APLD_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INDV_APLD_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: LAST_UPDT_RUN_CYC_NO
              type: int
              width: 4
              nullable: N
            - name: RejectERRORCODE
              type: int
              width: 4
              nullable: N
            - name: RejectERRORTEXT
              type: varchar
              width: 512
              nullable: N
          keys:
            - SRC_SYS_CD
            - CLM_ID
        - name: Copy_222
          type: map
          source: CLM_DM_DRUG_CLM_ACCUM_IMPCT
          alias: DSLink224
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "DSLink224.SRC_SYS_CD"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "DSLink224.CLM_ID"
            - name: NCP_COUPON_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "DSLink224.NCP_COUPON_TYP_CD"
            - name: NCP_COUPON_TYP_NM
              type: varchar
              width: 255
              nullable: N
              map: "DSLink224.NCP_COUPON_TYP_NM"
            - name: CCAA_APLD_IN
              type: char
              width: 1
              nullable: N
              map: "DSLink224.CCAA_APLD_IN"
            - name: CCAA_COUPON_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DSLink224.CCAA_COUPON_AMT"
            - name: FMLY_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DSLink224.FMLY_ACCUM_DEDCT_AMT"
            - name: FMLY_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DSLink224.FMLY_ACCUM_OOP_AMT"
            - name: INDV_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DSLink224.INDV_ACCUM_DEDCT_AMT"
            - name: INDV_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DSLink224.INDV_ACCUM_OOP_AMT"
            - name: INDV_APLD_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DSLink224.INDV_APLD_DEDCT_AMT"
            - name: INDV_APLD_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "DSLink224.INDV_APLD_OOP_AMT"
            - name: LAST_UPDT_RUN_CYC_NO
              type: int
              width: 4
              nullable: N
              map: "DSLink224.LAST_UPDT_RUN_CYC_NO"
          keys:
            - SRC_SYS_CD
            - CLM_ID


