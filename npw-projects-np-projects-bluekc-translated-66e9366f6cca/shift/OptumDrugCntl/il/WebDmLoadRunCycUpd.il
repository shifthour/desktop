model:
  config:
    origin: datastage
  jobs:
    - name: WebDmLoadRunCycUpd
      origin: datastage
      type: dataflow
      folder: "WebDataMart/Claim/CycUpdt"
      description: "Web Data Mart Claim extract from IDS run cycle load indicator update"
      fullDescription: "COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\nDESCRIPTION:    Update the P_RUN_CYC table CDM load indicator fields to show those records have been copied to the Claim Data Mart.\n      \nPROCESSING:  Each IDS Claim source has a seperate SQL to extract the maximum run cycle value.  These run cycle values are used to update the IDS P_RUN_CYCLE, load indicator field for the particular source and subject.  All run cycles less than the maximum where the indicator is \"N\" are updated to \"Y\".\n\nCalled by :IdsWebDMDrugLoadSeq\n  \nMODIFICATIONS:\nDate                 Developer                Change Description\n------------------      ----------------------------     --------------------------------------------------------------------------------------------------------\n2006-04-07      Brent Leland            Original Programming.\n2006-05-08      Brent Leland            Added Argus SQL\n2006-07-03      Brent Leland            Changed SQL update statement.  Was incorrectly updating EDW_LOAD_IN.  Changed to update CDM_LOAD_IN.\n\n                                                 Project/Altiris #                      Change Description                                                                    Development                 Code                          Date \nDeveloper          Date                                                                                                                                                                Project                           Reviewer                   Reviewed       \n------------------       ----------------       ------------------------                      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       \nSandrew           2008-10-03     3784(PBM)                            Added ESI as a Source to update CDM_LOAD_IN                      devlIDSnew                  Steph Goddard           10/28/2008\n\nSrinidhi              2019-10-11   6131- PBM Replacement       Added new data source for OPTUMRX                                        IntegrateDev1                Kalyan Neelam            2019-11-25\nKambham\n\nReddy sanam  2020-09-14   US281070                              Added new data source for Lumeris and Medimpact                      IntegrateDev2                Kalyan Neelam            2020-09-15\n\nReddy sanam  2020-12-09   US329127                              Added new data source for EyeMed and DentaQuest                    IntegrateDev2                \n\n\nGoutham Kalidindi 2021-05-05   US-377804                       Added new data source for LIVONGO(Lvnghlth)                            IntegrateDev2                 Jeyaprasanna             2021-05-06"
      parameters:
        - name: FilePath
          type: string
          description: "EDW File Path"
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
        - name: IDSDSN
          type: string
          description: "IDS ODBC DSN"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
      steps:
        - name: Facets_Clm
          type: dbReader
          originalDb: db2
          query: "SELECT  SRC_SYS_CD,  max(LAST_UPDT_RUN_CYC_EXCTN_SK)\nFROM #$IDSOwner#.W_WEBDM_ETL_DRVR\nWHERE SRC_SYS_CD = 'FACETS'\nGROUP BY SRC_SYS_CD"
          tableAction: append
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD
            - LAST_UPDT_RUN_CYC_EXCTN_SK
        - name: Argus_Clm
          type: dbReader
          originalDb: db2
          query: "SELECT  SRC_SYS_CD,  max(LAST_UPDT_RUN_CYC_EXCTN_SK)\nFROM #$IDSOwner#.W_WEBDM_ETL_DRVR\nWHERE SRC_SYS_CD = 'ARGUS'\nGROUP BY SRC_SYS_CD"
          tableAction: append
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD
            - LAST_UPDT_RUN_CYC_EXCTN_SK
        - name: ESI_Clm
          type: dbReader
          originalDb: db2
          query: "SELECT  SRC_SYS_CD,  max(LAST_UPDT_RUN_CYC_EXCTN_SK)\nFROM #$IDSOwner#.W_WEBDM_ETL_DRVR\nWHERE SRC_SYS_CD = 'ESI'\nGROUP BY SRC_SYS_CD\n"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD
            - LAST_UPDT_RUN_CYC_EXCTN_SK
        - name: OPTUMRX_Clm
          type: dbReader
          originalDb: db2
          query: "SELECT  SRC_SYS_CD,  max(LAST_UPDT_RUN_CYC_EXCTN_SK)\nFROM #$IDSOwner#.W_WEBDM_ETL_DRVR\nWHERE SRC_SYS_CD = 'OPTUMRX'\nGROUP BY SRC_SYS_CD\n"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD
            - LAST_UPDT_RUN_CYC_EXCTN_SK
        - name: MedImpact_Clm
          type: dbReader
          originalDb: db2
          query: "SELECT  SRC_SYS_CD,  max(LAST_UPDT_RUN_CYC_EXCTN_SK)\nFROM #$IDSOwner#.W_WEBDM_ETL_DRVR\nWHERE SRC_SYS_CD = 'MEDIMPACT'\nGROUP BY SRC_SYS_CD"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD
            - LAST_UPDT_RUN_CYC_EXCTN_SK
        - name: Lumeris_Clm
          type: dbReader
          originalDb: db2
          query: "SELECT  SRC_SYS_CD,  max(LAST_UPDT_RUN_CYC_EXCTN_SK)\nFROM #$IDSOwner#.W_WEBDM_ETL_DRVR\nWHERE SRC_SYS_CD = 'LUMERIS'\nGROUP BY SRC_SYS_CD"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD
            - LAST_UPDT_RUN_CYC_EXCTN_SK
        - name: EyeMed_Clm
          type: dbReader
          originalDb: db2
          query: "SELECT  SRC_SYS_CD,  max(LAST_UPDT_RUN_CYC_EXCTN_SK)\nFROM #$IDSOwner#.W_WEBDM_ETL_DRVR\nWHERE SRC_SYS_CD = 'EYEMED'\nGROUP BY SRC_SYS_CD"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD
            - LAST_UPDT_RUN_CYC_EXCTN_SK
        - name: DentaQuest_Clm
          type: dbReader
          originalDb: db2
          query: "SELECT  SRC_SYS_CD,  max(LAST_UPDT_RUN_CYC_EXCTN_SK)\nFROM #$IDSOwner#.W_WEBDM_ETL_DRVR\nWHERE SRC_SYS_CD = 'DENTAQUEST'\nGROUP BY SRC_SYS_CD"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD
            - LAST_UPDT_RUN_CYC_EXCTN_SK
        - name: Nasco_Clm
          type: dbReader
          originalDb: db2
          query: "SELECT  SRC_SYS_CD,  max(LAST_UPDT_RUN_CYC_EXCTN_SK)\nFROM #$IDSOwner#.W_WEBDM_ETL_DRVR\nWHERE SRC_SYS_CD = 'NPS'\nGROUP BY SRC_SYS_CD"
          tableAction: append
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD
            - LAST_UPDT_RUN_CYC_EXCTN_SK
        - name: Lvnghlth_Clm
          type: dbReader
          originalDb: db2
          query: "SELECT  SRC_SYS_CD,  max(LAST_UPDT_RUN_CYC_EXCTN_SK)\nFROM #$IDSOwner#.W_WEBDM_ETL_DRVR\nWHERE SRC_SYS_CD = 'LVNGHLTH'\nGROUP BY SRC_SYS_CD"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
          keys:
            - SRC_SYS_CD
            - LAST_UPDT_RUN_CYC_EXCTN_SK
        - name: MapColumns
          type: map
          source: Collector
          alias: Combined
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Combined.SRC_SYS_CD"
            - name: SUBJ_CD
              type: varchar
              width: 20
              nullable: N
              map: "'CLAIM'"
            - name: TRGT_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "'IDS'"
            - name: RUN_CYC_NO
              type: int
              width: 4
              nullable: N
              map: "Combined.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CDM_LOAD_IN
              type: char
              width: 1
              nullable: N
              map: "'Y'"
          keys:
            - SRC_SYS_CD
            - SUBJ_CD
            - TRGT_SYS_CD
            - RUN_CYC_NO
        - name: Collector
          type: unknown
          source: P_RUN_CYC
          alias: 
          origType: CCollector
        - name: P_RUN_CYC
          type: unknown
          source: P_RUN_CYC
          origType: CODBCStage


