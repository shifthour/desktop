model:
  config:
    origin: datastage
  jobs:
    - name: IDSDrugDailyCleanupSeq
      origin: datastage
      type: controlflow
      folder: "claim/SeqDrug"
      checkpoint: true
      description: "IDS Drug Cleanup"
      parameters:
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: ExclusionList
          type: string
          description: ExclusionList
          defaultType: expr
          defaultValue: "\"\""
        - name: RunID
          type: integer
          description: "Run ID"
        - name: IDSAcct
          type: string
          description: "IDS DB Account"
          defaultType: expr
          defaultValue: " "
        - name: IDSPW
          type: encrypted
          description: "IDS DB Password"
        - name: IDSOwner
          type: string
          description: "IDS DB Owner"
          defaultType: project
        - name: IDSInstance
          type: string
          description: "IDS DB Instance"
          defaultType: project
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSDSN
          type: string
          description: "IDS DSN"
          defaultType: project
      activities:
        - name: Any1
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Any2
        - name: Any3
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_2A
        - name: StartLoop
          type: startLoop
          loopType: list
          loopList: "'W_CLM_RVRSL,W_DRUG_CLM,W_DRUG_ENR,W_PCS_CLM_PCP,K_CLM_TEMP,K_CLM_LN_TEMP,K_CLM_PROV_TEMP,K_CLM_COB_TEMP'"
          delimiter: ","
          triggers: 
            - activity: Truncate_Table
        - name: EndLoop
          type: endLoop
          startID: StartLoop
          triggers: 
            - activity: Or_2A
        - name: Any2
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Any3
        - name: If_1A
          type: switch
          triggers: 
            - activity: Or_1A
              conditionType: Otherwise
            - activity: NascoFile
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"MvFiles\" :\";\") = 0"
        - name: Or_1A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Any1
        - name: MvFiles
          type: shell
          path: cd
          command: "#$FilePath#/load/processed;  mv -f  ../ClmRvversalUpdates.#RunID# . ; mv -f ../CLM_PCA_UPDT.dat ."
          triggers: 
            - activity: Or_1A
        - name: Truncate_Table
          type: routine
          routinename: "DB.TRUNCATE"
          parameters:
            - name: "DBInstance"
              expression: "$IDSInstance"
            - name: "DBName"
              expression: "$IDSDB"
            - name: "DBOwner"
              expression: "$IDSOwner"
            - name: "DBAcct"
              expression: "$IDSAcct"
            - name: "DBPW"
              expression: "$IDSPW"
            - name: "FilePath"
              expression: "$FilePath"
            - name: "DBTable"
              expression: "StartLoop.$Counter"
          triggers: 
            - activity: EndLoop
              conditionType: OK
        - name: Or_2A
          type: sequencer
          sequencerType: any
        - name: If_2A
          type: switch
          triggers: 
            - activity: Or_2A
              conditionType: Otherwise
            - activity: StartLoop
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"Truncate_Table\" :\";\") = 0"
        - name: NascoFile
          type: wait
          path: "#$FilePath#/load/ClmRvversalUpdates.#RunID#"
          checkMode: appear
          timeout: 2
          triggers: 
            - activity: MvFiles
              conditionType: OK
            - activity: Any2
              conditionType: Otherwise
        - name: WEB_DRVR
          type: wait
          path: "#$FilePath#/load/W_WEBDM_ETL_DRVR.dat.Drug.#RunID#"
          checkMode: appear
          timeout: 2
          triggers: 
            - activity: Mv_Webdrvr
              conditionType: OK
            - activity: Any2
              conditionType: Otherwise
        - name: Mv_Webdrvr
          type: shell
          path: cd
          command: "#$FilePath#/load/processed; mv -f  ../W_WEBDM_ETL_DRVR.dat.Drug.#RunID# . "
          triggers: 
            - activity: Or_1B
        - name: Or_1B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Any1
        - name: If_1B
          type: switch
          triggers: 
            - activity: Or_1B
              conditionType: Otherwise
            - activity: WEB_DRVR
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"MvFiles\" :\";\") = 0"
        - name: If_2B
          type: switch
          triggers: 
            - activity: Or_2B
              conditionType: Otherwise
            - activity: WEB_DRVR_RevAdj
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"MvFiles\" :\";\") = 0"
        - name: Or_2B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Any1
        - name: Mv_WebdrvrRevAdj
          type: shell
          path: cd
          command: "#$FilePath#/load/processed; mv -f  ../W_WEBDM_ETL_DRVR.dat.DrugReversalAdj.#RunID# . "
          triggers: 
            - activity: Or_2B
        - name: WEB_DRVR_RevAdj
          type: wait
          path: "#$FilePath#/load/W_WEBDM_ETL_DRVR.dat.DrugReversalAdj.#RunID#"
          checkMode: appear
          timeout: 2
          triggers: 
            - activity: Mv_WebdrvrRevAdj
              conditionType: OK
            - activity: Any2
              conditionType: Otherwise
        - name: WEB_DRVR_RevOrig
          type: wait
          path: "#$FilePath#/load/W_WEBDM_ETL_DRVR.dat.DrugReversalOrig.#RunID#"
          checkMode: appear
          timeout: 2
          triggers: 
            - activity: Mv_WebdrvrRevsOrig
              conditionType: OK
            - activity: Any2
              conditionType: Otherwise
        - name: Mv_WebdrvrRevsOrig
          type: shell
          path: cd
          command: "#$FilePath#/load/processed; mv -f  ../W_WEBDM_ETL_DRVR.dat.DrugReversalOrig.#RunID# . "
          triggers: 
            - activity: Or_3B
        - name: Or_3B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Any1
        - name: If_3B
          type: switch
          triggers: 
            - activity: Or_3B
              conditionType: Otherwise
            - activity: WEB_DRVR_RevOrig
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"MvFiles\" :\";\") = 0"


