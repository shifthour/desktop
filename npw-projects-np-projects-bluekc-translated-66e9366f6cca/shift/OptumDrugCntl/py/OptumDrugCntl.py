#!/usr/bin/python3

from npadf import *

def OptumDrugCntlActivities(ctx):
  def Initial():
    return [], [SetVariableActivity(
      name = "Initial",
      variable_name = "JobName",
      value = "@pipeline().parameters.DSJobName")]
  def UV1_Initial():
    return [], [SetVariableActivity(
      name = "UV1_Initial",
      variable_name = "GroupName",
      value = "'IDS_OPTUMRX_DRUG'",
      depends_on = [
        ActivityDependency(
          activity = "Initial",
          dependency_conditions = [DependencyCondition.SUCCEEDED])])]
  def UV2_Initial():
    return [], [SetVariableActivity(
      name = "UV2_Initial",
      variable_name = "Source",
      value = "'OPTUMRX'",
      depends_on = [
        ActivityDependency(
          activity = "UV1_Initial",
          dependency_conditions = [DependencyCondition.SUCCEEDED])])]
  def UV3_Initial():
    return [], [SetVariableActivity(
      name = "UV3_Initial",
      variable_name = "Target",
      value = "'IDS'",
      depends_on = [
        ActivityDependency(
          activity = "UV2_Initial",
          dependency_conditions = [DependencyCondition.SUCCEEDED])])]
  def UV4_Initial():
    return [], [SetVariableActivity(
      name = "UV4_Initial",
      variable_name = "Subject",
      value = "'CLAIM'",
      depends_on = [
        ActivityDependency(
          activity = "UV3_Initial",
          dependency_conditions = [DependencyCondition.SUCCEEDED])])]
  def UV5_Initial():
    return [], [SetVariableActivity(
      name = "UV5_Initial",
      variable_name = "Frequency",
      value = "'DAILY'",
      depends_on = [
        ActivityDependency(
          activity = "UV4_Initial",
          dependency_conditions = [DependencyCondition.SUCCEEDED])])]
  def ProcessCheckExtr():
    parameters= {
      "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
      "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
      "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
      "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')},
      "Subject": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@variables('Subject')", ' ', 'S'), '\'', 'B')},
      "TargetSys": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@variables('Target')", ' ', 'S'), '\'', 'B')},
      "SourceSys": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@variables('Source')", ' ', 'S'), '\'', 'B')}}
    parameters.update(ctx.parameter_override)
    activity = ExecutePipelineActivity(
      name = "ProcessCheckExtr",
      wait_on_completion = True,
      pipeline = PipelineReference(type="PipelineReference",reference_name="ProcessCheckExtr"),
      depends_on = [
        ActivityDependency(
          activity = "UV5_Initial",
          dependency_conditions = [DependencyCondition.COMPLETED])],
      parameters = parameters)
    return [], [activity]
  def LinkCount():
    return [], [SetVariableActivity(
      name = "LinkCount",
      variable_name = "LinkCount",
      value = "@JobLinkCount(concat('ProcessCheckExtr.', variables('JobName')), 'Trans1', 'Output')",
      depends_on = [
        ActivityDependency(
          activity = "ProcessCheckExtr",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
  def UV1_LinkCount():
    return [], [SetVariableActivity(
      name = "UV1_LinkCount",
      variable_name = "ExclusionList",
      value = "@GetExclusionList(variables('JobName'))",
      depends_on = [
        ActivityDependency(
          activity = "LinkCount",
          dependency_conditions = [DependencyCondition.SUCCEEDED])])]
  def CountChk():
    def Email_DAT():
      return [], [WaitActivity(
        name = "Email_DAT",
        description = """Placeholder for send email activity
      from: _DATA_WAREHOUSE_SUPPORT@bcbskc.com
      to: @pipeline().parameters.EmailSupport
      subject: Optum RX Claim Process Abort
      body: The Optum RX Claim processing cannot run while IDS claim is being updated or extracted from.

      Source:  Optum RX
      Target:    IDS
      Subject:  Claim""",
        wait_time_in_seconds = 1)]
    def Failed():
      activities = [DatabricksNotebookActivity(
        name = "Failed",
        depends_on = [
          ActivityDependency(
            activity = "Email_DAT",
            dependency_conditions = [DependencyCondition.COMPLETED])],
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.notebook_linked_service),
        notebook_path = ctx.notebook_path,
        base_parameters = {"command": "text""type""field""name""type""text""type""name""type""text""type""field""name""type""text""type"})]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "CountChk",
      expression = Expression(type="Expression", value="@not(equals(variables('LinkCount'),0))"),
      if_true_activities = collect(artifacts,
        Email_DAT(),
        Failed()),
      depends_on = [
        ActivityDependency(
          activity = "UV1_LinkCount",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  return merge(
    CountChk(),
    UV1_LinkCount(),
    LinkCount(),
    ProcessCheckExtr(),
    UV5_Initial(),
    UV4_Initial(),
    UV3_Initial(),
    UV2_Initial(),
    UV1_Initial(),
    Initial())

def OptumDrugCntl(ctx):
  name = "OptumDrugCntl"
  artifacts, activities = OptumDrugCntlActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim"),
    activities = activities,
    description = """
      Extract OPTUMRX claims and load to IDS called via CLAIMS_OPTUMRX_DLY_WRHS_000
      Parameters:
      -----------
      APT_CONFIG_FILE:
        Configuration file
        The Parallel job configuration file.
      FilePath:
        File Path
      EDWFilePath:
        EDW File Path
      EmailSupport:
        Warehouse Support Email
      IDSInstance:
        IDS DB Instance
      IDSDSN:
        IDS DSN
      IDSDB:
        IDS Database
      IDSOwner:
        IDS DB Owner
      IDSAcct:
        IDS DB Account
      IDSPW:
        IDS DB Password
      CactusServer:
        Cactus Server
      CactusDB:
        Cactus Database
      CactusOwner:
        Cactus Table Owner
      CactusAcct:
        Cactus Account
      CactusPW:
        Cactus Password
      ClmMartAcct:
        Claim Mart Account
      ClmMartPW:
        Claim Mart Password
      ClmMartServer:
        Claim Mart Server
      ClmMartDB:
        Claim Mart Database
      ClmMartOwner:
        Claim Mart Table Owner
      UWSDB:
        UWS Database
      UWSOwner:
        UWS Table Owner
      UWSPW:
        UWS Password
      UWSAcct:
        UWS Account
      LANAcct:
        LAN Account
      LANPW:
        LAN Password
      IDSDB2ArraySize:
        IDS DB2 Array Size for Load
      IDSDB2RecordCount:
        IDS DB2 Record Count for Load
      VendorEmail:
        VendorEmail
      Env:
        Env
      MbrTermDate:
        MbrTermDate
        Used in IDS Member Extract for matching back to Optum claims.  Should not be receiving claims for termed members.  To avoid all members for past 20 years.
      LOB:
        LOB""",
    parameters = {
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String"),
      "CactusAcct": ParameterSpecification(type="String"),
      "CactusPW": ParameterSpecification(type="String"),
      "ClmMartAcct": ParameterSpecification(type="String"),
      "ClmMartPW": ParameterSpecification(type="String"),
      "UWSPW": ParameterSpecification(type="String"),
      "UWSAcct": ParameterSpecification(type="String"),
      "LANAcct": ParameterSpecification(type="String"),
      "LANPW": ParameterSpecification(type="String"),
      "VendorEmail": ParameterSpecification(type="String"),
      "Env": ParameterSpecification(type="String", default_value="T"),
      "MbrTermDate": ParameterSpecification(type="String"),
      "LOB": ParameterSpecification(type="String")},
    variables = {
      "CountChk_fired": VariableSpecification(type="String", default_value="n"),
      "JobName": VariableSpecification(type="String"),
      "LinkCount": VariableSpecification(type="String"),
      "GroupName": VariableSpecification(type="String"),
      "Source": VariableSpecification(type="String"),
      "Target": VariableSpecification(type="String"),
      "Subject": VariableSpecification(type="String"),
      "Frequency": VariableSpecification(type="String"),
      "ExclusionList": VariableSpecification(type="String")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (OptumDrugCntl(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
