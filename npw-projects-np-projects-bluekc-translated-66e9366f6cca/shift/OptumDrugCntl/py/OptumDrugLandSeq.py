#!/usr/bin/python3

from npadf import *

def OptumDrugLandSeqActivities(ctx):
  def If_1A():
    def OptumClmLandExtr():
      parameters= {
        "FilePath": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.FilePath", ' ', 'S'), '\'', 'B')},
        "IDSDB": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSDB", ' ', 'S'), '\'', 'B')},
        "IDSOwner": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSOwner", ' ', 'S'), '\'', 'B')},
        "IDSAcct": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSAcct", ' ', 'S'), '\'', 'B')},
        "IDSPW": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.IDSPW", ' ', 'S'), '\'', 'B')},
        "RunID": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunID", ' ', 'S'), '\'', 'B')},
        "SrcSysCd": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCd", ' ', 'S'), '\'', 'B')},
        "SrcSysCdSK": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.SrcSysCdSK", ' ', 'S'), '\'', 'B')},
        "RunDate": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.CurrentDate", ' ', 'S'), '\'', 'B')},
        "InFile": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.InFile", ' ', 'S'), '\'', 'B')},
        "MbrTermDate": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.MbrTermDate", ' ', 'S'), '\'', 'B')},
        "RunCycle": {"type": "Expression", "value": ctx.npwTrim(ctx.npwTrim("@pipeline().parameters.RunCycle", ' ', 'S'), '\'', 'B')}}
      parameters.update(ctx.parameter_override)
      activity = ExecutePipelineActivity(
        name = "OptumClmLandExtr",
        wait_on_completion = True,
        pipeline = PipelineReference(type="PipelineReference",reference_name="OptumClmLandExtr"),
        parameters = parameters)
      return [], [activity]
    artifacts = []
    activities = [IfConditionActivity(name = "If_1A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('OptumClmLandExtr', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        OptumClmLandExtr()))]
    return artifacts, activities
  def If_3A():
    def LoadW_DRUG_ENR():
      activities = [CustomActivity(
        name = "LoadW_DRUG_ENR",
        command = ctx.getRoutineCommand("DB.LOAD"),
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_3A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('LoadW_DRUG_ENR', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        LoadW_DRUG_ENR()),
      depends_on = [
        ActivityDependency(
          activity = "If_1A",
          dependency_conditions = [DependencyCondition.COMPLETED])])]
    return artifacts, activities
  return merge(
    If_3A(),
    If_1A())

def OptumDrugLandSeq(ctx):
  name = "OptumDrugLandSeq"
  artifacts, activities = OptumDrugLandSeqActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/SeqOptum"),
    activities = activities,
    description = """
      OptumRX Drug Claim Landing Sequencer
      Parameters:
      -----------
      CurrentDate:
        CurrentDate
      FilePath:
        File Path
      RunCycle:
        Run Cycle
      ExclusionList:
        ExclusionList
      RunID:
        Run ID
      SrcSysCd:
        Source System
      SrcSysCdSK:
        Source Sys Cd SK
      IDSInstance:
        IDS DB Instance
      IDSDB:
        IDS Database
      IDSOwner:
        IDS DB Owner
      IDSDSN:
        IDS DSN
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
      VendorEmail:
        VendorEmail
      Logging:
        Logging
      LANAcct:
        LAN Account
      LANPW:
        LAN Password
      ProvRunCycle:
        ProvRunCycle
      InFile:
        InFile
      Env:
        Env
      MbrTermDate:
        MbrTermDate
        Used in IDS Member Extract for matching back to Optum claims.  Should not be receiving claims for termed members.  To avoid all members for past 20 years.""",
    parameters = {
      "CurrentDate": ParameterSpecification(type="String"),
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "RunCycle": ParameterSpecification(type="Int"),
      "ExclusionList": ParameterSpecification(type="String", default_value="\"\""),
      "RunID": ParameterSpecification(type="String"),
      "SrcSysCd": ParameterSpecification(type="String"),
      "SrcSysCdSK": ParameterSpecification(type="Int"),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSDSN": ParameterSpecification(type="String", default_value=ctx.IDSDSN),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String"),
      "CactusServer": ParameterSpecification(type="String", default_value=ctx.CactusServer),
      "CactusDB": ParameterSpecification(type="String", default_value=ctx.CactusDB),
      "CactusOwner": ParameterSpecification(type="String", default_value=ctx.CactusOwner),
      "CactusAcct": ParameterSpecification(type="String"),
      "CactusPW": ParameterSpecification(type="String"),
      "VendorEmail": ParameterSpecification(type="String"),
      "Logging": ParameterSpecification(type="String"),
      "LANAcct": ParameterSpecification(type="String"),
      "LANPW": ParameterSpecification(type="String"),
      "ProvRunCycle": ParameterSpecification(type="Int"),
      "InFile": ParameterSpecification(type="String"),
      "Env": ParameterSpecification(type="String"),
      "MbrTermDate": ParameterSpecification(type="String", default_value="2016-01-01")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (OptumDrugLandSeq(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
