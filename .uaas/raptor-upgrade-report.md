
# RaptorIO project upgrade report
## Project details
Name | Description
---- | -----------
Path to project POM |	pom.xml
Target Version |	0.19.5-RELEASE
Upgrade job ID | None
Full upgrade log | [link](raptor-upgrade-debug.log)
Upgrade warnings only log | [link](raptor-upgrade-warn.log)

     ## Summary

| Operation | Details |
| ---- | ----------- |
|[com.ebay.uaas.raptor.rule.MavenAddPlugisnRule](#MavenAddPlugisnRule) | impacted 0 file(s) |
|[com.ebay.uaas.raptor.rule.raptorio.RaptorIOMavenAddDependenciesRule](#RaptorIOMavenAddDependenciesRule) | impacted 0 file(s) |
|[com.ebay.rtran.maven.MavenRemovePluginsRule](#MavenRemovePluginsRule) | impacted 1 file(s) |
|[com.ebay.uaas.raptor.rule.MavenCheckAndAddDependenciesRule](#MavenCheckAndAddDependenciesRule) | impacted 4 file(s) |
|[com.ebay.uaas.raptor.rule.RaptorStackBasedRule](#RaptorStackBasedRule) | impacted 5 file(s) |
|[com.ebay.uaas.raptor.rule.raptorio.ServiceConfigRemoveXmnParameterRule](#ServiceConfigRemoveXmnParameterRule) | impacted 0 file(s) |
|[com.ebay.uaas.raptor.rule.RaptorPlatformVersionUpdateRule](#RaptorPlatformVersionUpdateRule) | impacted 2 file(s) |
|[com.ebay.uaas.raptor.rule.dryrun.UpsertZeusExtensionVersionRule](#UpsertZeusExtensionVersionRule) | impacted 0 file(s) |
|[com.ebay.uaas.raptor.rule.raptorio.RaptorEnableMethodSecurityRule](#RaptorEnableMethodSecurityRule) | impacted 0 file(s) |

### RaptorPlatformVersionUpdateRule
Modify the dependencies and plugins to be managed by RaptorPlatform:
      
#### File [pom.xml](/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=org.apache.kafka, artifactId=kafka-clients, version=${kafka.version}, type=jar}|
