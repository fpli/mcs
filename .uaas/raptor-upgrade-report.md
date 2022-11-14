
# RaptorIO project upgrade report
## Project details
Name | Description
---- | -----------
Path to project POM |	pom.xml
Target Version |	0.16.1-RELEASE
Upgrade job ID | None
Full upgrade log | [link](raptor-upgrade-debug.log)
Upgrade warnings only log | [link](raptor-upgrade-warn.log)

     ## Summary

| Operation | Details |
| ---- | ----------- |
|[com.ebay.uaas.raptor.rule.MavenAddPlugisnRule](#MavenAddPlugisnRule) | impacted 0 file(s) |
|[com.ebay.uaas.raptor.rule.raptorio.RaptorIOMavenAddDependenciesRule](#RaptorIOMavenAddDependenciesRule) | impacted 0 file(s) |
|[com.ebay.rtran.maven.MavenRemovePluginsRule](#MavenRemovePluginsRule) | impacted 12 file(s) |
|[com.ebay.uaas.raptor.rule.MavenCheckAndAddDependenciesRule](#MavenCheckAndAddDependenciesRule) | impacted 10 file(s) |
|[com.ebay.uaas.raptor.rule.RaptorStackBasedRule](#RaptorStackBasedRule) | impacted 5 file(s) |
|[com.ebay.uaas.raptor.rule.raptorio.ServiceConfigRemoveXmnParameterRule](#ServiceConfigRemoveXmnParameterRule) | impacted 0 file(s) |
|[com.ebay.uaas.raptor.rule.RaptorPlatformVersionUpdateRule](#RaptorPlatformVersionUpdateRule) | impacted 42 file(s) |
|[com.ebay.uaas.raptor.rule.dryrun.UpsertZeusExtensionVersionRule](#UpsertZeusExtensionVersionRule) | impacted 0 file(s) |
|[com.ebay.uaas.raptor.rule.raptorio.RaptorEnableMethodSecurityRule](#RaptorEnableMethodSecurityRule) | impacted 0 file(s) |

### RaptorPlatformVersionUpdateRule
Modify the dependencies and plugins to be managed by RaptorPlatform:
      
#### File [event-listener/pom.xml](/event-listener/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-migration, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-nukv, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-failover, version=${dukes.version}, type=jar}|

#### File [adservice/pom.xml](/adservice/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-couchbase2, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-couchbase, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-dal3, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-nukv, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-ukernel, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-simple, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-api, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-mcf, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-memcached, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-migration, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-fountclient, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-failover, version=${dukes.version}, type=jar}|

#### File [spark-nrt/pom.xml](/spark-nrt/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-migration, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-nukv, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-failover, version=${dukes.version}, type=jar}|

#### File [listener/pom.xml](/listener/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes, version=${dukes.version}, type=jar}|

#### File [pom.xml](/pom.xml)
|Operation|artifact|
|------|----|
|RemoveFromDependencyManagement|Dependency {groupId=org.apache.kafka, artifactId=kafka-clients, version=${kafka.version}, type=jar}|

#### File [flink-nrt/pom.xml](/flink-nrt/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-migration, version=1.6.26, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-nukv, version=1.6.26, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-fountclient, version=1.6.26, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes, version=1.6.26, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-simple, version=1.6.26, type=jar}|

#### File [filter/pom.xml](/filter/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-simple, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-api, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-failover, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-couchbase, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-nukv, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-couchbase2, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-memcached, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-ukernel, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-mcf, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-fountclient, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-dal3, version=${dukes.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.ebay.dukes, artifactId=dukes-migration, version=${dukes.version}, type=jar}|
