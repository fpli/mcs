
# RaptorIO project upgrade report
## Project details
Name | Description
---- | -----------
Path to project POM |	pom.xml
Target Version |	0.14.3-RELEASE
Upgrade job ID | Some(4645253317143358)
Full upgrade log | [link](raptor-upgrade-debug-4645253317143358.log)
Upgrade warnings only log | [link](raptor-upgrade-warn-4645253317143358.log)

     ## Summary

| Operation | Details |
| ---- | ----------- |
|[com.ebay.uaas.raptor.rule.MavenAddPlugisnRule](#MavenAddPlugisnRule) | impacted 0 file(s) |
|[com.ebay.uaas.raptor.rule.raptorio.RaptorIOMavenAddDependenciesRule](#RaptorIOMavenAddDependenciesRule) | impacted 5 file(s) |
|[com.ebay.rtran.maven.MavenRemovePluginsRule](#MavenRemovePluginsRule) | impacted 19 file(s) |
|[com.ebay.uaas.raptor.rule.MavenCheckAndAddDependenciesRule](#MavenCheckAndAddDependenciesRule) | impacted 54 file(s) |
|[com.ebay.uaas.raptor.rule.RaptorStackBasedRule](#RaptorStackBasedRule) | impacted 4 file(s) |
|[com.ebay.uaas.raptor.rule.UpdateZeusExtensionVersionRule](#UpdateZeusExtensionVersionRule) | impacted 0 file(s) |
|[com.ebay.rtran.maven.MavenRemoveDependenciesRule](#MavenRemoveDependenciesRule) | impacted 0 file(s) |
|[com.ebay.uaas.raptor.rule.raptorio.ServiceConfigRemoveXmnParameterRule](#ServiceConfigRemoveXmnParameterRule) | impacted 0 file(s) |
|[com.ebay.uaas.raptor.rule.MavenPropertiesOverrideRule](#MavenPropertiesOverrideRule) | impacted 3 file(s) |
|[com.ebay.uaas.raptor.rule.RaptorPlatformVersionUpdateRule](#RaptorPlatformVersionUpdateRule) | impacted 41 file(s) |
|[com.ebay.uaas.raptor.rule.dryrun.UpsertZeusExtensionVersionRule](#UpsertZeusExtensionVersionRule) | impacted 0 file(s) |
|[com.ebay.uaas.raptor.rule.raptorio.RaptorEnableMethodSecurityRule](#RaptorEnableMethodSecurityRule) | impacted 0 file(s) |

### RaptorPlatformVersionUpdateRule
Modify the dependencies and plugins to be managed by RaptorPlatform:
      
#### File [adservice/pom.xml](adservice/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=javax.persistence, artifactId=javax.persistence-api, version=2.2, type=jar}|

#### File [spark-nrt/pom.xml](spark-nrt/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.core, artifactId=jackson-annotations, version=${jackson.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.module, artifactId=jackson-module-scala_2.11, version=${jackson.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.module, artifactId=jackson-module-paranamer, version=${jackson.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.core, artifactId=jackson-core, version=${jackson.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.core, artifactId=jackson-databind, version=${jackson.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.codehaus.janino, artifactId=janino, version=3.0.8, type=jar}|

#### File [pom.xml](pom.xml)
|Operation|artifact|
|------|----|
|RemoveFromDependencyManagement|Dependency {groupId=org.apache.kafka, artifactId=kafka-clients, version=${kafka.version}, type=jar}|

#### File [flink-nrt/pom.xml](flink-nrt/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=org.glassfish.jersey.core, artifactId=jersey-client, version=2.26, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.glassfish.jersey.media, artifactId=jersey-media-json-jackson, version=2.30, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.jaxrs, artifactId=jackson-jaxrs-json-provider, version=2.9.8, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.asynchttpclient, artifactId=async-http-client, version=2.12.1, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.core, artifactId=jackson-databind, version=2.8.7, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.slf4j, artifactId=slf4j-api, version=1.7.15, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=log4j, artifactId=log4j, version=1.2.17, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.core, artifactId=jackson-annotations, version=2.8.7, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.apache.kafka, artifactId=kafka-log4j-appender, version=2.3.0, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.glassfish.jersey.inject, artifactId=jersey-hk2, version=2.26, type=jar}|

#### File [tools/OracleCouchbase/pom.xml](tools/OracleCouchbase/pom.xml)
|Operation|artifact|
|------|----|
|RemoveFromDependencyManagement|Dependency {groupId=log4j, artifactId=log4j, version=${log4j.version}, type=jar}|

#### File [tools/couchbase-tool/pom.xml](tools/couchbase-tool/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=org.elasticsearch.client, artifactId=elasticsearch-rest-high-level-client, version=6.4.3, type=jar}|
