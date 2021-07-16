
# RaptorIO project upgrade report
## Project details
Name | Description
---- | -----------
Path to project POM |	pom.xml
Target Version |	0.12.4-RELEASE
Upgrade job ID | Some(10721613891285587)
Full upgrade log | [link](raptor-upgrade-debug-10721613891285587.log)
Upgrade warnings only log | [link](raptor-upgrade-warn-10721613891285587.log)

     ## Summary

| Operation | Details |
| ---- | ----------- |
|[com.ebay.uaas.raptor.rule.MavenAddPlugisnRule](#MavenAddPlugisnRule) | impacted 0 file(s) |
|[com.ebay.uaas.raptor.rule.raptorio.RaptorIOMavenAddDependenciesRule](#RaptorIOMavenAddDependenciesRule) | impacted 19 file(s) |
|[com.ebay.rtran.maven.MavenRemovePluginsRule](#MavenRemovePluginsRule) | impacted 19 file(s) |
|[com.ebay.uaas.raptor.rule.MavenCheckAndAddDependenciesRule](#MavenCheckAndAddDependenciesRule) | impacted 54 file(s) |
|[com.ebay.uaas.raptor.rule.RaptorStackBasedRule](#RaptorStackBasedRule) | impacted 3 file(s) |
|[com.ebay.rtran.maven.MavenRemoveDependenciesRule](#MavenRemoveDependenciesRule) | impacted 0 file(s) |
|[com.ebay.uaas.raptor.rule.raptorio.ServiceConfigRemoveXmnParameterRule](#ServiceConfigRemoveXmnParameterRule) | impacted 0 file(s) |
|[com.ebay.uaas.raptor.rule.RaptorPlatformVersionUpdateRule](#RaptorPlatformVersionUpdateRule) | impacted 126 file(s) |
|[com.ebay.uaas.raptor.rule.raptorio.RaptorEnableMethodSecurityRule](#RaptorEnableMethodSecurityRule) | impacted 0 file(s) |

### RaptorPlatformVersionUpdateRule
Modify the dependencies and plugins to be managed by RaptorPlatform:
      
#### File [event-listener/pom.xml](event-listener/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=javax.persistence, artifactId=javax.persistence-api, version=2.2, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=io.swagger, artifactId=swagger-annotations, version=${swagger.annotations.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.jetbrains.kotlin, artifactId=kotlin-stdlib, version=${kotlin.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.asynchttpclient, artifactId=async-http-client, version=2.12.1, type=jar}|

#### File [tools/zookeeper-tool/pom.xml](tools/zookeeper-tool/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=com.couchbase.client, artifactId=java-client, version=2.5.3, type=jar}|
|RemovePluginVersion|Plugin [org.apache.maven.plugins:maven-checkstyle-plugin]|
|RemovePluginVersion|Plugin [org.apache.maven.plugins:maven-source-plugin]|

#### File [adservice/pom.xml](adservice/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=javax.persistence, artifactId=javax.persistence-api, version=2.2, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=io.swagger, artifactId=swagger-annotations, version=${swagger.annotations.version}, type=jar}|

#### File [spark-nrt/pom.xml](spark-nrt/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.core, artifactId=jackson-databind, version=${jackson.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.core, artifactId=jackson-core, version=${jackson.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.module, artifactId=jackson-module-scala_2.11, version=${jackson.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.core, artifactId=jackson-annotations, version=${jackson.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.module, artifactId=jackson-module-paranamer, version=${jackson.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.codehaus.janino, artifactId=janino, version=3.0.8, type=jar}|

#### File [tools/Td-Bridge/pom.xml](tools/Td-Bridge/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.core, artifactId=jackson-databind, version=2.6.6, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.apache.kafka, artifactId=kafka-clients, version=${kafka.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=junit, artifactId=junit, version=4.12, type=jar}|

#### File [tools/hadoop-distcp/pom.xml](tools/hadoop-distcp/pom.xml)
|Operation|artifact|
|------|----|
|RemovePluginVersion|Plugin [org.apache.maven.plugins:maven-checkstyle-plugin]|
|RemovePluginVersion|Plugin [org.apache.maven.plugins:maven-source-plugin]|

#### File [listener/pom.xml](listener/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=javax.persistence, artifactId=javax.persistence-api, version=2.2, type=jar}|

#### File [pom.xml](pom.xml)
|Operation|artifact|
|------|----|
|RemoveFromPluginManagement|Plugin [org.apache.maven.plugins:maven-compiler-plugin]|
|RemovePluginVersion|Plugin [org.apache.maven.plugins:maven-surefire-plugin]|
|RemoveFromDependencyManagement|Dependency {groupId=org.springframework, artifactId=spring-web, version=5.2.8.RELEASE, type=jar}|
|RemoveFromDependencyManagement|Dependency {groupId=org.apache.kafka, artifactId=kafka-clients, version=${kafka.version}, type=jar}|
|RemoveFromDependencyManagement|Dependency {groupId=com.couchbase.client, artifactId=java-client, version=${couchbase.client.version}, type=jar}|
|RemoveFromPluginManagement|Plugin [org.apache.maven.plugins:maven-failsafe-plugin]|

#### File [flink-nrt/pom.xml](flink-nrt/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.core, artifactId=jackson-databind, version=2.8.7, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.core, artifactId=jackson-annotations, version=2.8.7, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.asynchttpclient, artifactId=async-http-client, version=2.12.1, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.apache.kafka, artifactId=kafka-log4j-appender, version=2.3.0, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.jaxrs, artifactId=jackson-jaxrs-json-provider, version=2.9.8, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.slf4j, artifactId=slf4j-api, version=1.7.15, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.glassfish.jersey.core, artifactId=jersey-client, version=2.26, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.glassfish.jersey.media, artifactId=jersey-media-json-jackson, version=2.30, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.glassfish.jersey.inject, artifactId=jersey-hk2, version=2.26, type=jar}|

#### File [tools/OracleCouchbase/pom.xml](tools/OracleCouchbase/pom.xml)
|Operation|artifact|
|------|----|
|RemoveFromDependencyManagement|Dependency {groupId=org.elasticsearch.client, artifactId=elasticsearch-rest-high-level-client, version=${elasticsearch.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.google.code.gson, artifactId=gson, version=2.8.5, type=jar}|
|RemoveFromPluginManagement|Plugin [org.apache.maven.plugins:maven-compiler-plugin]|
|RemoveFromDependencyManagement|Dependency {groupId=org.elasticsearch.client, artifactId=elasticsearch-rest-client, version=${elasticsearch.version}, type=jar}|
|RemovePluginVersion|Plugin [org.jacoco:jacoco-maven-plugin]|
|RemoveDependencyVersion|Dependency {groupId=com.couchbase.client, artifactId=java-client, version=${couchbase.client.version}, type=jar}|
|RemoveFromDependencyManagement|Dependency {groupId=com.couchbase.client, artifactId=java-client, version=${couchbase.client.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.elasticsearch.client, artifactId=elasticsearch-rest-client, version=6.3.1, type=jar}|
|RemoveFromPluginManagement|Plugin [org.apache.maven.plugins:maven-failsafe-plugin]|

#### File [tools/CheckRecover-tool/pom.xml](tools/CheckRecover-tool/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.core, artifactId=jackson-annotations, version=${jackson.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.module, artifactId=jackson-module-paranamer, version=${jackson.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.core, artifactId=jackson-databind, version=${jackson.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.module, artifactId=jackson-module-scala_2.11, version=${jackson.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.fasterxml.jackson.core, artifactId=jackson-core, version=${jackson.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.codehaus.janino, artifactId=janino, version=3.0.8, type=jar}|

#### File [filter/pom.xml](filter/pom.xml)
|Operation|artifact|
|------|----|
|RemoveDependencyVersion|Dependency {groupId=org.mockito, artifactId=mockito-core, version=1.10.19, type=jar}|

#### File [tools/couchbase-tool/pom.xml](tools/couchbase-tool/pom.xml)
|Operation|artifact|
|------|----|
|RemovePluginVersion|Plugin [org.apache.maven.plugins:maven-source-plugin]|
|RemoveDependencyVersion|Dependency {groupId=org.elasticsearch, artifactId=elasticsearch, version=${elasticsearch.version}, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=com.couchbase.client, artifactId=java-client, version=2.7.11, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.mockito, artifactId=mockito-core, version=1.10.19, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=org.elasticsearch.client, artifactId=elasticsearch-rest-high-level-client, version=6.4.3, type=jar}|
|RemoveDependencyVersion|Dependency {groupId=junit, artifactId=junit, version=4.12, type=jar}|
