#word-count-beam

Java Quickstart for Apache Beam

<https://beam.apache.org/get-started/quickstart-java>

## Set up Environment

- Java
- Maven
## My Project:

- Group Repo: https://github.com/aneela123/Big-Data-Group6Project
- My Wiki Page: https://github.com/aneela123/Big-Data-Group6Project/wiki/Aneela
- My individual Project Workspace: https://github.com/aneela123/Big-Data-Group6Project/tree/main/Aneela

## Get the Sample Project

```PowerShell
mvn archetype:generate `
 -D archetypeGroupId=org.apache.beam `
 -D archetypeArtifactId=beam-sdks-java-maven-archetypes-examples `
 -D archetypeVersion=2.36.0 `
 -D groupId=org.example `
 -D artifactId=word-count-beam `
 -D version="0.1" `
 -D package=org.apache.beam.examples `
 -D interactiveMode=false`
```

## Execute using DirectRunner

```PowerShell
mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--inputFile=sample.txt --output=counts" -P direct-runner
```

## Execute PR Quick Start

```PowerShell
mvn compile exec:java -D exec.mainClass=edu.nwmissouri.BigDataGroup6.MinimalPageRankAneela 
```
