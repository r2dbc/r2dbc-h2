#!/bin/bash

set -euo pipefail

#MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home"

# Build H2.next
rm -Rf h2database
git clone https://github.com/h2database/h2database.git
cd h2database/h2
../../mvnw clean install -DskipTests -B
../../mvnw -q org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version
H2_VERSION=$(../../mvnw org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -o | grep -v INFO)
cd ../..

echo "Testing against H2 ${H2_VERSION}"
./mvnw clean dependency:list test -Dsort -Dh2.version=${H2_VERSION} -B
