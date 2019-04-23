#!/usr/bin/env bash

set -euo pipefail

[[ -d $PWD/maven && ! -d $HOME/.m2 ]] && ln -s $PWD/maven $HOME/.m2

r2dbc_h2_artifactory=$(pwd)/r2dbc-h2-artifactory

rm -rf $HOME/.m2/repository/io/r2dbc 2> /dev/null || :

cd r2dbc-h2
./mvnw deploy \
    -DaltDeploymentRepository=distribution::default::file://${r2dbc_h2_artifactory}
