#!/usr/bin/env bash

set -e -u

[[ -d $PWD/maven && ! -d $HOME/.m2 ]] && ln -s $PWD/maven $HOME/.m2

r2dbc_h2_artifactory=$(pwd)/r2dbc-h2-artifactory
r2dbc_spi_artifactory=$(pwd)/r2dbc-spi-artifactory

rm -rf $HOME/.m2/repository/io/r2dbc 2> /dev/null || :

cd r2dbc-h2
./mvnw deploy \
    -DaltDeploymentRepository=distribution::default::file://${r2dbc_h2_artifactory} \
    -Dr2dbcSpiArtifactory=file://${r2dbc_spi_artifactory}
