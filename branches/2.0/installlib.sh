#!/bin/bash

if [ -a ./lib/drmaa.jar ] 
then 
    mvn install:install-file -Dfile=./lib/drmaa.jar -Dpackaging=jar -DgroupId=org.ggf.drmaa -Dversion=1.0 -DartifactId=drmaa
else
    echo "Can not find drmaa.jar file: should be in lib directory."
fi

