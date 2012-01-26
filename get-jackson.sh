#!/bin/bash
j="jackson-all-1.9.4.jar"
if [ ! -f jar/"$j" ]
then
  wget -O jar/"$j" \
    http://jackson.codehaus.org/1.9.4/jackson-all-1.9.4.jar
fi
