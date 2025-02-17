#!/bin/bash

#1. Clone the ck repository first (to calculate metrics)
echo "Cloning ck.."
cd ..
git clone https://github.com/mauricioaniche/ck
echo "Ck cloned!"

#2. Enter that repo and compile it
echo "Compiling ck..."
cd ck || exit
#mvn clean compile package
echo "Ck compiled!"

#3. Run metrics test
echo "Running metrics.."
cd target || exit
java -jar ck-0.7.1-SNAPSHOT-jar-with-dependencies.jar \
    ../../sqlancer \
    true \
    0 \
    false \
    ../../output
cd ..
cd ..
echo "ALL DONE!"
