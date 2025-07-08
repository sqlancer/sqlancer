#!/bin/bash

SPARK_BIN="$SPARK_HOME/bin/beeline -u jdbc:hive2://172.17.0.3:10000/ --silent=true  --outputformat=csv2"

databases=$($SPARK_BIN -e "SHOW DATABASES LIKE 'database*';")

num=0
for db in $databases; do
    if [[ $db == database* ]]; then
      echo "Deleting database: $db"
      $SPARK_BIN -e "DROP DATABASE $db CASCADE;"
      num=$((num+1))
    fi
done

echo "$num matching databases have been deleted in hive."

