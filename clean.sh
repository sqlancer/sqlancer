#!/bin/bash

HIVE_BIN="$HIVE_HOME/bin/hive" 

databases=$($HIVE_BIN -e "SHOW DATABASES LIKE 'database*';")

for db in $databases; do
    echo "Deleting database: $db"
    $HIVE_BIN -e "DROP DATABASE $db CASCADE"
done

echo "All matching databases have been deleted in hive."

