#!/bin/bash

echo -e "\n >>>  Create database and import schemas <<< \n"

echo $(pwd)

createdb -h 192.168.1.36 -U dengueadmin denguedb2

psql -h 192.168.1.36 -d denguedb2  -U dengueadmin < crawlclima/utilities/dbschema/roles-infodengue_roles.sql

gunzip -c crawlclima/utilities/dbschema/schemas_dengue_2021_10_04.sql.gz | psql -h 192.168.1.36 -U dengueadmin denguedb2
