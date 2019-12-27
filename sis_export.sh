#!/bin/bash

MONGO_SIS_HOSTS=$(echo $SIS_URL | sed "s/mongodb:\/*//" | sed "s/\/clever//" | sed "s/\?replicaSet.*//")
mongoexport --host ${MONGO_SIS_HOSTS} --username ${SIS_USERNAME} --password ${SIS_PASSWORD} --ssl --authenticationDatabase admin --db clever --collection students --fields=_data_timestamp,id,grade,school,district_id,created,has_weighted_gpa,has_unweighted_gpa,has_home_language,graduation_year,schools_array --type json | aws s3 cp - s3://clever-analytics-dev/mongo/sis_test/students.json
# mongoexport --host ${MONGO_SIS_HOSTS} --username ${SIS_USERNAME} --password ${SIS_PASSWORD} --ssl --authenticationDatabase admin --db clever --collection schools --fields=_data_timestamp,id,name,district_id,created,principal_name,principal_email,address,city,state,zip_code,mdr_number,phone,school_number,sis_id,nces_id,low_grade,high_grade --type json | aws s3 cp - s3://clever-analytics-dev/mongo/sis_test/schools.json
