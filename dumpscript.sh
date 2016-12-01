#!/bin/bash

db=$1

read -d '' sql <<- EOF
.headers on

.mode insert flowcell
.output ${db}_flowcell.txt
select * from flowcell;

.output ${db}_experiment.txt
.mode insert experiment
select * from experiment;

.mode insert trackedfiles
.output ${db}_trackedfiles.txt
select experiment_id, uuid, md5, filepath, sequenced_date from trackedfiles;

.mode insert basecall
.output ${db}_basecall.txt
select filepath, basecaller_id, group_id, template, template_length from basecall;
EOF

echo "$sql" | sqlite3 $db
