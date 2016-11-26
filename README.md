# poredb

Poredb is a way to manage very large Oxford Nanopore datasets on the basic principle:

	-- track the files, rather than moving them around

## Usage

poredb create myexperiment.db
find . -name "*.fast5" > filelist.txt
poredb import myexperiment.db filelist.txt


