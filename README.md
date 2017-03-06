# poredb

Poredb is a way to manage very large Oxford Nanopore datasets on the basic principle:

	-- track the files, rather than moving them around

## Running straight from github

	git clone https://github.com/nickloman/poredb.git
	rmdir poredb/poretools
	cd poredb
	git clone https://github.com/arq5x/poretools
	touch poretools/__init__.py

## Usage

### Create a DB

	python poredb_main.py create myexperiment.db
	find . -name "*.fast5" > filelist.txt
	python poredb_main.py poredb import myexperiment.db filelist.txt

## Extract all basecalls in FASTQ format

	poredb fastq myexperiment.db > myexperiment.fastq


# Thanks

Andreas Klosterman for dask suggestion.

