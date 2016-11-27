# poredb

Poredb is a way to manage very large Oxford Nanopore datasets on the basic principle:

	-- track the files, rather than moving them around

## Usage

### Create a DB

	poredb create myexperiment.db
	find . -name "*.fast5" > filelist.txt
	poredb import myexperiment.db filelist.txt

## Extract all basecalls in FASTQ format

	poredb fastq myexperiment.db > myexperiment.fastq



