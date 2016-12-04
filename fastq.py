# Thanks for Andreas Klosterman for dask suggestion
from poretools.poretools.Fast5File import Fast5File
from dask import compute, delayed
import dask.multiprocessing
import dask.threaded
import sys
import md5
import hashlib
import re
import argparse

import sqlite3
import logging

logging.basicConfig()
logger = logging.getLogger('poretools')

def run(parser, args):
	conn = sqlite3.connect(args.db, check_same_thread=False, timeout=30)
	c = conn.cursor()

	uuiddb = set()

	if args.group_by_asic:
		filecache = {}

		last_asic = None
		fh = None
		c.execute("pragma temp_store = 2")

		statement = """select asic_id, flowcell.flowcell_id, template, template_length, trackedfiles.uuid
                               from basecall
                               join basecaller using (basecaller_id) 
                               join trackedfiles using (filepath) 
                               join experiment using (experiment_id)
                               join flowcell using (asic_id)
                               where basecaller.name = 'ONT Sequencing Workflow'"""
		for r in c.execute(statement):
			if not r[2]: continue

			if r[4] in uuiddb:
				print >>sys.stderr, "duplicate read! %s" % (r[4])
				continue
			else:
				uuiddb.add(r[4])

			asic_id = r[0]
			if asic_id not in filecache:
				filecache[asic_id] = open("rel2-nanopore-wgs-%s-%s.fastq" % (asic_id, r[1]), "w")
	
			filecache[asic_id].write(r[2])

		"""
		statement = SELECT asic_id, template, template_length, trackedfiles.uuid FROM flowcell
		               JOIN experiment USING (asic_id)
                               JOIN trackedfiles USING (experiment_id)
			       JOIN basecall USING (filepath)
                               JOIN basecaller USING (basecaller_id)
                               WHERE basecaller.name = 'ONT Sequencing Workflow'
                               ORDER BY asic_id
		for r in c.execute(statement):

			if r[3] in uuiddb:
				print >>sys.stderr, "duplicate read! %s" % (r[3,])
			uuiddb.add(r[3])

			if not last_asic or last_asic != r[0]:
				if fh:
					fh.close()
				fh = open("asic-%s.fastq" % (r[0]), "w")
				last_asic = r[0]
			if r[1]:
				fh.write(r[1])
		if last_asic:
			fh.close()
		"""
	else:
		resultset = c.execute(statement)
		for r in resultset:
			if args.report_lengths:
				print r[1]
			else:
				print r[0],

