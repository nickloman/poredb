# Thanks for Andreas Klosterman for dask suggestion
from poretools.poretools.Fast5File import Fast5File
import sys
import md5
import hashlib
import re
import argparse
from collections import defaultdict

import sqlite3
import logging

logging.basicConfig()
logger = logging.getLogger('poretools')

def run(parser, args):
	conn = sqlite3.connect(args.db, check_same_thread=False, timeout=30)
	c = conn.cursor()

	uuiddb = set()

	if args.group_by_asic:
		statscache = defaultdict(lambda : defaultdict(int))

		c.execute("pragma temp_store = 2")
		c.execute("PRAGMA cache_size = 10000000")

		statement = """select asic_id, trackedfiles.uuid, template
                               from basecall
                               join basecaller using (basecaller_id) 
                               join trackedfiles using (filepath) 
                               join experiment using (experiment_id)
                               join flowcell using (asic_id)
                               where basecaller.name = 'ONT Sequencing Workflow'"""
		n = 0
		for r in c.execute(statement):
			n += 1
			if n % 1000 == 0: print n
			template = r[2]
			uuid = r[1]

			if uuid in uuiddb:
				continue
			else:
				uuiddb.add(uuid)

			if template:
				a,b,c,d,e = template.split("\n")
				l = len(b.strip())
				statscache[r[0]]['reads'] += 1
				statscache[r[0]]['bases'] += l

		"""
		trackedfiles = {}
		n = 0
		statement = "select asic_id, filepath FROM trackedfiles JOIN experiment USING (experiment_id)"
		for r in c.execute(statement):
			if n % 1000 == 0: print n

			n += 1
			trackedfiles[r[1]] = r[0]

		statement = "select asic_id, template FROM basecall join trackedfiles using (filepath) join experiment using (experiment_id)"
		statement = "select template, filepath from basecall"
		n = 0
		for r in c.execute(statement):
			if r[0]:
				a,b,c,d,e = r[0].split("\n")
				l = len(b.strip())

				statscache[trackedfiles[r[1]]]['reads'] += 1
				statscache[trackedfiles[r[1]]]['bases'] += l
			n += 1
			if n % 1000 == 0: print n
		"""


		for asic, stats in statscache.iteritems():
			print "%s\t%s\t%s" % (asic, stats['reads'], stats['bases'])
