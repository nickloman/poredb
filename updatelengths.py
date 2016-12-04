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

	statement = """SELECT ROWID, template FROM basecall WHERE template IS NOT NULL"""

	c1 = conn.cursor()

	c1.execute('pragma journal_mode=DELETE;')
	c1.execute('pragma synchronous=1;')

	resultset = c.execute(statement)
	for r in resultset:
		a,b,c,d,e = r[1].split("\n")			
		l = len(b.strip())
		c1.execute("UPDATE basecall SET template_length = ? WHERE ROWID = ?", (l, r[0]))
	
	conn.commit()


