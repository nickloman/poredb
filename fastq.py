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

	statement = """SELECT template FROM basecall WHERE template IS NOT NULL"""

	resultset = c.execute(statement)
	for r in resultset:
		print r[0],

