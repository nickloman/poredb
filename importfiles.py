# Thanks for Andreas Klosterman for dask suggestion
from poretools.poretools.Fast5File import Fast5File
from dask import compute, delayed
import dask.multiprocessing
import dask.threaded
import sys
import md5
import hashlib
import re
import sqlite3
import logging

logging.basicConfig()
logger = logging.getLogger('poretools')

def flowcell_get_or_create(db, flowcell_id, asic_id):
	sql = "SELECT flowcell_id, asic_id FROM flowcell WHERE flowcell_id = ? AND asic_id = ?"
	db.c.execute(sql, (flowcell_id, asic_id))
	r = db.c.fetchone()
	if r:
		return

	sql = "INSERT INTO flowcell ( flowcell_id, asic_id ) VALUES ( ?, ? )"
	db.c.execute(sql, (flowcell_id, asic_id))
	db.conn.commit()

def experiment_get_or_create(db, flowcell_id, experiment_id, library_name, script_name, exp_start_time, host_name, minion_id):
	sql = "SELECT experiment_id FROM experiment WHERE experiment_id = ?"
	db.c.execute(sql, (experiment_id,))
	r = db.c.fetchone()
	if r:
		return

	sql = "INSERT INTO experiment ( flowcell_id, experiment_id, library_name, script_name, exp_start_time, host_name, minion_id ) VALUES ( ?, ?, ?, ?, ?, ?, ? )"
	db.c.execute(sql, (flowcell_id, experiment_id, library_name, script_name, exp_start_time, host_name, minion_id))
	db.conn.commit()

def md5(fname):
	hash_md5 = hashlib.md5()
	with open(fname, "rb") as f:
		for chunk in iter(lambda: f.read(1024*200), b""):
			hash_md5.update(chunk)
		return hash_md5.hexdigest()

def trackedfiles_find(db, fn):
	sql = "SELECT file_id FROM trackedfiles WHERE filepath = ?"
	db.c.execute(sql, (fn,))
	return db.c.fetchone()

def trackedfiles_add(db, experiment_id, uuid, md5sig, filepath, sequenced_date):
	sql = "INSERT INTO trackedfiles ( experiment_id, uuid, md5, filepath, sequenced_date ) VALUES ( ?, ?, ?, ?, ? )"
	db.c.execute(sql, (experiment_id, uuid, md5sig, filepath, sequenced_date))
	return db.c.lastrowid

def get_basecaller_version(g):
	try:
		return g.attrs['chimaera version']
	except:
		pass

	try:
		return g.attrs['version']
	except:
		return None	

def basecaller_get_or_delete(db, name, version):
	sql = "SELECT basecaller_id FROM basecaller WHERE name = ? AND version = ?"
	db.c.execute(sql, (name, version))
	row = db.c.fetchone()
	if row:
		return int(row[0])

	sql = "INSERT INTO basecaller ( name, version ) VALUES ( ?, ? )"
	db.c.execute(sql, (name, version))
	db.conn.commit()
	return db.c.lastrowid

def basecall_add(db, read_id, basecaller_id, group, template):
	sql = "INSERT INTO basecall ( file_id, basecaller_id, group_id, template ) VALUES ( ?, ?, ?, ? )"
	db.c.execute(sql, (read_id, basecaller_id, group, template))

class Db:
	def __init__(self, dbname):
		self.conn = sqlite3.connect(dbname, check_same_thread=False, timeout=30)
		self.c = self.conn.cursor()

	def __del__(self):
		self.conn.close()

def process(db, lofn):
	matcher = re.compile('Basecall_1D_(\d+)')
	n_added = 0

	for fn in lofn:
		print >>sys.stderr, "Processing %s" % (fn,)

		# how to handle files
		# first - is fn in database?
		#   no -- add it as a tracked file - this is heuristic
		#  yes -- is it the same file ?
		#            check md5
		#            if md5 different & path same -- update contents
		#            if md5 same & path different -- update path
		#            if md5 same & path same -- skip
		#            or skip it

		tracked = trackedfiles_find(db, fn)
		print tracked

		if not tracked:
			fast5 = Fast5File(fn)
			if not fast5.is_open:
				print >>sys.stderr, "Cannot open %s" % (fn,)
				continue

			print >>sys.stderr, fn
			block = fast5.find_read_number_block_fixed_raw()
			uuid = block.attrs['read_id']

			# get flowcell
			flowcell_id = fast5.get_flowcell_id()
			asic_id = fast5.get_asic_id()
			flowcell_get_or_create(db, flowcell_id, asic_id)

			# get experiment
			experiment_id = fast5.get_run_id()
			library_name = fast5.get_sample_name()
			script_name = fast5.get_script_name()
			exp_start_time = fast5.get_exp_start_time()
			host_name = fast5.get_host_name()
			minion_id = fast5.get_device_id()

			experiment_get_or_create(db, flowcell_id, experiment_id, library_name, script_name, exp_start_time, host_name, minion_id)

			# add trackedfile
			sequenced_date = int(block.attrs['start_time'])
			sample_frequency = int(fast5.get_sample_frequency())
			md5sig = md5(fn)
			start_time = exp_start_time + (sequenced_date / sample_frequency)
			read_id = trackedfiles_add(db, experiment_id, uuid, md5sig, fn, start_time)

			# basecalls
			analyses = fast5.hdf5file.get('Analyses')
			if analyses:
				for k, g in analyses.iteritems():
					m = matcher.match(k)
					if m:
						basecaller_name = g.attrs['name']
						group = m.group(1)
						version = get_basecaller_version(g)

						basecaller_id = basecaller_get_or_delete(db, basecaller_name, version)

						try:
							template = analyses.get("%s/BaseCalled_template" % (k,))['Fastq'][()]
						except:
							template = None

						basecall_add(db, read_id, basecaller_id, group, template)
			db.conn.commit()

			n_added += 1
			if n_added % 1000 == 0:
				print >>sys.stderr, "Committing"
		else:
			print >>sys.stderr, "Already seen file %s, skipping" % (fn,)
		db.conn.commit()

def run(parser, args):
	db = Db(args.db)
	files = [fn.rstrip() for fn in open(args.fofn)]
	process(db, files)

def import_reads_parallel(fofn):
	files = [fn.rstrip() for fn in open(fofn)]
	f = lambda A, n=1000: [A[i:i+n] for i in range(0, len(A), n)]

	print >>sys.stderr, "%s files in list" % ( len(files), )

	values = [delayed(process)(x) for x in f(files)]
	results = compute(*values, get=dask.threaded.get)

