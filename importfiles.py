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
import mmap

logging.basicConfig()
logger = logging.getLogger('poretools')

flowcell_cache = set()

def flowcell_get_or_create(db, flowcell_id, asic_id):
	check_cache = "%s-%s" % (flowcell_id, asic_id)
	if check_cache in flowcell_cache:
		return

	sql = "SELECT flowcell_id, asic_id FROM flowcell WHERE flowcell_id = ? AND asic_id = ?"
	db.c.execute(sql, (flowcell_id, asic_id))
	r = db.c.fetchone()
	if r:
		return

	print "ADD: flowcell_id %s asic_id %s" % (flowcell_id, asic_id)
	sql = "INSERT INTO flowcell ( flowcell_id, asic_id ) VALUES ( ?, ? )"
	try:
	        db.c.execute(sql, (flowcell_id, asic_id))
		db.conn.commit()
	except sqlite3.IntegrityError:
		print "unique constraint, skipping"

	flowcell_cache.add(check_cache)

experiment_cache = set()

def experiment_get_or_create(db, flowcell_id, asic_id, experiment_id, library_name, script_name, exp_start_time, host_name, minion_id):
	if experiment_id in experiment_cache:
		return

	sql = "SELECT experiment_id FROM experiment WHERE experiment_id = ?"
	db.c.execute(sql, (experiment_id,))
	r = db.c.fetchone()
	if r:
		return

	sql = "INSERT INTO experiment ( flowcell_id, asic_id, experiment_id, library_name, script_name, exp_start_time, host_name, minion_id ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ? )"
	db.c.execute(sql, (flowcell_id, asic_id, experiment_id, library_name, script_name, exp_start_time, host_name, minion_id))

	try:
	        db.c.execute(sql, (flowcell_id, asic_id, experiment_id, library_name, script_name, exp_start_time, host_name, minion_id))
		db.conn.commit()
	except sqlite3.IntegrityError:
		print "unique constraint, skipping"

	experiment_cache.add(experiment_id)

def md5(fname):
	hash_md5 = hashlib.md5()
	with open(fname, "rb") as f:
		mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)

		for chunk in iter(lambda: mm.read(1024*1024*1), b""):
			hash_md5.update(chunk)

		mm.close()

		return hash_md5.hexdigest()

def trackedfiles_find(db, fn):
	sql = "SELECT filepath FROM trackedfiles WHERE filepath = ?"
	db.c.execute(sql, (fn,))
	return db.c.fetchone()

def trackedfiles_add(db, experiment_id, uuid, md5sig, filepath, sequenced_date):
	sql = "INSERT INTO trackedfiles ( experiment_id, uuid, md5, filepath, sequenced_date ) VALUES ( ?, ?, ?, ?, ? )"
	db.addcommand(sql, (experiment_id, uuid, md5sig, filepath, sequenced_date))

#	db.c.execute(sql, (experiment_id, uuid, md5sig, filepath, sequenced_date))
#	return db.c.lastrowid

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

def basecall_add(db, filepath, basecaller_id, group, template, template_length):
	sql = "INSERT INTO basecall ( filepath, basecaller_id, group_id, template, template_length ) VALUES ( ?, ?, ?, ?, ? )"
	db.addcommand(sql, (filepath, basecaller_id, group, template, template_length))
	#db.c.execute(sql, (read_id, basecaller_id, group, template, template_length))

class Db:
	def __init__(self, dbname, isolation_level):
		self.conn = sqlite3.connect(dbname, check_same_thread=False, timeout=60, isolation_level=isolation_level)
		self.c = self.conn.cursor()

		self.c.execute ("PRAGMA journal_mode = WAL")
		self.c.execute ("PRAGMA synchronous = NORMAL")

		self.commands = []

	def __del__(self):
		self.conn.close()

	def addcommand(self, c, args):
		self.commands.append((c, args))
	
	def runcommands(self):
		for cmd in self.commands:
			self.c.execute(cmd[0], cmd[1])

def process(db, db2, lofn, args):
	matcher = re.compile('Basecall_1D_(\d+)')
	n_added = 0
	n_skipped = 0

	for fn in lofn:
		#print >>sys.stderr, "Processing %s" % (fn,)

		# how to handle files
		# first - is fn in database?
		#   no -- add it as a tracked file - this is heuristic
		#  yes -- is it the same file ?
		#            check md5
		#            if md5 different & path same -- update contents
		#            if md5 same & path different -- update path
		#            if md5 same & path same -- skip
		#            or skip it

		realfn = fn
		if args.alternate_path:
			realfn = "%s/%s" % (args.alternate_path, fn.split("/")[-1])

		print "Realfn %s Actual fn %s" % (realfn, fn)

		tracked = trackedfiles_find(db, fn)

		if not tracked:
			print "Processing %s %s" % (n_added+n_skipped, fn)
			try:
				md5sig = md5(realfn)
			except:
				print >>sys.stderr, "Exception with md5!"
				continue

			fast5 = Fast5File(realfn)
			if not fast5.is_open:
				print >>sys.stderr, "Cannot open %s" % (fn,)
				continue

			#print >>sys.stderr, fn
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

			experiment_get_or_create(db, flowcell_id, asic_id, experiment_id, library_name, script_name, exp_start_time, host_name, minion_id)

			# add trackedfile
			sequenced_date = int(block.attrs['start_time'])
			sample_frequency = int(fast5.get_sample_frequency())
			start_time = exp_start_time + (sequenced_date / sample_frequency)
			trackedfiles_add(db2, experiment_id, uuid, md5sig, fn, start_time)

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

						if template:
							a,b,c,d,e = template.split("\n")
							template_length = len(b.strip())
						else:
							template_length = None
						basecall_add(db2, fn, basecaller_id, group, template, template_length)

			n_added += 1
		else:
			print "%d: Already seen file %s, skipping" % (n_added+n_skipped, fn,)
			n_skipped += 1

		if (n_added + n_skipped) % 1000 == 0:
			print >>sys.stderr, "Added %s, skipped %s" % (n_added, n_skipped)

	db2.runcommands()
	db2.conn.commit()

def run(parser, args):
	db1 = Db(args.db, None)
	db2 = Db(args.db, 'DEFERRED')
	process(db1, db2, (fn.rstrip() for fn in open(args.fofn)), args)

def import_reads_parallel(fofn):
	files = [fn.rstrip() for fn in open(fofn)]
	f = lambda A, n=1000: [A[i:i+n] for i in range(0, len(A), n)]

	print >>sys.stderr, "%s files in list" % ( len(files), )

	values = [delayed(process)(x) for x in f(files)]
	results = compute(*values, get=dask.threaded.get)

