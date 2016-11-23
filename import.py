from poretools.poretools.Fast5File import Fast5File
import sys
import md5
import hashlib
import re

"""
create table flowcell (
  flowcell_id varchar(50) primary key not null,
  asic_id varchar(50) not null
);

create unique index flowcell_idx on flowcell ( flowcell_id, asic_id );

create table experiment (
  flowcell_id varchar(50) references flowcell ( flowcell_id ) not null,
  experiment_id varchar(100) not null,
  library_name varchar(40) not null,
  script_name varchar(100) not null,
  host_name varchar(100) not null,
  exp_start_time integer not null,
  minion_id varchar(40) not null
);

-- all files in file system
create table trackedfiles (
  file_id integer primary key not null,
  experiment_id varchar(100) references experiment ( experiment_id ) not null,
  uuid varchar(64) not null,
  md5 varchar(64) not null,
  filepath text not null,
  sequenced_date integer not null
);

create unique index trackedfiles_filepath on trackedfiles ( filepath );
create index trackedfiles_uuid on trackedfiles ( uuid );

-- basecaller
create table basecaller (
  basecaller_id integer primary key not null,
  name varchar(100) not null,
  version varchar(100) not null
);

-- basecealls
-- TODO: complement
-- TODO: 2d
-- TODO: barcoding

create table basecall (
  file_id integer references trackedfiles ( file_id ) not null,
  basecaller_id integer references basecaller ( basecaller_id ) not null,
  group_id integer not null,
  template text null
);
"""

import sqlite3
conn = sqlite3.connect(sys.argv[1])
c = conn.cursor()

def flowcell_get_or_create(flowcell_id, asic_id):
	sql = "SELECT flowcell_id, asic_id FROM flowcell WHERE flowcell_id = ? AND asic_id = ?"
	c.execute(sql, (flowcell_id, asic_id))
	r = c.fetchone()
	if r:
		return

	sql = "INSERT INTO flowcell ( flowcell_id, asic_id ) VALUES ( ?, ? )"
	c.execute(sql, (flowcell_id, asic_id))

def experiment_get_or_create(flowcell_id, experiment_id, library_name, script_name, exp_start_time, host_name, minion_id):
	sql = "SELECT experiment_id FROM experiment WHERE experiment_id = ?"
	c.execute(sql, (experiment_id,))
	r = c.fetchone()
	if r:
		return

	sql = "INSERT INTO experiment ( flowcell_id, experiment_id, library_name, script_name, exp_start_time, host_name, minion_id ) VALUES ( ?, ?, ?, ?, ?, ?, ? )"
	c.execute(sql, (flowcell_id, experiment_id, library_name, script_name, exp_start_time, host_name, minion_id))

def md5(fname):
	hash_md5 = hashlib.md5()
	with open(fname, "rb") as f:
		for chunk in iter(lambda: f.read(1024*200), b""):
			hash_md5.update(chunk)
		return hash_md5.hexdigest()

def trackedfiles_find(fn):
	sql = "SELECT file_id FROM trackedfiles WHERE filepath = ?"
	c.execute(sql, (fn,))
	return c.fetchone()

def trackedfiles_add(experiment_id, uuid, md5sig, filepath, sequenced_date):
	sql = "INSERT INTO trackedfiles ( experiment_id, uuid, md5, filepath, sequenced_date ) VALUES ( ?, ?, ?, ?, ? )"
	c.execute(sql, (experiment_id, uuid, md5sig, filepath, sequenced_date))
	return c.lastrowid

def get_basecaller_version(g):
	try:
		return g.attrs['chimaera version']
	except:
		pass

	try:
		return g.attrs['version']
	except:
		return None	

def basecaller_get_or_delete(name, version):
	sql = "SELECT basecaller_id FROM basecaller WHERE name = ? AND version = ?"
	c.execute(sql, (name, version))
	row = c.fetchone()
	if row:
		return int(row[0])

	sql = "INSERT INTO basecaller ( name, version ) VALUES ( ?, ? )"
	c.execute(sql, (name, version))
	return c.lastrowid

def basecall_add(read_id, basecaller_id, group, template):
	sql = "INSERT INTO basecall ( file_id, basecaller_id, group_id, template ) VALUES ( ?, ?, ?, ? )"
	c.execute(sql, (read_id, basecaller_id, group, template))

def import_reads(fofn):
	matcher = re.compile('Basecall_1D_(\d+)')

	for fn in open(fofn):
		fn = fn.rstrip()
		print >>sys.stderr, "Processing %s" % (fn,)
		fast5 = Fast5File(fn)
		if not fast5.is_open:
			print >>sys.stderr, "Cannot open %s" % (fn,)
			continue


		# how to handle files
		# first - is fn in database?
		#   no -- add it as a tracked file - this is heuristic
		#  yes -- is it the same file ?
		#            check md5
		#            if md5 different & path same -- update contents
                #            if md5 same & path different -- update path
		#            if md5 same & path same -- skip
                #            or skip it

		block = fast5.find_read_number_block_fixed_raw()
		uuid = block.attrs['read_id']

		if not trackedfiles_find(fn):
			# get flowcell
			flowcell_id = fast5.get_flowcell_id()
			asic_id = fast5.get_asic_id()
			flowcell_get_or_create(flowcell_id, asic_id)

			# get experiment
			experiment_id = fast5.get_run_id()
			library_name = fast5.get_sample_name()
			script_name = fast5.get_script_name()
			exp_start_time = fast5.get_exp_start_time()
			host_name = fast5.get_host_name()
			minion_id = fast5.get_device_id()

			experiment_get_or_create(flowcell_id, experiment_id, library_name, script_name, exp_start_time, host_name, minion_id)

			# add trackedfile
			sequenced_date = int(block.attrs['start_time'])
			sample_frequency = int(fast5.get_sample_frequency())
			md5sig = md5(fn)
			start_time = exp_start_time + (sequenced_date / sample_frequency)
			read_id = trackedfiles_add(experiment_id, uuid, md5sig, fn, start_time)

			# basecalls
			analyses = fast5.hdf5file.get('Analyses')
			for k, g in analyses.iteritems():
				m = matcher.match(k)
				if m:
					basecaller_name = g.attrs['name']
					group = m.group(1)
					version = get_basecaller_version(g)

					basecaller_id = basecaller_get_or_delete(basecaller_name, version)

					try:
						template = analyses.get("%s/BaseCalled_template" % (k,))['Fastq'][()]
					except:
						template = None

					basecall_add(read_id, basecaller_id, group, template)
				else:
					print >>sys.stderr, "Skipping %s" % (k,)

		fast5.close()


import_reads(sys.argv[2])
conn.commit()

