from poretools.poretools.Fast5File import Fast5File
import sys

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

-- basecaller
create table basecaller (
  basecaller_id integer primary key not null,
  name varchar(100) not null,
  version varchar(100) not null
);

-- basecealls
create table basecall (
  file_id integer references trackedfiles ( file_id ) not null,
  basecaller integer references basecaller ( basecalled_id ) not null,
  barcode varchar(10) null,
  group_id integer not null,
  template text not null,
  complement text null,
  twod text null
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


def import_reads(fofn):
	for fn in open(fofn):
		fast5 = Fast5File(fn.rstrip())
		flowcell_id = fast5.get_flowcell_id()
		asic_id = fast5.get_asic_id()
		flowcell_get_or_create(flowcell_id, asic_id)
		print fn, fast5.get_flowcell_id()

		experiment_id = fast5.get_experiment_id()
		library_name = fast5.get_sample_name()
		script_name = fast5.get_script_name()
		exp_start_time = fast5.get_exp_start_time()
		minion_id = fast5.get_device_id()
		#experiment_get_or_create(flowcell_id, 

import_reads(sys.argv[2])
conn.commit()

