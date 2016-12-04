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

create_schema = ["""
create table flowcell (
  flowcell_id varchar(50) not null,
  asic_id varchar(50) primary key not null
);""",

"""create unique index flowcell_idx on flowcell ( flowcell_id, asic_id );""",

"""create table experiment (
  flowcell_id varchar(50) references flowcell ( flowcell_id ) not null,
  experiment_id varchar(100) not null,
  asic_id varchar(50) references flowcell ( asic_id ) not null,
  library_name varchar(40) not null,
  script_name varchar(100) not null,
  host_name varchar(100) not null,
  exp_start_time integer not null,
  minion_id varchar(40) not null
);""",

"""create unique index experiment_id on experiment ( experiment_id );""",

"""-- all files in file system
create table trackedfiles (
  file_id integer null,
  experiment_id varchar(100) references experiment ( experiment_id ) not null,
  uuid varchar(64) not null,
  md5 varchar(64) not null,
  filepath text primary key not null,
  sequenced_date integer not null
);""",

"""-- create unique index trackedfiles_filepath on trackedfiles ( filepath );""",

"""-- create index trackedfiles_uuid on trackedfiles ( uuid );""",

"""-- basecaller
create table basecaller (
  basecaller_id integer primary key not null,
  name varchar(100) not null,
  version varchar(100) not null
);""",

"""-- basecealls
-- TODO: complement
-- TODO: 2d
-- TODO: barcoding

create table basecall (
  file_id integer null,
  filepath text not null references trackedfiles ( filepath ) not null,
  basecaller_id integer references basecaller ( basecaller_id ) not null,
  group_id integer not null,
  template text null,
  template_length integer null
);""",

"""-- create index basecall_filepath on basecall ( filepath );"""]

import sqlite3
import logging

logging.basicConfig()
logger = logging.getLogger('poretools')

def run(parser, args):
	conn = sqlite3.connect(args.db, check_same_thread=False, timeout=30)
	c = conn.cursor()

	for statement in create_schema:
		c.execute(statement)

	conn.commit()

