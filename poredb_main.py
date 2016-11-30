# Thanks for Andreas Klosterman for dask suggestion
# Thanks to Aaron Quinlan for the argparse implementation from poretools.

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
import version

def run_subtool(parser, args):
	if args.command == 'create':
		import create as submodule
	elif args.command == 'import':
		import importfiles as submodule
	elif args.command == 'fastq':
		import fastq as submodule
	elif args.command == 'updatelengths':
		import updatelengths as submodule
	elif args.command == 'worklist':
		import worklist as submodule

	# run the chosen submodule.
	submodule.run(parser, args)

class ArgumentParserWithDefaults(argparse.ArgumentParser):
	def __init__(self, *args, **kwargs):
		super(ArgumentParserWithDefaults, self).__init__(*args, **kwargs)
		self.add_argument("-q", "--quiet", help="Do not output warnings to stderr",
						  action="store_true",
						  dest="quiet")

def main():
	parser = argparse.ArgumentParser(prog='poredb', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument("-v", "--version", help="Installed poretools version",
                        action="version",
                        version="%(prog)s " + str(version.__version__))
	subparsers = parser.add_subparsers(title='[sub-commands]', dest='command', parser_class=ArgumentParserWithDefaults)

	# newdb
	parser_create = subparsers.add_parser('create',
                                          help='Create an empty poredb database')
	parser_create.add_argument('db', metavar='DB',
                             help='The name of the database.')
	parser_create.set_defaults(func=run_subtool)

	# import
	parser_import = subparsers.add_parser('import',
	                                      help='Import files into a poredb database')
	parser_import.add_argument('db', metavar='DB',
	                           help='The poredb database.')
	parser_import.add_argument('fofn', metavar='FOFN',
	                           help='A file containing a list of file names.')
	parser_import.add_argument('--alternate-path', metavar='alternate_path')
	parser_import.set_defaults(func=run_subtool)

	# fastq
	parser_fastq = subparsers.add_parser('fastq',
                                          help='Export basecalls from database in FASTQ format')
	parser_fastq.add_argument('db', metavar='DB',
                             help='The name of the database.')
	parser_fastq.add_argument('-l', '--lengths', dest='report_lengths', default=False, action='store_true', help="Report lengths instead of bases")
	parser_fastq.set_defaults(func=run_subtool)

	# worklist
	parser_worklist = subparsers.add_parser('worklist')
	parser_worklist.add_argument('db', metavar='DB',
                             help='The name of the database.')
	parser_worklist.add_argument('fofn', metavar='FOFN',
                                   help='A file containing a list of file names.')
        parser_worklist.set_defaults(func=run_subtool)

	# updatelengths
	parser_updatelengths = subparsers.add_parser('updatelengths', help='')
	parser_updatelengths.add_argument('db', metavar='DB', help='The name of the database.')
	parser_updatelengths.set_defaults(func=run_subtool)

	args = parser.parse_args()

	if args.quiet:
		logger.setLevel(logging.ERROR)

	args.func(parser, args)

if __name__ == "__main__":
	    main()
