from importfiles import trackedfiles_find, Db

def process(db, lofn):
	for fn in lofn:
		tracked = trackedfiles_find(db, fn)
		if tracked is None:
			print fn

def run(parser, args):
	db = Db(args.db, None)
	process(db, (fn.rstrip() for fn in open(args.fofn)))

