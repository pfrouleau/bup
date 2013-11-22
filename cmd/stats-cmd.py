#!/usr/bin/env python
import sys, os, sqlite3
from bup import git, options
from bup.helpers import *

git.check_repo_or_die()
db = None

SKIP_KNOWN = True

def create_indexes(db):
    log("Creating indexes...")
    # db.execute("create index if not exists idx_obj_sha on objects(sha)")
    # db.execute("create index if not exists idx_obj_type on objects(type)")
    # db.execute("create index if not exists idx_refs on refs(a, name)")
    # log(" DONE\n")


def open_database(reset, must_exist):
    db_path = git.repo('bupstats-partial.sqlite3')

    if reset:
        if os.path.exists(db_path):
            os.unlink(db_path)
    elif must_exist:
        if not os.path.exists(db_path):
            o.fatal("no database available (%s)" % db_path);

    db = sqlite3.connect(db_path)
    db.execute('CREATE TABLE IF NOT EXISTS objects (id INTEGER PRIMARY KEY AUTOINCREMENT, sha TEXT UNIQUE, type TEXT, size INTEGER);')
    db.execute('CREATE TABLE IF NOT EXISTS refs (r_id INTEGER, o_id INTEGER, mode INTEGER, name TEXT, PRIMARY KEY (r_id, o_id, name));')

    return db

# Out: object's id
def insert_object(sha, type, size):
    db.execute('INSERT OR IGNORE INTO objects VALUES (null,?,?,?)', (sha, type, size))

    cur = db.cursor()
    cur.execute('SELECT id FROM objects WHERE sha=:sha', {"sha": sha})
    return cur.fetchone()[0]


def insert_ref(r_id, o_id, mode, name):
    if r_id:
        try:
            db.execute('INSERT OR IGNORE INTO refs VALUES (?,?,?,?)',
                (r_id, o_id, mode, name))
        except:
            # how should we handle unicode file name?
            db.execute('INSERT OR IGNORE INTO refs VALUES (?,?,?,?)',
                (r_id, o_id, mode, 'unicode_name'))


# yield: type, sha, length
def traverse_commit(cp, needed_objects, sha_hex):

    it = iter(cp.get(sha_hex))
    type = it.next()
    assert(type == 'commit')
    content = "".join(it)
    length = len(content)

    o_id = insert_object(sha_hex, type, length)

    tree_sha = content.split("\n")[0][5:].rstrip(" ")

    yield (type, sha_hex, length)
    for obj in traverse_objects(cp, needed_objects, False,
                        o_id, 0, 'commit', tree_sha):
        yield obj


# yield: type, sha, length
def traverse_hash(cp, needed_objects, sha_hex):
    for obj in traverse_objects(cp, needed_objects, True, 0, 0, '-', sha_hex):
        yield obj


# yield: type, sha, length
def traverse_objects(cp, needed_objects, check_dup, r_id, r_mode, r_name, sha_hex):
    it = iter(cp.get(sha_hex))
    type = it.next()

    content = "".join(it)
    length = len(content)
    o_id = insert_object(sha_hex, type, length)
    insert_ref(r_id, o_id, r_mode, r_name)

    yield (type, sha_hex, length)

    if type == 'blob':
        return

    elif type == 'tree':
        for (mode, mangled_name, sha) in git.tree_decode(content):
            for obj in traverse_objects(cp, needed_objects, check_dup,
                                o_id, mode, mangled_name, sha.encode('hex')):
                yield obj

    elif type == 'commit':
        tree_sha = content.split("\n")[0][5:].rstrip(" ")

        for obj in traverse_objects(cp, needed_objects, check_dup,
                        o_id, r_mode, tree_sha):
            yield obj


def fill_database(show_progress):
    global db

    cp = git.CatPipe()

    pl = git.PackIdxList(git.repo('objects/pack'))
    needed_objects = git.NeededObjects(pl)

    refs = git.list_refs()
    refnames = [name for name, sha in refs]

    db = open_database(True, False)

    # Find needed objects reachable from commits
    traversed_objects_counter = 0

    for refname in refnames:
        if not refname.startswith('refs/heads/'):
            continue
        log('Traversing %s to find needed objects...\n' % refname[11:])
        for date, sha_hex in ((date, sha.encode('hex')) for date, sha in
                              git.rev_list(refname)):
            log('Traversing commit %s to find needed objects...\n' % sha_hex)
            for type, sha_, size in traverse_commit(cp, needed_objects, sha_hex):
                if show_progress and not type == 'blob':
                    log("%8s  %s  %5d\n" % (type, sha_, size))
                traversed_objects_counter += 1
                qprogress('Traversing objects: %d\r' % traversed_objects_counter)

    # Find needed objects reachable from tags
    tags = git.tags()
    if len(tags) > 0:
        for key in tags:
            log('Traversing tag %s to find needed objects...\n' % ", ".join(tags[key]))
            for type, sha, size in traverse_commit(cp, needed_objects, sha):
                if not type == 'blob':
                    log("%8s  %s  %5d\n" % (type, sha_, size))
                traversed_objects_counter += 1
                qprogress('Traversing objects: %d\r' % traversed_objects_counter)

    progress('Traversing objects: %d, done.\n' % traversed_objects_counter)
    if traversed_objects_counter == 0:
        o.fatal('No reachable objects found.')

    create_indexes(db)
    db.commit()


def _show_blobs(db, hash, ofs, depth):
    c = db.cursor()
    c.execute('SELECT id FROM objects WHERE sha=:h', {"h": hash})
    row = c.fetchone()

    if row == None:
        o.fatal('Unknown hash (%s)' % hash)

    cur = db.cursor()
    cur.execute('SELECT o.sha, o.type, o.size, r.name FROM refs r JOIN objects o WHERE r.r_id=:h AND r.o_id=o.id',
                {"h": row[0]})
    total = ofs
    for sha, type, size, name in cur.fetchall():
        if type == 'blob':
            total += size
            yield (total, sha, size, ofs, type, depth)
            ofs += size
        elif type == 'tree':
            yield (total, sha, size, ofs, type, depth)
            for total1, sha1, size1, ofs1, type1, depth1 in _show_blobs(db, sha, total, depth+1):
                total = total1
                yield (total, sha1, size1, ofs1, type1, depth1)


def show_blobs(hash):
    log("# hash=%s\n" % hash)
    log("#\n")
    db = open_database(False, True)
    t = 0
    min = 32768+1
    for total, sha, size, ofs, type, depth in _show_blobs(db, hash, t, 0):
        print("%12d %12d %s  %s %d" % (size, ofs, sha, type, depth))
        t = total
        if (min > size and size > 0 and type == 'blob'): min = size;
    print("#------------------------")
    print("# Total =    %12d" % t)
    print("# min   = %d" % min)


def show_parent(hash):
    global db

    db = open_database(False, True)

    cur = db.cursor()
    cur.execute('SELECT DISTINCT a FROM refs WHERE refs.b=:b ', {"b": hash})

    print "Parent of %s" % hash
    print cur.fetchall()


def show_tree_size():
    global db

    db = open_database(False, True)

    cur = db.cursor()
    cur.execute('SELECT a, count(b) as c FROM refs group by a order by c')

    print "Tree sizes:"
    for hash, n in cur.fetchall():
        print("%s %d" % (hash,n))


def add_objects(hash):
    global db

    cp = git.CatPipe()

    pl = git.PackIdxList(git.repo('objects/pack'))
    needed_objects = git.NeededObjects(pl)

    db = open_database(False, False)

    cur = db.cursor()
    cur.execute('SELECT 1 FROM objects WHERE sha=:sha', {"sha": hash})
    if cur.fetchone():
        create_indexes(db)
        log('# %s is already in the database\n' % hash)
        return

    # Find needed objects reachable from hash
    traversed_objects_counter = 0

    for type, sha_, size in traverse_hash(cp, hash, needed_objects):
        if not type == 'blob':
            log("%s  %s  %12d  %5d\n" % (type, sha_, sum, size))
        traversed_objects_counter += 1
        qprogress('Traversing objects: %d\r' % traversed_objects_counter)
    progress('Traversing objects: %d, done.\n' % traversed_objects_counter)

    create_indexes(db)
    db.commit()


optspec = """
bup stats
--
a,add=     add the specified hash
f,reset    reset the database
p,parent=  show the parent of a hash
s,show=    show blobs for hash
t,tree     show tree's size
q,quiet    don't show progress meter
"""
o = options.Options(optspec)
(opt, flags, extra) = o.parse(sys.argv[1:])

handle_ctrl_c()
opt.progress = (istty2 and not opt.quiet)

if opt.show:
    show_blobs(opt.show)
elif opt.reset:
    fill_database(opt.progress)
elif opt.parent:
    show_parent(opt.parent)
elif opt.add:
    add_objects(opt.add)
elif opt.tree:
    show_tree_size()
else:
    print "Nothing to do"
