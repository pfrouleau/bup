#!/usr/bin/env python
import sys, os, sqlite3
from bup import git, options
from bup.helpers import *

git.check_repo_or_die()
cp = None
db = None

SKIP_KNOWN = True

def create_indexes():
    log("Creating indexes...")
    # db.execute("create index if not exists idx_obj_sha on objects(sha)")
    # db.execute("create index if not exists idx_obj_type on objects(type)")
    db.execute("create index if not exists idx_refs on refs(o_id)")
    log(" DONE\n")


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


def get_object_id(sha):
    cur = db.cursor()
    cur.execute('SELECT id FROM objects WHERE sha=:h', {"h": sha})
    row = cur.fetchone()

    if row == None:
        return None

    return row[0]


# Out: object's id, is_new
def insert_object(sha, type, size):
    cur = db.cursor()
    cur.execute('INSERT OR IGNORE INTO objects VALUES (null,?,?,?)', (sha, type, size))

    if cur.rowcount == 1:
       return cur.lastrowid, True
    else:
        log('# present (%s)\n' % sha)
        return get_object_id(sha), False


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
def traverse_commit(sha_hex):

    it = iter(cp.get(sha_hex))
    type = it.next()
    assert(type == 'commit')
    content = "".join(it)
    length = len(content)

    o_id, new = insert_object(sha_hex, type, length)

    if new:
        tree_sha = content.split("\n")[0][5:].rstrip(" ")

        yield (type, sha_hex, length)
        for obj in traverse_objects(False,
                            o_id, 0, 'commit', tree_sha):
            yield obj


# yield: type, sha, length
def traverse_hash(sha_hex):
    for obj in traverse_objects(True, 0, 0, '-', sha_hex):
        yield obj


# yield: type, sha, length
def traverse_objects(check_dup, r_id, r_mode, r_name, sha_hex):
    o_id = get_object_id(sha_hex)
    if not o_id is None:
        insert_ref(r_id, o_id, r_mode, r_name)
        return

    it = iter(cp.get(sha_hex))
    type = it.next()
    content = "".join(it)
    length = len(content)

    o_id, new = insert_object(sha_hex, type, length)
    insert_ref(r_id, o_id, r_mode, r_name)

    yield (type, sha_hex, length)

    if type == 'blob':
        return

    elif type == 'tree':
        for (mode, mangled_name, sha) in git.tree_decode(content):
            for obj in traverse_objects(check_dup,
                            o_id, mode, mangled_name, sha.encode('hex')):
                yield obj

    elif type == 'commit':
        tree_sha = content.split("\n")[0][5:].rstrip(" ")

        for obj in traverse_objects(check_dup, o_id, r_mode, tree_sha):
            yield obj


def fill_database(show_progress):
    global cp, db

    cp = git.CatPipe()

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
            for type, sha_, size in traverse_commit(sha_hex):
                traversed_objects_counter += 1
                if show_progress and not type == 'blob':
                    log("%8s  %s  %5d\n" % (type, sha_, size))
                    qprogress('Traversing objects: %d\r' % traversed_objects_counter)

    # Find needed objects reachable from tags
    tags = git.tags()
    if len(tags) > 0:
        for key in tags:
            log('Traversing tag %s to find needed objects...\n' % ", ".join(tags[key]))
            for type, sha, size in traverse_commit(sha):
                traversed_objects_counter += 1
                if show_progress and not type == 'blob':
                    log("%8s  %s  %5d\n" % (type, sha_, size))
                    qprogress('Traversing objects: %d\r' % traversed_objects_counter)

    progress('Traversing objects: %d, done.\n' % traversed_objects_counter)
    if traversed_objects_counter == 0:
        o.fatal('No reachable objects found.')

    create_indexes()
    db.commit()


def _show_blobs(hash, ofs, depth):
    id = get_object_id(hash)

    cur = db.cursor()
    cur.execute('SELECT o.sha, o.type, o.size, r.name FROM refs r JOIN objects o WHERE r.r_id=:h AND r.o_id=o.id',
                {"h": id})
    for sha, type, size, name in cur.fetchall():
        if type == 'blob':
            yield (sha, size, ofs, type, depth)
            ofs += size
        elif type == 'tree':
            yield (sha, size, ofs, type, depth)
            for sha1, size1, ofs1, type1, depth1 in _show_blobs(sha, ofs, depth+1):
                if type1 == 'blob':
                    ofs += size1
                yield (sha1, size1, ofs1, type1, depth1)


def show_blobs(hash, show_details):
    global db

    db = open_database(False, True)
    create_indexes()

    id = get_object_id(hash)
    if id is None:
        o.fatal('hash not found (%s)' % hash)

    pl = git.PackIdxList(git.repo('objects/pack'))
    known_objects = git.NeededObjects(pl)

    blob_min = 32768+1
    bs_tot = bs_dedup = b_count = 0
    ts_dedup = t_count = 0

    print("# hash=%s" % hash)
    print("#")
    for sha, size, ofs, type, depth in _show_blobs(hash, 0, 0):
        if type == 'blob':
            bs_tot += size
        k = ""
        if sha not in known_objects:
            known_objects.add(sha)
            k = " +"
            if type == 'blob':
                bs_dedup += size
                b_count  += 1
                if (blob_min > size and size > 0): blob_min = size;
            else:
                ts_dedup += size
                t_count  += 1
        if show_details:
            print("%s  %12d %6s %5d %2d %s" % (sha, ofs, type, size, depth, k))

    print("#---------------------------------------------------------------------")
    print("# Blobs:                         Total =  %12d   min = %4d" % (bs_tot, blob_min))
    print("#                                Dedup =  %12d  diff = %4d" % (bs_dedup, bs_tot-bs_dedup))
    print("#        Count = %8d          Avg =      %12.3f" % (b_count, bs_dedup/b_count))
    print("#.....................................................................")
    print("# Trees:                         Dedup =  %12d" % (ts_dedup))
    print("#        Count = %8d          Avg =      %12.3f" % (t_count, ts_dedup/t_count))


def show_parent(sha):
    global db

    db = open_database(False, True)

    id = get_object_id(sha)
    if id is None:
        o.fatal('Unknown hash (%s)' % hash)

    cur = db.cursor()
    cur.execute('SELECT DISTINCT o.sha FROM refs r JOIN objects o WHERE r.o_id=:k AND r.r_id=o.id',
                {"k": id})

    print "Parent of %s" % sha
    print cur.fetchall()


def show_tree_size():
    global db

    db = open_database(False, True)
    create_indexes()

    cur = db.cursor()
    cur.execute('SELECT o.sha, count(r.o_id) as c FROM refs r JOIN objects o WHERE r.r_id = o.id GROUP BY r.r_id ORDER BY c')

    print "Tree sizes:"
    for hash, n in cur.fetchall():
        print("%s %d" % (hash,n))


def add_objects(sha):
    global cp, db

    cp = git.CatPipe()

    db = open_database(False, False)

    id = get_object_id(sha)
    if id:
        create_indexes()
        log('# %s is already in the database\n' % hash)
        return

    # Find needed objects reachable from hash
    traversed_objects_counter = 0

    for type, sha_, size in traverse_hash(hash):
        if not type == 'blob':
            log("%s  %s  %12d  %5d\n" % (type, sha_, sum, size))
        traversed_objects_counter += 1
        qprogress('Traversing objects: %d\r' % traversed_objects_counter)
    progress('Traversing objects: %d, done.\n' % traversed_objects_counter)

    create_indexes()
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

# For now, ignore the midx to avoid a possible bug in NeededObject's logic.
git.ignore_midx = 1

if opt.show:
    show_blobs(opt.show, opt.progress)
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
