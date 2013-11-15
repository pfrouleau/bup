#!/usr/bin/env python
import sys, os, sqlite3
from bup import git, options
from bup.helpers import *

git.check_repo_or_die()
db = None

SKIP_KNOWN = True

def create_indexes(db):
    log("Creating indexes...")
    db.execute("create index if not exists idx_obj_sha on objects(sha)")
    db.execute("create index if not exists idx_obj_type on objects(type)")
    db.execute("create index if not exists idx_refs on refs(a, name)")
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
    db.execute('CREATE TABLE IF NOT EXISTS objects (sha text, type text, size integer);')
    db.execute('CREATE TABLE IF NOT EXISTS refs (a text, b text, mode integer, name text);')

    return db


def traverse_commit(cp, sha_hex, needed_objects):
    if sha_hex not in needed_objects or not SKIP_KNOWN:
        needed_objects.add(sha_hex)

        it = iter(cp.get(sha_hex))
        type = it.next()
        assert(type == 'commit')
        content = "".join(it)
        tree_sha = content.split("\n")[0][5:].rstrip(" ")
        db.execute('INSERT INTO refs VALUES (?,?,?,?)', (sha_hex, tree_sha, 0, 'commit'))
        sum = len(content)
        for (t,s,c,l) in traverse_objects(cp, tree_sha, needed_objects, False):
            sum += c
            yield (t,s,c,l)
            db.execute('INSERT INTO objects VALUES (?,?,?)', (s, t, c))
        db.execute('INSERT INTO objects VALUES (?,?,?)', (sha_hex, type, sum))
        yield ('commit', sha_hex, sum, len(content))


def traverse_hash(cp, sha_hex, needed_objects):
    it = iter(cp.get(sha_hex))
    type = it.next()
    it = None

    if type == 'blob':
        (t,s,c,l) = traverse_objects(cp, sha_hex, needed_objects, True)
        yield (t,s,c,l)
        db.execute('INSERT INTO objects VALUES (?,?,?)', (s, t, c))

    elif type == 'commit':
        # TODO: must check for DUPS
        yield traverse_commit(pc, sha_hex, needed_objects)

    elif type == 'tree':
        log("# hash type is tree\n")
        for (t,s,c,l) in traverse_objects(cp, sha_hex, needed_objects, True):
            yield (t,s,c,l)
            db.execute('INSERT OR IGNORE INTO objects VALUES (?,?,?)', (s, t, c))


def traverse_objects(cp, sha_hex, needed_objects, check_dup):
    if sha_hex not in needed_objects or not SKIP_KNOWN:
        needed_objects.add(sha_hex)
        it = iter(cp.get(sha_hex))
        type = it.next()

        if type == 'commit':

            content = "".join(it)
            yield ('commit', sha_hex, len(content))
            tree_sha = content.split("\n")[0][5:].rstrip(" ")

            for obj in traverse_objects(cp, tree_sha, needed_objects, check_dup):
                yield obj

        if type == 'tree':
            content = "".join(it)
            sum = len(content)
            for (mode,mangled_name,sha) in git.tree_decode(content):
                try:
                    db.execute('INSERT OR IGNORE INTO refs VALUES (?,?,?,?)',
                        (sha_hex, sha.encode('hex'), mode, mangled_name))
                except:
                    # how should we handle unicode file name?
                    db.execute('INSERT OR IGNORE INTO refs VALUES (?,?,?,?)',
                        (sha_hex, sha.encode('hex'), mode, 'unicode_name'))

                for (t,s,c,l) in traverse_objects(cp, sha.encode('hex'),
                                                  needed_objects, check_dup):
                    sum += c
                    yield (t,s,c,l)
            yield ('tree', sha_hex, sum, len(content))

        elif type == 'blob':
            content = "".join(it)
            yield ('blob', sha_hex, len(content), len(content))


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
            for type, sha_, sum, size in traverse_commit(cp, sha_hex, needed_objects):
                if show_progress and not type == 'blob':
                    log("%s  %s  %12d  %5d\n" % (type, sha_, sum, size))
                traversed_objects_counter += 1
                qprogress('Traversing objects: %d\r' % traversed_objects_counter)

    # Find needed objects reachable from tags
    tags = git.tags()
    if len(tags) > 0:
        for key in tags:
            log('Traversing tag %s to find needed objects...\n' % ", ".join(tags[key]))
            for type, sha, sum, size in traverse_commit(cp, sha, needed_objects):
                if not type == 'blob':
                    log("%s  %s  %12d  %5d\n" % (type, sha_, sum, size))
                traversed_objects_counter += 1
                qprogress('Traversing objects: %d\r' % traversed_objects_counter)

    progress('Traversing objects: %d, done.\n' % traversed_objects_counter)
    if traversed_objects_counter == 0:
        o.fatal('No reachable objects found.')

    create_indexes(db)
    db.commit()


def _show_blobs(db, hash, ofs):
    #print("# %12d %s" % (ofs, hash))
    cur = db.cursor()
    #db.execute('CREATE TABLE objects (sha text, type text, size integer);')
    #db.execute('CREATE TABLE refs (a text, b text);')
    cur.execute('select o.sha, o.type, o.size, r.name, r.mode from objects o join refs r where r.a=:h and r.b=o.sha',
                {"h": hash})
    total = ofs
    for sha, type, size, name, mode in cur.fetchall():
        #print("# %s %s %X %12d %s" % (sha, type, mode, size, name))
        if type == 'tree':
            for total1, sha1, size1, ofs1, type1 in _show_blobs(db, sha, total):
                total = total1
                yield (total, sha1, size1, ofs1, type1)
        elif type == 'blob':
            total += size
            yield (total, sha, size, ofs, type)
            ofs += size


def show_blobs(hash):
    log("# hash=%s\n" % hash)
    log("#\n")
    db = open_database(False, True)
    t = 0
    min = 32768+1
    for total, sha, size, ofs, type in _show_blobs(db, hash, t):
        print("%12d %12d %s  %s" % (size, ofs, sha, type))
        t = total
        if (min > size and size > 0): min = size;
    print("#------------------------")
    print("# Total =    %12d" % t)
    print("# min   = %d" % min)


def show_parent(hash):
    global db

    db = open_database(False)

    cur = db.cursor()
    cur.execute('SELECT DISTINCT a FROM refs WHERE refs.b=:b ', {"b": hash})

    print "Parent of %s" % hash
    print cur.fetchall()


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

    for type, sha_, sum, size in traverse_hash(cp, hash, needed_objects):
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
else:
    print "Nothing to do"
