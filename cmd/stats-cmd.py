#!/usr/bin/env python
import sys, os, sqlite3
from bup import git, options
from bup.helpers import *

git.check_repo_or_die()
db_path = git.repo('bupstats.sqlite3')
db = None

SKIP_KNOWN = True

def create_indexes(db):
    log("Creating indexes...")
    db.execute("create index if not exists idx_obj_sha on objects(sha)")
    db.execute("create index if not exists idx_obj_type on objects(type)")
    db.execute("create index if not exists idx_refs on refs(a, name)")
    log(" DONE\n")

def traverse_commit(cp, sha_hex, needed_objects):
    if sha_hex not in needed_objects or not SKIP_KNOWN:
        needed_objects.add(sha_hex)

        it = iter(cp.get(sha_hex))
        type = it.next()
        assert(type == 'commit')
        content = "".join(it)
        tree_sha = content.split("\n")[0][5:].rstrip(" ")
        db.execute('INSERT INTO refs VALUES (?,?,?,?)', (sha_hex, tree_sha, 0,'commit'))
        sum = len(content)
        for (t,s,c,l) in traverse_objects(cp, tree_sha, needed_objects):
            sum += c
            yield (t,s,c,l)
            db.execute('INSERT INTO objects VALUES (?,?,?)', (s, t, c))
        db.execute('INSERT INTO objects VALUES (?,?,?)', (sha_hex, type, sum))
        yield ('commit', sha_hex, sum, len(content))


def traverse_objects(cp, sha_hex, needed_objects):
    if sha_hex not in needed_objects or not SKIP_KNOWN:
        needed_objects.add(sha_hex)
        it = iter(cp.get(sha_hex))
        type = it.next()

        if type == 'commit':

            content = "".join(it)
            yield ('commit', sha_hex, len(content))
            tree_sha = content.split("\n")[0][5:].rstrip(" ")

            for obj in traverse_objects(cp, tree_sha, needed_objects):
                yield obj

        if type == 'tree':
            content = "".join(it)
            sum = len(content)
            for (mode,mangled_name,sha) in git.tree_decode(content):
                try:
                    db.execute('INSERT INTO refs VALUES (?,?,?,?)',
                        (sha_hex, sha.encode('hex'), mode, mangled_name))
                except:
                    # how should we handle unicode file name?
                    db.execute('INSERT INTO refs VALUES (?,?,?,?)',
                        (sha_hex, sha.encode('hex'), mode, 'unicode_name'))

                for (t,s,c,l) in traverse_objects(cp, sha.encode('hex'),
                                                  needed_objects):
                    sum += c
                    yield (t,s,c,l)
            yield ('tree', sha_hex, sum, len(content))

        elif type == 'blob':
            content = "".join(it)
            yield ('blob', sha_hex, len(content), len(content))


def fill_database():
    global db
    if os.path.exists(db_path):
        os.unlink(db_path)

    db = sqlite3.connect(db_path)
    db.execute('CREATE TABLE objects (sha text, type text, size integer);')
    db.execute('CREATE TABLE refs (a text, b text, mode integer, name text);')

    cp = git.CatPipe()

    opt.progress = (istty2 and not opt.quiet)
    refs = git.list_refs()
    refnames = [name for name, sha in refs]

    pl = git.PackIdxList(git.repo('objects/pack'))

    needed_objects = git.NeededObjects(pl)

    # Find needed objects reachable from commits
    traversed_objects_counter = 0

    for refname in refnames:
        if not refname.startswith('refs/heads/'):
            continue
        log('Traversing %s to find needed objects...\n' % refname[11:])
        for date, sha in ((date, sha.encode('hex')) for date, sha in
                          git.rev_list(refname)):
            log('Traversing commit %s to find needed objects...\n' % sha)
            for type, sha_, sum, size in traverse_commit(cp, sha, needed_objects):
                if not type == 'blob':
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

    create_indexes(db)

    db.commit()

    if traversed_objects_counter == 0:
        o.fatal('No reachable objects found.')


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
            for total1, sha1, size1, ofs1 in _show_blobs(db, sha, total):
                total = total1
                yield (total, sha1, size1, ofs1)
        elif type == 'blob':
            total += size
            yield (total, sha, size, ofs)
            ofs += size


def show_blobs():
    print "# hash=%s" % opt.show
    print "#"
    db = sqlite3.connect(db_path)
    t = 0
    for total, sha, size, ofs in _show_blobs(db, opt.show, t):
        print("%12d %12d %s" % (size, ofs, sha))
        sys.stdout.flush()
        t = total
    print "#------------------------"
    print "# Total =    %12d" % t


optspec = """
bup stats
--
f,reset    reset the database
s,show=    show blobs for hash
q,quiet    don't show progress meter
"""
o = options.Options(optspec)
(opt, flags, extra) = o.parse(sys.argv[1:])

handle_ctrl_c()

if opt.show:
    show_blobs()
elif opt.reset:
    fill_database()
else:
    print "Nothing to do"
