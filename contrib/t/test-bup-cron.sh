#!/usr/bin/env bash
. ./wvtest-bup.sh

set -o pipefail

WVSTART "Testing bup-cron..."

top="$(WVPASS pwd)" || exit $?
#tmpdir="$(WVPASS wvmktempdir)" || exit $?
tmpdir="$top/t/tmp/bup-cron-test"
mkdir -p "$tmpdir"

export BUP_DIR="$tmpdir/repo.bup"
export GIT_DIR="$BUP_DIR"

bup() { "$top/bup" "$@"; }
bup-cron() { "$top/contrib/bup-cron" --pidfile "$tmpdir/bup-cron.pid" "$@"; }

WVPASS bup init
WVPASS cd "$tmpdir"

# Does bup-cron can be called
WVPASS bup-cron -h

# Create some data to backup
WVSTART "create src data"
WVPASS mkdir -p "$tmpdir/src/"{dir1,dir2}
WVPASS date    > "$tmpdir/src/dir1/d10"
WVPASS date -u > "$tmpdir/src/dir1/d11"
WVPASS date -u > "$tmpdir/src/dir2/d20"
WVPASS date    > "$tmpdir/src/dir2/d21"

# Basic options
WVSTART "bup-cron: basic options"
#WVFAIL BUP_DIR= bup-cron
branch_name="$HOSTNAME-${tmpdir//\//_}_src"
WVPASS bup-cron "$tmpdir/src/dir1"
WVPASSEQ "$(WVPASS bup ls /)" "$branch_name"
WVPASSEQ "$(WVPASS bup ls /$branch_name/latest/dir1/)" "d10
d11"
WVPASS bup restore -C "$tmpdir/dst/dir1" "$branch_name/latest/"
WVPASSEQ "$(WVPASS ls "$tmpdir/dst")" "dir1"
WVPASS "$top/t/compare-trees" "$tmpdir/src/dir1" "$tmpdir/dst/dir1"
WVPASS rm -fr "$tmpdir/dst"

# test --name and branch isolation
branch_name="B2-${tmpdir//\//_}_src"
WVPASS bup-cron --name B2 "$tmpdir/src/dir2"
WVPASSEQ "$(WVPASS bup ls /$branch_name/latest/dir2/)" "d20
d21"
# - stuff from dir1 must not be in B2
WVPASS bup restore -C "$tmpdir/dst/dir2" "$branch_name/latest/"
WVPASSEQ "$(WVPASS ls "$tmpdir/dst")" "dir2"
WVPASS "$top/t/compare-trees" "$tmpdir/src/dir2" "$tmpdir/dst/dir2"
WVPASS rm -fr "$tmpdir/dst"

# test jobs
function write_json()
{
	# $1 = work path
	# $2 = json's filename
	# $3 = include dir

	cat <<-EOF > "$1/$2"
	[{
		"job_name":"test job",
		"targets":[{
			$SNAPSHOT_ARGS
			"repos":[{
				"local_rep":"$1/repo.bup",
				$REMOTE_REPO
				"branch":"job",
				$GRAFT
				$STATS
				"includes":[
					"$1/$3"
				],
				"excludes":[
					$EXCLUDES
				],
				"excludes_rx":[
					$EXCLUDES_RX
				]
			}]
		}]
	}]
	EOF
}

WVSTART "bup-cron: jobs"
WVPASS write_json "$tmpdir" bup.job "src/dir1"
WVPASS bup-cron --jobfile "$tmpdir/bup.job" -l "$tmpdir/log" 2> "$tmpdir/err"


# TODO:
# test logfile
# test jobfile
# if ROOT:
# - test snapshot
#	- lvm
#	- VSS

#WVPASS rm -fr "$tmpdir"
