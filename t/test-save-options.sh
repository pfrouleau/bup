#!/usr/bin/env bash
. ./wvtest-bup.sh || exit $?

WVSTART 'test save''s options'

top="$(WVPASS pwd)" || exit $?
tmpdir="$(WVPASS wvmktempdir)" || exit $?
export BUP_DIR="$tmpdir/bup"
export GIT_DIR="$BUP_DIR"

bup() { "$top/bup" "$@"; }

WVPASS mkdir -p "$tmpdir/src"
WVPASS touch "$tmpdir/src/foo"
WVPASS bup init
WVPASS bup index "$tmpdir/src"

WVPASS bup save -n master "$tmpdir/src"
WVPASSEQ "$(git fsck --unreachable)" ""

WVPASS bup save -n src0 "$tmpdir/src"
WVPASSEQ "$(git fsck --unreachable)" ""

WVPASS bup save -n 201501 "$tmpdir/src"
WVPASSEQ "$(git fsck --unreachable)" ""

WVPASS bup save -n 2015-02 "$tmpdir/src"
WVPASSEQ "$(git fsck --unreachable)" ""

WVPASS bup save -n 2015/03 "$tmpdir/src"
WVPASSEQ "$(git fsck --unreachable)" ""

WVSTART 'check saved names'

WVPASSEQ "$(bup ls)" "2015-02
2015/03
201501
master
src0"

WVPASS rm -rf "$tmpdir"
