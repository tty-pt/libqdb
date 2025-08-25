#!/bin/sh -e

qdb=./bin/qdb

assert() {
	file=snap/$1.txt
	shift
	echo $@ >&2
	if "$@" | diff $file -; then
		return 0;
	else
		echo Test FAILED! $file != $@ >&2
		return 1
	fi
}

rm *.db 2>/dev/null || true
adb=a.db:a:u
bdb=b.db:a
sdb=s.db
cdb=c.db:2u:u
assert 0 $qdb -p 0:1 $adb
assert 01v $qdb -p hi -p hello $bdb
assert 01 $qdb -l $adb
assert 1hello0hi $qdb -l $bdb

assert none $qdb -g hallo $bdb
assert 0 $qdb -g hi $bdb
assert none $qdb -g 2 $bdb
assert none $qdb -g 0 $bdb

assert justhi $qdb -rg hallo $bdb
assert justhi $qdb -rg hi $bdb
assert none $qdb -rg 2 $bdb
assert justhi $qdb -rg 0 $bdb

assert 0 $qdb -q $bdb -g hi $bdb # 0 key
assert 0 $qdb -q $bdb -g hallo $bdb # 0 key
assert none $qdb -q $bdb -g 2 $bdb
assert 0 $qdb -q $bdb -g 0 $bdb

assert justhi $qdb -q $bdb -rg hi $bdb
assert none $qdb -q $bdb -rg hallo $bdb
assert none $qdb -q $bdb -rg 2 $bdb
assert none $qdb -q $bdb -rg 0 $bdb

assert 0 $qdb -q $bdb -q $bdb -g hi $bdb
assert none $qdb -q $bdb -q $bdb -g hallo $bdb
assert none $qdb -q $bdb -q $bdb -g 2 $bdb
assert none $qdb -q $bdb -q $bdb -g 0 $bdb

assert justhi $qdb -q $bdb -q $bdb -rg hi $bdb
assert justhi $qdb -q $bdb -q $bdb -rg hallo $bdb
assert none $qdb -q $bdb -q $bdb -rg 2 $bdb
assert justhi $qdb -q $bdb -q $bdb -rg 0 $bdb

assert hikey $qdb -p 'hi:Hi how are you' $sdb
assert hi $qdb -l $sdb
assert hi2key $qdb -p 'hi2:Hi how are you 2' $sdb
assert empty $qdb -rd 'hi2' $sdb
assert hi $qdb -l $sdb
assert hi2key $qdb -p 'hi2:Hi how are you 2' $sdb
assert empty $qdb -d 'Hi how are you 2' $sdb
assert hi $qdb -l $sdb

# FIXME No DUP PRIMARY support yet
assert 50 $qdb -p 5:9 -p 0:1 $cdb # but this still works
# assert missing $qdb -q $adb -L $cdb
# assert empty $qdb -q $cdb -L $adb
# assert 01 $qdb -q $cdb -rL $adb
assert 5 $qdb -p 5:9 $adb
assert empty $qdb -d 9 $adb
assert 01 $qdb -l $adb
assert 5 $qdb -p 5:9 $adb
assert empty $qdb -d 5:9 $adb
assert 01 $qdb -l $adb
assert 5 $qdb -p 5:9 $adb
assert empty $qdb -rd 9:5 $adb
assert 01 $qdb -l $adb
assert 55 $qdb -p 5:9 -p 5:8 $adb
assert empty $qdb -rd 5 $adb
assert 01 $qdb -l $adb

assert assoc $qdb -a $sdb -rl $bdb
assert assoc-bail $qdb -a $bdb -l $adb
assert assoc3 $qdb -a $bdb -a $sdb -l $adb
assert assoc-bail $qdb -xa $bdb -a $sdb -l $adb
assert assoc-bail2 $qdb -xa $bdb -a $sdb -rl $adb

# TODO test random (normal and reverse?)
# TODO test AINDEX col insert id
