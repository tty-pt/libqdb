#!/bin/sh -e

assert() {
	file=$1
	shift
	$@ > $file
}

assert() {
	file=$1
	shift
	echo $@ >&2
	if "$@" | diff $file -; then
		return 0;
	else
		echo Test FAILED! $file != $@ >&2
		return 1
	fi
}

NONE=snap/5.txt
ONE=snap/6.txt
EMPTY=snap/1.txt

rm *.db || true
assert $EMPTY ./qhash -p 0:1 a.db:t:u
assert snap/2.txt ./qhash -p hi -p hello b.db:a:s
assert snap/3.txt ./qhash -l a.db
assert snap/4.txt ./qhash -l b.db

assert $NONE ./qhash -g hallo b.db
assert $ONE ./qhash -g hi b.db
assert $NONE ./qhash -g 2 b.db
assert $NONE ./qhash -g 0 b.db

assert $ONE ./qhash -rg hallo b.db
assert $ONE ./qhash -rg hi b.db
assert $NONE ./qhash -rg 2 b.db
assert $ONE ./qhash -rg 0 b.db

assert $ONE ./qhash -q b.db -g hi b.db # 0 key
assert $ONE ./qhash -q b.db -g hallo b.db # 0 key
assert $NONE ./qhash -q b.db -g 2 b.db
assert $ONE ./qhash -q b.db -g 0 b.db

assert $ONE ./qhash -q b.db -rg hi b.db
assert $NONE ./qhash -q b.db -rg hallo b.db
assert $NONE ./qhash -q b.db -rg 2 b.db
assert $NONE ./qhash -q b.db -rg 0 b.db

assert $ONE ./qhash -q b.db -q b.db -g hi b.db
assert $NONE ./qhash -q b.db -q b.db -g hallo b.db
assert $NONE ./qhash -q b.db -q b.db -g 2 b.db
assert $NONE ./qhash -q b.db -q b.db -g 0 b.db

assert $ONE ./qhash -q b.db -q b.db -rg hi b.db
assert $ONE ./qhash -q b.db -q b.db -rg hallo b.db
assert $NONE ./qhash -q b.db -q b.db -rg 2 b.db
assert $ONE ./qhash -q b.db -q b.db -rg 0 b.db

assert $EMPTY ./qhash -p 'hi:Hi how are you' s.db:s
assert snap/hi.txt ./qhash -l s.db
assert $EMPTY ./qhash -p 'hi2:Hi how are you 2' s.db
assert $EMPTY ./qhash -rd 'hi2' s.db
assert snap/hi.txt ./qhash -l s.db
assert $EMPTY ./qhash -p 'hi2:Hi how are you 2' s.db
assert $EMPTY ./qhash -d 'Hi how are you 2' s.db
assert snap/hi.txt ./qhash -l s.db

assert $EMPTY ./qhash -p 5:9 -p 0:1 c.db:t:u
assert snap/missing.txt ./qhash -q a.db -L c.db
assert $EMPTY ./qhash -q c.db -L a.db
assert snap/3.txt ./qhash -q c.db -rL a.db
assert $EMPTY ./qhash -p 5:9 a.db
assert $EMPTY ./qhash -d 9 a.db
assert snap/3.txt ./qhash -l a.db
assert $EMPTY ./qhash -p 5:9 a.db
assert $EMPTY ./qhash -d 5:9 a.db
assert snap/3.txt ./qhash -l a.db
# TODO test reverse del

# TODO test associative printing (normal and reverse)
# TODO test bail
#
# TODO test random (normal and reverse?)
