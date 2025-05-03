#!/bin/sh -e

assert() {
	file=snap/$1.txt
	shift
	"$@" > $file
	return 0
}

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

rm *.db || true
assert 0 ./qhash -p 0:1 a.db:t:u
assert 01v ./qhash -p hi -p hello b.db:a:s
assert 01 ./qhash -l a.db
assert 1hello0hi ./qhash -l b.db

assert none ./qhash -g hallo b.db
assert 0hi ./qhash -g hi b.db
assert none ./qhash -g 2 b.db
assert none ./qhash -g 0 b.db

assert 0hi ./qhash -rg hallo b.db
assert 0hi ./qhash -rg hi b.db
assert none ./qhash -rg 2 b.db
assert 0hi ./qhash -rg 0 b.db

assert 0hi ./qhash -q b.db -g hi b.db # 0 key
assert 0hi ./qhash -q b.db -g hallo b.db # 0 key
assert none ./qhash -q b.db -g 2 b.db
assert 0hi ./qhash -q b.db -g 0 b.db

assert 0hi ./qhash -q b.db -rg hi b.db
assert none ./qhash -q b.db -rg hallo b.db
assert none ./qhash -q b.db -rg 2 b.db
assert none ./qhash -q b.db -rg 0 b.db

assert 0hi ./qhash -q b.db -q b.db -g hi b.db
assert none ./qhash -q b.db -q b.db -g hallo b.db
assert none ./qhash -q b.db -q b.db -g 2 b.db
assert none ./qhash -q b.db -q b.db -g 0 b.db

assert 0hi ./qhash -q b.db -q b.db -rg hi b.db
assert 0hi ./qhash -q b.db -q b.db -rg hallo b.db
assert none ./qhash -q b.db -q b.db -rg 2 b.db
assert 0hi ./qhash -q b.db -q b.db -rg 0 b.db

assert hikey ./qhash -p 'hi:Hi how are you' s.db:s
assert hi ./qhash -l s.db
assert hi2key ./qhash -p 'hi2:Hi how are you 2' s.db
assert empty ./qhash -rd 'hi2' s.db
assert hi ./qhash -l s.db
assert hi2key ./qhash -p 'hi2:Hi how are you 2' s.db
assert empty ./qhash -d 'Hi how are you 2' s.db
assert hi ./qhash -l s.db

assert 50 ./qhash -p 5:9 -p 0:1 c.db:t:u
assert missing ./qhash -q a.db -L c.db
assert empty ./qhash -q c.db -L a.db
assert 01 ./qhash -q c.db -rL a.db
assert 5 ./qhash -p 5:9 a.db
assert empty ./qhash -d 9 a.db
assert 01 ./qhash -l a.db
assert 5 ./qhash -p 5:9 a.db
assert empty ./qhash -d 5:9 a.db
assert 01 ./qhash -l a.db
assert 5 ./qhash -p 5:9 a.db
assert empty ./qhash -rd 9:5 a.db
assert 01 ./qhash -l a.db
assert 55 ./qhash -p 5:9 -p 5:8 a.db
assert empty ./qhash -rd 5 a.db
assert 01 ./qhash -l a.db

# TODO test associative printing (normal and reverse)
# TODO test bail
#
# TODO test random (normal and reverse?)
