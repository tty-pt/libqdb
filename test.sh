#!/bin/sh -e

qdb=./bin/qhash

assert() {
	file=snap/$1.txt
	shift
	# echo $@ >&2
	if "$@" | diff $file -; then
		return 0;
	else
		echo Test FAILED! $file != $@ >&2
		return 1
	fi
}

rm *.db 2>/dev/null || true
assert 0 $qdb -p 0:1 a.db:2:u
assert 01v $qdb -p hi -p hello b.db:a:s
assert 01 $qdb -l a.db
assert 1hello0hi $qdb -l b.db

assert none $qdb -g hallo b.db
assert 0hi $qdb -g hi b.db
assert none $qdb -g 2 b.db
assert none $qdb -g 0 b.db

assert 0hi $qdb -rg hallo b.db
assert 0hi $qdb -rg hi b.db
assert none $qdb -rg 2 b.db
assert 0hi $qdb -rg 0 b.db

assert 0hi $qdb -q b.db -g hi b.db # 0 key
assert 0hi $qdb -q b.db -g hallo b.db # 0 key
assert none $qdb -q b.db -g 2 b.db
assert 0hi $qdb -q b.db -g 0 b.db

assert 0hi $qdb -q b.db -rg hi b.db
assert none $qdb -q b.db -rg hallo b.db
assert none $qdb -q b.db -rg 2 b.db
assert none $qdb -q b.db -rg 0 b.db

assert 0hi $qdb -q b.db -q b.db -g hi b.db
assert none $qdb -q b.db -q b.db -g hallo b.db
assert none $qdb -q b.db -q b.db -g 2 b.db
assert none $qdb -q b.db -q b.db -g 0 b.db

assert 0hi $qdb -q b.db -q b.db -rg hi b.db
assert 0hi $qdb -q b.db -q b.db -rg hallo b.db
assert none $qdb -q b.db -q b.db -rg 2 b.db
assert 0hi $qdb -q b.db -q b.db -rg 0 b.db

assert hikey $qdb -p 'hi:Hi how are you' s.db:s:s
assert hi $qdb -l s.db
assert hi2key $qdb -p 'hi2:Hi how are you 2' s.db
assert empty $qdb -rd 'hi2' s.db
assert hi $qdb -l s.db
assert hi2key $qdb -p 'hi2:Hi how are you 2' s.db
assert empty $qdb -d 'Hi how are you 2' s.db
assert hi $qdb -l s.db

assert 50 $qdb -p 5:9 -p 0:1 c.db:2:u
assert missing $qdb -q a.db -L c.db
assert empty $qdb -q c.db -L a.db
assert 01 $qdb -q c.db -rL a.db
assert 5 $qdb -p 5:9 a.db
assert empty $qdb -d 9 a.db
assert 01 $qdb -l a.db
assert 5 $qdb -p 5:9 a.db
assert empty $qdb -d 5:9 a.db
assert 01 $qdb -l a.db
assert 5 $qdb -p 5:9 a.db
assert empty $qdb -rd 9:5 a.db
assert 01 $qdb -l a.db
assert 55 $qdb -p 5:9 -p 5:8 a.db
assert empty $qdb -rd 5 a.db
assert 01 $qdb -l a.db

assert assoc $qdb -a s.db -rl b.db
assert assoc-bail $qdb -a b.db -l a.db
assert assoc3 $qdb -a b.db -a s.db -l a.db
assert assoc-bail $qdb -xa b.db -a s.db -l a.db
assert assoc-bail2 $qdb -xa b.db -a s.db -rl a.db

# TODO test random (normal and reverse?)
# TODO test AINDEX col insert id
