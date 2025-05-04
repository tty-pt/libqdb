PREFIX ?= /usr/local
debug := -fsanitize=address -fstack-protector-strong
pwd != pwd
prefix := ${pwd} /usr/local
CFLAGS := ${prefix:%=-I%/include} -g -O3 -Wall -Wextra -Wpedantic
LDFLAGS	+= -lqdb -ldb ${prefix:%=-L%/lib} ${prefix:%=-Wl,-rpath,%/lib}
dirs := bin lib

all: lib/libqdb.so bin/qdb

lib/libqdb.so: libqdb.c include/qdb.h lib
	${CC} -o $@ libqdb.c ${CFLAGS} -fPIC -shared

bin/qdb: qdb.c include/qdb.h bin
	cc -o $@ qdb.c ${CFLAGS} ${LDFLAGS}

$(dirs):
	mkdir $@ 2>/dev/null || true

install: lib/libqdb.so qdb
	install -d ${DESTDIR}${PREFIX}/lib/pkgconfig
	install -m 644 lib/libqdb.so ${DESTDIR}${PREFIX}/lib
	install -m 644 qdb.pc $(DESTDIR)${PREFIX}/lib/pkgconfig
	install -d ${DESTDIR}${PREFIX}/include
	install -m 644 include/qdb.h $(DESTDIR)${PREFIX}/include
	install -d ${DESTDIR}${PREFIX}/bin
	install -m 755 bin/qdb $(DESTDIR)${PREFIX}/bin

clean:
	rm bin/qdb lib/libqdb.so || true

.PHONY: all install clean
