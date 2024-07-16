PREFIX ?= /usr/local
LIBDIR := $(DESTDIR)${PREFIX}/lib

LD := ${CC}

LDFLAGS := -ldb -L/usr/lib -L/usr/local/lib
CFLAGS := -Iinclude -I/usr/local/include
lib-LDFLAGS := ${LDFLAGS} -fPIC -shared

libqhash.so: qhash.c include/qhash.h
	${CC} -o $@ qhash.c ${CFLAGS} ${lib-LDFLAGS}

install: libqhash.so
	install -d ${DESTDIR}${PREFIX}/lib/pkgconfig
	install -m 644 libqhash.so ${DESTDIR}${PREFIX}/lib
	install -m 644 qhash.pc $(DESTDIR)${PREFIX}/lib/pkgconfig
	install -d ${DESTDIR}${PREFIX}/include
	install -m 644 include/qhash.h $(DESTDIR)${PREFIX}/include

.PHONY: install
