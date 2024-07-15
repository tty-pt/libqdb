PREFIX ?= /usr/local
LIBDIR := $(DESTDIR)${PREFIX}/lib

LD := gcc

LDFLAGS := -ldb -L/usr/lib
CFLAGS := -Iinclude
lib-LDFLAGS := ${LDFLAGS} -fPIC -shared

libqhash.so: qhash.c include/qhash.h
	${LD} -o $@ $< ${CFLAGS} ${lib-LDFLAGS}

install: libqhash.so
	install -d ${DESTDIR}${PREFIX}/lib/pkgconfig
	install -m 644 libqhash.so ${DESTDIR}${PREFIX}/lib
	install -m 644 qhash.pc $(DESTDIR)${PREFIX}/lib/pkgconfig
	install -d ${DESTDIR}${PREFIX}/include
	install -m 644 include/qhash.h $(DESTDIR)${PREFIX}/include

.PHONY: install
