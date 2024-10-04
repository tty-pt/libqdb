PREFIX ?= /usr/local
LIBDIR := $(DESTDIR)${PREFIX}/lib

libqhash.so: qhash.c include/qhash.h
	${CC} -o $@ qhash.c -I/usr/local/include -g -O3 -fPIC -shared

install: libqhash.so
	install -d ${DESTDIR}${PREFIX}/lib/pkgconfig
	install -m 644 libqhash.so ${DESTDIR}${PREFIX}/lib
	install -m 644 qhash.pc $(DESTDIR)${PREFIX}/lib/pkgconfig
	install -d ${DESTDIR}${PREFIX}/include
	install -m 644 include/qhash.h $(DESTDIR)${PREFIX}/include

.PHONY: install
