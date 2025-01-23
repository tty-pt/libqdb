PREFIX ?= /usr/local
LIBDIR := ${DESTDIR}${PREFIX}/lib
debug := -fsanitize=address -fstack-protector-strong
pwd != pwd
libdir := /usr/local/lib ${pwd}
LDFLAGS	+= -lqhash -ldb ${libdir:%=-L%} ${libdir:%=-Wl,-rpath,%}

libqhash.so: libqhash.c include/qhash.h
	${CC} -o $@ libqhash.c -I/usr/local/include -g -fPIC -shared

bin: qhash

qhash: qhash.c include/qhash.h
	cc -o $@ qhash.c -I/usr/local/include -g -O3 ${LDFLAGS}

install: libqhash.so
	install -d ${DESTDIR}${PREFIX}/lib/pkgconfig
	install -m 644 libqhash.so ${DESTDIR}${PREFIX}/lib
	install -m 644 qhash.pc $(DESTDIR)${PREFIX}/lib/pkgconfig
	install -d ${DESTDIR}${PREFIX}/include
	install -m 644 include/qhash.h $(DESTDIR)${PREFIX}/include

install-bin: qhash
	install -d ${DESTDIR}${PREFIX}/bin
	install -m 755 qhash $(DESTDIR)${PREFIX}/bin

clean:
	rm qhash libqhash.so || true

.PHONY: all install install-bin clean
