PREFIX ?= /usr/local
debug := -fsanitize=address -fstack-protector-strong
pwd != pwd
prefix := ${pwd} /usr/local
CFLAGS := ${prefix:%=-I%/include} -g -O3 -Wall -Wextra -Wpedantic
LDFLAGS	+= -lqhash -ldb ${prefix:%=-L%/lib} ${prefix:%=-Wl,-rpath,%/lib}
dirs := bin lib

all: lib/libqhash.so bin/qhash

lib/libqhash.so: libqhash.c include/qhash.h lib
	${CC} -o $@ libqhash.c ${CFLAGS} -fPIC -shared

bin/qhash: qhash.c include/qhash.h bin
	cc -o $@ qhash.c ${CFLAGS} ${LDFLAGS}

$(dirs):
	mkdir $@ 2>/dev/null || true

install: lib/libqhash.so qhash
	install -d ${DESTDIR}${PREFIX}/lib/pkgconfig
	install -m 644 lib/libqhash.so ${DESTDIR}${PREFIX}/lib
	install -m 644 qhash.pc $(DESTDIR)${PREFIX}/lib/pkgconfig
	install -d ${DESTDIR}${PREFIX}/include
	install -m 644 include/qhash.h $(DESTDIR)${PREFIX}/include
	install -d ${DESTDIR}${PREFIX}/bin
	install -m 755 bin/qhash $(DESTDIR)${PREFIX}/bin

clean:
	rm bin/qhash lib/libqhash.so || true

.PHONY: all install clean
