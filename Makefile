LIB-LDLIBS := -lqmap -ldb -lqsys
LDLIBS := -lqsys
LIB := qdb
INSTALL-BIN := qdb
CFLAGS := -g
prefix-Darwin := $(shell brew --prefix berkeley-db)

npm-lib := @tty-pt/qsys @tty-pt/qmap

-include ../mk/include.mk
