LIB-LDLIBS := -lqmap -ldb -lqsys
LDLIBS := -lqsys
LIB := qdb
INSTALL-BIN := qdb

npm-lib := @tty-pt/qsys @tty-pt/qmap

-include ../mk/include.mk
