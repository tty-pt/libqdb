LIB-LDLIBS := -lqmap -ldb -lqsys
LIB := qdb
INSTALL-BIN := qdb

npm-lib := @tty-pt/qsys @tty-pt/qmap

-include node_modules/@tty-pt/mk/include.mk
