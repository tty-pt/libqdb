all := libqdb qdb
LDLIBS-libqdb := -lqmap -ldb -lqsys
LDLIBS-qdb := -lqdb -lqmap -lqsys
add-prefix-Darwin := $(shell brew --prefix berkeley-db)
CFLAGS := -g

-include ../mk/include.mk
