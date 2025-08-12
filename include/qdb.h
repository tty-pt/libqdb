#ifndef QDB_H
#define QDB_H

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/queue.h>
#include <sys/types.h>
#include <syslog.h>
#ifdef __OpenBSD__
#include <db4/db.h>
#else
#include <db.h>
#endif
#include <qmap.h>

enum qdb_type {
	QDB_KEY = 0,
	QDB_VALUE = 2,
	QDB_REVERSE = 1,
};

extern qmap_type_t qdb_string, qdb_unsigned;
extern unsigned types_hd, qdb_min;

/* we have this config object mostly to avoid having
 * to specify much when opening databases */
struct qdb_config {
	int mode;
	unsigned flags;
	DBTYPE type;
	char *file;
};
extern struct qdb_config qdb_config;

/* this is useful for custom logging */
typedef void (*log_t)(int type, const char *fmt, ...);
void qdb_set_logger(log_t logger);
extern log_t qdblog;

static inline void qdblog_perror(char *str) {
        qdblog(LOG_ERR, "%s: %s", str, strerror(errno));
}

__attribute__((noreturn))
static inline void qdblog_err(char *str) {
	qdblog(LOG_ERR, "%s\n", str);
	raise(SIGTRAP);
        exit(EXIT_FAILURE);
}

/* some flags that are useful for us */
enum qdb_flags {
	QH_DUP = QMAP_DUP, // 1 - duplicate keys
	QH_AINDEX = QMAP_AINDEX, // 2 - auto-index
	QH_PGET = QMAP_PGET, // 4 - get = pget
	QH_TWO_WAY = QMAP_TWO_WAY, // 8 - lookup both ways
	QH_RDONLY = 16, // it's read-only
	QH_TMP = 32, // is secondary (useless?)
};

/* open a database (specify much) */
unsigned qdb_openc(const char *file, const char *database,
		int mode, unsigned flags, int type,
		char *key_tid, char *value_tid);

/* get a key or value's length */
size_t qdb_len(unsigned hd, unsigned type, void *key);

/* get the type of a key or value */
char *qdb_type(unsigned hd, unsigned type);

/* initialize the system */
void qdb_init(void);

/* close a database */
void qdb_close(unsigned hd, unsigned flags);

/* sync database to disk */
void qdb_sync(unsigned hd);

/* return our flags */
unsigned qdb_flags(unsigned hd);

/* the same but just give a length (for simple types) */
void qdb_reg(char *key, size_t len);

/* print a key or value */
void qdb_print(char *target, unsigned hd,
		unsigned type, void *thing);

/* open a database (specify little) */
static inline int
qdb_open(char *database, char *key_tid,
		char *value_tid, unsigned flags)
{
	return qdb_openc(qdb_config.file, database,
			qdb_config.mode,
			flags | qdb_config.flags,
			qdb_config.type, key_tid, value_tid);
}

/* register a type of key / value */
static inline void
qdb_regc(char *key, qmap_type_t *type) {
	qmap_put(types_hd, key, &type);
}

#endif
