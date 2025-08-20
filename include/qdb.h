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

#define qdb_iter qmap_iter

typedef size_t qdb_measure_t(const void *thing);
typedef void qdb_print_t(const void *thing);

typedef struct {
	size_t len;
	qdb_measure_t *measure;
	qdb_print_t *print;
} qdb_type_t;

/* we have this config object mostly to avoid having
 * to specify much when opening databases */
struct qdb_config {
	int mode;
	unsigned flags;
	DBTYPE type;
	char *file;
};

extern struct qdb_config qdb_config;

/* some flags that are useful for us */
enum qdb_flags {
	QH_AINDEX = QM_AINDEX, // 1 - auto-index
	QH_MIRROR = QM_MIRROR, // 2 - lookup both ways
	/* QH_PGET = QM_PGET, // 4 - get = pget */
	QH_TMP = 8, // is secondary (useless?)
	QH_RDONLY = 16, // it's read-only
};

enum qdb_mbr {
	QDB_KEY,
	QDB_VALUE
};

/* open a database (specify much) */
unsigned qdb_openc(qdb_type_t *ktype, qdb_type_t *vtype,
		unsigned mask, unsigned flags,
		const char *file, const char *database,
		int mode, int type);

/* initialize the system */
void qdb_init(void);

/* close a database */
void qdb_close(unsigned hd, unsigned flags);

/* sync database to disk */
void qdb_sync(unsigned hd);

/* open a database (specify little) */
static inline int
qdb_open(qdb_type_t *ktype, qdb_type_t *vtype,
		unsigned mask, unsigned flags,
		char *database)
{
	return qdb_openc(ktype, vtype, mask,
			flags | qdb_config.flags,
			qdb_config.file, database,
			qdb_config.mode, qdb_config.type);
}

unsigned qdb_put(unsigned hd, void *key, void *value);
void *qdb_get(unsigned hd, void *key);

#endif
