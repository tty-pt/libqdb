#ifndef QDB_H
#define QDB_H

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <ttypt/queue.h>
#include <sys/types.h>
#include <syslog.h>
#ifdef __OpenBSD__
#include <db4/db.h>
#else
#include <db.h>
#endif
#include <ttypt/qmap.h>

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
	QH_PGET = QM_PGET, // 4 - get = pget
	QH_TMP = 8, // is secondary (useless?)
	QH_RDONLY = 16, // it's read-only
};

/* open a database (specify much) */
unsigned qdb_openc(const char *file, const char *database,
		unsigned ktype, unsigned vtype,
		unsigned mask, unsigned flags,
		int mode, int type);

/* close a database */
void qdb_close(unsigned hd, unsigned flags);

/* sync database to disk */
void qdb_sync(unsigned hd);

/* open a database (specify little) */
static inline int
qdb_open(char *database, unsigned ktype, unsigned vtype,
		unsigned mask, unsigned flags)
{
	return qdb_openc(qdb_config.file, database,
			ktype, vtype, mask,
			flags | qdb_config.flags,
			qdb_config.mode, qdb_config.type);
}

#endif
