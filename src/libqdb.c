#include "./../include/qdb.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/queue.h>
#include <qmap.h>
#include <qidm.h>
#include <qsys.h>

enum cur_flags {
	/* this is used for iteration */
	QH_NOT_FIRST = 512,
};

struct qdb_config qdb_config = {
	.mode = 0644,
	.type = DB_HASH,
	.file = NULL,
};

typedef struct {
	unsigned hd;
	DBT data, key, pkey;
	DBC *cursor;
	int flags;
} qdb_cur_t;

DB *qdb_dbs[QMAP_MAX];
static qdb_cur_t qdb_cursors[QMAP_MAX];
static idm_t qdb_cursor_idm;

static unsigned
_qdb_iter(unsigned hd, void *key)
{
	unsigned cur_id = idm_new(&qdb_cursor_idm);
	qdb_cur_t *cur = &qdb_cursors[cur_id];
	DB *db = qdb_dbs[hd];

	cur->hd = hd;
	cur->cursor = NULL;

	memset(&cur->pkey, 0, sizeof(DBT));
	memset(&cur->key, 0, sizeof(DBT));
	memset(&cur->data, 0, sizeof(DBT));

	db->cursor(db, NULL, &cur->cursor, 0);

	cur->pkey.data = cur->key.data = key;
	cur->key.flags = cur->pkey.flags
		= cur->data.flags = 0;
	cur->pkey.size = cur->key.size
		= key ? qmap_len(hd, key, QMAP_KEY) : 0;

	cur->flags = key ? QMAP_DUP : 0;

	return cur_id;
}

static int
_qdb_next(void *key, void *value, unsigned cur_id)
{
	qdb_cur_t *cur = &qdb_cursors[cur_id];
	ssize_t ret;
	int flags;

	flags = DB_FIRST;

	if (cur->flags & QMAP_DUP) {
		if (cur->flags & QH_NOT_FIRST)
			flags = DB_NEXT_DUP;
		else
			flags = DB_SET;
	} else if (cur->flags & QH_NOT_FIRST)
		flags = DB_NEXT;
	cur->flags |= QH_NOT_FIRST;

	ret = cur->cursor->get(cur->cursor, &cur->pkey,
			&cur->data, flags);

	if (ret) {
		if (ret != DB_NOTFOUND)
			WARN("%u %d %s", cur->hd,
					cur->flags,
					db_strerror(ret));
		cur->cursor->close(cur->cursor);
		return 0;
	}

	if (key) {
		memcpy(key, cur->pkey.data, cur->pkey.size);
		memcpy(value, cur->data.data,
				cur->data.size);
	}

	memset(&cur->key, 0, sizeof(DBT));
	memset(&cur->pkey, 0, sizeof(DBT));
	memset(&cur->data, 0, sizeof(DBT));
	return 1;
}

static inline
void qdb_open_cache(unsigned hd)
{
	unsigned cur_id = _qdb_iter(hd, NULL);
	qdb_cur_t *cur = &qdb_cursors[cur_id];
	char key[QMAP_MAX_COMBINED_LEN];
	char value[QMAP_MAX_COMBINED_LEN];

	while (_qdb_next(key, value, cur_id))
		qmap_put(cur->hd, key, value);
}

void
qdb_init(void)
{
	qdb_config.mode = 0644;
	qdb_config.type = DB_HASH;
	qdb_config.file = NULL;
	qdb_config.flags = 0;
}

unsigned
_qdb_openc(unsigned hd, const char *file,
		const char *database, int mode,
		unsigned flags, int type)
{
	unsigned dbflags = 0;
	DB *db;

	CBUG(db_create(&db, NULL, 0),
			"db_create\n");

	if (flags & QMAP_DUP && !(flags & QMAP_TWO_WAY))
		CBUG(db->set_flags(db, DB_DUP),
				"set_flags\n");

	dbflags = 0;
	if (file && access(file, R_OK) == 0
			&& (flags & QH_RDONLY))

		dbflags |= DB_RDONLY;
	else
		dbflags |= DB_CREATE;

	CBUG(db->open(db, NULL, file, database,
				type, dbflags, mode),
			"open\n");

	qdb_dbs[hd] = db;
	qdb_open_cache(hd);

	return hd;
}

unsigned
qdb_openc(const char *file, const char *database, int mode,
		unsigned flags, int type,
		char *key_tid, char *value_tid)
{
	unsigned hd;

	if (!file || (access(file, R_OK)
				&& (flags & QH_RDONLY)))
		flags |= QH_TMP;

	hd = qmap_open(key_tid, value_tid, 0, flags);

#if 0
	fprintf(stderr, "qdb_openc %u %s %s %u %s %s %u\n",
			hd, file, database,
			flags, key_tid, value_tid, flags);
#endif
	if (flags & QH_TMP)
		return hd;

	return _qdb_openc(hd, file, database,
			mode, flags, type);
}

int
qdb_cdel(unsigned cur_id)
{
	qdb_cur_t *cur = &qdb_cursors[cur_id];
	if (!cur->cursor) {
		ERR("tried deleting with cursor not set\n");
		return 1;
	}
	return cur->cursor->c_del(cur->cursor, 0);
}

static int
qdb_putc(unsigned hd, void *key_r, size_t key_len,
		void *value, size_t value_len)
{
	DB *db = qdb_dbs[hd];
	DBT key, data;
	int ret;

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = key_r;
	key.size = key_len;
	data.data = value;
	data.size = value_len;
	data.flags = DB_DBT_MALLOC;
	int dupes;
	u_int32_t flags;

	CBUG(db->get_flags(db, &flags), "get_flags\n");

	dupes = flags & DB_DUP;

	ret = db->put(db, NULL, &key, &data, 0);

	if (ret && (ret != DB_KEYEXIST || !dupes))
		WARN("qdb_putc\n");

	return ret;
}

static void
_qdb_put(unsigned hd, void *key, void *value)
{
	size_t key_len, value_len;
	unsigned flags = qmap_flags(hd);

	key_len = qmap_len(hd, key, QMAP_KEY);

	if ((flags & QMAP_TWO_WAY) && (flags & QMAP_DUP)) {
		key_len = qmap_len(hd + 2, key, QMAP_KEY);
		value_len = qmap_len(hd + 2,
				value, QMAP_VALUE);
		char buf[key_len + value_len];
		memcpy(buf, key, key_len);
		memcpy(buf + key_len, value, value_len);
		qdb_putc(hd, buf, key_len + value_len,
				value, value_len);
	}

	value_len = qmap_len(hd, value, QMAP_VALUE);
	qdb_putc(hd, key, key_len, value, value_len);
}

static inline
void _qdb_sync(unsigned hd)
{
	unsigned cur_id = qmap_iter(hd, NULL);
	char key[QMAP_MAX_COMBINED_LEN];
	char value[QMAP_MAX_COMBINED_LEN];

	while (qmap_next(key, value, cur_id))
		_qdb_put(hd, key, value);

	cur_id = _qdb_iter(hd, NULL);
	while (_qdb_next(key, value, cur_id))
		if (qmap_get(hd, value, key))
			qdb_cdel(cur_id);
}

void
qdb_sync(unsigned hd)
{
	DB *db = qdb_dbs[hd];
	_qdb_sync(hd);
	db->sync(db, 0);
}

void
qdb_close(unsigned hd, unsigned flags)
{
	DB *db = qdb_dbs[hd];
	if (!(qmap_flags(hd) & (QH_RDONLY | QH_TMP)))
		_qdb_sync(hd);
	qmap_close(hd);
	db->close(db, flags);
	qdb_dbs[hd] = NULL;
}
