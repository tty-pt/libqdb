#include "./../include/ttypt/qdb.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
#include <ttypt/queue.h>
#include <ttypt/qmap.h>
#include <ttypt/idm.h>
#include <ttypt/qsys.h>

#define QM_MAX 1024
#define LEN_MAX (BUFSIZ * 32)
#define TYPES_MASK 0xFF

#define DEBUG_LVL 1

#define DEBUG(lvl, ...) \
	if (DEBUG_LVL > lvl) WARN(__VA_ARGS__)

enum cur_flags {
	/* this is used for iteration */
	QH_VAL_PTR = 512,
	QH_NOT_FIRST = 1024,
};

struct qdb_config qdb_config = {
	.mode = 0644,
	.type = DB_HASH,
	.file = NULL,
};

typedef struct {
	unsigned flags, ktype, vtype;
	DB *db;
} qdb_meta_t;

typedef struct {
	unsigned hd;
	DBT data, key, pkey;
	DBC *cursor;
	int flags;
} qdb_cur_t;

static qdb_meta_t qdbs[QM_MAX];
static qdb_cur_t qdb_cursors[QM_MAX];
static idm_t qdb_cursor_idm;
static ids_t qdb_hds;

static unsigned
_qdb_iter(unsigned hd, void *key)
{
	qdb_meta_t *meta = &qdbs[hd];
	unsigned cur_id = idm_new(&qdb_cursor_idm);
	qdb_cur_t *cur = &qdb_cursors[cur_id];

	cur->hd = hd;
	cur->cursor = NULL;

	memset(&cur->pkey, 0, sizeof(DBT));
	memset(&cur->key, 0, sizeof(DBT));
	memset(&cur->data, 0, sizeof(DBT));

	meta->db->cursor(meta->db, NULL, &cur->cursor, 0);

	cur->pkey.data = cur->key.data = key;
	cur->key.flags = cur->pkey.flags
		= cur->data.flags = 0;
	cur->pkey.size = cur->key.size
		= key ? qmap_len(meta->ktype, key) : 0;

	return cur_id;
}

static int
_qdb_next(const void **key, const void **value, unsigned cur_id)
{
	qdb_cur_t *cur = &qdb_cursors[cur_id];
	ssize_t ret;
	int flags;

	flags = DB_FIRST;

	if (cur->flags & QH_NOT_FIRST)
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

	*key = cur->pkey.data;
	*value = cur->data.data;

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
	const void *key, *value;

	while (_qdb_next(&key, &value, cur_id))
		qmap_put(cur->hd, key, value);
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

	dbflags = 0;
	if (file && access(file, R_OK) == 0
			&& (flags & QH_RDONLY))

		dbflags |= DB_RDONLY;
	else
		dbflags |= DB_CREATE;

	CBUG(db->open(db, NULL, file, database,
				type, dbflags, mode),
			"open\n");

	qdbs[hd].db = db;
	qdb_open_cache(hd);
	ids_push(&qdb_hds, hd);

	return hd;
}

static inline void
qdb_openc_meta(unsigned hd,
		unsigned ktype_id,
		unsigned vtype_id,
		unsigned flags)
{
	qdb_meta_t *meta;

	meta = &qdbs[hd];

	if (ktype_id == QM_PTR)
		flags |= QH_VAL_PTR;

	meta->ktype = ktype_id;
	meta->vtype = vtype_id;
	meta->flags = flags;
}

unsigned
qdb_openc(const char *file, const char *database,
		unsigned ktype_id, unsigned vtype_id,
		unsigned mask, unsigned flags,
		int mode, int type)
{
	unsigned hd;

	if (!file || (access(file, R_OK)
				&& (flags & QH_RDONLY)))
		flags |= QH_TMP;

	hd = qmap_open(ktype_id,
			vtype_id,
			mask, flags);

	qdb_openc_meta(hd, ktype_id, vtype_id, flags);

	DEBUG(1, "qdb_openc %u %s %s %u %u %u %u\n",
			hd, file, database,
			flags, ktype_id, vtype_id, flags);

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
qdb_putc(unsigned hd, const void *key_r, size_t key_len,
		const void *value, size_t value_len)
{
	DB *db = qdbs[hd].db;
	DBT key, data;
	int ret;

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = (void *) key_r;
	key.size = key_len;
	data.data = (void *) value;
	data.size = value_len;
	data.flags = DB_DBT_MALLOC;
	u_int32_t flags;

	CBUG(db->get_flags(db, &flags), "get_flags\n");

	ret = db->put(db, NULL, &key, &data, 0);

	if (ret && ret != DB_KEYEXIST)
		WARN("qdb_putc\n");

	return ret;
}

static void
_qdb_put(unsigned hd, const void *key, const void *value)
{
	qdb_meta_t *meta = &qdbs[hd];
	size_t key_len, value_len;

	key_len = qmap_len(meta->ktype, key);
	value_len = qmap_len(meta->vtype, value);

	qdb_putc(hd, key, key_len, value, value_len);
}

static inline
void _qdb_sync(unsigned hd)
{
	unsigned cur_id = qmap_iter(hd, NULL, 0);

	const void *key, *value;

	while (qmap_next(&key, &value, cur_id))
		_qdb_put(hd, key, value);

	cur_id = _qdb_iter(hd, NULL);
	while (_qdb_next(&key, &value, cur_id)) {
		const void *evalue = qmap_get(hd, key);
		if (!evalue)
			qdb_cdel(cur_id);
	}
}

void
qdb_sync(unsigned hd)
{
	DB *db = qdbs[hd].db;
	_qdb_sync(hd);
	db->sync(db, 0);
}

void
qdb_close(unsigned hd, unsigned flags)
{
	qdb_meta_t *meta = &qdbs[hd];
	if (!(meta->flags & (QH_RDONLY | QH_TMP)))
		_qdb_sync(hd);
	qmap_close(hd);
	meta->db->close(meta->db, flags);
	meta->db = NULL;
	ids_free(&qdb_hds, hd);
}

__attribute__((destructor)) static void
qdb_exit() {
	idsi_t *cur = ids_iter(&qdb_hds);
	unsigned hd;

	while (ids_next(&hd, &cur)) {
		qdb_meta_t *meta = &qdbs[hd];

		if (meta->flags & (QH_RDONLY | QH_TMP))
			continue;

		qdb_close(hd, 0);
	}
}

__attribute__((constructor)) static void
qdb_init(void)
{
	qdb_config.mode = 0644;
	qdb_config.type = DB_HASH;
	qdb_config.file = NULL;
	qdb_config.flags = 0;
}
