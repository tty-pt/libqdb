#include "./include/qdb.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/queue.h>
#include <qmap.h>
#include <qidm.h>

#define UNUSED __attribute__((unused))

#define LOG(TYPE, ...) { \
	qdblog(LOG_##TYPE, "%s: ", __func__); \
	qdblog(LOG_##TYPE, __VA_ARGS__); \
}

#define WARN(...) LOG(WARNING, __VA_ARGS__)
#define ERR(...) LOG(ERR, __VA_ARGS__)

#define CBUG(cond, ...) \
	if (cond) { \
		ERR(__VA_ARGS__); \
		abort(); \
	}

enum cur_flags {
	/* this is used for iteration */
	QH_NOT_FIRST = 512,
};

unsigned types_hd, qdb_min = 0;

struct qdb_config qdb_config = {
	.mode = 0644,
	.type = DB_HASH,
	.file = NULL,
	.env = NULL,
};

/* in-memory metadata for each db */
typedef struct meta {
	unsigned flags;
	char type_str[2][8];
	qmap_type_t *type[2];
} qdb_meta_t;

typedef int get_t(DBC *dbc, DBT *key, DBT *pkey,
		DBT *data, unsigned flags);

typedef struct {
	unsigned hd;
	DBT data, key, pkey;
	DBC *cursor;
	int flags;
	get_t *get;
} qdb_cur_t;

qdb_meta_t qdb_meta[QMAP_MAX];
DB *qdb_dbs[QMAP_MAX];
static qdb_cur_t qdb_cursors[QMAP_MAX];
static idm_t qdb_cursor_idm;

static int qdb_first = 1;

int
u_print(char * const target, const void * const value)
{
	return sprintf(target, "%u", * (unsigned *) value);
}

int
s_print(char * const target, const void * const value)
{
	return sprintf(target, "%s", (char *) value);
}

size_t
s_measure(const void * const value)
{
	return value ? strlen(value) + 1 : 0;
}

qmap_type_t qdb_string = {
	.print = s_print,
	.measure = s_measure,
}, qdb_unsigned = {
	.print = u_print,
	.len = sizeof(unsigned),
}, qdb_ptr = {
	.len = sizeof(void *),
}, qdb_unsigned_long = {
	.len = sizeof(unsigned long),
};

static void
qdb_logger_stderr(int type UNUSED, const char *fmt, ...)
{
	va_list va;
	va_start(va, fmt);
	vfprintf(stderr, fmt, va);
	va_end(va);
}

log_t qdblog = qdb_logger_stderr;

void
qdb_set_logger(log_t logger)
{
	qdblog = logger;
}

static int
primary_get(DBC *dbc, DBT *key UNUSED, DBT *pkey,
		DBT *data, unsigned flags)
{
	return dbc->get(dbc, pkey, data, flags);
}

static unsigned
_qdb_iter(unsigned hd, void *key)
{
	unsigned cur_id = idm_new(&qdb_cursor_idm);
	qdb_cur_t *cur = &qdb_cursors[cur_id];
	DB *db = qdb_dbs[hd];
	qdb_meta_t *meta = &qdb_meta[hd];

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
		= key ? qdb_len(hd, QDB_KEY, key) : 0;

	cur->get = (meta->flags & QH_SEC)
		? cur->cursor->pget
		: primary_get;

	cur->flags = key ? QH_DUP : 0;

	return cur_id;
}

static int
_qdb_next(void *key, void *value, unsigned cur_id)
{
	qdb_cur_t *cur = &qdb_cursors[cur_id];
	ssize_t ret;
	int flags;

	flags = DB_FIRST;

	if (cur->flags & QH_DUP) {
		if (cur->flags & QH_NOT_FIRST)
			flags = DB_NEXT_DUP;
		else
			flags = DB_SET;
	} else if (cur->flags & QH_NOT_FIRST)
		flags = DB_NEXT;
	cur->flags |= QH_NOT_FIRST;

	ret = cur->get(cur->cursor, &cur->key, &cur->pkey,
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
		memcpy(value, cur->data.data, cur->data.size);
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
	qmap_init();
	types_hd = qmap_open(&qdb_string, &qdb_ptr, 0, 0);
	qdb_regc("s", &qdb_string);
	qdb_regc("u", &qdb_unsigned);
	qdb_regc("p", &qdb_ptr);
	qdb_reg("ul", sizeof(unsigned long));
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
	DB *db;
	unsigned dbflags = 0;
	qdb_meta_t *meta = &qdb_meta[hd];

	qdb_first = 0;
	meta->flags = flags;

	CBUG(db_create(&db, qdb_config.env, 0),
			"db_create\n");

	if (flags & QH_DUP && !(flags & QH_TWO_WAY))
		CBUG(db->set_flags(db, DB_DUP),
				"set_flags\n");

	dbflags = qdb_config.flags & QH_THREAD ? DB_THREAD : 0;
	if (file && access(file, R_OK) == 0 && (flags & QH_RDONLY))
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
	qmap_type_t *key_type, *value_type;
	qdb_meta_t *meta;
	unsigned hd;

	CBUG(qmap_get(types_hd, &key_type, key_tid),
			"key type was not registered\n");

	CBUG(qmap_get(types_hd, &value_type, value_tid),
			"value type was not registered\n");

	hd = qmap_open(key_type, value_type, 0, flags);

	meta = &qdb_meta[hd];
	strcpy(meta->type_str[QDB_KEY], key_tid);
	strcpy(meta->type_str[QDB_VALUE >> 1], value_tid);
	meta->type[QDB_KEY] = key_type;
	meta->type[QDB_VALUE >> 1] = value_type;

	if (flags & QMAP_TWO_WAY) {
		meta = &qdb_meta[hd + 1];
		strcpy(meta->type_str[QDB_KEY], value_tid);
		strcpy(meta->type_str[QDB_VALUE >> 1], key_tid);
		meta->type[QDB_KEY] = value_type;
		meta->type[QDB_VALUE >> 1] = key_type;
	}

#if 0
	fprintf(stderr, "qdb_openc %u %s %s %u %s %s %u\n",
			hd, file, database,
			flags, key_tid, value_tid, flags);
#endif
	if (flags & QH_TMP)
		return hd;

	if (!file || (access(file, R_OK) && (flags & QH_RDONLY)))
		flags |= QH_TMP;

	return _qdb_openc(hd, file, database, mode, flags, type);
}

#if 0
static inline int
qdb_rem(unsigned hd, void *key_data, void *value_data)
{
	DB *db = qdb_dbs[hd];
	DBT key, data, pkey;
	DBC *cursor;
	int ret;

	qdb_meta_t *meta = &qdb_meta[hd];

	if ((ret = db->cursor(db, NULL, &cursor, 0)) != 0) {
		ERR("db->cursor\n");
		return ret;
	}

	memset(&key, 0, sizeof(DBT));
	memset(&pkey, 0, sizeof(DBT));
	pkey.data = key.data = key_data;
	pkey.size = key.size = qdb_len(hd, QDB_KEY, key_data);
	pkey.flags = key.flags = 0;

	memset(&data, 0, sizeof(DBT));
	data.flags = 0; /* was DB_DBT_MALLOC */
	data.data = value_data;
	data.size = qdb_len(hd, QDB_VALUE, value_data);

	get_t *get = (meta->flags & QH_SEC)
		? cursor->pget
		: primary_get;

	if (!(ret = get(cursor, &key, &pkey, &data, DB_GET_BOTH)))
		ret = cursor->del(cursor, 0);

	cursor->close(cursor);
	if (ret) {
		if (ret != DB_NOTFOUND)
			ERR("get\n");
	}

	return ret;
}

static inline void
qdb_idel(unsigned hd, void *key_r, size_t key_len)
{
	DB *db = qdb_dbs[hd];
	DBT key;
	int ret;

	memset(&key, 0, sizeof(key));

	key.data = key_r;
	key.size = key_len;

	ret = db->del(db, NULL, &key, 0);

	CBUG(ret && ret != DB_NOTFOUND);
}
#endif

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

#if 0
static inline void
qdb_del(unsigned hd, void *key, void *value)
{
	qdb_meta_t *meta = &qdb_meta[hd];
	unsigned c;

	if (value != NULL) {
		qdb_rem(hd, key, value);
		return;
	}

	qmap_type_t *kt = meta->type[QDB_KEY];
	if (kt == &qdb_unsigned) {
		if (meta->flags & QH_DUP)
			goto normal;

		qdb_idel(hd, key, sizeof(unsigned));
		return;
	}

normal:
	c = _qdb_iter(hd, key);

	while (_qdb_next(NULL, NULL, c))
		qdb_cdel(c);
}
#endif

#if 0
static inline
void qdb_fin(unsigned cur_id) {
	qdb_cur_t *cur = &qdb_cursors[cur_id];
	cur->cursor->close(cur->cursor);
}
#endif

size_t qdb_len(unsigned hd, unsigned type, void *key) {
	if (!key)
		return 0;
	type >>= 1;
	return qmap_len(hd, key, type);
}

char *qdb_type(unsigned hd, unsigned type) {
	unsigned otype = type;
	type >>= 1;
	if (otype & QDB_REVERSE)
		type = !type;
	return qdb_meta[hd].type_str[type];
}

void qdb_print(char *target, unsigned hd, unsigned type, void *thing) {
	unsigned otype = type;
	type >>= 1;
	if (otype & QDB_REVERSE)
		type = !type;
	qmap_print(target, hd, type, thing);
}

static
int qdb_putc(unsigned hd, void *key_r, size_t key_len,
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

static unsigned
_qdb_put(unsigned hd, void *key, void *value)
{
	size_t key_len, value_len;
	unsigned id = 0, flags = qdb_meta[hd].flags;

	key_len = qdb_len(hd, QDB_KEY, key);
	if (!strcmp(qdb_type(hd, QDB_KEY), "u"))
		id = * (unsigned *) key;

	if ((flags & QH_TWO_WAY) && (flags & QH_DUP)) {
		key_len = qdb_len(hd + 2, QDB_KEY, key);
		value_len = qdb_len(hd + 2, QDB_VALUE, value);
		char buf[key_len + value_len];
		memcpy(buf, key, key_len);
		memcpy(buf + key_len, value, value_len);
		return qdb_putc(hd, buf, key_len + value_len, value, value_len);
	}

	value_len = qdb_len(hd, QDB_VALUE, value);
	if (qdb_putc(hd, key, key_len, value, value_len))
		return QMAP_MISS;

	return id;
}

#if 0
static unsigned
qdb_put(unsigned hd, void *key, void *value) {
	unsigned ret = qmap_put(hd, key, value);
	qdb_meta_t *meta = &qdb_meta[hd];
	if (meta->flags & QDB_SYNC)
		_qdb_put(hd, key, value);
	return ret;
}
#endif

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
	if (!(qdb_meta[hd].flags & QH_RDONLY))
		_qdb_sync(hd);
	qmap_close(hd);
	db->close(db, flags);
	qdb_dbs[hd] = NULL;
}

unsigned
qdb_flags(unsigned hd)
{
	return qdb_meta[hd].flags;
}

void
qdb_reg(char *key, size_t len)
{
	qmap_type_t *type = (qmap_type_t *) malloc(sizeof(qmap_type_t));
	type->measure = NULL;
	type->len = len;
	type->print = NULL;
	qmap_put(types_hd, key, &type);
}

#if 0
static
void *qdb_getc(unsigned hd, size_t *size,
		void *key_r, size_t key_len)
{
	DB *db = qdb_dbs[hd];
	DBT key, data;
	int ret;

	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));

	key.data = (void *) key_r;
	key.size = key_len;
	data.flags = DB_DBT_MALLOC;

	ret = db->get(db, NULL, &key, &data, 0);

	if (ret == DB_NOTFOUND)
		return NULL;

	CBUG(ret, "db->get\n");

	*size = data.size;
	return data.data;
}

static inline void *
qdb_pgetc(unsigned hd, size_t *len, void *key_r)
{
	unsigned flags = qdb_meta[hd].flags;

	DB *db = qdb_dbs[hd];
	DBT key, pkey, data;
	int ret;

	memset(&key, 0, sizeof(key));
	memset(&pkey, 0, sizeof(pkey));
	memset(&data, 0, sizeof(data));

	key.data = (void *) key_r;
	key.size = qdb_len(hd, QDB_KEY, key_r);
	pkey.flags = data.flags = DB_DBT_MALLOC;

	ret = db->pget(db, NULL, &key, &pkey, &data, 0);

	if (ret == DB_NOTFOUND)
		return NULL;

	CBUG(ret, "db->pget\n");

	*len = pkey.size;
	return pkey.data;
}

static int
_qdb_get(unsigned hd, void *value, void *key)
{
	size_t size;
	void *value_r;

	if (qdb_meta[hd].flags & QH_PGET)
		value_r = qdb_pgetc(hd, &size, key);
	else
		value_r = qdb_getc(hd, &size, key,
				qdb_len(hd, QDB_KEY, key));

	if (!value_r)
		return 1;

	memcpy(value, value_r, size);
	return 0;
}

#endif
