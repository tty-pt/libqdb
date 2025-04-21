#include "./include/qhash.h"
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <syslog.h>

#define HASH_NOT_FOUND NULL

enum qhash_priv_flags {
	QH_NOT_FIRST = 1, // internal iteration flag
};

static DB *hash_dbs[HASH_DBS_MAX];

static struct idm idm;

static int hash_first = 1;
DB_ENV *env = NULL;

static void
hash_logger_stderr(int type __attribute__((unused)), const char *fmt, ...)
{
	va_list va;
	va_start(va, fmt);
	vfprintf(stderr, fmt, va);
	va_end(va);
}

log_t hashlog = hash_logger_stderr;

static inline void hashlog_perror(char *str) {
        hashlog(LOG_ERR, "%s: %s", str, strerror(errno));
}

static inline void hashlog_err(char *str) {
        hashlog_perror(str);
        exit(EXIT_FAILURE);
}

void hash_set_logger(log_t logger) {
	hashlog = logger;
}

int
hash_put(unsigned hd, void *key_r, size_t key_len, void *value, size_t value_len)
{
	DB *db = hash_dbs[hd];
	DBT key, data;
	int ret;

	if (qdb_meta[hd].flags & QH_AINDEX) {
		unsigned id = * (unsigned *) key_r, last;
		if (qdb_meta[hd].idm.last <= id)
			qdb_meta[hd].idm.last = id + 1;
		for (last = qdb_meta[hd].idm.last; last < id; last++)
			idml_push(&qdb_meta[hd].idm.free, last);
	}

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = (void *) key_r;
	key.size = key_len;
	data.data = value;
	data.size = value_len;
	data.flags = DB_DBT_MALLOC;
	int dupes;
	u_int32_t flags;

	if (db->get_flags(db, &flags))
		hashlog_err("hash_put get_flags");
	dupes = flags & DB_DUP;

	ret = db->put(db, hd && (qdb_meta[hd].flags & QH_TXN) ? txnid : NULL, &key, &data, 0);
	if (ret && (ret != DB_KEYEXIST || !dupes))
		hashlog(LOG_WARNING, "hash_put");
	return ret;
}

int qdb_put(unsigned hd, void *key, void *value) {
	return hash_put(hd, key, QDB_LEN(hd, key, key), value, QDB_LEN(hd, value, value));
}

unsigned
qdb_initc(const char *file, const char *database, int mode, int flags, int type, qdb_type_t *key_type, qdb_type_t *value_type)
{
	DB *db;
	unsigned id, dbflags = 0;

	if (hash_first) {
		idm = idm_init();
		idm_new(&idm);

		if (db_create(&hash_dbs[0], NULL, 0))
			hashlog_err("qdb_initc: db_create ids_db");

		if (hash_dbs[0]->open(hash_dbs[0], NULL, NULL, NULL, DB_HASH, DB_CREATE, 644))
			hashlog_err("qdb_initc: open ids_db");

		memset(qdb_meta, 0, sizeof(qdb_meta));
	}

	hash_first = 0;
	id = idm_new(&idm);
	qdb_meta[id].flags = flags;
	qdb_meta[id].key = key_type;
	qdb_meta[id].value = value_type;

	if (db_create(&hash_dbs[id], env, 0))
		hashlog_err("qdb_initc: db_create");

	// this is needed for associations
	db = hash_dbs[id];
	hash_put(0, &db, sizeof(DB *), &id, sizeof(unsigned));

	if (flags & QH_DUP)
		if (db->set_flags(db, DB_DUP))
			hashlog_err("qdb_initc: set_flags");

	dbflags = (env ? DB_THREAD : 0) | (flags & QH_RDONLY ? DB_RDONLY : DB_CREATE);
	if (db->open(db, (flags & QH_TXN) ? txnid : NULL, file, database, flags & DB_RDONLY ? DB_UNKNOWN : type, dbflags, mode))
		hashlog_err("qdb_initc: open");

	return id;
}

void * __hash_get(unsigned hd, size_t *size, void *key_r, size_t key_len)
{
	DB *db = hash_dbs[hd];
	DBT key, data;
	int ret;

	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));

	key.data = (void *) key_r;
	key.size = key_len;
	data.flags = DB_DBT_MALLOC;

	ret = db->get(db, hd && (qdb_meta[hd].flags & QH_TXN) ? txnid : NULL, &key, &data, 0);

	if (ret == DB_NOTFOUND)
		return NULL;
	else if (ret)
		hashlog_err("hash_get");

	*size = data.size;
	return data.data;
}

int hash_get(unsigned hd, void *value_r, void *key_r, size_t key_len)
{
	size_t size;
	void *value = __hash_get(hd, &size, key_r, key_len);

	if (!value)
		return 1;

	memcpy(value_r, value, size);
	return 0;
}

int hash_exists(unsigned hd, void *key_r, size_t key_len)
{
	size_t size;

	if (__hash_get(hd, &size, key_r, key_len))
		return 1;

	return 0;
}

int hash_pget(unsigned hd, void *pkey_r, void *key_r, size_t key_len) {
	DB *db = hash_dbs[hd];
	DBT key, pkey, data;
	int ret;

	memset(&key, 0, sizeof(key));
	memset(&pkey, 0, sizeof(pkey));
	memset(&data, 0, sizeof(data));

	key.data = (void *) key_r;
	key.size = key_len;
	pkey.flags = data.flags = DB_DBT_MALLOC;

	ret = db->pget(db, (qdb_meta[hd].flags & QH_TXN) ? txnid : NULL, &key, &pkey, &data, 0);

	if (ret == DB_NOTFOUND)
		return 1;
	else if (ret)
		hashlog_err("hash_get");

	memcpy(pkey_r, pkey.data, pkey.size);
	return 0;
}

int
map_assoc(DB *sec, const DBT *key, const DBT *data, DBT *result)
{
	memset(result, 0, sizeof(DBT));
	result->flags = DB_DBT_MALLOC;
	unsigned hd;
	hash_get(0, &hd, &sec, sizeof(DB *)); // assumed == 0
	qdb_meta[hd].assoc(&result->data, &result->size, key->data, data->data);
	return 0;
}

void
qdb_assoc(unsigned hd, unsigned link, qdb_assoc_t cb)
{
	DB *db = hash_dbs[hd];
	DB *ldb = hash_dbs[link];
	qdb_meta[hd].assoc = cb;
	qdb_meta[hd].flags |= QH_SEC;

	if (ldb->associate(ldb, (qdb_meta[hd].flags & QH_TXN) ? txnid : NULL, db, map_assoc, DB_CREATE | DB_IMMUTABLE_KEY))
		hashlog_err("qdb_assoc");
}

int
hash_vdel(unsigned hd, void *key_data, size_t key_size, void *value_data, size_t value_size)
{
	DB *db = hash_dbs[hd];
	DBT key, data;
	DBC *cursor;
	int ret, flags = DB_SET;

	if ((ret = db->cursor(db, hd && (qdb_meta[hd].flags & QH_TXN) ? txnid : NULL, &cursor, 0)) != 0) {
		hashlog(LOG_ERR, "cursor: %s", db_strerror(ret));
		return ret;
	}

	memset(&key, 0, sizeof(DBT));
	key.data = key_data;
	key.size = key_size;
	memset(&data, 0, sizeof(DBT));
	data.flags = DB_DBT_MALLOC;
	data.data = value_data;
	data.size = value_size;

	while (!(ret = cursor->get(cursor, &key, &data, flags))) {
		flags = DB_NEXT_DUP;

		if (data.size == value_size && memcmp(data.data, value_data, value_size) == 0) {
			ret = cursor->del(cursor, 0);
			break;
		}
	}

	cursor->close(cursor);
	return ret == DB_NOTFOUND ? 0 : ret;
}

int qdb_rem(unsigned hd, void *key, void *value) {
	return hash_vdel(hd, key, QDB_LEN(hd, key, key), value, QDB_LEN(hd, value, value));
}

typedef int get_t(DBC *dbc, DBT *key, DBT *pkey, DBT *data, unsigned flags);

int primary_get(DBC *dbc, DBT *key __attribute__((unused)), DBT *pkey, DBT *data, unsigned flags) {
	return dbc->get(dbc, pkey, data, flags);
}

struct hash_internal {
	unsigned hd;
	DBT data, key, pkey;
	DBC *cursor;
	get_t *get;
};

qdb_cur_t
hash_iter(unsigned hd, void *key, size_t key_len) {
	DB *db = hash_dbs[hd];
	qdb_cur_t cur;
	struct hash_internal *internal = malloc(sizeof(struct hash_internal));
	cur.internal = internal;
	internal->hd = hd;
	internal->cursor = NULL;
	db->cursor(db, hd && (qdb_meta[hd].flags & QH_TXN) ? txnid : NULL, &internal->cursor, 0);
	memset(&internal->pkey, 0, sizeof(DBT));
	memset(&internal->key, 0, sizeof(DBT));
	memset(&internal->data, 0, sizeof(DBT));
	internal->pkey.data = internal->key.data = key;
	internal->key.flags = internal->pkey.flags = internal->data.flags = DB_DBT_MALLOC;
	internal->pkey.size = internal->key.size = key_len;
	internal->get = (qdb_meta[hd].flags & QH_SEC)
		? internal->cursor->pget
		: primary_get;
	cur.flags = 0;
	if (key)
		cur.flags = QH_DUP;
	return cur;
}

void qdb_fin(qdb_cur_t *cur) {
	struct hash_internal *internal = cur->internal;
	internal->cursor->close(internal->cursor);
	free(internal);
	cur->internal = NULL;
}

int
qdb_next(void *key, void *value, qdb_cur_t *cur)
{
	struct hash_internal *internal = cur->internal;
	ssize_t ret;
	int flags = DB_FIRST;
	if (cur->flags & QH_DUP) {
		if (cur->flags & QH_NOT_FIRST)
			flags = DB_NEXT_DUP;
		else
			flags = DB_SET;
	} else if (cur->flags & QH_NOT_FIRST)
		flags = DB_NEXT;
	cur->flags |= QH_NOT_FIRST;

	if ((ret = internal->get(internal->cursor, &internal->key, &internal->pkey, &internal->data, flags))) {
		if (ret != DB_NOTFOUND)
			hashlog(LOG_ERR, "qdb_next: %u %d %s", internal->hd, cur->flags, db_strerror(ret));
		internal->cursor->close(internal->cursor);
		free(internal);
		cur->internal = NULL;
		return 0;
	}

	if ((qdb_meta[internal->hd].flags & QH_AINDEX)
			&& * (unsigned *) key == (unsigned) -2)
		return 0;

	if (key) {
		memcpy(key, internal->pkey.data, internal->pkey.size);
		memcpy(value, internal->data.data, internal->data.size);
	}

	memset(&internal->key, 0, sizeof(DBT));
	memset(&internal->pkey, 0, sizeof(DBT));
	memset(&internal->data, 0, sizeof(DBT));
	return 1;
}

int
hash_cdel(qdb_cur_t *cur) {
	struct hash_internal *internal = cur->internal;
	return internal->cursor->c_del(internal->cursor, 0);
}

int qdb_drop(unsigned hd) {
	DB *db = hash_dbs[hd];
	DBT key, data;
	DBC *cursor;
	int ret, flags = DB_FIRST;

	if ((ret = db->cursor(db, (qdb_meta[hd].flags & QH_TXN) ? txnid : NULL, &cursor, 0)) != 0) {
		hashlog(LOG_ERR, "qdb_drop: cursor: %s\n", db_strerror(ret));
		return ret;
	}

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	data.flags = DB_DBT_MALLOC;

	while (!(cursor->get(cursor, &key, &data, flags))) {
		flags = DB_NEXT;
		cursor->del(cursor, 0);
	}

	cursor->close(cursor);
	return 0;
}

void
qdb_close(unsigned hd, unsigned flags) {
	DB *db = hash_dbs[hd];
	db->close(db, flags);
	idm_del(&idm, hd);
}

void
qdb_sync(unsigned hd) {
	DB *db = hash_dbs[hd];
	db->sync(db, 0);
}

unsigned idm_new(struct idm *idm) {
	unsigned ret = idml_pop(&idm->free);

	if (ret == (unsigned) -1)
		return idm->last++;

	return ret;
}

unsigned lhash_cinit(size_t item_len, const char *file, const char *database, int mode, unsigned flags) {
	unsigned hd = qdb_initc(file, database, mode, flags, DB_HASH, &qdb_unsigned, NULL), last = 0;
	unsigned ign;
	char buf[item_len];
	qdb_meta[hd].len = item_len;
	qdb_meta[hd].flags |= QH_AINDEX;
	SLIST_INIT(&qdb_meta[hd].idm.free);
	qdb_cur_t c = qdb_iter(hd, NULL);

	while (qdb_next(&ign, buf, &c))
		if (ign >= last)
			last = ign + 1;

	qdb_meta[hd].idm.last = last;
	for (last = 0; last < qdb_meta[hd].idm.last; last++) {
		size_t size;
		void *value = __hash_get(hd, &size, &last, sizeof(unsigned));
		if (!value)
			idml_push(&qdb_meta[hd].idm.free, last);
	}

	return hd;
}

static unsigned lh_len(unsigned hd, char *item) {
	size_t len = qdb_meta[hd].len;

	if (len != 0)
		return len;

	return strlen(item) + 1;
}

unsigned qdb_new(unsigned hd, void *item) {
	unsigned id = idm_new(&qdb_meta[hd].idm);
	qdb_put(hd, &id, item);
	return id;
}

void qdb_env_set(void *value) {
	env = (DB_ENV *) value;
}

void *qdb_env_pop(void) {
	DB_ENV *ret = env;
	env = NULL;
	return ret;
}
