#include "./include/qhash.h"
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <syslog.h>

#define HASH_DBS_MAX (64 * 512)
#define HASH_NOT_FOUND NULL

enum qhash_priv_flags {
	QH_NOT_FIRST = 1,
};

struct meta {
	size_t len;
	int flags;
	assoc_t assoc;
	struct idm idm;
};

static DB *hash_dbs[HASH_DBS_MAX];
struct meta meta[HASH_DBS_MAX];

static struct idm idm;

static int hash_first = 1;
DB_ENV *env;

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

	ret = db->put(db, hd && (meta[hd].flags & QH_TXN) ? txnid : NULL, &key, &data, 0);
	if (ret && (ret != DB_KEYEXIST || !dupes))
		hashlog(LOG_WARNING, "hash_put");
	return ret;
}

unsigned
hash_cinit(const char *file, const char *database, int mode, int flags)
{
	DB *db;
	unsigned id, dbflags = 0;

	if (hash_first) {
		idm = idm_init();
		idm_new(&idm);

		if (db_create(&hash_dbs[0], NULL, 0))
			hashlog_err("hash_cinit: db_create ids_db");

		if (hash_dbs[0]->open(hash_dbs[0], NULL, NULL, NULL, DB_HASH, DB_CREATE, 644))
			hashlog_err("hash_cinit: open ids_db");

		memset(meta, 0, sizeof(meta));
	}

	hash_first = 0;
	id = idm_new(&idm);
	meta[id].flags = flags;

	if (db_create(&hash_dbs[id], env, 0))
		hashlog_err("hash_cinit: db_create");

	// this is needed for associations
	db = hash_dbs[id];
	hash_put(0, &db, sizeof(DB *), &id, sizeof(unsigned));

	if (flags & QH_DUP)
		if (db->set_flags(db, DB_DUP))
			hashlog_err("hash_cinit: set_flags");

	dbflags = 0;
	if (flags & QH_RDONLY)
		dbflags |= DB_RDONLY;
	else
		dbflags |= DB_CREATE | DB_THREAD;
	if (db->open(db, (flags & QH_TXN) ? txnid : NULL, file, database, DB_HASH, dbflags, mode))
		hashlog_err("hash_cinit: open");

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

	ret = db->get(db, hd && (meta[hd].flags & QH_TXN) ? txnid : NULL, &key, &data, 0);

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

	ret = db->pget(db, (meta[hd].flags & QH_TXN) ? txnid : NULL, &key, &pkey, &data, 0);

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
	meta[hd].assoc(&result->data, &result->size, key->data, data->data);
	return 0;
}

void
hash_assoc(unsigned hd, unsigned link, assoc_t cb)
{
	DB *db = hash_dbs[hd];
	DB *ldb = hash_dbs[link];
	meta[hd].assoc = cb;
	meta[hd].flags |= QH_SEC;

	if (ldb->associate(ldb, (meta[hd].flags & QH_TXN) ? txnid : NULL, db, map_assoc, DB_CREATE | DB_IMMUTABLE_KEY))
		hashlog_err("hash_assoc");
}

int
hash_vdel(unsigned hd, void *key_data, size_t key_size, void *value_data, size_t value_size)
{
	DB *db = hash_dbs[hd];
	DBT key, data;
	DBC *cursor;
	int ret, flags = DB_SET;

	if ((ret = db->cursor(db, hd && (meta[hd].flags & QH_TXN) ? txnid : NULL, &cursor, 0)) != 0) {
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

struct hash_cursor
hash_iter(unsigned hd, void *key, size_t key_len) {
	DB *db = hash_dbs[hd];
	struct hash_cursor cur;
	struct hash_internal *internal = malloc(sizeof(struct hash_internal));
	cur.internal = internal;
	internal->hd = hd;
	internal->cursor = NULL;
	db->cursor(db, hd && (meta[hd].flags & QH_TXN) ? txnid : NULL, &internal->cursor, 0);
	memset(&internal->pkey, 0, sizeof(DBT));
	memset(&internal->key, 0, sizeof(DBT));
	memset(&internal->data, 0, sizeof(DBT));
	internal->pkey.data = internal->key.data = key;
	internal->key.flags = internal->pkey.flags = internal->data.flags = DB_DBT_MALLOC;
	internal->pkey.size = internal->key.size = key_len;
	internal->get = (meta[hd].flags & QH_SEC)
		? internal->cursor->pget
		: primary_get;
	cur.flags = 0;
	if (key)
		cur.flags = QH_DUP;
	return cur;
}

void hash_fin(struct hash_cursor *cur) {
	struct hash_internal *internal = cur->internal;
	internal->cursor->close(internal->cursor);
	free(internal);
	cur->internal = NULL;
}

int
hash_next(void *key, void *value, struct hash_cursor *cur)
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
			hashlog(LOG_ERR, "hash_next: %u %d %s", internal->hd, cur->flags, db_strerror(ret));
		internal->cursor->close(internal->cursor);
		free(internal);
		cur->internal = NULL;
		return 0;
	} else if (key) {
		memcpy(key, internal->pkey.data, internal->pkey.size);
		memcpy(value, internal->data.data, internal->data.size);
	}

	memset(&internal->key, 0, sizeof(DBT));
	memset(&internal->pkey, 0, sizeof(DBT));
	memset(&internal->data, 0, sizeof(DBT));
	return 1;
}

int
hash_cdel(struct hash_cursor *cur) {
	struct hash_internal *internal = cur->internal;
	return internal->cursor->c_del(internal->cursor, 0);
}

int hash_drop(unsigned hd) {
	DB *db = hash_dbs[hd];
	DBT key, data;
	DBC *cursor;
	int ret, flags = DB_FIRST;

	if ((ret = db->cursor(db, (meta[hd].flags & QH_TXN) ? txnid : NULL, &cursor, 0)) != 0) {
		hashlog(LOG_ERR, "hash_drop: cursor: %s\n", db_strerror(ret));
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
hash_close(unsigned hd, unsigned flags) {
	DB *db = hash_dbs[hd];
	db->close(db, flags);
	idm_del(&idm, hd);
}

void
hash_sync(unsigned hd) {
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
	unsigned hd = hash_cinit(file, database, mode, flags), last = 0;
	unsigned ign;
	char buf[item_len];
	meta[hd].len = item_len;
	SLIST_INIT(&meta[hd].idm.free);
	struct hash_cursor c = lhash_iter(hd);

	while (lhash_next(&ign, buf, &c))
		if (ign >= last)
			last = ign + 1;

	meta[hd].idm.last = last;
	for (last = 0; last < meta[hd].idm.last; last++) {
		size_t size;
		void *value = __hash_get(hd, &size, &last, sizeof(unsigned));
		if (!value)
			idml_push(&meta[hd].idm.free, last);
	}

	return hd;
}

static unsigned lh_len(unsigned hd, char *item) {
	size_t len = meta[hd].len;

	if (len != 0)
		return len;

	return strlen(item) + 1;
}

unsigned lhash_new(unsigned hd, void *item) {
	unsigned id = idm_new(&meta[hd].idm);
	uhash_put(hd, id, item, lh_len(hd, item));
	return id;
}

void lhash_del(unsigned hd, unsigned ref) {
	idm_del(&meta[hd].idm, ref);
	uhash_del(hd, ref);
}

int lhash_put(unsigned hd, unsigned id, void *source) {
	unsigned last;
	if (meta[hd].idm.last <= id)
		meta[hd].idm.last = id + 1;
	for (last = meta[hd].idm.last; last < id; last++)
		idml_push(&meta[hd].idm.free, last);
	return uhash_put(hd, id, source, lh_len(hd, source));
}

void hash_env_set(void *value) {
	env = (DB_ENV *) value;
}

void *hash_env_pop(void) {
	DB_ENV *ret = env;
	env = NULL;
	return ret;
}
