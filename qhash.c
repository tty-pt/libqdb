#include "./include/qhash.h"
#include <err.h>
#include <string.h>
#include <sys/queue.h>
#include <stdlib.h>

#ifdef __OpenBSD__
#include <db4/db.h>
#else
#include <db.h>
#endif

#define HASH_DBS_MAX (64 * 512)
#define HASH_NOT_FOUND NULL

enum qhash_priv_flags {
	QH_NOT_FIRST = 1,
};

static DB *hash_dbs[HASH_DBS_MAX], *ids_db;
static assoc_t assoc[HASH_DBS_MAX];

static struct idm idm;

static unsigned hash_n = 0;
static int hash_first = 1;
void *txnid = NULL;

static inline void
_hash_put(DB *db, void *key_r, size_t key_len, void *value, size_t value_len)
{
	DBT key, data;
	int ret;

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = (void *) key_r;
	key.size = key_len;
	data.data = value;
	data.size = value_len;
	int flags, dupes;

	if (db->get_flags(db, &flags))
		err(1, "hash_put get_flags");
	dupes = flags & DB_DUP;

	ret = db->put(db, txnid, &key, &data, 0);
	if (ret && (ret != DB_KEYEXIST || !dupes))
		err(1, "hash_put");
}

unsigned
hash_cinit(const char *file, const char *database, int mode, int flags)
{
	DB *db;
	unsigned id;

	if (hash_first) {
		idm = idm_init();
		if (db_create(&ids_db, NULL, 0))
			err(1, "hash_init: db_create ids_db");

		if (ids_db->open(ids_db, txnid, NULL, NULL, DB_HASH, DB_CREATE, 644))
			err(1, "hash_init: open ids_db");
	}

	hash_first = 0;
	id = idm_new(&idm);

	if (db_create(&hash_dbs[id], NULL, 0))
		err(1, "hash_init: db_create");

	// this is needed for associations
	db = hash_dbs[id];
	_hash_put(ids_db, &db, sizeof(DB *), &id, sizeof(unsigned));

	if (flags & QH_DUP)
		if (db->set_flags(db, DB_DUPSORT))
			err(1, "hash_init: set_flags");

	if (db->open(db, txnid, file, database, DB_HASH, DB_CREATE, mode))
		err(1, "hash_init: open");

	return id;
}

void
hash_put(unsigned hd, void *key_r, size_t key_len, void *value, size_t value_len)
{
	DB *db = hash_dbs[hd];
	_hash_put(db, key_r, key_len, value, value_len);
}

static inline int _hash_get(DB *db, void *value_r, void *key_r, size_t key_len)
{
	DBT key, data;
	int ret;

	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));

	key.data = (void *) key_r;
	key.size = key_len;

	ret = db->get(db, txnid, &key, &data, 0);

	if (ret == DB_NOTFOUND)
		return 1;
	else if (ret)
		err(1, "hash_get");

	memcpy(value_r, data.data, data.size);
	return 0;
}

int hash_get(unsigned hd, void *value_r, void *key_r, size_t key_len)
{
	DB *db = hash_dbs[hd];
	return _hash_get(db, value_r, key_r, key_len);
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

	ret = db->pget(db, txnid, &key, &pkey, &data, 0);

	if (ret == DB_NOTFOUND)
		return 1;
	else if (ret)
		err(1, "hash_get");

	memcpy(pkey_r, pkey.data, pkey.size);
	return 0;
}

int
map_assoc(DB *sec, const DBT *key, const DBT *data, DBT *result)
{
	memset(result, 0, sizeof(DBT));
	unsigned hd;
	_hash_get(ids_db, &hd, &sec, sizeof(DB *)); // assumed == 0
	assoc[hd](&result->data, &result->size, key->data, data->data);
	return 0;
}

void
hash_assoc(unsigned hd, unsigned link, assoc_t cb)
{
	DB *db = hash_dbs[hd];
	DB *ldb = hash_dbs[link];
	assoc[hd] = cb;

	if (ldb->associate(ldb, NULL, db, map_assoc, DB_CREATE | DB_IMMUTABLE_KEY))
		err(1, "hash_assoc");
}

void
hash_del(unsigned hd, void *key_r, size_t len)
{
	DB *db = hash_dbs[hd];
	DBT key;

	memset(&key, 0, sizeof(key));
	key.data = key_r;
	key.size = len;

	if (db->del(db, txnid, &key, 0))
		err(1, "hash_del");
}

int
hash_vdel(unsigned hd, void *key_data, size_t key_size, void *value_data, size_t value_size)
{
	DB *db = hash_dbs[hd];
	DBT key, data;
	DBC *cursor;
	int ret, flags = DB_SET;

	if ((ret = db->cursor(db, txnid, &cursor, 0)) != 0) {
		fprintf(stderr, "cursor: %s\n", db_strerror(ret));
		return ret;
	}

	memset(&key, 0, sizeof(DBT));
	key.data = key_data;
	key.size = key_size;
	memset(&data, 0, sizeof(DBT));
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

void
shash_table(unsigned hd, char *table[]) {
	for (register char **t = table; *t; t++)
		hash_put(hd, *t, strlen(*t), *t + strlen(*t) + 1, sizeof(*t));
}

struct hash_internal {
	unsigned hd;
	DBT data, key;
	DBC *cursor;
};

struct hash_cursor
hash_iter(unsigned hd, void *key, size_t key_len) {
	struct hash_cursor cur;
	struct hash_internal *internal = malloc(sizeof(struct hash_internal));
	cur.internal = internal;
	internal->hd = hd;
	DB *db = hash_dbs[hd];
	internal->cursor = NULL;
	db->cursor(db, NULL, &internal->cursor, 0);
	memset(&internal->key, 0, sizeof(DBT));
	memset(&internal->data, 0, sizeof(DBT));
	internal->key.data = key;
	internal->key.size = key_len;
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
	if ((ret = internal->cursor->get(internal->cursor, &internal->key, &internal->data, flags))) {
		if (ret != DB_NOTFOUND)
			fprintf(stderr, "hash_cnext: %u %d %s\n", internal->hd, cur->flags, db_strerror(ret));
		internal->cursor->close(internal->cursor);
		free(internal);
		cur->internal = NULL;
		return 0;
	} else {
		memcpy(key, internal->key.data, internal->key.size);
		memset(&internal->key, 0, sizeof(DBT));
		memcpy(value, internal->data.data, internal->data.size);
		memset(&internal->data, 0, sizeof(DBT));
		return 1;
	}
}

int hash_drop(unsigned hd) {
	DB *db = hash_dbs[hd];
	DBT key, data;
	DBC *cursor;
	int ret, flags = DB_FIRST;

	if ((ret = db->cursor(db, txnid, &cursor, 0)) != 0) {
		fprintf(stderr, "hash_drop: cursor: %s\n", db_strerror(ret));
		return ret;
	}

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	while (!(cursor->get(cursor, &key, &data, flags))) {
		flags = DB_NEXT;
		cursor->del(cursor, 0);
	}

	cursor->close(cursor);
}
void
hash_close(unsigned hd) {
	DB *db = hash_dbs[hd];
	db->close(db, 0);
	idm_del(&idm, hd);
}

void
hash_sync(unsigned hd) {
	DB *db = hash_dbs[hd];
	db->sync(db, 0);
}

void idml_push(struct idm_list *list, unsigned id) {
	struct idm_item *item = malloc(sizeof(struct idm_item));
	item->value = id;
	SLIST_INSERT_HEAD(list, item, entry);
}

unsigned idml_pop(struct idm_list *list) {
	struct idm_item *popped = SLIST_FIRST(list);

	if (!popped)
		return -1;

	unsigned ret = popped->value;
	SLIST_REMOVE_HEAD(list, entry);
	free(popped);
	return ret;
}

unsigned idm_new(struct idm *idm) {
	unsigned ret = idml_pop(&idm->free);

	if (ret == (unsigned) -1)
		return idm->last++;

	return ret;
}
