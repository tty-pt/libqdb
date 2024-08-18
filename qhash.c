#include "qhash.h"
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

DB *hash_dbs[HASH_DBS_MAX];
struct free_id {
	unsigned hd;
	SLIST_ENTRY(free_id) entry;
};

SLIST_HEAD(free_list_head, free_id);
struct free_list_head free_list;

size_t hash_n = 0;
int hash_first = 1;
void *txnid = NULL;

unsigned
hash_cinit(const char *file, const char *database, int mode)
{
	DB **db;
	struct free_id *new_id = NULL;
	unsigned id = hash_n;

	if (hash_first) {
		SLIST_INIT(&free_list);
		hash_first = 0;
	} else {
		new_id = SLIST_FIRST(&free_list);
		if (new_id) {
			id = new_id->hd;
			SLIST_REMOVE_HEAD(&free_list, entry);
		}
	}

	/* fprintf(stderr, "qhash new! %d\n", id); */
	db = &hash_dbs[id];
	if (db_create(db, NULL, 0) || (*db)->open(*db, txnid, file, database, DB_HASH, DB_CREATE, mode))
		err(1, "hash_init");
	if (!new_id)
		hash_n++;
	return id;
}

unsigned
hash_init()
{
	return hash_cinit(NULL, NULL, 0644);
}

void
hash_cput(unsigned hd, void *key_r, size_t key_len, void *value, size_t value_len)
{
	DB *db = hash_dbs[hd];
	DBT key, data;

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = (void *) key_r;
	key.size = key_len;
	data.data = value;
	data.size = value_len;

	if (db->put(db, txnid, &key, &data, 0))
		err(1, "hash_put");
}

void
hash_put(unsigned hd, void *key_r, size_t key_len, void *value)
{
	hash_cput(hd, key_r, key_len, &value, sizeof(void *));
}

static void *
_hash_cget(unsigned hd, size_t *value_len, void *key_r, size_t key_len)
{
	DB *db = hash_dbs[hd];
	DBT key, data;
	int ret;

	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));

	key.data = (void *) key_r;
	key.size = key_len;

	ret = db->get(db, txnid, &key, &data, 0);

	if (ret == DB_NOTFOUND)
		return HASH_NOT_FOUND;
	else if (ret)
		err(1, "hash_get");

	*value_len = data.size;
	return data.data;
}

ssize_t
hash_cget(unsigned hd, void *value_r, void *key_r, size_t key_len)
{
	size_t value_len;
	void *value = _hash_cget(hd, &value_len, key_r, key_len);

	if (!value)
		return -1;

	memcpy(value_r, value, value_len);
	return value_len;
}

void *
hash_get(unsigned hd, void *key_r, size_t key_len)
{
	size_t value_len;
	void **ret = _hash_cget(hd, &value_len, key_r, key_len);
	if (!ret)
		return NULL;
	return *ret;
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

void
shash_table(unsigned hd, char *table[]) {
	for (register char **t = table; *t; t++)
		SHASH_PUT(hd, *t, *t + strlen(*t) + 1);
}

struct {
	DB *db;
	DBT data, key;
	DBC *cursor;
} c;

struct hash_cursor
hash_iter_start(unsigned hd) {
	struct hash_cursor cur;
	c.db = hash_dbs[hd];
	c.db->cursor(c.db, NULL, &c.cursor, 0);
	return cur;
}

void *
hash_iter_get(struct hash_cursor *cur)
{
	int ret;
	memset(&c.key, 0, sizeof(DBT));
	memset(&c.data, 0, sizeof(DBT));

	if ((ret = c.cursor->get(c.cursor, &c.key, &c.data, DB_NEXT))) {
		if (ret != DB_NOTFOUND)
			fprintf(stderr, "HASH_ITER: %s\n", db_strerror(ret));
		c.cursor->close(c.cursor);
		return NULL;
	} else {
		cur->key = c.key.data;
		cur->key_len = c.key.size;
		cur->data = * (void **) c.data.data;
		return &c;
	}
}

void *
hash_iter_cget(void *key, void *value, struct hash_cursor *cur)
{
	int ret;
	memset(&c.key, 0, sizeof(DBT));
	memset(&c.data, 0, sizeof(DBT));

	if ((ret = c.cursor->get(c.cursor, &c.key, &c.data, DB_NEXT))) {
		if (ret != DB_NOTFOUND)
			fprintf(stderr, "HASH_ITER: %s\n", db_strerror(ret));
		c.cursor->close(c.cursor);
		return NULL;
	} else {
		memcpy(key, c.key.data, c.key.size);
		memcpy(value, c.data.data, c.data.size);
		return &c;
	}
}

void
hash_close(unsigned hd) {
	DB *db = hash_dbs[hd];
	db->close(db, 0);
	if (hd == hash_n - 1)
		hash_n --;
	else {

		struct free_id *new_id = malloc(sizeof(struct free_id));
		new_id->hd = hd;
		SLIST_INSERT_HEAD(&free_list, new_id, entry);
	}
}

void
hash_sync(unsigned hd) {
	DB *db = hash_dbs[hd];
	db->sync(db, 0);
}
