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

enum qhash_priv_flags {
	QH_NOT_FIRST = 1,
};

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
hash_cinit(const char *file, const char *database, int mode, int flags)
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
	if (db_create(db, NULL, 0))
		err(1, "hash_init: db_create");

	if (flags & QH_DUP)
		if ((*db)->set_flags(*db, DB_DUPSORT))
			err(1, "hash_init: set_flags");

	if ((*db)->open(*db, txnid, file, database, DB_HASH, DB_CREATE, mode))
		err(1, "hash_init");

	if (!new_id)
		hash_n++;
	return id;
}

unsigned
hash_init()
{
	return hash_cinit(NULL, NULL, 0644, 0);
}

void
hash_cput(unsigned hd, void *key_r, size_t key_len, void *value, size_t value_len)
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
	int flags, dupes;

	db->get_flags(db, &flags);
	dupes = flags & DB_DUP;

	ret = db->put(db, txnid, &key, &data, 0);
	if (ret && (ret != DB_KEYEXIST || !dupes))
		err(1, "hash_put");
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
	void *ret = _hash_cget(hd, &value_len, key_r, key_len);
	if (!ret)
		return NULL;
	return ret;
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
		hash_sput(hd, *t, *t + strlen(*t) + 1);
}

struct hash_internal {
	unsigned hd;
	DB *db;
	DBT data, key;
	DBC *cursor;
};

struct hash_cursor
hash_citer(unsigned hd, void *key, size_t key_len) {
	struct hash_cursor cur;
	struct hash_internal *internal = malloc(sizeof(struct hash_internal));
	cur.internal = internal;
	internal->hd = hd;
	internal->db = hash_dbs[hd];
	internal->cursor = NULL;
	internal->db->cursor(internal->db, NULL, &internal->cursor, 0);
	memset(&internal->key, 0, sizeof(DBT));
	memset(&internal->data, 0, sizeof(DBT));
	internal->key.data = key;
	internal->key.size = key_len;
	cur.flags = 0;
	if (key)
		cur.flags = QH_DUP;
	return cur;
}

struct hash_cursor
hash_iter(unsigned hd) {
	return hash_citer(hd, NULL, 0);
}

ssize_t
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
		ret = internal->key.size;
		memcpy(key, internal->key.data, internal->key.size);
		memcpy(value, internal->data.data, internal->data.size);
		memset(&internal->key, 0, sizeof(DBT));
		memset(&internal->data, 0, sizeof(DBT));
		return ret;
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
