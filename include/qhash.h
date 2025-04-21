#ifndef QHASH_H
#define QHASH_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <sys/types.h>
#ifdef __OpenBSD__
#include <db4/db.h>
#else
#include <db.h>
#endif

#define fhash_iter(hd, thing) hash_iter(hd, &thing, sizeof(thing))
extern DB_TXN *txnid;

#define FILO(name, TYPE) \
	struct name ## _item { \
		TYPE value; \
		SLIST_ENTRY(name ## _item) entry; \
	}; \
	SLIST_HEAD(name, name ## _item); \
	static inline struct name name ## _init(void) { \
		struct name list;\
		SLIST_INIT(&list);\
		return list;\
	}\
	static inline void name ## _push(struct name *list, TYPE id) { \
		struct name ## _item *item = (struct name ## _item *) \
			malloc(sizeof(struct name ## _item)); \
		item->value = id; \
		SLIST_INSERT_HEAD(list, item, entry); \
	} \
	static inline TYPE name ## _pop(struct name *list) { \
		struct name ## _item *popped = SLIST_FIRST(list); \
		if (!popped) return -1; \
		TYPE ret = popped->value; \
		SLIST_REMOVE_HEAD(list, entry); \
		free(popped); \
		return ret; \
	} \
	static inline struct name ## _item *name ## _iter(struct name *list) { \
		return SLIST_FIRST(list); \
	} \
	static inline struct name ## _item *name ## _next(TYPE *id, struct name ## _item *last) { \
		*id = last->value; \
		return SLIST_NEXT(last, entry); \
	} \
	static inline void name ## _drop(struct name *list) { \
		while (!name ## _pop(list)); \
	}

#define QDB_LEN(hd, MEMBER, ptr) (qdb_meta[hd].MEMBER->measure ? qdb_meta[hd].MEMBER->measure(ptr) : qdb_meta[hd].MEMBER->len)
#define HASH_DBS_MAX (64 * 512)

typedef void qdb_print_t(void *value);
typedef size_t qdb_measure_t(void *value);

typedef struct {
	qdb_print_t *print;
	qdb_measure_t *measure;
	size_t len;
} qdb_type_t;

FILO(idml, unsigned)

struct idm {
	struct idml free;
	unsigned last;
};

typedef void (*qdb_assoc_t)(void **data, uint32_t *len, void *key, void *value);

typedef struct meta {
	size_t len;
	int flags;
	qdb_assoc_t assoc;
	struct idm idm;
	qdb_type_t *key, *value;
} qdb_meta_t;

qdb_meta_t qdb_meta[HASH_DBS_MAX];

void u_print(void *value) {
	printf("%u", * (unsigned *) value);
}

void s_print(void *value) {
	printf("%s", (char *) value);
}

size_t s_measure(void *value) {
	return value ? strlen(value) + 1 : 0;
}

qdb_type_t qdb_string = {
	.print = s_print,
	.measure = s_measure,
}, qdb_unsigned = {
	.print = u_print,
	.len = sizeof(unsigned),
}, qdb_ptr = {
	.print = u_print,
	.len = sizeof(void *),
}, qdb_unsigned_pair = {
	.len = sizeof(unsigned) * 2,
};

static struct hash_config {
	int mode, flags;
	DBTYPE type;
	char *file;
} hash_config = { .mode = 0644, .flags = 0, .type = DB_HASH, .file = NULL };

typedef void (*log_t)(int type, const char *fmt, ...);

void hash_set_logger(log_t logger);

/* initialize an id management unit */
static inline struct idm idm_init(void) {
	struct idm idm;
	idm.free = idml_init();
	idm.last = 0;
	return idm;
}

/* delete an id from an idm */
static inline void idm_del(struct idm *idm, unsigned id) {
	if (id + 1 == idm->last)
		idm->last--;
	else
		idml_push(&idm->free, id);
}

/* get a new id from an idm */
unsigned idm_new(struct idm *idm);

/* HASH TABLE */

enum qhash_flags {
	QH_DUP = 2,
	QH_SEC = 4, // secondary (internal)
	QH_TXN = 8, // transaction support
	QH_RDONLY = 16, // read only mode
	QH_AINDEX = 32, // auto index db (internal - implies unsigned key)
};

typedef struct hash_cursor {
	int flags;
	void *internal;
} qdb_cur_t;

unsigned qdb_initc(const char *file, const char *database, int mode, int flags, int type, qdb_type_t *key_type, qdb_type_t *value_type);
int qdb_put(unsigned hd, void *key, void *value);
int qdb_rem(unsigned hd, void *key, void *value);
void qdb_fin(qdb_cur_t *cur);
int qdb_next(void *key, void *value, struct hash_cursor *cur);
void qdb_close(unsigned hd, unsigned flags);
void qdb_sync(unsigned hd);
void qdb_assoc(unsigned hd, unsigned link, qdb_assoc_t assoc);
int qdb_drop(unsigned hd);

unsigned qdb_new(unsigned hd, void *item);

/* initialize a hash (generic) */
__attribute__((deprecated))
static inline unsigned hash_cinit(const char *file, const char *database, int mode, int flags, int type, qdb_type_t *key_type, qdb_type_t *value_type) {
	return qdb_initc(file, database, mode, flags, type, key_type, value_type);
}

/* associate as a secondary DB */
__attribute__((deprecated))
static inline void hash_assoc(unsigned hd, unsigned link, qdb_assoc_t assoc) {
	qdb_assoc(hd, link, assoc);
}

/* put a value into an hash */
__attribute__((deprecated))
int hash_put(unsigned hd, void *key, size_t key_len, void *value, size_t value_len);

/* get a value from a hash */
__attribute__((deprecated))
int hash_get(unsigned hd, void *value, void *key, size_t key_len);

/* check if hash key exists */
__attribute__((deprecated))
int hash_exists(unsigned hd, void *key, size_t key_len);

// TODO major breaking changes
static inline int qdb_exists(unsigned hd, void *key) {
	return hash_exists(hd, key, QDB_LEN(hd, key, key));
}

/* get a primary key from a secondary hash key */
__attribute__((deprecated))
int hash_pget(unsigned hd, void *pkey, void *key, size_t key_len);

// TODO major breaking changes
static inline int qdb_pget(unsigned hd, void *pkey, void *key) {
	return hash_pget(hd, pkey, key, QDB_LEN(hd, key, key));
}

/* drop the values from a hash */
__attribute__((deprecated))
static inline int hash_drop(unsigned hd) {
	return qdb_drop(hd);
}

/* delete a certain value from a hash that supports dupes */
int hash_vdel(unsigned hd, void *key_data, size_t key_size, void *value_data, size_t value_size);

/* start iterating on a hash (maybe within a certain key) */
__attribute__((deprecated))
struct hash_cursor hash_iter(unsigned hd, void *key, size_t key_len);

// TODO major breaking changes
static inline qdb_cur_t qdb_iter(unsigned hd, char *key) {
	return hash_iter(hd, key, QDB_LEN(hd, key, key));
}


/* close a hash */
__attribute__((deprecated))
static inline void hash_close(unsigned hd, unsigned flags) {
	qdb_close(hd, flags);
}


/* get next value from hash iteration */
__attribute__((deprecated))
static inline int hash_next(void *key, void *value, struct hash_cursor *cur) {
	return qdb_next(key, value, cur);
}

/* delete value under cursor */
int hash_cdel(struct hash_cursor *cur);

/* delete a value from an hash */
static inline void qdb_del(unsigned hd, void *key) {
	if (qdb_meta[hd].flags & QH_AINDEX)
		idm_del(&qdb_meta[hd].idm, * (unsigned *) key);

	qdb_cur_t c = qdb_iter(hd, key);

	while (qdb_next(NULL, NULL, &c))
		hash_cdel(&c);
}

__attribute__((deprecated))
static inline void hash_del(unsigned hd, void *key_r, size_t len __attribute__((unused))) {
	qdb_del(hd, key_r);
}

/* finalize iteration if exiting it earlier */
__attribute__((deprecated))
static inline void hash_fin(qdb_cur_t *cur) {
	qdb_fin(cur);
}

/* write a hash to disk */
__attribute__((deprecated))
static inline void hash_sync(unsigned hd) {
	qdb_sync(hd);
}

/* initialize an memory-only database */
__attribute__((deprecated))
static inline unsigned hash_init(char *database) {
	return qdb_initc(hash_config.file, database, hash_config.mode, hash_config.flags, hash_config.type, NULL, NULL);
}

/* NEW API for later simplification QDB with generic types and still simple API */

static inline int qdb_init(char *database, qdb_type_t *key_type, qdb_type_t *value_type) {
	return qdb_initc(hash_config.file, database, hash_config.mode,
			hash_config.flags, hash_config.type, key_type, value_type);
}

static inline unsigned qdb_ainitc(char *fname, char *dbname, int mode, unsigned flags) {
	return qdb_initc(fname, dbname, mode, QH_DUP | flags, DB_HASH, &qdb_unsigned, &qdb_unsigned);
}

static inline unsigned qdb_ainit(char *database) {
	return qdb_ainitc(hash_config.file, database, hash_config.mode, hash_config.flags);
}

static inline int qdb_get(unsigned hd, void *target, void *key) {
	return hash_get(hd, target, key, QDB_LEN(hd, key, key));
}

/*
 * UNSIGNED TO ANYTHING HASH TABLE (UHASH)
 */

/* get an item from an uhash */
__attribute__((deprecated))
static inline int uhash_get(unsigned hd, void *target, unsigned ref) {
	return hash_get(hd, target, &ref, sizeof(ref));
}

/* check if uhash key exists */
__attribute__((deprecated))
static inline int uhash_exists(unsigned hd, unsigned ref) {
	return qdb_exists(hd, &ref);
}

/* get a primary key from a secondary hash key */
__attribute__((deprecated))
static inline int uhash_pget(unsigned hd, void *pkey, unsigned ref) {
	return qdb_pget(hd, pkey, &ref);
}

/* set a uhash key's value */
__attribute__((deprecated))
static inline int uhash_put(unsigned hd, unsigned ref, void *value, unsigned value_len __attribute__((unused))) {
	return qdb_put(hd, &ref, value);
}

/* delete a value from an uhash */
__attribute__((deprecated))
static inline void uhash_del(unsigned hd, unsigned key) {
	qdb_del(hd, &key);
}

/* delete a certain value from a uhash that supports dupes */
__attribute__((deprecated)) static inline int
uhash_vdel(unsigned hd, unsigned key, void *value_data, size_t value_size) {
	return hash_vdel(hd, &key, sizeof(key), value_data, value_size);
}

/*
 * MANAGED ID HASH TABLE (LHASH)
 * a hash table with items of fixed size
 * that has ids managed automatically
 * 1-1
 */

/* initialize a lhash (generic) */
__attribute__((unused))
unsigned lhash_cinit(size_t item_len, const char *file, const char *database, int mode, unsigned flags);

/* initialize a memory-only lhash */
__attribute__((unused))
static inline unsigned lhash_init(size_t item_len, char *database) {
	return lhash_cinit(item_len, hash_config.file, database, hash_config.mode, hash_config.flags);
}

unsigned qdb_linitc(const char *file, const char *database, int mode, unsigned flags, qdb_type_t *value_type) {
	return lhash_cinit(value_type->len, file, database, mode, flags);
}

/* final version */
static inline unsigned qdb_linit(char *database, qdb_type_t *value) {
	return lhash_cinit(value->len, hash_config.file, database, hash_config.mode, hash_config.flags);
}

/* add an item to a lhash */
__attribute__((deprecated))
unsigned lhash_new(unsigned hd, void *item) {
	return qdb_new(hd, item);
}

/* get an item from an lhash */
__attribute__((deprecated))
static inline int lhash_get(unsigned hd, void *target, unsigned ref) {
	return qdb_get(hd, target, &ref);
}

/* set an lhash item */
__attribute__((deprecated)) static inline
int lhash_put(unsigned hd, unsigned ref, void *source) {
	return qdb_put(hd, &ref, source);
}

/* delete lhash item */
__attribute__((deprecated))
static inline void lhash_del(unsigned hd, unsigned ref) {
	qdb_del(hd, &ref);
}

/* start iterating through lhash */
__attribute__((deprecated))
static inline struct hash_cursor lhash_iter(unsigned hd) {
	return qdb_iter(hd, NULL);
}

/* iterate through lhash */
__attribute__((deprecated))
static inline int lhash_next(unsigned *key, void *value, struct hash_cursor *cur) {
	return qdb_next(key, value, cur);
}

/*
 * ASSOCIATION HASH TABLE (AHASH) METHODS
 * lhash unsigned to unsigned tables can be used for associations
 * these methods help out with those. These are memory-only,
 * as associations can be stored via a reference from item to container.
 * these have duplicates by default.
 */

/* initialize ahash */
__attribute__((deprecated))
static inline unsigned ahash_cinit(char *fname, char *dbname, int mode, unsigned flags) {
	return qdb_ainitc(fname, dbname, mode, flags);
}

/* initialize ahash */
__attribute__((deprecated))
static inline unsigned ahash_init(char *database) {
	return qdb_ainit(database);
}

/* add an association */
__attribute__((deprecated))
static inline int ahash_add(unsigned ahd, unsigned container, unsigned item) {
	return qdb_put(ahd, &container, &item);
}

/* remove an association */
__attribute__((deprecated))
static inline void ahash_remove(unsigned ahd, unsigned container, unsigned item) {
	qdb_rem(ahd, &container, &item);
}

/* get next value from hash iteration */
__attribute__((deprecated))
static inline int ahash_next(void *value, qdb_cur_t *cur) {
	unsigned ign;
	return qdb_next(&ign, value, cur);
}

/*
 * STRING TO ANYTHING HASH TABLE (SHASH)
 * this can be used to assocatie a string with anything
 * it mostly just avoids having to call strlen manually
 * memory-only, 1-1
 */

/* put a value into a shash */
__attribute__((deprecated))
static inline int shash_put(unsigned hd, char *key, void *value, size_t value_len) {
	return qdb_put(hd, key, value);
}

/* get a value from an shash */
static inline ssize_t shash_get(unsigned hd, void *value, char *key) {
	return hash_get(hd, value, key, strlen(key) + 1);
}

/* get a primary key from a secondary hash key */
__attribute__((deprecated))
static inline int shash_pget(unsigned hd, void *pkey, char *ref) {
	return hash_pget(hd, pkey, ref, strlen(ref) + 1);
}

/* check if shash key exists */
__attribute__((deprecated))
static inline int shash_exists(unsigned hd, char *key) {
	return qdb_exists(hd, key);
}

/* delete a value from an shash */
__attribute__((deprecated))
static inline void shash_del(unsigned hd, char *key) {
	qdb_del(hd, key);
}

/* start iterating on a hash table (maybe within a certain key) */
__attribute__((deprecated))
static inline qdb_cur_t shash_iter(unsigned hd, char *key) {
	return qdb_iter(hd, key);
}

/*
 * STRING TO UNSIGNED HASH TABLE (SUHASH)
 * this can be used to assocatie a string with an unsigned number,
 * which can be a lhash item, or another hashtable.
 * memory-only, 1-1
 */

/* put a value into a shash */
__attribute__((deprecated))
static inline int suhash_put(unsigned hd, char *key, unsigned value) {
	return qdb_put(hd, key, &value);
}

/*
 * STRING TO POINTER HASH TABLE (SPHASH)
 * this can be used to assocatie a string with a pointer,
 * memory-only, 1-1
 */

/* put a value into a shash */
__attribute__((deprecated))
static inline int sphash_put(unsigned hd, char *key, void *value) {
	return qdb_put(hd, key, &value);
}

/*
 * UNSIGNED TO STRING HASH TABLE (USHASH)
 */

__attribute__((deprecated))
static inline int ushash_put(unsigned hd, unsigned key, char *value) {
	return qdb_put(hd, &key, value);
}

/*
 * STRING TO STRING HASH TABLE (SSHASH)
 */

__attribute__((deprecated))
static inline int sshash_put(unsigned hd, char *key, char *value) {
	return qdb_put(hd, key, value);
}

/* AUTO TABLES */

/* initialize a string to string table \0 between key and value */
__attribute__((deprecated))
static inline void
shash_table(unsigned hd, char *table[]) {
	for (char **t = table; *t; t++)
		shash_put(hd, *t, *t + strlen(*t) + 1, sizeof(*t));
}

/* initialize a string to unsigned index table. */
__attribute__((deprecated))
static inline void
suhash_table(int hd, char *table[]) {
	for (unsigned i = 0; table[i]; i++) {
		char *str = table[i];
		size_t len = strlen(str);
		qdb_put(hd, str, &i);
		str += len + 1;
		if (*str)
			qdb_put(hd, str, &i);
	}
}

void qdb_env_set(void *value);
void *qdb_env_pop(void);

__attribute__((deprecated))
static inline void hash_env_set(void *value) {
	qdb_env_set(value);
}

__attribute__((deprecated))
static inline void *hash_env_pop(void) {
	return qdb_env_pop();
}

#endif
