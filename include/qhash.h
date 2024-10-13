#ifndef QHASH_H
#define QHASH_H

#include <stddef.h>
#include <sys/types.h>
#include <sys/queue.h>
#include <string.h>
#include <stdint.h>

#define fhash_iter(hd, thing) hash_iter(hd, &thing, sizeof(thing))

/* ID MANAGEMENT UNIT */

struct idm_item {
	unsigned value;
	SLIST_ENTRY(idm_item) entry;
};

SLIST_HEAD(idm_list, idm_item);

struct idm {
	struct idm_list free;
	unsigned last;
};

/* initialize id management list */
static inline struct idm_list idml_init() {
	struct idm_list idml;
	SLIST_INIT(&idml);
	return idml;
}

/* initialize an id management unit */
static inline struct idm idm_init() {
	struct idm idm;
	SLIST_INIT(&idm.free);
	idm.last = 0;
	return idm;
}

/* push an element into an idml */
void idml_push(struct idm_list *list, unsigned id);

/* delete an id from an idm */
static inline void idm_del(struct idm *idm, unsigned id) {
	if (id + 1 == idm->last)
		idm->last--;
	else
		idml_push(&idm->free, id);
}

/* pop unsigned from idml */
unsigned idml_pop(struct idm_list *list);

/* drop an idml */
static inline void idml_drop(struct idm_list *list) {
	while (!idml_pop(list));
}

/* get a new id from an idm */
unsigned idm_new(struct idm *idm);

/* HASH TABLE */

enum qhash_flags {
	QH_DUP = 2,
};

struct hash_cursor {
	int flags;
	void *internal;
};

typedef void (*assoc_t)(void **data, uint32_t *len, void *key, void *value);

/* initialize a hash (generic) */
unsigned hash_cinit(const char *file, const char *database, int mode, int flags);

/* associate as a secondary DB */
void hash_assoc(unsigned hd, unsigned link, assoc_t assoc);

/* put a value into an hash */
void hash_put(unsigned hd, void *key, size_t key_len, void *value, size_t value_len);

/* get a value from a hash */
int hash_get(unsigned hd, void *value, void *key, size_t key_len);

/* check if hash key exists */
int hash_exists(unsigned hd, void *key, size_t key_len);

/* get a primary key from a secondary hash key */
int hash_pget(unsigned hd, void *pkey, void *key, size_t key_len);

/* drop the values from a hash */
int hash_drop(unsigned hd);

/* delete a value from an hash */
int hash_del(unsigned hd, void *key, size_t key_len);

/* delete a certain value from a hash that supports dupes */
int hash_vdel(unsigned hd, void *key_data, size_t key_size, void *value_data, size_t value_size);

/* start iterating on a hash (maybe within a certain key) */
struct hash_cursor hash_iter(unsigned hd, void *key, size_t key_len);

/* close a hash */
void hash_close(unsigned hd);

/* get next value from hash iteration */
int hash_next(void *key, void *value, struct hash_cursor *cur);

/* finalize iteration if exiting it earlier */
void hash_fin(struct hash_cursor *cur);

/* write a hash to disk */
void hash_sync(unsigned hd);

/* initialize an memory-only database */
static inline unsigned hash_init() {
	return hash_cinit(NULL, NULL, 0644, 0);
}

/*
 * UNSIGNED TO ANYTHING HASH TABLE (UHASH)
 */

/* get an item from an uhash */
static inline int uhash_get(unsigned hd, void *target, unsigned ref) {
	return hash_get(hd, target, &ref, sizeof(ref));
}

/* check if uhash key exists */
static inline int uhash_exists(unsigned hd, unsigned ref) {
	return hash_exists(hd, &ref, sizeof(ref));
}

/* get a primary key from a secondary hash key */
static inline int uhash_pget(unsigned hd, void *pkey, unsigned ref) {
	return hash_pget(hd, pkey, &ref, sizeof(ref));
}

/* set a uhash key's value */
static inline void uhash_put(unsigned hd, unsigned ref, void *value, unsigned value_len) {
	hash_put(hd, &ref, sizeof(ref), value, value_len);
}

/* delete a value from an uhash */
static inline int uhash_del(unsigned hd, unsigned key) {
	return hash_del(hd, &key, sizeof(key));
}

/* delete a certain value from a uhash that supports dupes */
static inline int
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
unsigned lhash_cinit(size_t item_len, const char *file, const char *database, int mode);

/* initialize a memory-only lhash */
static inline unsigned lhash_init(size_t item_len) {
	return lhash_cinit(item_len, NULL, NULL, 0644);
}

/* add an item to a lhash */
unsigned lhash_new(unsigned hd, void *item);

/* get an item from an lhash */
static inline int lhash_get(unsigned hd, void *target, unsigned ref) {
	return uhash_get(hd, target, ref);
}

/* set an lhash item */
void lhash_put(unsigned hd, unsigned ref, void *source);

/* delete lhash item */
int lhash_del(unsigned hd, unsigned ref);

/* start iterating through lhash */
static inline struct hash_cursor lhash_iter(unsigned hd) {
	return hash_iter(hd, NULL, 0);
}

/* iterate through lhash */
static inline int lhash_next(unsigned *key, void *value, struct hash_cursor *cur) {
	int ret = hash_next(key, value, cur);
	if (ret && *key == (unsigned) -1)
		ret = hash_next(key, value, cur);
	return ret;
}

void lhash_close(unsigned hd);

/*
 * ASSOCIATION HASH TABLE (AHASH) METHODS
 * lhash unsigned to unsigned tables can be used for associations
 * these methods help out with those. These are memory-only,
 * as associations can be stored via a reference from item to container.
 * these have duplicates by default.
 */

/* initialize ahash */
static inline unsigned ahash_cinit(char *fname, char *dbname, int mode) {
	return hash_cinit(fname, dbname, mode, QH_DUP);
}

/* initialize ahash */
static inline unsigned ahash_init() {
	return ahash_cinit(NULL, NULL, 0644);
}

/* add an association */
static inline void ahash_add(unsigned ahd, unsigned container, unsigned item) {
	uhash_put(ahd, container, &item, sizeof(item));
}

/* remove an association */
static inline void ahash_remove(unsigned ahd, unsigned container, unsigned item) {
	uhash_vdel(ahd, container, &item, sizeof(item));
}

/* get next value from hash iteration */
static inline int ahash_next(void *value, struct hash_cursor *cur) {
	unsigned ign;
	return hash_next(&ign, value, cur);
}

/*
 * STRING TO ANYTHING HASH TABLE (SHASH)
 * this can be used to assocatie a string with anything
 * it mostly just avoids having to call strlen manually
 * memory-only, 1-1
 */

/* put a value into a shash */
static inline void shash_put(unsigned hd, char *key, void *value, size_t value_len) {
	hash_put(hd, key, strlen(key) + 1, value, value_len);
}

/* get a value from an shash */
static inline ssize_t shash_get(unsigned hd, void *value, char *key) {
	return hash_get(hd, value, key, strlen(key) + 1);
}

/* check if shash key exists */
static inline int shash_exists(unsigned hd, char *key) {
	return hash_exists(hd, key, strlen(key) + 1);
}

/* delete a value from an shash */
static inline int shash_del(unsigned hd, char *key) {
	return hash_del(hd, key, strlen(key) + 1);
}

/* start iterating on a hash table (maybe within a certain key) */
static inline struct hash_cursor shash_iter(unsigned hd, char *key) {
	return hash_iter(hd, key, key ? strlen(key) + 1 : 0);
}

/*
 * STRING TO UNSIGNED HASH TABLE (SUHASH)
 * this can be used to assocatie a string with an unsigned number,
 * which can be a lhash item, or another hashtable.
 * memory-only, 1-1
 */

/* put a value into a shash */
static inline void suhash_put(unsigned hd, char *key, unsigned value) {
	shash_put(hd, key, &value, sizeof(value));
}

/*
 * STRING TO POINTER HASH TABLE (SPHASH)
 * this can be used to assocatie a string with a pointer,
 * memory-only, 1-1
 */

/* put a value into a shash */
static inline void sphash_put(unsigned hd, char *key, void *value) {
	shash_put(hd, key, value, sizeof(value));
}

/*
 * UNSIGNED TO STRING HASH TABLE (USHASH)
 */

static inline void ushash_put(unsigned hd, unsigned key, char *value) {
	uhash_put(hd, key, value, strlen(value) + 1);
}

/*
 * STRING TO STRING HASH TABLE (SSHASH)
 */

static inline void sshash_put(unsigned hd, char *key, char *value) {
	hash_put(hd, key, strlen(key) + 1, value, strlen(value) + 1);
}

/* AUTO TABLES */

/* initialize a string to string table \0 between key and value */
static inline void
shash_table(unsigned hd, char *table[]) {
	for (char **t = table; *t; t++)
		shash_put(hd, *t, *t + strlen(*t) + 1, sizeof(*t));
}

/* initialize a string to unsigned index table. */
static inline void
suhash_table(int hd, char *table[]) {
	for (unsigned i = 0; table[i]; i++) {
		char *str = table[i];
		size_t len = strlen(str);
		suhash_put(hd, str, i);
		str += len + 1;
		if (*str)
			suhash_put(hd, str, i);
	}
}

#endif
