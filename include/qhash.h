#ifndef QHASH_H
#define QHASH_H

#include <stddef.h>
#include <sys/types.h>
#include <sys/queue.h>
#include <string.h>

#define LHASH_DECL(name, type) \
	void name ## _init(); \
	unsigned name ## _new(type *name); \
	type name ## _get(unsigned ref); \
	void name ## _set(unsigned ref, type *name); \
	struct hash_cursor name ## _iter(); \
	unsigned name ## _next(type *name, struct hash_cursor *c);

#define LHASH_DEF(name, type) \
	struct lhash name ## _lhash; \
	unsigned name ## _new(type *name) { \
		return lhash_new(&name ## _lhash, name, sizeof(type)); \
	} \
	type name ## _get(unsigned ref) { \
		type name; \
		lhash_cget(&name ## _lhash, &name, ref); \
		return name; \
	} \
	void name ## _set(unsigned ref, type *name) { \
		lhash_set(&name ## _lhash, ref, name, sizeof(type)); \
	} \
	struct hash_cursor name ## _iter() { \
		return hash_iter(name ## _lhash.hd); \
	} \
	unsigned name ## _next(type *name, struct hash_cursor *c) { \
		unsigned ref; \
		return hash_next(&ref, name, c) > 0 ? ref : -1; \
	}

#define LHASH_ASSOC_DECL(name, cont_name, item_name) \
	void name ## _add(unsigned cont_ref, unsigned item_ref); \
	struct hash_cursor name ## _iter(unsigned cont_ref); \
	unsigned name ## _next(struct hash_cursor *c); \
	void name ## _remove(unsigned cont_ref, unsigned item_ref);

#define LHASH_ASSOC_DEF(name, cont_name, item_name) \
	unsigned name ## _ahd; \
	void name ## _add(unsigned cont_ref, unsigned item_ref) { \
		hash_cput(name ## _ahd, &cont_ref, sizeof(cont_ref), &item_ref, sizeof(item_ref)); \
	} \
	struct hash_cursor name ## _iter(unsigned cont_ref) { \
		return hash_citer(name ## _ahd, &cont_ref, sizeof(cont_ref)); \
	} \
	unsigned name ## _next(struct hash_cursor *c) { \
		unsigned key, value; \
		return hash_next(&key, &value, c) >= 0 ? value : -1; \
	} \
	void name ## _remove(unsigned cont_ref, unsigned item_ref) { \
		hash_vdel(name ## _ahd, &cont_ref, sizeof(cont_ref), &item_ref, sizeof(item_ref)); \
	}

enum qhash_flags {
	QH_DUP = 2,
};

struct hash_cursor {
	int flags;
	void *internal;
};

struct unsigned_item {
	unsigned value;
	SLIST_ENTRY(unsigned_item) entry;
};

SLIST_HEAD(free_list, unsigned_item);

struct lhash {
	unsigned hd, last;
	struct free_list free;
};

unsigned hash_cinit(const char *file, const char *database, int mode, int flags);
void hash_cput(unsigned hd, void *key, size_t key_len, void *value, size_t value_len);
void *hash_get(unsigned hd, void *key, size_t key_len);
ssize_t hash_cget(unsigned hd, void *value, void *key, size_t key_len);
void hash_del(unsigned hd, void *key, size_t key_len);
int hash_vdel(unsigned hd, void *key_data, size_t key_size, void *value_data, size_t value_size);
void shash_table(unsigned hd, char *table[]);
struct hash_cursor hash_citer(unsigned hd, void *key, size_t key_len);
void hash_close(unsigned hd);
ssize_t hash_next(void *key, void *value, struct hash_cursor *cur);
void hash_fin(struct hash_cursor *cur);
void hash_sync(unsigned hd);

static inline unsigned hash_init() {
	return hash_cinit(NULL, NULL, 0644, 0);
}

static inline void hash_put(unsigned hd, void *key_r, size_t key_len, void *value) {
	hash_cput(hd, key_r, key_len, &value, sizeof(void *));
}

static inline void * hash_sget(unsigned hd, char *key_r) {
	void **ret = hash_get(hd, key_r, strlen(key_r));
	return ret ? *ret : NULL;
}

static inline void hash_sput(unsigned hd, char *key_r, void *value) {
	hash_cput(hd, key_r, strlen(key_r), &value, sizeof(void *));
}

static inline void hash_sdel(unsigned hd, char *key) {
	hash_del(hd, key, strlen(key));
}

static inline struct hash_cursor hash_iter(unsigned hd) {
	return hash_citer(hd, NULL, 0);
}

struct lhash lhash_cinit(const char *file, const char *database, int mode, int flags);
void lhash_del(struct lhash *lhash, unsigned id);
unsigned lhash_new(struct lhash *lhash, void *value, size_t value_len);

static inline struct lhash lhash_init() {
	return lhash_cinit(NULL, NULL, 0644, 0);
}

static inline void *lhash_get(struct lhash *lhash, unsigned id) {
	return hash_get(lhash->hd, &id, sizeof(id));
}

static inline ssize_t lhash_cget(struct lhash *lhash, void *value_r, unsigned id) {
	return hash_cget(lhash->hd, value_r, &id, sizeof(id));
}

static inline void lhash_set(struct lhash *lhash, unsigned id, void *value, size_t value_len) {
	hash_cput(lhash->hd, &id, sizeof(id), value, value_len);
}

#endif
