#ifndef QHASH_H
#define QHASH_H

#include <stddef.h>
#include <sys/types.h>
#include <string.h>

enum qhash_flags {
	QH_DUP = 2,
};

struct hash_cursor {
	int flags;
	void *internal;
};

unsigned hash_cinit(const char *file, const char *database, int mode, int flags);
unsigned hash_init();
void hash_cput(unsigned hd, void *key, size_t key_len, void *value, size_t value_len);
void *hash_get(unsigned hd, void *key, size_t key_len);
ssize_t hash_cget(unsigned hd, void *value, void *key, size_t key_len);
void hash_del(unsigned hd, void *key, size_t key_len);
int hash_vdel(unsigned hd, void *key_data, size_t key_size, void *value_data, size_t value_size);
void shash_table(unsigned hd, char *table[]);
struct hash_cursor hash_citer(unsigned hd, void *key, size_t key_len);
struct hash_cursor hash_iter(unsigned hd);
void hash_close(unsigned hd);
ssize_t hash_next(void *key, void *value, struct hash_cursor *cur);
void hash_fin(struct hash_cursor *cur);
void hash_sync(unsigned hd);

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

#endif
