#ifndef QHASH_H
#define QHASH_H

#include <stddef.h>

#define SHASH_PUT(hd, key, data)	hash_put(hd, key, strlen(key), data)
#define SHASH_GET(hd, key)		hash_get(hd, key, strlen(key))
#define SHASH_DEL(hd, key)		hash_del(hd, key, strlen(key))

struct hash_cursor {
	void *key;
	size_t key_len;
	void *data;
};

unsigned hash_init();
void hash_put(unsigned hd, void *key, size_t key_len, void *value);
void *hash_get(unsigned hd, void *key, size_t key_len);
void hash_del(unsigned hd, void *key, size_t key_len);
void shash_table(unsigned hd, char *table[]);
struct hash_cursor hash_iter_start(unsigned hd);
void *hash_iter_get(struct hash_cursor *cur);
void hash_close(unsigned hd);

#endif

