#ifndef QHASH_H
#define QHASH_H

#include <stddef.h>

#define SHASH_PUT(hd, key, data)	hash_put(hd, key, strlen(key), data)
#define SHASH_GET(hd, key)		hash_get(hd, key, strlen(key))
#define SHASH_DEL(hd, key)		hash_del(hd, key, strlen(key))

typedef void (*hash_cb_t)(void *key, size_t key_size, void *data, void *arg);

unsigned hash_init();
void hash_put(unsigned hd, void *key, size_t key_len, void *value);
void *hash_get(unsigned hd, void *key, size_t key_len);
void hash_del(unsigned hd, void *key, size_t key_len);
void shash_table(unsigned hd, char *table[]);
void hash_iter(unsigned hd, hash_cb_t callback, void *arg);
void hash_close(unsigned hd);

#endif

