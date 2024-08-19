#ifndef QHASH_H
#define QHASH_H

#include <stddef.h>
#include <sys/types.h>

#define SHASH_PUT(hd, key, data)	hash_put(hd, key, strlen(key), data)
#define SHASH_GET(hd, key)		hash_sget(hd, key, strlen(key))
#define SHASH_DEL(hd, key)		hash_del(hd, key, strlen(key))

enum qhash_flags {
	QH_DUP = 2,
};

struct hash_cursor {
	int flags;
	void *internal;
};

unsigned hash_cinit(const char *file, const char *database, int mode, int flags);
unsigned hash_init();
void hash_put(unsigned hd, void *key, size_t key_len, void *value);
void hash_cput(unsigned hd, void *key, size_t key_len, void *value, size_t value_len);
void *hash_get(unsigned hd, void *key, size_t key_len);
void *hash_sget(unsigned hd, void *key, size_t key_len);
ssize_t hash_cget(unsigned hd, void *value, void *key, size_t key_len);
void hash_del(unsigned hd, void *key, size_t key_len);
int hash_vdel(unsigned hd, void *key_data, size_t key_size, void *value_data, size_t value_size);
void shash_table(unsigned hd, char *table[]);
struct hash_cursor hash_citer(unsigned hd, void *key, size_t key_len);
struct hash_cursor hash_iter(unsigned hd);
void hash_close(unsigned hd);
ssize_t hash_next(void *key, void *value, struct hash_cursor *cur);
void hash_sync(unsigned hd);

#endif
