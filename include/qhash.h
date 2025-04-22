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
	unsigned flags;
	qdb_assoc_t assoc;
	struct idm idm;
	qdb_type_t *key, *value;
} qdb_meta_t;

extern qdb_meta_t qdb_meta[HASH_DBS_MAX];

extern qdb_type_t qdb_string, qdb_unsigned, qdb_ptr, qdb_unsigned_pair;

static struct qdb_config {
	int mode, flags;
	DBTYPE type;
	char *file;
} qdb_config = { .mode = 0644, .flags = 0, .type = DB_HASH, .file = NULL };

typedef void (*log_t)(int type, const char *fmt, ...);

void qdb_set_logger(log_t logger);

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

enum qdb_flags {
	QH_DUP = 2,
	QH_SEC = 4, // secondary (internal)
	QH_TXN = 8, // transaction support
	QH_RDONLY = 16, // read only mode
	QH_AINDEX = 32, // auto index db (internal - implies unsigned key)
};

typedef struct {
	int flags;
	void *internal;
} qdb_cur_t;

unsigned qdb_initc(const char *file, const char *database, int mode, unsigned flags, int type, qdb_type_t *key_type, qdb_type_t *value_type);
int qdb__put(unsigned hd, void *key, size_t key_len, void *value, size_t value_len);

static inline int
qdb_put(unsigned hd, void *key_r, void *value)
{
	qdb_meta_t *meta = &qdb_meta[hd];
	return qdb__put(hd,
			key_r, meta->key->len ? meta->key->len : meta->key->measure(key_r),
			value, meta->value->len ? meta->value->len : meta->value->measure(value));
}

int qdb_rem(unsigned hd, void *key, void *value);
void qdb_fin(qdb_cur_t *cur);
int qdb_next(void *key, void *value, qdb_cur_t *cur);
void qdb_close(unsigned hd, unsigned flags);
void qdb_sync(unsigned hd);
void qdb_assoc(unsigned hd, unsigned link, qdb_assoc_t assoc);
int qdb_drop(unsigned hd);
unsigned qdb_new(unsigned hd, void *item);
int qdb_exists(unsigned hd, void *key);
int qdb_pget(unsigned hd, void *pkey, void *key);
qdb_cur_t qdb_iter(unsigned hd, void *key);
int qdb_cdel(qdb_cur_t *cur);

static inline void qdb_key_print(unsigned hd, void *key) {
	qdb_meta[hd].key->print(key);
}

static inline void qdb_value_print(unsigned hd, void *value) {
	qdb_meta[hd].value->print(value);
}

/* delete a value from an hash */
static inline void qdb_del(unsigned hd, void *key) {
	if (qdb_meta[hd].flags & QH_AINDEX)
		idm_del(&qdb_meta[hd].idm, * (unsigned *) key);

	qdb_cur_t c = qdb_iter(hd, key);

	while (qdb_next(NULL, NULL, &c))
		qdb_cdel(&c);
}

static inline int qdb_init(char *database, qdb_type_t *key_type, qdb_type_t *value_type) {
	return qdb_initc(qdb_config.file, database, qdb_config.mode,
			qdb_config.flags, qdb_config.type, key_type, value_type);
}

static inline unsigned qdb_ainitc(char *fname, char *dbname, int mode, unsigned flags) {
	return qdb_initc(fname, dbname, mode, QH_DUP | QH_AINDEX | flags, DB_HASH, &qdb_unsigned, &qdb_unsigned);
}

static inline unsigned qdb_ainit(char *database) {
	return qdb_ainitc(qdb_config.file, database, qdb_config.mode, qdb_config.flags);
}

int qdb_get(unsigned hd, void *target, void *key);

unsigned qdb_linitc(const char *file, const char *database, int mode, unsigned flags, qdb_type_t *value_type);

/* final version */
static inline unsigned qdb_linit(char *database, qdb_type_t *value_type) {
	return qdb_linitc(qdb_config.file, database, qdb_config.mode, qdb_config.flags, value_type);
}

void qdb_env_set(void *value);
void *qdb_env_pop(void);

#endif
