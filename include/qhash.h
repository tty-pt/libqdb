#ifndef QDB_H
#define QDB_H

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <sys/types.h>
#include <syslog.h>
#ifdef __OpenBSD__
#include <db4/db.h>
#else
#include <db.h>
#endif

#define FILO(name, TYPE, INVALID) \
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
	static inline TYPE name ## _peek(struct name *list) { \
		struct name ## _item *top = SLIST_FIRST(list); \
		return top ? top->value : INVALID; \
	} \
	static inline TYPE name ## _pop(struct name *list) { \
		struct name ## _item *popped = SLIST_FIRST(list); \
		if (!popped) return INVALID; \
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

#define QDB_DBS_MAX (64 * 512)
#define QHD_NOT_FOUND qdb_meta_id;

typedef void qdb_print_t(void *value);
typedef size_t qdb_measure_t(void *value);

typedef struct {
	qdb_print_t *print;
	qdb_measure_t *measure;
	size_t len;
} qdb_type_t;

FILO(idml, unsigned, -1)
FILO(txnl, DB_TXN *, NULL)

/* a struct for reusable ids */
struct idm {
	struct idml free;
	unsigned last;
};

enum qdb_type {
	QDB_KEY = 0,
	QDB_VALUE = 1,
};

/* associations are made using these callbacks */
typedef void (*qdb_assoc_t)(void **data, uint32_t *len, void *key, void *value);

/* we store some metadata in the database to know
 * types and stuff later on */
typedef struct {
	char key[8];
	char value[8];
	unsigned flags;
	unsigned extra;
} qdb_smeta_t;

/* we also have an in-memory metadata for each */
typedef struct meta {
	unsigned flags;
	qdb_assoc_t assoc;
	struct idm idm;
	qdb_type_t *type[2];
} qdb_meta_t;

extern qdb_meta_t qdb_meta[QDB_DBS_MAX];

extern qdb_type_t qdb_string, qdb_unsigned;
extern unsigned types_hd, qdb_meta_id;

/* we have this config object mostly to avoid having
 * to specify much when opening databases */
struct qdb_config {
	int mode;
	unsigned flags;
	DBTYPE type;
	char *file;
	DB_ENV *env;
	struct txnl txnl;
};
extern struct qdb_config qdb_config;

/* this is useful for custom logging */
typedef void (*log_t)(int type, const char *fmt, ...);
void qdb_set_logger(log_t logger);
extern log_t qdblog;

static inline void qdblog_perror(char *str) {
        qdblog(LOG_ERR, "%s: %s", str, strerror(errno));
}

__attribute__((noreturn))
static inline void qdblog_err(char *str) {
        qdblog_perror(str);
        exit(EXIT_FAILURE);
}

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

/* some flags that are useful for us */
enum qdb_flags {
	/* allows duplicate keys */
	QH_DUP = 2,

	/* is a secondary db */
	QH_SEC = 4,

	/* use transaction support */
	QH_TXN = 8,

	/* open in read-only mode */
	QH_RDONLY = 16, 

	/* auto-index / auto-key (unsigned) */
	QH_AINDEX = 32,
};

/* we use these cursors for iteration */
typedef struct {
	int flags;
	void *internal;
} qdb_cur_t;

/* open a database (specify much) */
unsigned qdb_openc(const char *file, const char *database, int mode, unsigned flags, int type, char *key_tid, char *value_tid);

/* put a key-value pair (not type aware) */
int qdb_putc(unsigned hd, void *key, size_t key_len, void *value, size_t value_len);

/* get a key or value's length */
static inline size_t qdb_len(unsigned hd, unsigned type, void *key) {
	qdb_type_t *mthing = qdb_meta[hd].type[type];
	return mthing->len ? mthing->len : mthing->measure(key);
}

/* put a key-value pair (type aware) */
static inline unsigned
qdb_put(unsigned hd, void *key, void *value)
{
	size_t key_len, value_len;
	unsigned id = 0;

	if (key != NULL)
		key_len = qdb_len(hd, QDB_KEY, key);
	else if (qdb_meta[hd].flags & QH_AINDEX) {
		id = idm_new(&qdb_meta[hd].idm);
		key = &id;
		key_len = sizeof(unsigned);
	} else
		qdblog_err("qdb_put NULL key without QH_AINDEX");

	value_len = qdb_len(hd, QDB_VALUE, value);
	if (qdb_putc(hd, key, key_len, value, value_len))
		return (unsigned) qdb_meta_id;

	return id;
}

/* initialize the system */
void qdb_init(void);

/* remove a specific key-value pair */
int qdb_rem(unsigned hd, void *key, void *value);

/* stop iteration early */
void qdb_fin(qdb_cur_t *cur);

/* get next iteration */
int qdb_next(void *key, void *value, qdb_cur_t *cur);

/* close a database */
void qdb_close(unsigned hd, unsigned flags);

/* sync database to disk */
void qdb_sync(unsigned hd);

/* associate two databases */
void qdb_assoc(unsigned hd, unsigned link, qdb_assoc_t assoc);

/* drop everything in a database */
int qdb_drop(unsigned hd);

/* get an item from a database (not type aware) */
void *qdb_getc(unsigned hd, size_t *size, void *key_r, size_t key_len);

/* check key's existence (not type aware) */
static inline int qdb_existsc(unsigned hd, void *key_r, size_t key_len)
{
	size_t size;
	return qdb_getc(hd, &size, key_r, key_len)
		? 1 : 0;
}

/* check key's existence (type aware) */
static inline int qdb_exists(unsigned hd, void *key)
{
	return qdb_existsc(hd, key, qdb_len(hd, QDB_KEY, key));
}

/* get a primary key from a secondary one */
int qdb_pget(unsigned hd, void *pkey, void *key);

/* start iterating */
qdb_cur_t qdb_iter(unsigned hd, void *key);

/* delete item under cursor (iteration) */
int qdb_cdel(qdb_cur_t *cur);

/* delete all values matching a key */
static inline void qdb_del(unsigned hd, void *key) {
	if (qdb_meta[hd].flags & QH_AINDEX)
		idm_del(&qdb_meta[hd].idm, * (unsigned *) key);

	qdb_cur_t c = qdb_iter(hd, key);

	while (qdb_next(NULL, NULL, &c))
		qdb_cdel(&c);
}

/* open a database (specify little) */
static inline int qdb_open(char *database, char *key_tid, char *value_tid, unsigned flags) {
	return qdb_openc(qdb_config.file, database, qdb_config.mode,
			flags | qdb_config.flags, qdb_config.type, key_tid, value_tid);
}

/* get the first value for a given key (type aware) */
static inline int qdb_get(unsigned hd, void *value, void *key)
{
	size_t size;
	void *value_r = qdb_getc(hd, &size, key, qdb_len(hd, QDB_KEY, key));

	if (!value_r)
		return 1;

	memcpy(value, value_r, size);
	return 0;
}

/* register a type of key / value */
static inline void
qdb_regc(char *key, qdb_type_t *type) {
	qdb_put(types_hd, key, &type);
}

/* the same but just give a length (for simple types) */
static inline void
qdb_reg(char *key, size_t len) {
	qdb_type_t *type = (qdb_type_t *) malloc(sizeof(qdb_type_t));
	type->measure = NULL;
	type->print = NULL;
	type->len = len;
	qdb_put(types_hd, key, &type);
}

/* print a key or value */
static inline void qdb_print(unsigned hd, unsigned type, void *thing) {
	qdb_meta[hd].type[type]->print(thing);
}

/* TRANSACTIONS */

/* create a database environment */
static inline DB_ENV *qdb_env_create(void) {
	DB_ENV *env;
	db_env_create(&env, 0);
	env->set_flags(env, DB_AUTO_COMMIT, 1);
	env->set_lk_detect(env, DB_LOCK_OLDEST);
	env->set_tx_max(env, 5 * 60);
	return env;
}

/* open a database environment */
void qdb_env_open(DB_ENV *env, char *path);

/* begin a transaction */
static inline DB_TXN *qdb_begin(void) {
	DB_TXN *txn;

	if (qdb_config.env->txn_begin(qdb_config.env, txnl_peek(&qdb_config.txnl), &txn, 0)) {
		qdblog(LOG_ERR, "Txn begin failed\n");
		return NULL;
	}

	txnl_push(&qdb_config.txnl, txn);
	return txn;
}

/* commit a transation */
static inline void qdb_commit(void) {
	DB_TXN *txn = txnl_pop(&qdb_config.txnl);

	if (!txn) {
		qdblog(LOG_ERR, "No txn to commit\n");
		return;
	}

	if (txn->commit(txn, 0))
		qdblog(LOG_ERR, "Txn commit failed\n");
}

/* abort a transation */
static inline void qdb_abort(void) {
	DB_TXN *txn = txnl_pop(&qdb_config.txnl);

	if (!txn) {
		qdblog(LOG_ERR, "No txn to abort\n");
		return;
	}

	if (txn->abort(txn))
		qdblog(LOG_ERR, "Txn abort failed\n");
}

/* abort a transation */
static inline void qdb_checkpoint(unsigned kbytes, unsigned min, unsigned flags) {
	qdb_config.env->txn_checkpoint(qdb_config.env, kbytes, min, flags);
}

#endif
