#include "./include/qdb.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/queue.h>

#define QDB_NOT_FOUND NULL

enum {
	/* this is used for iteration */
	QH_NOT_FIRST = 1,

	/* repurpose the primary db with different types */
	QH_REPURPOSE = 64, // internal iteration flag
};

qdb_meta_t qdb_meta[QDB_DBS_MAX];
static DB *qdb_dbs[QDB_DBS_MAX];
unsigned types_hd = QDB_DBS_MAX - 1, qdb_meta_id = QDB_NOTFOUND, qdb_min = 0;
struct txnl txnl_empty;

struct qdb_config qdb_config = {
	.mode = 0644,
	.type = DB_HASH,
	.file = NULL,
	.env = NULL,
};

struct idmap {
	unsigned *map, *omap;
	unsigned min, m, n, last;
};


static inline void memset32(uint32_t *dest, uint32_t value, size_t count) {
    for (size_t i = 0; i < count / 4; ++i) {
        dest[i] = value;
    }
}

static inline struct idmap idmap_init(void) {
	struct idmap idmap = {
		.map = malloc(32 * sizeof(unsigned)),
		.omap = malloc(32 * sizeof(unsigned)),
		.m = 32, .n = 0, .min = 0,
		.last = qdb_meta_id
	};
	memset(idmap.map, qdb_meta_id, 32 * sizeof(unsigned));
	memset(idmap.omap, qdb_meta_id, 32 * sizeof(unsigned));
	return idmap;
}

static inline void idmap_put(struct idmap *idmap, unsigned id) {
	/* if (idmap->min < id) { */
	/* } */

	if (idmap->min + idmap->m < id) {
		unsigned old_m = idmap->m;
		idmap->m = id - idmap->min;
		idmap->m *= 2;
		idmap->map = realloc(idmap->map, idmap->m * sizeof(unsigned));
		idmap->omap = realloc(idmap->omap, idmap->m * sizeof(unsigned));
		memset(idmap->map + old_m, -1, (idmap->m - old_m) * sizeof(unsigned));
		memset(idmap->omap + old_m, -1, (idmap->m - old_m) * sizeof(unsigned));
	}

	fprintf(stderr, "idmap put %u, %u, %p[%u/%u]\n", id,
			idmap->min, idmap->map, idmap->n,
			idmap->m);
	if (idmap->map[id - idmap->min] != qdb_meta_id)
		return;

	idmap->omap[idmap->n] = id;
	idmap->map[id - idmap->min] = idmap->n;
	idmap->n++;
	idmap->last = id;
}

static inline void idmap_del(struct idmap *idmap, unsigned id) {
	if (idmap->min < id)
		return; // not present

	if (idmap->min + idmap->m < id)
		return; // not present

	unsigned n_index = idmap->map[id - idmap->min];

	memcpy(&idmap->omap[n_index], &idmap->omap[n_index + 1], idmap->n - n_index);
	idmap->n--;
	idmap->map[id - idmap->min] = qdb_meta_id;
}

static struct idm idm;

static int qdb_first = 1;

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
	.len = sizeof(void *),
}, qdb_unsigned_long = {
	.len = sizeof(unsigned long),
};

static void
qdb_logger_stderr(int type __attribute__((unused)), const char *fmt, ...)
{
	va_list va;
	va_start(va, fmt);
	vfprintf(stderr, fmt, va);
	va_end(va);
}

log_t qdblog = qdb_logger_stderr;

void qdb_set_logger(log_t logger) {
	qdblog = logger;
}

static inline int qdb_cache_putc(unsigned hd, qdb_meta_t *meta, void *key_r, void *value) {
	register unsigned id = * (unsigned *) key_r;
	if (id == qdb_meta_id)
		return 1;

	qdb_type_t
		*kt = meta->type[QDB_KEY],
		*vt = meta->type[QDB_VALUE >> 1];

	register size_t kl = kt->len, vl = vt->len,
		 il = kl + vl;

	char *c;

	if (kt != &qdb_unsigned) {
		id = meta->cache_n;
		meta->cache_n++;

		if (meta->cache_n >= meta->cache_m) {
			meta->cache_m *= meta->cache_n * 2;
			meta->cache = realloc(meta->cache, meta->cache_m * il);
		}

		c = meta->cache + id * il;
		memcpy(c + kl, value, vl);
		memcpy(c, key_r, kl);
		return 0;
	}

	if (id >= meta->cache_n)
		meta->cache_n = id + 1;

	if (meta->cache_n >= meta->cache_m) {
		meta->cache_m *= meta->cache_n * 2;
		meta->cache = realloc(meta->cache, meta->cache_m * il);
	}

	if ((meta->flags & QH_DUP) && vt == &qdb_unsigned) {
		vl = sizeof(struct idmap);
		il = kl + vl;
		c = meta->cache + id * il;
		struct idmap *idmap = (struct idmap *) c + kl;
		if (* (unsigned *) c != id) {
			*idmap = idmap_init();
			fprintf(stderr, "contents init!!! %p\n", idmap->map);
		}
		idmap_put(idmap, * (unsigned *) value);
		fprintf(stderr, "contents put %u <- %u // %p\n", id, * (unsigned *) value, idmap->map);
	} else {
		c = meta->cache + id * il;
		memcpy(c + kl, value, vl);
	}

	memcpy(c, key_r, kl);
	return 0;
}

int
qdb_putc(unsigned hd, void *key_r, size_t key_len, void *value, size_t value_len) {
	unsigned id;
	DB *db = qdb_dbs[hd];
	DBT key, data;
	int ret;

	qdb_meta_t *meta = &qdb_meta[hd];

	if ((meta->flags & QH_AINDEX) && (id = * (unsigned *) key_r) != qdb_meta_id) {
		if (meta->idm.last < id)
			meta->idm.last = id;

		// this is for lhash put. It's to ensure there are free numbers in between.
		// let's deactivate it for now because it's causing errors atm
		unsigned last;
		for (last = meta->idm.last; last < id; last++)
			idml_push(&meta->idm.free, last);
	}

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));

	key.data = key_r;
	key.size = key_len;
	data.data = value;
	data.size = value_len;
	data.flags = DB_DBT_MALLOC;
	int dupes;
	u_int32_t flags;

	if (db->get_flags(db, &flags))
		qdblog_err("qdb_putc get_flags");
	dupes = flags & DB_DUP;

	ret = db->put(db, hd && (meta->flags & QH_TXN)
			? txnl_peek(&qdb_config.txnl)
			: NULL, &key, &data, 0);

	if (ret) {
		if (ret != DB_KEYEXIST || !dupes)
			qdblog(LOG_WARNING, "qdb_putc\n");
	} else if (meta->flags & QH_CACHE)
		qdb_cache_putc(hd, meta, key_r, value);

	return ret;
}

void *qdb_getc(unsigned hd, size_t *size, void *key_r, size_t key_len)
{
	DB *db;
	DBT key, data;
	int ret;

	qdb_meta_t *meta = &qdb_meta[hd];
	qdb_type_t *kt = meta->type[QDB_KEY];

	if ((meta->flags & QH_CACHE) && kt == &qdb_unsigned) {
		register unsigned id = * (unsigned *) key_r;

		if (id == qdb_meta_id)
			return NULL;

		register size_t vl = meta->type[QDB_VALUE >> 1]->len;

		if (id >= meta->cache_n)
			return NULL;

		char *c = meta->cache + id * (sizeof(unsigned) + vl);
		if (id != * (unsigned *) c)
			return NULL;

		if (meta->flags & QH_DUP) {
			// TODO warn only available on unsinged
			*size = sizeof(unsigned);
			return &((struct idmap *) (meta->cache + sizeof(unsigned) + id * (sizeof(unsigned) + sizeof(struct idmap))))->last;
		}

		c += sizeof(unsigned);
		*size = vl;
		if (hd == 21)
			fprintf(stderr, "cache_get %u %u %zu - %u\n", hd, id, vl, * (unsigned *) c);
		return c;
	}
	
	db = qdb_dbs[hd];

	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));

	key.data = (void *) key_r;
	key.size = key_len;
	data.flags = DB_DBT_MALLOC;

	ret = db->get(db, hd && (meta->flags & QH_TXN)
			? txnl_peek(&qdb_config.txnl)
			: NULL, &key, &data, 0);

	if (ret == DB_NOTFOUND)
		return NULL;
	else if (ret)
		qdblog_err("__qdb_get");

	*size = data.size;
	return data.data;
}

unsigned _qdb_lopenc(unsigned hd, qdb_type_t *value_type) {
	unsigned ign, last = 0;
	qdb_meta_t *meta = &qdb_meta[hd];
	char buf[value_type->len];
	SLIST_INIT(&meta->idm.free);
	struct txnl txnl = qdb_config.txnl;
	qdb_config.txnl = txnl_empty;
	qdb_cur_t c = qdb_iter(hd, NULL);

	while (qdb_next(&ign, buf, &c)) if (ign >= last)
		last = ign + 1;

	meta->idm.last = last;
	for (last = 0; last < meta->idm.last; last++)
		if (!qdb_existsc(hd, &last, sizeof(last)))
			idml_push(&meta->idm.free, last);

	qdb_config.txnl = txnl;

	return hd;
}

void
qdb_init(void) {
	idm = idm_init();
	idm_new(&idm);

	if (db_create(&qdb_dbs[0], NULL, 0))
		qdblog_err("qdb_init: db_create ids_db");

	if (qdb_dbs[0]->open(qdb_dbs[0], NULL, NULL, NULL, DB_HASH, DB_CREATE, 644))
		qdblog_err("qdb_init: open ids_db");

	memset(qdb_meta, 0, sizeof(qdb_meta));
	qdb_meta[0].type[QDB_KEY] = &qdb_ptr;
	qdb_meta[0].type[QDB_VALUE >> 1] = &qdb_unsigned;

	if (db_create(&qdb_dbs[types_hd], NULL, 0))
		qdblog_err("qdb_init: db_create types");

	if (qdb_dbs[types_hd]->open(qdb_dbs[types_hd], NULL, NULL, NULL, DB_HASH, DB_CREATE, 644))
		qdblog_err("qdb_init: open types");

	qdb_meta[types_hd].type[QDB_KEY] = &qdb_string;
	qdb_meta[types_hd].type[QDB_VALUE >> 1] = &qdb_ptr;
	qdb_regc("s", &qdb_string);
	qdb_regc("u", &qdb_unsigned);
	qdb_reg("p", sizeof(void *));
	qdb_reg("ul", sizeof(unsigned long));
	qdb_unsigned.dbl = "ul";
	qdb_config.mode = 0644;
	qdb_config.type = DB_HASH;
	qdb_config.file = NULL;
	qdb_config.flags = 0;
	qdb_config.txnl = txnl_init();
	txnl_empty = txnl_init();
}

static inline void qdb_recache(unsigned hd)
{
	qdb_meta_t *meta = &qdb_meta[hd];
	register size_t
		kl = meta->type[QDB_KEY]->len,
		vl = meta->type[QDB_VALUE >> 1]->len;

	if (!(kl && vl))
		qdblog_err("qdb_openc: variable size cache is not supported\n");

	char key_buf[kl], value_buf[vl], *cache;
	size_t item_len = kl + vl;
	meta->flags &= ~QH_CACHE;
	qdb_cur_t c;

	if (meta->flags & QH_AINDEX)
		meta->cache_n = meta->idm.last;
	else {
		c = qdb_iter(hd, NULL);

		while (qdb_next(key_buf, value_buf, &c))
			meta->cache_n++;
	}

	meta->cache_m = meta->cache_n
		? 2 * meta->cache_n
		: 64;

	if (meta->cache)
		free(meta->cache);
	size_t cache_len = meta->cache_m * item_len;
	cache = meta->cache = malloc(cache_len);
	memset(cache, qdb_meta_id, cache_len);

	c = qdb_iter(hd, NULL);

	while (qdb_next(key_buf, value_buf, &c)) {
		memcpy(cache, key_buf, kl);
		memcpy(cache + kl, value_buf, vl);
		cache += item_len;
	}

	meta->flags |= QH_CACHE;
}

unsigned
_qdb_openc(const char *file, const char *database, int mode, unsigned flags, int type, char *key_tid, char *value_tid)
{
	DB *db;
	unsigned id, dbflags = 0;
	DB_TXN *txn = NULL; // local transaction just for open

	qdb_type_t *key_type = NULL, *value_type = NULL;

	if ((flags & QH_AINDEX) && strcmp(key_tid, "u"))
		qdblog_err("qdb_openc: AINDEX without 'u' key\n");

	if (qdb_get(types_hd, &key_type, key_tid))
		qdblog_err("qdb_openc: key type was not registered\n");

	if (qdb_get(types_hd, &value_type, value_tid))
		qdblog_err("qdb_openc: value type was not registered\n");

	qdb_first = 0;
	id = idm_new(&idm);
	id += qdb_min;

	if (db_create(&qdb_dbs[id], qdb_config.env, 0))
		qdblog_err("qdb_openc: db_create\n");

	// this is needed for associations
	db = qdb_dbs[id];
	qdb_put(0, &qdb_dbs[id], &id);

	if (flags & QH_DUP && !(flags & QH_THRICE))
		if (db->set_flags(db, DB_DUP))
			qdblog_err("qdb_openc: set_flags\n");

	dbflags = (qdb_config.flags & QH_THREAD ? DB_THREAD : 0) | (flags & QH_RDONLY ? DB_RDONLY : DB_CREATE);
	if (db->open(db, txn, file, database, type, dbflags, mode))
		qdblog_err("qdb_openc: open\n");

	if (flags & (QH_SEC | QH_REPURPOSE))
		goto out;

	size_t val_len;
	qdb_smeta_t *smeta = 
		qdb_getc(id, &val_len, &qdb_meta_id, sizeof(qdb_meta_id));

	if (smeta) {
		key_tid = smeta->key;
		value_tid = smeta->value;
		flags = smeta->flags;
	} else {
		qdb_smeta_t put_smeta = {
			.flags = flags,
			.extra = 0,
		};

		strcpy(put_smeta.key, key_tid);
		strcpy(put_smeta.value, value_tid);

		qdb_putc(id, &qdb_meta_id, sizeof(qdb_meta_id),
				&put_smeta, sizeof(put_smeta));
	}

	qdb_get(types_hd, &key_type, key_tid);
	qdb_get(types_hd, &value_type, value_tid);

	if (flags & QH_AINDEX)
		_qdb_lopenc(id, value_type);

out:
	strcpy(qdb_meta[id].type_str[QDB_KEY], key_tid);
	strcpy(qdb_meta[id].type_str[QDB_VALUE >> 1], value_tid);
	qdb_meta[id].phd = qdb_meta_id;
	qdb_meta[id].flags = (flags & ~QH_CACHE);
	qdb_meta[id].type[QDB_KEY] = key_type;
	qdb_meta[id].type[QDB_VALUE >> 1] = value_type;

	if (flags & QH_CACHE)
		qdb_recache(id);
	else {
		qdb_meta[id].cache = NULL;
		qdb_meta[id].cache_m = qdb_meta[id].cache_n = 0;
	}
	qdb_meta[id].flags = flags;

#if 1
	fprintf(stderr, "qdb_openc %u %s %s %u %s %s %u %u\n",
			id, file, database,
			flags, key_tid, value_tid, flags, dbflags);
#endif

	return id;
}

int qdb_assoc_rhd(void **skey, void *key __attribute__((unused)), void *value) {
	*skey = value; // string
	return 0;
}

unsigned
qdb_openc(const char *file, const char *database, int mode, unsigned flags, int type, char *key_tid, char *value_tid)
{
	char buf[BUFSIZ];
	size_t len;

	if (!(flags & QH_THRICE))
		return _qdb_openc(file, database, mode, flags, type, key_tid, value_tid);

	if (flags & QH_DUP) {
		qdb_type_t *vtype;
		if (qdb_get(types_hd, &vtype, value_tid) && vtype->dbl)
			key_tid = vtype->dbl;
	}

	snprintf(buf, sizeof(buf), "%sphd", database);
	unsigned phd = _qdb_openc(file, buf, mode, flags, DB_HASH, key_tid, value_tid);
	unsigned read_only = flags & QH_RDONLY;

	qdb_smeta_t *smeta = qdb_getc(phd, &len, &qdb_meta_id, sizeof(qdb_meta_id));
	key_tid = smeta->key;
	value_tid = smeta->value;
	flags = smeta->flags;
	flags &= ~(QH_THRICE | QH_AINDEX);
	flags |= QH_DUP | QH_SEC;
	if (read_only)
		flags |= QH_RDONLY;

	// for secondaries, we don't need dbl keys
	if (smeta->flags & QH_DUP)
		key_tid = value_tid;

	snprintf(buf, sizeof(buf), "%shd", database);
	_qdb_openc(file, buf, mode, flags, DB_BTREE, key_tid, value_tid);
	qdb_assoc(phd + 1, phd, NULL);

	snprintf(buf, sizeof(buf), "%srhd", database);
	_qdb_openc(file, buf, mode, flags, DB_BTREE, value_tid, key_tid);
	qdb_assoc(phd + 2, phd, qdb_assoc_rhd);
	return phd;
}

void *qdb_pgetc(unsigned hd, size_t *len, void *key_r) {
	qdb_meta_t *meta = &qdb_meta[hd];
	qdb_type_t *kt = meta->type[QDB_KEY];
	if ((meta->flags & QH_CACHE) && kt == &qdb_unsigned) {
		unsigned id = * (unsigned *) key_r;
		return meta->cache + (sizeof(unsigned) + meta->type[QDB_VALUE >> 1]->len) * id;
	}

	unsigned flags = meta->flags;

	DB *db = qdb_dbs[hd];
	DBT key, pkey, data;
	int ret;

	memset(&key, 0, sizeof(key));
	memset(&pkey, 0, sizeof(pkey));
	memset(&data, 0, sizeof(data));

	key.data = (void *) key_r;
	key.size = qdb_len(hd, QDB_KEY, key_r);
	pkey.flags = data.flags = DB_DBT_MALLOC;

	ret = db->pget(db, (flags & QH_TXN)
			? txnl_peek(&qdb_config.txnl)
			: NULL, &key, &pkey, &data, 0);

	if (ret == DB_NOTFOUND)
		return NULL;
	else if (ret)
		qdblog_err("qdb_pget");

	if (flags & QH_REPURPOSE) {
		*len = data.size;
		return data.data;
	}

	*len = pkey.size;
	return pkey.data;
}

int
map_assoc(DB *sec, const DBT *key, const DBT *data, DBT *result)
{
	unsigned hd;
	void *skey;

	if (* (unsigned *) key->data == QDB_NOTFOUND)
		return DB_DONOTINDEX;

	if (qdb_get(0, &hd, &sec)) {
		// TODO WARNING
		return DB_DONOTINDEX;
	}

	memset(result, 0, sizeof(DBT));
	
	if (qdb_meta[hd].assoc(&skey, key->data, data->data) != 0)
		return DB_DONOTINDEX;

	result->data = skey;
	result->size = qdb_len(hd, QDB_KEY, result->data);
	result->flags = 0;
	return 0;
}

int
qdb_twin_assoc(void **skey, void *key, void *value __attribute__((unused))) {
	*skey = key;
	return 0;
}

void
qdb_assoc(unsigned hd, unsigned link, qdb_assoc_t cb)
{
	DB *db = qdb_dbs[hd];
	DB *ldb = qdb_dbs[link];
	qdb_meta_t *meta = &qdb_meta[hd];

	if (!cb) {
		meta->flags |= QH_REPURPOSE;
		cb = qdb_twin_assoc;
	}

	meta->assoc = cb;
	meta->phd = link;

	meta->flags |= QH_SEC;

	if (ldb->associate(ldb, (qdb_meta[hd].flags & QH_TXN)
				? txnl_peek(&qdb_config.txnl) : NULL,
				db, map_assoc, DB_CREATE))

		qdblog_err("qdb_assoc");
}

typedef int get_t(DBC *dbc, DBT *key, DBT *pkey, DBT *data, unsigned flags);

int primary_get(DBC *dbc, DBT *key __attribute__((unused)), DBT *pkey, DBT *data, unsigned flags) {
	return dbc->get(dbc, pkey, data, flags);
}

static inline int
qdb_rem(unsigned hd, void *key_data, void *value_data)
{
	DB *db = qdb_dbs[hd];
	DBT key, data, pkey;
	DBC *cursor;
	int ret;

	qdb_meta_t *meta = &qdb_meta[hd];

	if ((ret = db->cursor(db, hd && (meta->flags & QH_TXN)
					? txnl_peek(&qdb_config.txnl) : NULL,
					&cursor, 0)) != 0)
	{
		qdblog(LOG_ERR, "cursor: %s\n", db_strerror(ret));
		return ret;
	}

	memset(&key, 0, sizeof(DBT));
	memset(&pkey, 0, sizeof(DBT));
	pkey.data = key.data = key_data;
	pkey.size = key.size = qdb_len(hd, QDB_KEY, key_data);
	pkey.flags = key.flags = 0;

	memset(&data, 0, sizeof(DBT));
	data.flags = 0; /* was DB_DBT_MALLOC */
	data.data = value_data;
	data.size = qdb_len(hd, QDB_VALUE, value_data);

	get_t *get = (meta->flags & QH_SEC)
		? cursor->pget
		: primary_get;

	if (!(ret = get(cursor, &key, &pkey, &data, DB_GET_BOTH)))
		ret = cursor->del(cursor, 0);

	cursor->close(cursor);
	if (ret) {
		if (ret != DB_NOTFOUND)
			qdblog(LOG_ERR, "qdb_rem: %s\n", db_strerror(ret));
	} else if ((meta->flags & QH_AINDEX) && !qdb_exists(hd, key_data))
		idm_del(&meta->idm, * (unsigned *) key_data);

	return ret;
}

static inline void qdb_idel(unsigned hd, void *key_r, size_t key_len) {
	DB *db = qdb_dbs[hd];
	DBT key;
	int ret;

	memset(&key, 0, sizeof(key));

	key.data = key_r;
	key.size = key_len;

	ret = db->del(db, (qdb_meta[hd].flags & QH_TXN)
			? txnl_peek(&qdb_config.txnl)
			: NULL, &key, 0);

	if (ret && ret != DB_NOTFOUND)
		qdblog_err("qdb_idel");
}

// FIXME this should return a status code
void qdb_del(unsigned hd, void *key, void *value) {
	if (value != NULL) {
		qdb_rem(hd, key, value);
		return;
	}

	qdb_meta_t *meta = &qdb_meta[hd];
	qdb_cur_t c;

	qdb_type_t *kt = meta->type[QDB_KEY];
	if (kt == &qdb_unsigned) {
		if (meta->flags & QH_AINDEX)
			idm_del(&meta->idm, * (unsigned *) key);
		else if (meta->flags & QH_DUP)
			goto normal;

		qdb_idel(hd, key, sizeof(unsigned));
		return;
	}

normal:
	c = qdb_iter(hd, key);

	while (qdb_next(NULL, NULL, &c))
		qdb_cdel(&c);
}

struct qdb_internal {
	unsigned hd;
	DBT data, key, pkey;
	DBC *cursor;
	get_t *get;
};

qdb_cur_t
qdb_iter(unsigned hd, void *key) {
	DB *db = qdb_dbs[hd];
	qdb_meta_t *meta = &qdb_meta[hd];
	qdb_cur_t cur;
	struct qdb_internal *internal = malloc(sizeof(struct qdb_internal));
	cur.internal = internal;
	internal->hd = hd;
	internal->cursor = NULL;

	memset(&internal->pkey, 0, sizeof(DBT));
	memset(&internal->key, 0, sizeof(DBT));
	memset(&internal->data, 0, sizeof(DBT));

	if (meta->flags & QH_CACHE) {
		qdb_type_t *kt = meta->type[QDB_KEY];

		char *c;
		if (!key) {
			if (meta->flags & QH_DUP) {
				// TODO not supported (kt != vt || kt != qdb_unsigned)
				c = meta->cache;

				cur.flags = QH_CACHE | QH_NOT_FIRST;
				internal->key.data = c;
				internal->key.size = 0;
				internal->data.size = sizeof(unsigned);
				internal->data.data = c + sizeof(unsigned);
				return cur;
			}

			cur.flags = QH_CACHE;
			internal->key.data = meta->cache;
			internal->key.size = meta->type[QDB_KEY]->len;
			internal->data.size = meta->type[QDB_VALUE >> 1]->len;
			internal->data.data = meta->cache + kt->len;
			return cur;
		}

		if (kt != &qdb_unsigned)
			goto normal;

		qdb_type_t *vt = meta->type[QDB_VALUE >> 1];
		unsigned id = * (unsigned *) key;

		if (id == qdb_meta_id) {
			internal->key.data = NULL;
			return cur;
		}

		if (meta->flags & QH_DUP) {
			/* goto normal; // TODO remove this */
			/* if (kt != vt) */
			/* 	goto normal; */

			cur.flags = QH_CACHE | QH_NOT_FIRST;

			c = meta->cache + id * (sizeof(unsigned) + sizeof(struct idmap));

			internal->key.data = c;
			fprintf(stderr, "going contents! %p\n", ((struct idmap *) c + sizeof(unsigned))->map);
			internal->key.size = sizeof(unsigned);
			internal->data.size = 0;
			fprintf(stderr, "contents! %u iter %p - %u %p\n", hd, key, id);
			return cur;
		}

		c = meta->cache
			+ id * (sizeof(unsigned) + vt->len);

		cur.flags = QH_CACHE;
		internal->key.data = c;
		internal->key.size = sizeof(unsigned);
		internal->data.size = vt->len;
		if (* (unsigned *) c != id)
			internal->data.data = NULL;
		else
			internal->data.data = c + sizeof(unsigned);

		return cur;
	}
normal:

	db->cursor(db, hd && (meta->flags & QH_TXN)
			? txnl_peek(&qdb_config.txnl)
			: NULL, &internal->cursor, 0);

	internal->pkey.data = internal->key.data = key;
	internal->key.flags = internal->pkey.flags = internal->data.flags = 0; /* was DB_DBT_MALLOC */
	internal->pkey.size = internal->key.size
		= key ? qdb_len(hd, QDB_KEY, key) : 0;
	internal->get = (meta->flags & QH_SEC)
		? internal->cursor->pget
		: primary_get;
	cur.flags = key ? QH_DUP : 0;
	return cur;
}

void qdb_fin(qdb_cur_t *cur) {
	struct qdb_internal *internal = cur->internal;
	if (!(cur->flags & QH_CACHE))
		internal->cursor->close(internal->cursor);
	free(internal);
	cur->internal = NULL;
}

int
qdb_next(void *key, void *value, qdb_cur_t *cur)
{
	if (cur->flags & QH_CACHE) {
		char *c;
		struct qdb_internal *internal = cur->internal;
		qdb_meta_t *meta = &qdb_meta[internal->hd];
		register size_t kl = meta->type[QDB_KEY]->len,
			 vl = meta->type[QDB_VALUE >> 1]->len,
			 il = kl + vl;
cagain:
		c = internal->key.data;

		if (!c)
			return 0;

		if (cur->flags & QH_NOT_FIRST) {
			vl = sizeof(struct idmap);
			il = kl + vl;
		}

		if (c >= meta->cache + meta->cache_n * il)
			return 0;

		unsigned v;

		v = * (unsigned *) c;

		if (internal->hd == 14)
			fprintf(stderr, "v %u!\n", * (unsigned *) c);

		if (v == qdb_meta_id) {
			c += il;
			internal->key.data = c;
			internal->data.data = c + kl;
			internal->data.size = 0;
			goto cagain;
		}

		if (key)
			memcpy(key, c, kl);

		c += kl;

		if (!(cur->flags & QH_NOT_FIRST)) {
			if (value)
				memcpy(value, c, vl);

			internal->key.data = c + vl;
			return 1;
		}

		struct idmap *idmap = (struct idmap *) internal->key.data + sizeof(unsigned);

		c = (char *) idmap;
		v = idmap->omap[(unsigned) internal->data.size];

		if (internal->hd == 14)
			fprintf(stderr, "iv %u!\n", v);

		if (v == qdb_meta_id) {
			if (internal->key.size) {
				fprintf(stderr, "keyed! ret!\n");
				return 0;
			}
			c += vl;
			internal->key.data = c;
			internal->data.data = c + kl;
			internal->data.size = 0;
			goto cagain;
		}

		v = ((struct idmap *) c)->omap[(unsigned) internal->data.size];

		if (value)
			memcpy(value, &v, sizeof(v));

		if (internal->data.size >= idmap->n) {
			internal->key.data = (char *) internal->key.data + sizeof(unsigned) + sizeof(struct idmap);
			internal->data.size = 0;
		} else
			internal->data.size++;

		return 1;
	}

	struct qdb_internal *internal;
	ssize_t ret;
	int flags;
	internal = cur->internal;
	flags = DB_FIRST;

again:
	if (cur->flags & QH_DUP) {
		if (cur->flags & QH_NOT_FIRST)
			flags = DB_NEXT_DUP;
		else
			flags = DB_SET;
	} else if (cur->flags & QH_NOT_FIRST)
		flags = DB_NEXT;
	cur->flags |= QH_NOT_FIRST;

	if ((ret = internal->get(internal->cursor, &internal->key, &internal->pkey, &internal->data, flags))) {
		if (ret != DB_NOTFOUND)
			qdblog(LOG_ERR, "qdb_next: %u %d %s\n", internal->hd, cur->flags, db_strerror(ret));
		internal->cursor->close(internal->cursor);
		free(internal);
		cur->internal = NULL;
		return 0;
	}

	if (* (unsigned *) internal->pkey.data == qdb_meta_id)
		goto again;

	if (key) {
		memcpy(key, internal->pkey.data, internal->pkey.size);
		memcpy(value, internal->data.data, internal->data.size);
	}

	memset(&internal->key, 0, sizeof(DBT));
	memset(&internal->pkey, 0, sizeof(DBT));
	memset(&internal->data, 0, sizeof(DBT));

	return 1;
}

int
qdb_cdel(qdb_cur_t *cur) {
	struct qdb_internal *internal = cur->internal;
	return internal->cursor->c_del(internal->cursor, 0);
}

int qdb_drop(unsigned hd) {
	DB *db = qdb_dbs[hd];
	DBT key, data;
	DBC *cursor;
	int ret, flags = DB_FIRST;

	if ((ret = db->cursor(db, (qdb_meta[hd].flags & QH_TXN)
					? txnl_peek(&qdb_config.txnl) : NULL,
					&cursor, 0)) != 0)
	{
		qdblog(LOG_ERR, "qdb_drop: cursor: %s\n", db_strerror(ret));
		return ret;
	}

	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	data.flags = DB_DBT_MALLOC;

	while (!(cursor->get(cursor, &key, &data, flags))) {
		flags = DB_NEXT;
		cursor->del(cursor, 0);
	}

	cursor->close(cursor);
	return 0;
}

void
_qdb_close(unsigned hd, unsigned flags) {
	DB *db = qdb_dbs[hd];
	db->close(db, flags);
	qdb_dbs[hd] = NULL;
}

void
qdb_close(unsigned hd, unsigned flags) {
	if (qdb_meta[hd].flags & QH_THRICE) {
		_qdb_close(hd + 2, flags);
		_qdb_close(hd + 1, flags);
	}
	_qdb_close(hd, flags);
	idm_del(&idm, hd);
}

void
qdb_sync(unsigned hd) {
	DB *db = qdb_dbs[hd];
	db->sync(db, 0);
}

unsigned idm_new(struct idm *idm) {
	unsigned ret = idml_pop(&idm->free);

	if (ret == (unsigned) -1)
		return idm->last++;

	return ret;
}

void qdb_env_open(DB_ENV *env, char *path, unsigned flags) {
	unsigned iflags = DB_CREATE | DB_INIT_MPOOL;

	if (flags & QH_THREAD) {
		iflags |= DB_THREAD | DB_INIT_LOCK;
		qdb_config.flags |= QH_THREAD;
	} else
		iflags |= DB_PRIVATE;

	if (flags & QH_TXN) {
		env->set_flags(env, DB_AUTO_COMMIT, 1);
		env->set_tx_max(env, 5 * 60);
		iflags |= DB_INIT_TXN | DB_RECOVER;
	}

	if (path == NULL) {
		iflags |= DB_PRIVATE;
	} else {
		struct stat st;
		if (stat(path, &st) != 0)
			mkdir(path, 0755);
		iflags |= DB_INIT_LOG;
	}

	env->open(env, path, iflags, 0);
	qdb_config.env = env;
}
