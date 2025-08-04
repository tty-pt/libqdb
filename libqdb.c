#include "./include/qdb.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/queue.h>

enum {
	/* this is used for iteration */
	QH_NOT_FIRST = 1,

	/* repurpose the primary db with different types */
	QH_REPURPOSE = 64, // internal iteration flag
};

static DB *qdb_dbs[QDB_DBS_MAX];
unsigned types_hd = QDB_DBS_MAX - 1, qdb_min = 0;
struct txnl txnl_empty;

struct qdb_config qdb_config = {
	.mode = 0644,
	.type = DB_HASH,
	.file = NULL,
	.env = NULL,
};

struct idmap {
	unsigned *map, // buckets point to omap positions
		 *omap; // these map to ids

	char	 *vmap; // these map to values

	unsigned m, n;

	size_t value_len;
};

/* in-memory metadata for each db */
typedef struct meta {
	unsigned flags, phd;
	qdb_assoc_t assoc;
	struct idm idm;
	char type_str[2][8];
	qdb_type_t *type[2];
	struct idmap cache;
} qdb_meta_t;

qdb_meta_t qdb_meta[QDB_DBS_MAX];

static inline struct idmap idmap_init(size_t value_len) {
	struct idmap idmap = {
		.map = malloc(4 * sizeof(unsigned)),
		.omap = malloc(4 * sizeof(unsigned)),
		.vmap = malloc(4 * value_len),
		.m = 4, .n = 0,
		.value_len = value_len,
	};
	memset(idmap.map, 0, 4 * sizeof(unsigned));
	memset(idmap.omap, 0, 4 * sizeof(unsigned));
	memset(idmap.vmap, 0, 4 * value_len);
	return idmap;
}

static inline void idmap_put(struct idmap *idmap, unsigned id, void *value) {
	if (idmap->m <= id) {
		idmap->m = id;
		idmap->m *= 2; // technically this could give a number smaller
			       // than last idmap->m, this just doesn't happen
			       // because we have a protection against insertion
			       // of very large values as idmap keys
		idmap->map = realloc(idmap->map, idmap->m * sizeof(unsigned));
		idmap->omap = realloc(idmap->omap, idmap->m * sizeof(unsigned));
		idmap->vmap = realloc(idmap->vmap, idmap->m * idmap->value_len);
	}

	unsigned n = idmap->map[id];

	if (idmap->n > n && idmap->omap[n] == id) {
		memcpy(idmap->vmap + id * idmap->value_len, value, idmap->value_len);
		return;
	}

	idmap->omap[idmap->n] = id;
	idmap->map[id] = idmap->n;
	memcpy(idmap->vmap + id * idmap->value_len, value, idmap->value_len);
	idmap->n++;
}

static inline void idmap_del(struct idmap *idmap, unsigned id) {
	if (idmap->m < id)
		return; // not present

	unsigned n = idmap->map[id];

	if (n > idmap->n)
		return; // not present

	memmove(&idmap->omap[n], &idmap->omap[n + 1],
			(idmap->n - n - 1) * sizeof(unsigned));

	idmap->map[id] = idmap->n;
	idmap->n--;
	for (; n < idmap->n; n++) {
		unsigned id = idmap->omap[n];
		idmap->map[id] = n;
	}
}

static inline void *idmap_get(struct idmap *idmap, unsigned id, void *value __attribute__((unused))) {
	if (!idmap->n || id >= idmap->m)
		return NULL;

	unsigned n = idmap->map[id];

	if (n >= idmap->n || idmap->omap[n] != id)
		return NULL;

	return idmap->vmap + id * idmap->value_len;
}

static inline void *idmap_nth(struct idmap *idmap, unsigned n) {
	if (n >= idmap->n)
		return NULL;

	unsigned id = idmap->omap[n];
	return idmap->vmap + id * idmap->value_len;
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

static int is_cached(qdb_meta_t *meta) {
	if (meta->flags & QH_THREAD)
		return 0;

	if (meta->type[QDB_KEY] != &qdb_unsigned)
		return 0;

	return meta->type[QDB_VALUE >> 1]->len;
}

// should only be called for unsigned keys
static inline void
cached_put(qdb_meta_t *meta, void *key_r, void *value) {
	unsigned id = * (unsigned *) key_r;

	if (!(meta->flags & QH_DUP)) {
		idmap_put(&meta->cache, id, value);
		return;
	}

	// vt must be unsigned for now
	unsigned idv = * (unsigned *) value;
	struct idmap *idmap_r = idmap_get(&meta->cache, id, NULL);
	if (!idmap_r) {
		qdb_type_t *vt = meta->type[QDB_VALUE >> 1];
		struct idmap idmap = idmap_init(vt->len);
		idmap_put(&idmap, idv, &idv);
		idmap_put(&meta->cache, id, &idmap);
		return;
	}

	idmap_put(idmap_r, idv, &idv);
}

int
qdb_putc(unsigned hd, void *key_r, size_t key_len, void *value, size_t value_len) {
	qdb_meta_t *meta = &qdb_meta[hd];

	DB *db = qdb_dbs[hd];
	DBT key, data;
	int ret;

	if (meta->flags & QH_AINDEX) {
		unsigned id = * (unsigned *) key_r;

		if (meta->idm.last < id)
			meta->idm.last = id;

		// this is for lhash put. It's to ensure there are free numbers in between.
		// let's deactivate it for now because it's causing errors atm
		unsigned last;
		for (last = meta->idm.last; last < id; last++)
			idml_push(&meta->idm.free, last);
	}

	if ((meta->flags & QH_TMP) && is_cached(meta)) {
		cached_put(meta, key_r, value);
		return 0;
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
	} else if (is_cached(meta))
		cached_put(meta, key_r, value);

	return ret;
}

static inline void *
cached_get(qdb_meta_t *meta, unsigned id, size_t *size) {
	void *val;

	if (!(meta->flags & QH_DUP))
		val = idmap_get(&meta->cache, id, NULL);
	else {
		struct idmap *idmap = idmap_get(&meta->cache, id, NULL);

		if (!idmap)
			return NULL;

		val = idmap_nth(idmap, 0);
	}

	if (val)
		*size = meta->type[QDB_VALUE >> 1]->len;

	return val;
}

void *qdb_getc(unsigned hd, size_t *size, void *key_r, size_t key_len)
{
	DB *db;
	DBT key, data;
	int ret;

	qdb_meta_t *meta = &qdb_meta[hd];

	if (is_cached(meta))
		return cached_get(meta, * (unsigned *) key_r, size);
	
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
	qdb_regc("p", &qdb_ptr);
	qdb_reg("ul", sizeof(unsigned long));
	qdb_unsigned.dbl = "ul";
	qdb_config.mode = 0644;
	qdb_config.type = DB_HASH;
	qdb_config.file = NULL;
	qdb_config.flags = 0;
	qdb_config.txnl = txnl_init();
	txnl_empty = txnl_init();
}

unsigned
_qdb_openc(const char *file, const char *database, int mode, unsigned flags, int type, char *key_tid, char *value_tid)
{
	DB *db;
	unsigned id, dbflags = 0;
	DB_TXN *txn = NULL; // local transaction just for open
	qdb_type_t *key_type = NULL, *value_type = NULL;
	qdb_meta_t *meta;

	if ((flags & QH_AINDEX) && strcmp(key_tid, "u"))
		qdblog_err("qdb_openc: AINDEX without 'u' key\n");

	if (qdb_get(types_hd, &key_type, key_tid))
		qdblog_err("qdb_openc: key type was not registered\n");

	if (qdb_get(types_hd, &value_type, value_tid))
		qdblog_err("qdb_openc: value type was not registered\n");

	qdb_first = 0;
	id = idm_new(&idm);
	id += qdb_min;

	if (!file)
		flags |= QH_TMP;

	if (flags & QH_TMP) {
		file = NULL;
		database = NULL;
	}

	meta = &qdb_meta[id];
	strcpy(meta->type_str[QDB_KEY], key_tid);
	strcpy(meta->type_str[QDB_VALUE >> 1], value_tid);
	meta->phd = (unsigned) -1;
	meta->flags = flags;
	meta->type[QDB_KEY] = key_type;
	meta->type[QDB_VALUE >> 1] = value_type;

#if QDB_DEBUG
	fprintf(stderr, "qdb_openc? %u %s %s %u %s %s %u\n",
			id, file, database,
			flags, key_tid, value_tid, flags);
#endif

	if ((meta->flags & QH_TMP) && is_cached(meta))
		goto open_skip;

	if (db_create(&qdb_dbs[id], qdb_config.env, 0))
		qdblog_err("qdb_openc: db_create\n");

	// this is needed for associations
	db = qdb_dbs[id];
	qdb_put(0, &qdb_dbs[id], &id);

	if (flags & QH_DUP && !(flags & QH_THRICE))
		if (db->set_flags(db, DB_DUP))
			qdblog_err("qdb_openc: set_flags\n");

	dbflags = qdb_config.flags & QH_THREAD ? DB_THREAD : 0;
	if (file && access(file, R_OK) == 0 && (flags & QH_RDONLY))
		dbflags |= DB_RDONLY;
	else
		dbflags |= DB_CREATE;

	if (db->open(db, txn, file, database, type, dbflags, mode))
		qdblog_err("qdb_openc: open\n");

open_skip:
	if (flags & (QH_SEC | QH_REPURPOSE))
		goto out;

	if (flags & QH_AINDEX)
		_qdb_lopenc(id, value_type);

out:
	if (is_cached(meta))
		meta->cache = idmap_init(meta->flags & QH_DUP
				? sizeof(struct idmap)
				: value_type->len);

#ifdef QDB_DEBUG
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
	char buf[BUFSIZ], *backup_key_tid = key_tid;

	if (file && access(file, R_OK) && (flags & QH_RDONLY))
		flags |= QH_TMP;

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

	flags &= ~(QH_THRICE | QH_AINDEX);
	flags |= QH_DUP | QH_SEC;
	if (read_only)
		flags |= QH_RDONLY;

	// for secondaries, we don't need dbl keys
	if (flags & QH_DUP) {
		key_tid = value_tid;
		value_tid = backup_key_tid;
	}

	snprintf(buf, sizeof(buf), "%shd", database);
	_qdb_openc(file, buf, mode, flags, DB_BTREE, key_tid, value_tid);
	qdb_assoc(phd + 1, phd, NULL);

	flags |= QH_PGET;
	snprintf(buf, sizeof(buf), "%srhd", database);
	_qdb_openc(file, buf, mode, flags, DB_BTREE, value_tid, key_tid);
	qdb_assoc(phd + 2, phd, qdb_assoc_rhd);
	return phd;
}

void *qdb_pgetc(unsigned hd, size_t *len, void *key_r) {
	unsigned flags = qdb_meta[hd].flags;

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

static inline void cached_del(qdb_meta_t *meta, unsigned id, void *value)
{
	if (!(meta->flags & QH_DUP)) {
		idmap_del(&meta->cache, id); 
		return;
	}

	struct idmap *idmap = idmap_get(&meta->cache, id, NULL);

	if (!idmap || !idmap->n)
		return; // not present

	// TODO if !value

	unsigned vid = * (unsigned *) value;
	idmap_del(idmap, vid);
}

// FIXME this should return a status code
void qdb_del(unsigned hd, void *key, void *value) {
	qdb_meta_t *meta = &qdb_meta[hd];

	if (is_cached(meta)) {
		cached_del(meta, * (unsigned *) key, value);
		if (meta->flags & QH_TMP)
			return;
	}

	if (value != NULL) {
		qdb_rem(hd, key, value);
		return;
	}

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

	if (is_cached(meta)) {
		internal->data.size = 0;

		if (!key) {
			internal->key.data = NULL;
			internal->key.size = 0;
			return cur;
		}

		unsigned id = * (unsigned *) key;
		internal->key.data = key;
		internal->key.size = id > meta->cache.m
			? meta->cache.m
			: meta->cache.map[id];
		return cur;
	}

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
	if (!is_cached(&qdb_meta[internal->hd]))
		internal->cursor->close(internal->cursor);
	free(internal);
	cur->internal = NULL;
}

static inline int
cached_next(qdb_meta_t *meta, struct qdb_internal *internal, void *key, void *value) {
	unsigned n;
cagain:
	n = internal->key.size;

	if (n >= meta->cache.n)
		return 0;

	unsigned id = meta->cache.omap[n];

	// keyed search
	if (internal->key.data) {
		if (id != * (unsigned *) internal->key.data)
			return 0;

		if (meta->flags & QH_DUP) {
			struct idmap *idmap = idmap_get(&meta->cache, id, NULL);
			unsigned *nth = idmap_nth(idmap, internal->data.size);
			if (!nth)
				return 0;
			memcpy(value, nth, sizeof(unsigned));
			internal->data.size++;
			return 1;
		}
	}

	if (meta->flags & QH_DUP) {
		struct idmap *idmap = idmap_get(&meta->cache, id, NULL);
		unsigned *nth = idmap_nth(idmap, internal->data.size);

		if (!nth) {
			internal->key.size++;
			internal->data.size = 0;
			goto cagain;
		}

		memcpy(value, nth, sizeof(unsigned));
		internal->data.size++;
		return 1;
	}

	memcpy(key, &id, sizeof(id));
	memcpy(value, meta->cache.vmap + id * meta->cache.value_len, meta->cache.value_len);

	internal->key.size++;
	return 1;
}

int
qdb_next(void *key, void *value, qdb_cur_t *cur)
{
	struct qdb_internal *internal = cur->internal;
	ssize_t ret;
	int flags;
	qdb_meta_t *meta = &qdb_meta[internal->hd];

	if (is_cached(meta)) {
		if (!cached_next(meta, internal, key, value)) {
			free(internal);
			cur->internal = NULL;
			return 0;
		}
		return 1;
	}

	flags = DB_FIRST;

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
	if (!internal->cursor) {
		qdblog(LOG_ERR, "qdb_cdel: tried deleting with cursor when not set");
		return 1;
	}
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

size_t qdb_len(unsigned hd, unsigned type, void *key) {
	if (!key)
		return 0;
	type >>= 1;
	qdb_type_t *mthing = qdb_meta[hd].type[type];
	return mthing->len ? mthing->len : mthing->measure(key);
}

char *qdb_type(unsigned hd, unsigned type) {
	unsigned otype = type;
	type >>= 1;
	if (otype & QDB_REVERSE)
		type = !type;
	if (qdb_meta[hd].flags & QH_THRICE)
		hd += 1;
	return qdb_meta[hd].type_str[type];
}

void qdb_print(unsigned hd, unsigned type, void *thing) {
	unsigned otype = type;
	type >>= 1;
	if (otype & QDB_REVERSE)
		type = !type;
	if (qdb_meta[hd].flags & QH_THRICE)
		hd += 1;
	qdb_meta[hd].type[type]->print(thing);
}

unsigned
qdb_put(unsigned hd, void *key, void *value)
{
	size_t key_len, value_len;
	unsigned id = 0, flags = qdb_meta[hd].flags;

	if (key != NULL) {
		key_len = qdb_len(hd, QDB_KEY, key);
		if (!strcmp(qdb_type(hd, QDB_KEY), "u"))
			id = * (unsigned *) key;

		if (id > (((unsigned) -1) >> 7)) {
			qdblog(LOG_WARNING, "qdb_put %u BAD ID\n", hd);
			raise(SIGTRAP);
			return QDB_NOTFOUND;
		}

		if ((flags & QH_THRICE) && (flags & QH_DUP)) {
			key_len = qdb_len(hd + 2, QDB_KEY, key);
			value_len = qdb_len(hd + 2, QDB_VALUE, value);
			char buf[key_len + value_len];
			memcpy(buf, key, key_len);
			memcpy(buf + key_len, value, value_len);
			return qdb_putc(hd, buf, key_len + value_len, value, value_len);
		}
	} else if (qdb_meta[hd].flags & QH_AINDEX) {
		id = idm_new(&qdb_meta[hd].idm);
		key = &id;
		key_len = sizeof(unsigned);
	} else
		qdblog_err("qdb_put NULL key without QH_AINDEX");

	value_len = qdb_len(hd, QDB_VALUE, value);
	if (qdb_putc(hd, key, key_len, value, value_len))
		return QDB_NOTFOUND;

	return id;
}

unsigned qdb_flags(unsigned hd) {
	return qdb_meta[hd].flags;
}

void
qdb_reg(char *key, size_t len) {
	qdb_type_t *type = (qdb_type_t *) malloc(sizeof(qdb_type_t));
	type->measure = NULL;
	type->print = NULL;
	type->len = len;
	type->dbl = NULL;
	qdb_put(types_hd, key, &type);
}

/* get the first value for a given key (type aware) */
int qdb_get(unsigned hd, void *value, void *key)
{
	size_t size;
	void *value_r;

	if (qdb_meta[hd].flags & QH_PGET)
		value_r = qdb_pgetc(hd, &size, key);
	else
		value_r = qdb_getc(hd, &size, key, qdb_len(hd, QDB_KEY, key));

	if (!value_r)
		return 1;

	memcpy(value, value_r, size);
	return 0;
}
