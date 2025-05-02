#include "./include/qhash.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/queue.h>

#define QDB_NOT_FOUND NULL

enum {
	QH_NOT_FIRST = 1, // internal iteration flag
};

qdb_meta_t qdb_meta[QDB_DBS_MAX];
static DB *qdb_dbs[QDB_DBS_MAX];
unsigned types_hd = QDB_DBS_MAX - 1, qdb_meta_id = -2;
struct txnl txnl_empty;

struct qdb_config qdb_config = {
	.mode = 0644,
	.type = DB_HASH,
	.file = NULL,
	.env = NULL,
};

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

	if (ret && (ret != DB_KEYEXIST || !dupes))
		qdblog(LOG_WARNING, "qdb_putc");
	return ret;
}

void *qdb_getc(unsigned hd, size_t *size, void *key_r, size_t key_len)
{
	DB *db = qdb_dbs[hd];
	DBT key, data;
	int ret;

	qdb_meta_t *meta = &qdb_meta[hd];

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
	qdb_meta[0].type[QDB_VALUE] = &qdb_unsigned;

	if (db_create(&qdb_dbs[types_hd], NULL, 0))
		qdblog_err("qdb_init: db_create types");

	if (qdb_dbs[types_hd]->open(qdb_dbs[types_hd], NULL, NULL, NULL, DB_HASH, DB_CREATE, 644))
		qdblog_err("qdb_init: open types");

	qdb_meta[types_hd].type[QDB_KEY] = &qdb_string;
	qdb_meta[types_hd].type[QDB_VALUE] = &qdb_ptr;
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

unsigned
_qdb_openc(const char *file, const char *database, int mode, unsigned flags, int type, char *key_tid, char *value_tid)
{
	DB *db;
	unsigned id, dbflags = 0;
	DB_TXN *txn = NULL; // local transaction just for open

	qdb_type_t *key_type = NULL, *value_type = NULL;

	if (qdb_get(types_hd, &key_type, key_tid))
		qdblog_err("qdb_open: key type was not registered");

	if (qdb_get(types_hd, &value_type, value_tid))
		qdblog_err("qdb_open: value type was not registered");

	qdb_first = 0;
	id = idm_new(&idm);

	if (db_create(&qdb_dbs[id], qdb_config.env, 0))
		qdblog_err("qdb_openc: db_create");

	// this is needed for associations
	db = qdb_dbs[id];
	qdb_put(0, &qdb_dbs[id], &id);

	if (flags & QH_DUP && !(flags & QH_THRICE))
		if (db->set_flags(db, DB_DUP))
			qdblog_err("qdb_openc: set_flags");

	if (flags & QH_TXN)
		txn = qdb_begin();

	dbflags = (qdb_config.env ? DB_THREAD : 0) | (flags & QH_RDONLY ? DB_RDONLY : DB_CREATE);
	if (db->open(db, txn, file, database, type, dbflags, mode))
		qdblog_err("qdb_openc: open");

	if (flags & QH_TXN)
		qdb_commit();

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
	strcpy(qdb_meta[id].type_str[QDB_VALUE], value_tid);
	qdb_meta[id].phd = qdb_meta_id;
	qdb_meta[id].flags = flags;
	qdb_meta[id].type[QDB_KEY] = key_type;
	qdb_meta[id].type[QDB_VALUE] = value_type;

#if 0
	fprintf(stderr, "open %u %s %s %u %s %s\n",
			id, file, database,
			flags, key_tid, value_tid);
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
		qdb_get(types_hd, &vtype, value_tid);
		key_tid = vtype->dbl;
	}

	snprintf(buf, sizeof(buf), "%sphd", database);
	unsigned phd = _qdb_openc(file, buf, mode, flags, DB_HASH, key_tid, value_tid);

	qdb_smeta_t *smeta = qdb_getc(phd, &len, &qdb_meta_id, sizeof(qdb_meta_id));
	key_tid = smeta->key;
	value_tid = smeta->value;
	flags = smeta->flags;
	flags &= ~(QH_THRICE | QH_AINDEX);
	flags |= QH_DUP | QH_SEC;

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

	if (* (unsigned *) key->data == (unsigned) -2)
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

int
qdb_rem(unsigned hd, void *key_data, void *value_data)
{
	DB *db = qdb_dbs[hd];
	DBT key, data, pkey;
	DBC *cursor;
	int ret, flags = DB_SET;

	qdb_meta_t *meta = &qdb_meta[hd];

	if ((ret = db->cursor(db, hd && (meta->flags & QH_TXN)
					? txnl_peek(&qdb_config.txnl) : NULL,
					&cursor, 0)) != 0)
	{
		qdblog(LOG_ERR, "cursor: %s", db_strerror(ret));
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

	while (!(ret = get(cursor, &key, &pkey, &data, flags))) {
		flags = DB_NEXT_DUP;

		if (!memcmp(pkey.data, &qdb_meta_id, sizeof(qdb_meta_id)))
			continue;

		break;
	}

	ret = cursor->del(cursor, 0);

	cursor->close(cursor);
	if (ret)
		qdblog(LOG_ERR, "qdb_rem: %s", db_strerror(ret));
	return 0;
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
	qdb_cur_t cur;
	struct qdb_internal *internal = malloc(sizeof(struct qdb_internal));
	cur.internal = internal;
	internal->hd = hd;
	internal->cursor = NULL;
	qdb_meta_t *meta = &qdb_meta[hd];

	db->cursor(db, hd && (meta->flags & QH_TXN)
			? txnl_peek(&qdb_config.txnl)
			: NULL, &internal->cursor, 0);

	memset(&internal->pkey, 0, sizeof(DBT));
	memset(&internal->key, 0, sizeof(DBT));
	memset(&internal->data, 0, sizeof(DBT));
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
	internal->cursor->close(internal->cursor);
	free(internal);
	cur->internal = NULL;
}

int
qdb_next(void *key, void *value, qdb_cur_t *cur)
{
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
			qdblog(LOG_ERR, "qdb_next: %u %d %s", internal->hd, cur->flags, db_strerror(ret));
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
	unsigned iflags = DB_CREATE | DB_INIT_MPOOL | DB_INIT_LOCK | DB_INIT_LOG | DB_THREAD;

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
	}

	env->open(env, path, iflags, 0);
	qdb_config.env = env;
}
