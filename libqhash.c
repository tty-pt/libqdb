#include "./include/qhash.h"
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <syslog.h>

#define HASH_NOT_FOUND NULL

enum {
	QH_NOT_FIRST = 1, // internal iteration flag
};

qdb_meta_t qdb_meta[HASH_DBS_MAX];
static DB *qdb_dbs[HASH_DBS_MAX];
unsigned types_hd = HASH_DBS_MAX - 1;

struct qdb_config qdb_config = {
	.mode = 0644,
	.type = DB_HASH,
	.file = NULL
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

log_t hashlog = qdb_logger_stderr;

static inline void hashlog_perror(char *str) {
        hashlog(LOG_ERR, "%s: %s", str, strerror(errno));
}

static inline void hashlog_err(char *str) {
        hashlog_perror(str);
        exit(EXIT_FAILURE);
}

void qdb_set_logger(log_t logger) {
	hashlog = logger;
}

int
qdb_putc(unsigned hd, void *key_r, size_t key_len, void *value, size_t value_len) {
	unsigned id;
	DB *db = qdb_dbs[hd];
	DBT key, data;
	int ret;

	qdb_meta_t *meta = &qdb_meta[hd];

	if ((meta->flags & QH_AINDEX) && (id = * (unsigned *) key_r) != (unsigned) -2) {
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
		hashlog_err("qdb_putc get_flags");
	dupes = flags & DB_DUP;

	ret = db->put(db, hd && (meta->flags & QH_TXN) ? txnid : NULL, &key, &data, 0);
	if (ret && (ret != DB_KEYEXIST || !dupes))
		hashlog(LOG_WARNING, "qdb_putc");
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

	ret = db->get(db, hd && (meta->flags & QH_TXN) ? txnid : NULL, &key, &data, 0);

	if (ret == DB_NOTFOUND)
		return NULL;
	else if (ret)
		hashlog_err("__qdb_get");

	*size = data.size;
	return data.data;
}

unsigned _qdb_lopenc(unsigned hd, qdb_type_t *value_type) {
	unsigned ign, last = 0;
	qdb_meta_t *meta = &qdb_meta[hd];
	char buf[value_type->len];
	SLIST_INIT(&meta->idm.free);
	qdb_cur_t c = qdb_iter(hd, NULL);

	while (qdb_next(&ign, buf, &c)) if (ign >= last)
		last = ign + 1;

	meta->idm.last = last;
	for (last = 0; last < meta->idm.last; last++)
		if (!qdb_existsc(hd, &last, sizeof(last)))
			idml_push(&meta->idm.free, last);

	return hd;
}

void
qdb_init(void) {
	idm = idm_init();
	idm_new(&idm);

	if (db_create(&qdb_dbs[0], NULL, 0))
		hashlog_err("qdb_init: db_create ids_db");

	if (qdb_dbs[0]->open(qdb_dbs[0], NULL, NULL, NULL, DB_HASH, DB_CREATE, 644))
		hashlog_err("qdb_init: open ids_db");

	memset(qdb_meta, 0, sizeof(qdb_meta));
	qdb_meta[0].type[QDB_KEY] = &qdb_ptr;
	qdb_meta[0].type[QDB_VALUE] = &qdb_unsigned;

	if (db_create(&qdb_dbs[types_hd], NULL, 0))
		hashlog_err("qdb_init: db_create types");

	if (qdb_dbs[types_hd]->open(qdb_dbs[types_hd], NULL, NULL, NULL, DB_HASH, DB_CREATE, 644))
		hashlog_err("qdb_init: open types");

	qdb_meta[types_hd].type[QDB_KEY] = &qdb_string;
	qdb_meta[types_hd].type[QDB_VALUE] = &qdb_ptr;
	qdb_regc("s", &qdb_string);
	qdb_regc("u", &qdb_unsigned);
	qdb_reg("p", sizeof(void *));
	qdb_reg("ul", sizeof(unsigned long));
}

unsigned
qdb_openc(const char *file, const char *database, int mode, unsigned flags, int type, char *key_tid, char *value_tid)
{
	DB *db;
	unsigned id, dbflags = 0;
	qdb_type_t *key_type = NULL, *value_type = NULL;

	if (qdb_get(types_hd, &key_type, key_tid))
		hashlog_err("qdb_open: key type was not registered");

	if (qdb_get(types_hd, &value_type, value_tid))
		hashlog_err("qdb_open: value type was not registered");

	qdb_first = 0;
	id = idm_new(&idm);

	if (db_create(&qdb_dbs[id], qdb_config.env, 0))
		hashlog_err("qdb_openc: db_create");

	// this is needed for associations
	db = qdb_dbs[id];
	qdb_put(0, &qdb_dbs[id], &id);

	if (flags & QH_DUP)
		if (db->set_flags(db, DB_DUP))
			hashlog_err("qdb_openc: set_flags");

	dbflags = (qdb_config.env ? DB_THREAD : 0) | (flags & QH_RDONLY ? DB_RDONLY : DB_CREATE);
	if (db->open(db, (flags & QH_TXN) ? txnid : NULL, file, database, type, dbflags, mode))
		hashlog_err("qdb_openc: open");

	if (flags & QH_SEC)
		goto out;

	size_t val_len;
	unsigned minus_two = (unsigned) -2;
	qdb_smeta_t *smeta = 
		qdb_getc(id, &val_len, &minus_two, sizeof(minus_two));

	if (smeta) {
		qdb_get(types_hd, &key_type, smeta->key);
		qdb_get(types_hd, &value_type, smeta->value);
		qdb_meta[id].type[QDB_KEY] = key_type;
		qdb_meta[id].type[QDB_VALUE] = value_type;
		flags |= smeta->flags;
	} else {
		qdb_smeta_t put_smeta = {
			.flags = flags,
			.extra = 0,
		};

		strcpy(put_smeta.key, key_tid);
		strcpy(put_smeta.value, value_tid);

		qdb_putc(id, &minus_two, sizeof(minus_two),
				&put_smeta, sizeof(put_smeta));
	}

	if (flags & QH_AINDEX)
		_qdb_lopenc(id, value_type);

out:
	qdb_meta[id].flags = flags;
	qdb_meta[id].type[QDB_KEY] = key_type;
	qdb_meta[id].type[QDB_VALUE] = value_type;

	/* fprintf(stderr, "open %u %s %s %u %p %lu %u\n", id, file, database, flags, (void *) smeta, key_type->len, qdb_meta[id].idm.last); */

	return id;
}

int qdb_pget(unsigned hd, void *pkey_r, void *key_r) {
	DB *db = qdb_dbs[hd];
	DBT key, pkey, data;
	int ret;

	memset(&key, 0, sizeof(key));
	memset(&pkey, 0, sizeof(pkey));
	memset(&data, 0, sizeof(data));

	key.data = (void *) key_r;
	key.size = qdb_len(hd, QDB_KEY, key_r);
	pkey.flags = data.flags = DB_DBT_MALLOC;

	ret = db->pget(db, (qdb_meta[hd].flags & QH_TXN) ? txnid : NULL, &key, &pkey, &data, 0);

	if (ret == DB_NOTFOUND)
		return 1;
	else if (ret)
		hashlog_err("qdb_pget");

	memcpy(pkey_r, pkey.data, pkey.size);
	return 0;
}

int
map_assoc(DB *sec, const DBT *key, const DBT *data, DBT *result)
{
	memset(result, 0, sizeof(DBT));
	result->flags = DB_DBT_MALLOC;
	unsigned hd;
	qdb_get(0, &hd, &sec); // assumed success (0)
	qdb_meta[hd].assoc(&result->data, &result->size, key->data, data->data);
	return 0;
}

void
qdb_assoc(unsigned hd, unsigned link, qdb_assoc_t cb)
{
	DB *db = qdb_dbs[hd];
	DB *ldb = qdb_dbs[link];
	qdb_meta[hd].assoc = cb;
	qdb_meta[hd].flags |= QH_SEC;

	if (ldb->associate(ldb, (qdb_meta[hd].flags & QH_TXN) ? txnid : NULL, db, map_assoc, DB_CREATE | DB_IMMUTABLE_KEY))
		hashlog_err("qdb_assoc");
}

int
qdb_rem(unsigned hd, void *key_data, void *value_data)
{
	DB *db = qdb_dbs[hd];
	DBT key, data;
	DBC *cursor;
	int ret, flags = DB_SET;

	qdb_meta_t *meta = &qdb_meta[hd];

	if ((ret = db->cursor(db, hd && (meta->flags & QH_TXN) ? txnid : NULL, &cursor, 0)) != 0) {
		hashlog(LOG_ERR, "cursor: %s", db_strerror(ret));
		return ret;
	}

	memset(&key, 0, sizeof(DBT));
	key.data = key_data;
	key.size = qdb_len(hd, QDB_KEY, key_data);
	memset(&data, 0, sizeof(DBT));
	data.flags = DB_DBT_MALLOC;
	data.data = value_data;
	size_t supposed_size = data.size
		= qdb_len(hd, QDB_VALUE, value_data);

	while (!(ret = cursor->get(cursor, &key, &data, flags))) {
		flags = DB_NEXT_DUP;

		if (data.size == supposed_size && memcmp(data.data, value_data, supposed_size) == 0) {
			ret = cursor->del(cursor, 0);
			break;
		}
	}

	cursor->close(cursor);
	return ret == DB_NOTFOUND ? 0 : ret;
}

typedef int get_t(DBC *dbc, DBT *key, DBT *pkey, DBT *data, unsigned flags);

int primary_get(DBC *dbc, DBT *key __attribute__((unused)), DBT *pkey, DBT *data, unsigned flags) {
	return dbc->get(dbc, pkey, data, flags);
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
	db->cursor(db, hd && (meta->flags & QH_TXN) ? txnid : NULL, &internal->cursor, 0);
	memset(&internal->pkey, 0, sizeof(DBT));
	memset(&internal->key, 0, sizeof(DBT));
	memset(&internal->data, 0, sizeof(DBT));
	internal->pkey.data = internal->key.data = key;
	internal->key.flags = internal->pkey.flags = internal->data.flags = DB_DBT_MALLOC;
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

static inline int
_qdb_next(void *key, void *value, qdb_cur_t *cur)
{
	struct qdb_internal *internal;
	ssize_t ret;
	int flags;
	internal = cur->internal;
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
			hashlog(LOG_ERR, "qdb_next: %u %d %s", internal->hd, cur->flags, db_strerror(ret));
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
qdb_next(void *key, void *value, qdb_cur_t *cur)
{
	int ret;

	while ((ret = _qdb_next(key, value, cur))) {
		switch (* (unsigned *) key) {
			case (unsigned) -2:
				continue;
			default: break;
		}
		break;
	}

	return ret;
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

	if ((ret = db->cursor(db, (qdb_meta[hd].flags & QH_TXN) ? txnid : NULL, &cursor, 0)) != 0) {
		hashlog(LOG_ERR, "qdb_drop: cursor: %s\n", db_strerror(ret));
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
qdb_close(unsigned hd, unsigned flags) {
	DB *db = qdb_dbs[hd];
	db->close(db, flags);
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
