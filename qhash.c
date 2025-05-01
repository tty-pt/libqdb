/* FIXME
 * lookup is not working correctly
 */
#include "./include/qhash.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

unsigned QH_NOT_NEW = 1;

struct hdpair {
	unsigned phd, hd[2], flags;
	char fname[BUFSIZ];
} aux_hdp, prim;

char value_buf[BUFSIZ], key_buf[BUFSIZ], *col;

unsigned qhds, ahds, qhds_n = 0, ahds_n = 0;

unsigned reverse = 0, bail = 0;

void
usage(char *prog)
{
	fprintf(stderr, "Usage: %s [-qa ARG] [[-rl] [-Rpdg ARG] ...] file[:k:v]", prog);
	fprintf(stderr, "    Options:\n");
	fprintf(stderr, "        -r             reverse operation\n");
	fprintf(stderr, "        -l             list all values\n");
	fprintf(stderr, "        -L             list missing values\n");
	fprintf(stderr, "        -q file[:k:v]  db to use string lookups and printing\n");
	fprintf(stderr, "        -a file[:k:v]  db to use for reversed string lookups and printing\n");
	fprintf(stderr, "        -R KEY         get random value of key (empty key for any)\n");
	fprintf(stderr, "        -p KEY[:VAL]   put a key/value pair\n");
	fprintf(stderr, "        -d KEY[:VAL]   delete key/value pair(s)\n");
	fprintf(stderr, "        -g KEY         get value(s) of a key\n");
	fprintf(stderr, "        -x             when printing associations, bail on first result\n");
	fprintf(stderr, "    'k' and 'v' are key and value types. Supported values:\n");
	fprintf(stderr, "         u             unsigned\n");
	fprintf(stderr, "         s             string\n");
	fprintf(stderr, "         a             key only! unsigned with automatic index\n");
	fprintf(stderr, "         t             key only! twin the value type and support dupes\n");
}

static inline void *rec_query(unsigned qhds, char *tbuf, char *buf, unsigned tmprev) {
	tmprev = (qhds_n & 1) == tmprev;
	qdb_cur_t c2 = qdb_iter(qhds, NULL);
	struct idml rqs = idml_init();
	unsigned aux;
	char *aux2;

	char *lktype = qdb_type(prim.hd[!reverse], QDB_KEY);

	while (qdb_next(&aux, &aux_hdp, &c2)) {
		if (strcmp(qdb_type(aux_hdp.hd[tmprev], QDB_VALUE), lktype))
			tmprev = !tmprev;
		if (strcmp(qdb_type(aux_hdp.hd[tmprev], QDB_VALUE), lktype)) {
			fprintf(stderr, "Invalid query sequence\n");
			qdb_fin(&c2);
			return NULL;
		}
		lktype = qdb_type(aux_hdp.hd[tmprev], QDB_KEY);
		idml_push(&rqs, aux_hdp.hd[tmprev]);
	}

	while ((aux = idml_pop(&rqs)) != (unsigned) -1) {
		if (qdb_pget(aux, tbuf, buf)) {
			idml_drop(&rqs);
			return NULL;
		}
		aux2 = buf;
		buf = tbuf;
		tbuf = aux2;
	};

	return buf;
}

static inline int gen_cond() {
	qdb_cur_t c = qdb_iter(qhds, NULL);
	unsigned aux, rev = !reverse;
	char *type = qdb_type(prim.hd[rev], QDB_KEY);

	while (qdb_next(&aux, &aux_hdp, &c)) {
		rev = !rev;
		type = qdb_type(aux_hdp.hd[rev], QDB_KEY);
	}

	return !strcmp(type, "s");
}

inline static char *_gen_lookup(char *buf, char *str, unsigned qhds, unsigned qhds_n, unsigned is_value) {
	unsigned cond = gen_cond();
	char *ret = NULL;

	/* fprintf(stderr, "_gen_lookup %u %s\n", cond, buf); */
	if (cond)
		strcpy(buf, str);
	else {
		unsigned ret = strtoul(str, NULL, 10);
		memcpy(buf, &ret, sizeof(unsigned));
		/* fprintf(stderr, "_gen_lookup 2 %u\n", ret); */
	}

	if (qhds_n)
		ret = rec_query(qhds, buf, buf, !reverse);
	else
		ret = buf;

	return ret;
}

static char *gen_lookup(char *str) {
	char *ret, *other_buf = value_buf;

	if (!str)
		return NULL;

	col = strchr(str, ':');
	if (col) {
		*col = '\0';
		col++;
		_gen_lookup(value_buf, col, ahds, ahds_n, 1);
		other_buf = key_buf;
	}

	ret = _gen_lookup(other_buf, str, qhds, qhds_n, other_buf == value_buf);
	return ret;
}

static inline void gen_del() {
	char *iter_key = gen_lookup(optarg);

	if (!col)
		qdb_del(prim.hd[!reverse], value_buf);
	else
		qdb_rem(prim.hd[reverse], key_buf, value_buf);
}

static inline int assoc_exists(char *key_buf) {
	if (!ahds_n)
		return 1;

	static char alt_buf[BUFSIZ];
	memcpy(alt_buf, key_buf, sizeof(alt_buf));
	return !!rec_query(ahds, alt_buf, alt_buf, 1);
}

static inline void assoc_print() {
	static char alt_buf[BUFSIZ];
	unsigned aux;
	qdb_cur_t c2 = qdb_iter(ahds, NULL);

	while (qdb_next(&aux, &aux_hdp, &c2)) {
		unsigned hd = aux_hdp.hd[0];
		putchar(' ');
		if (qdb_get(hd, alt_buf, key_buf)) {
			printf("-1\n");
			continue;
		}
		qdb_print(hd, QDB_VALUE, alt_buf);
		if (bail) {
			qdb_fin(&c2);
			break;
		}
	}
}

static inline void _gen_get(void) {
	unsigned hd = prim.hd[0];
	qdb_print(hd, QDB_KEY, key_buf);
	putchar(' ');
	qdb_print(hd, QDB_VALUE, value_buf);
	assoc_print();
	printf("\n");
}

static inline void gen_rand() {
	unsigned count = 0, rand;
	qdb_cur_t c;
	char *iter_key = gen_lookup(strcmp(optarg, ".") ? optarg : NULL);

	c = qdb_iter(prim.hd[!reverse], iter_key);

	while (qdb_next((unsigned *) key_buf, value_buf, &c))
		if (assoc_exists(key_buf))
			count ++;

	if (count == 0) {
		printf("-1\n");
		return;
	}

	rand = random() % count;
	unsigned hd = prim.hd[!reverse];

	c = qdb_iter(hd, iter_key);

	while (qdb_next((unsigned *) key_buf, value_buf, &c))
		if (!assoc_exists(key_buf))
			continue;
		else if ((--count) <= rand) {
			qdb_fin(&c);
			break;
		}

	_gen_get();
}

static void gen_get(char *str) {
	char *iter_key = gen_lookup(str);
	qdb_cur_t c;
	unsigned nonce = 1;

	if (!iter_key) {
		printf("-1\n");
		return;
	}

	unsigned hd = prim.hd[!reverse];

	c = qdb_iter(hd, iter_key);

	while (qdb_next(key_buf, value_buf, &c))
		if (assoc_exists(key_buf)) {
			_gen_get();
			nonce = 0;
		}

	if (nonce)
		printf("-1\n");
}

static void gen_list() {
	qdb_cur_t c;
	unsigned cond;

	gen_lookup(NULL);
	cond = gen_cond();

	unsigned hd = prim.hd[!reverse];
	c = qdb_iter(hd, NULL);

	while (qdb_next((unsigned *) key_buf, value_buf, &c)) {
		rec_query(qhds, key_buf, value_buf, !cond);
		_gen_get();
	}
}

static inline void gen_put() {
	gen_lookup(optarg);

	if (prim.flags & QH_AINDEX) {
		char *key = col ? key_buf : NULL;
		unsigned id = qdb_put(prim.phd, key, value_buf);
		printf("%u\n", id);
	} else if (prim.flags & QH_TWIN) {
		size_t rlen = qdb_len(prim.hd[1], QDB_KEY, key_buf);
		char buf[2 * rlen];
		memcpy(buf, key_buf, rlen);
		memcpy(buf + rlen, value_buf, rlen);
		qdb_put(prim.phd, buf, buf + rlen);
	} else
		qdb_put(prim.phd, key_buf, value_buf);
}

static inline void gen_list_missing() {
	unsigned aux;
	qdb_cur_t c;
	gen_lookup(NULL);

	if (!qhds_n) {
		fprintf(stderr, "list missing needs a corresponding database\n");
		return;
	}

	unsigned phd = prim.hd[!reverse];
	c = qdb_iter(phd, NULL);
	while (qdb_next((unsigned *) key_buf, value_buf, &c)) {
		qdb_cur_t c2 = qdb_iter(qhds, NULL);

		while (qdb_next(&aux, &aux_hdp, &c2))
			if (qdb_get(aux_hdp.hd[!reverse],
						key_buf, value_buf))
				_gen_get();
	}
}

int assoc_rhd(void **data, void *key, void *value) {
	if (* (unsigned *) key == (unsigned) -2)
		return DB_DONOTINDEX;

	*data = value; // string
	return 0;
}

int assoc_hd(void **data, void *key, void *value) {
	if (* (unsigned *) key == (unsigned) -2)
		return DB_DONOTINDEX;

	*data = key; // unsigned
	return 0;
}

void gen_open(char *fname, unsigned flags) {
	char buf[BUFSIZ], *key_type = "u", *value_type = "s";
	static unsigned minus_two = -2;
	unsigned existed;

	strcpy(buf, fname);

	char *first_col = strchr(buf, ':'), *second_col;

	if (first_col) {
		*first_col = '\0';
		first_col++;
		second_col = strchr(first_col, ':');

		if (second_col) {
			*second_col = '\0';
			second_col++;
			value_type = second_col;
		}

		if (!strcmp(first_col, "a")) {
			flags |= QH_AINDEX;
			key_type = "u"; // just here to show
		} else if (!strcmp(first_col, "t")) {
			flags |= QH_TWIN;
			qdb_type_t *ktype;
			qdb_get(types_hd, &ktype, key_type);
			key_type = ktype->dbl;
		} else
			key_type = first_col;
	}

	existed = access(buf, F_OK) == 0;

	qdb_config.file = buf;
	qdb_config.mode = flags & QH_RDONLY ? 0644 : 0664;
	qdb_config.type = DB_HASH;
	qdb_config.flags = flags;

	aux_hdp.flags = flags;
	aux_hdp.phd = qdb_open("phd", key_type, value_type, aux_hdp.flags);
	strlcpy(aux_hdp.fname, buf, BUFSIZ);
	if (existed) {
		size_t len;
		qdb_smeta_t *smeta = qdb_getc(aux_hdp.phd, &len, &minus_two, sizeof(minus_two));
		aux_hdp.flags |= QH_NOT_NEW;
		key_type = smeta->key;
		value_type = smeta->value;
		flags = smeta->flags;
		// for secondaries, we don't need dbl keys
		if (smeta->flags & QH_TWIN)
			key_type = value_type;
	}

	aux_hdp.flags = flags;

	aux_hdp.hd[0] = qdb_open("hd", key_type, value_type, QH_DUP | QH_REPURPOSE);
	qdb_assoc(aux_hdp.hd[0], aux_hdp.phd, NULL);

	if (flags & QH_TWIN)
		qdb_config.type = DB_BTREE;

	aux_hdp.hd[1] = qdb_open("rhd", value_type, key_type, QH_DUP | QH_SEC);
	qdb_assoc(aux_hdp.hd[1], aux_hdp.phd, assoc_rhd);
}

static inline void hdpair_close(struct hdpair *pair) {
	qdb_close(pair->hd[1], 0);
	qdb_close(pair->hd[0], 0);
	if (pair->phd != pair->hd[0])
		qdb_close(pair->phd, 0);
	if (!(pair->flags & QH_NOT_NEW) && (pair->flags & QH_RDONLY))
		unlink(pair->fname);
}

int
main(int argc, char *argv[])
{
	static char *optstr = "xla:q:p:d:g:rR:L:?";
	char *fname = argv[argc - 1], ch;
	unsigned flags = QH_RDONLY;

	if (argc < 2) {
		usage(*argv);
		return EXIT_FAILURE;
	}

	qdb_init();
	qdb_reg("hdp", sizeof(struct hdpair));

	DB_ENV *env = qdb_env_create();
	qdb_env_open(env, NULL, 0);
	ahds = qdb_open(NULL, "u", "hdp", QH_AINDEX);
	qhds = qdb_open(NULL, "u", "hdp", QH_AINDEX);

	while ((ch = getopt(argc, argv, optstr)) != -1)
		switch (ch) {
		case 'a':
			gen_open(optarg, QH_RDONLY);
			qdb_put(ahds, NULL, &aux_hdp);
			ahds_n++;
			break;
		case 'q':
			gen_open(optarg, QH_RDONLY);
			qdb_put(qhds, NULL, &aux_hdp);
			qhds_n++;
			break;
		case 'x':
			bail = 1;
			break;
		case 'p':
		case 'd':
			/* TODO m1 can be inferred */
			flags &= ~QH_RDONLY;
		case 'l':
		case 'L':
		case 'R':
		case 'g':
		case 'r': break;
		default: usage(*argv); return EXIT_FAILURE;
		case '?': usage(*argv); return EXIT_SUCCESS;
		}

	optind = 1;
	gen_open(fname, flags);
	prim = aux_hdp;
	srandom(time(NULL));

	while ((ch = getopt(argc, argv, optstr)) != -1) switch (ch) {
	case 'R': gen_rand(); break;
	case 'L': gen_list_missing(); break;
	case 'l': gen_list(); break;
	case 'p': gen_put(); break;
	case 'd': gen_del(); break;
	case 'g': gen_get(optarg); break;
	case 'r': reverse = !reverse; break;
	}

	hdpair_close(&prim);

	unsigned key;
	qdb_cur_t c = qdb_iter(ahds, NULL);

	while (qdb_next(&key, &aux_hdp, &c))
		hdpair_close(&aux_hdp);

	c = qdb_iter(qhds, NULL);

	while (qdb_next(&key, &aux_hdp, &c))
		hdpair_close(&aux_hdp);
}
