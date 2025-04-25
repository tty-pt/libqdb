/* FIXME
 * lookup is not working correctly
 */
#include "./include/qhash.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

unsigned QH_NOT_NEW = 1;

typedef void gen_put_t(void *key, void *value);
typedef int gen_cond_t(unsigned qhds_n, unsigned is_value);

struct hdpair {
	unsigned phd, hd[2], flags;
	char fname[BUFSIZ];
} aux_hdp, prim;

typedef struct {
	gen_put_t *put;
	gen_cond_t *cond;
} gen_t;

char value_buf[BUFSIZ], key_buf[BUFSIZ], *col;

unsigned qhds, ahds, qhds_n = 0, ahds_n = 0;

unsigned mode = 0, reverse = 0, tmprev_q, tmprev_a, bail = 0;

void m0_put() {
	void *key = NULL;
	if (col)
		key = key_buf;
	unsigned id = qdb_put(prim.phd, key, value_buf);
	printf("%u\n", id);
}

void m1_put(void *key, void *value) {
	unsigned values[2] = { * (unsigned *) key, * (unsigned *) value };
	qdb_put(prim.phd, values, &values[1]);
}

int m0_cond(unsigned qhds_n, unsigned is_value) {
	return (!reverse) == is_value;
}

int m1_cond(unsigned qhds_n, unsigned is_value) {
	return qhds_n & 1;
}

gen_t m_gen[2] = {{
	.put = m0_put, .cond = m0_cond,
}, {
	.put = m1_put, .cond = m1_cond,
}}, gen;

void
usage(char *prog)
{
	fprintf(stderr, "Usage: %s [-mqa ARG] [[-rl] [-Rpdg ARG] ...] db_path", prog);
	fprintf(stderr, "    Options:\n");
	fprintf(stderr, "        -r           reverse operation\n");
	fprintf(stderr, "        -l           list all values\n");
	fprintf(stderr, "        -L           list missing values\n");
	fprintf(stderr, "        -q other_db  db to use string lookups and printing\n");
	fprintf(stderr, "        -a other_db  db to use for reversed string lookups and printing\n");
	fprintf(stderr, "        -R KEY       get random value of key (key is ignored in mode 0)\n");
	fprintf(stderr, "        -p KEY[:VAL] put a key/value pair\n");
	fprintf(stderr, "        -d KEY[:VAL] delete key/value pair(s)\n");
	fprintf(stderr, "        -g KEY       get value(s) of a key\n");
	fprintf(stderr, "        -m 0-1       select mode of operation\n");
	fprintf(stderr, "        -x           when printing associations, bail on first result\n");
	fprintf(stderr, "    Modes (default is 0):\n");
	fprintf(stderr, "         0           index mode (1 string : 1 id)\n");
	fprintf(stderr, "         1           associative mode (n id : n id)\n");
}

static inline void *rec_query(unsigned qhds, char *tbuf, char *buf, unsigned tmprev) {
	qdb_cur_t c2 = qdb_iter(qhds, NULL);
	unsigned aux;
	char *aux2;

	while (qdb_next(&aux, &aux_hdp, &c2)) {
		tmprev = !tmprev;
		if (qdb_pget(aux_hdp.hd[tmprev], tbuf, buf)) {
			qdb_fin(&c2);
			return NULL;
		}
		aux2 = buf;
		buf = tbuf;
		tbuf = aux2;
	}

	return buf;
}

inline static char *_gen_lookup(char *buf, unsigned *tmprev_r, char *str, unsigned qhds, unsigned qhds_n, unsigned is_value) {
	unsigned cond = gen.cond(qhds_n, is_value);
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
		ret = rec_query(qhds, buf, buf, !cond);
	else
		ret = buf;

	*tmprev_r = (qhds_n & 1) != reverse;
	return ret;
}

static char *gen_lookup(char *str) {
	char *ret, *other_buf = value_buf;

	tmprev_q = tmprev_a = reverse;

	if (!str)
		return NULL;

	col = strchr(str, ':');
	if (col) {
		*col = '\0';
		col++;
		_gen_lookup(value_buf, &tmprev_a, col, ahds, ahds_n, 1);
		other_buf = key_buf;
	}

	ret = _gen_lookup(other_buf, &tmprev_q, str, qhds, qhds_n, other_buf == value_buf);
	return ret;
}

static inline void gen_del() {
	char *iter_key = gen_lookup(optarg);
	unsigned hd = prim.hd[!tmprev_q];

	if (!col)
		qdb_del(prim.hd[!tmprev_q], value_buf);
	else
		qdb_rem(prim.hd[tmprev_q], key_buf, value_buf);
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

static inline void gen_rand() {
	unsigned count = 0, rand;
	qdb_cur_t c;
	char *iter_key = gen_lookup(mode ? optarg : NULL);

	c = qdb_iter(prim.hd[!tmprev_q], iter_key);

	while (qdb_next((unsigned *) key_buf, value_buf, &c))
		if (assoc_exists(key_buf))
			count ++;

	if (count == 0) {
		printf("-1\n");
		return;
	}

	rand = random() % count;
	unsigned hd = prim.hd[!tmprev_q];

	c = qdb_iter(hd, iter_key);

	while (qdb_next((unsigned *) key_buf, value_buf, &c))
		if (!assoc_exists(key_buf))
			continue;
		else if ((--count) <= rand) {
			qdb_fin(&c);
			break;
		}

	if (mode)
		qdb_print(hd, QDB_VALUE, key_buf);
	else {
		qdb_print(hd, QDB_VALUE, key_buf);
		putchar(' ');
		qdb_print(hd, QDB_KEY, key_buf);
	}

	assoc_print();
	printf("\n");
}

static inline void _gen_get(unsigned hd) {
	qdb_print(hd, QDB_VALUE, key_buf);
	putchar(' ');
	qdb_print(hd, QDB_KEY, value_buf);
	assoc_print();
	printf("\n");
}

static void gen_get(char *str) {
	char *iter_key = gen_lookup(str);
	qdb_cur_t c;

	if (!iter_key) {
		printf("-1\n");
		return;
	}

	unsigned hd = prim.hd[!tmprev_q];

	c = qdb_iter(hd, iter_key);

	/* fprintf(stderr, "gen_get iter_key %u\n", * (unsigned *) iter_key); */
	while (qdb_next(key_buf, value_buf, &c)) {
		/* fprintf(stderr, "gen_get key %u %u\n", * (unsigned *) key_buf, * (unsigned *) value_buf); */
		if (assoc_exists(key_buf))
			_gen_get(hd);
	}
}

static void gen_list() {
	qdb_cur_t c;
	unsigned cond;

	gen_lookup(NULL);
	cond = gen.cond(qhds_n, 1);

	unsigned hd = prim.hd[!tmprev_q];
	c = qdb_iter(hd, NULL);

	while (qdb_next((unsigned *) key_buf, value_buf, &c)) {
		rec_query(qhds, key_buf, value_buf, !cond);
		_gen_get(hd);
	}
}

static inline void gen_put() {
	gen_lookup(optarg);
	gen.put(key_buf, value_buf);
}

static inline void gen_list_missing() {
	unsigned aux;
	qdb_cur_t c;
	gen_lookup(NULL);

	if (!qhds_n) {
		fprintf(stderr, "list missing needs a corresponding database\n");
		return;
	}

	c = qdb_iter(prim.hd[!tmprev_q], NULL);
	while (qdb_next((unsigned *) key_buf, value_buf, &c)) {
		qdb_cur_t c2 = qdb_iter(qhds, NULL);

		while (qdb_next(&aux, &aux_hdp, &c2)) {
			unsigned hd = aux_hdp.hd[!tmprev_q];
			if (qdb_get(hd, key_buf, value_buf)) {
				qdb_print(hd, QDB_VALUE, key_buf);
				putchar(' ');
				qdb_print(hd, QDB_KEY, value_buf);
				assoc_print();
				putchar('\n');
			}
		}
	}
}

void m0_assoc_rhd(void **data, uint32_t *len, void *key, void *value) {
	if (* (unsigned *) key == (unsigned) -2) {
		*len = 0;
		*data = "\0";
		return;
	}

	*len = strlen(value) + 1;
	*data = value;
}

void assoc_hd(void **data, uint32_t *len, void *key, void *value) {
	*len = sizeof(unsigned);
	*data = key;
}

void m1_assoc_rhd(void **data, uint32_t *len, void *key, void *value) {
	if (* (unsigned *) key == (unsigned) -2) {
		*len = 0;
		*data = "\0";
		return;
	}

	*len = sizeof(unsigned);
	*data = ((unsigned *) value);
}

unsigned gen_open(char *fname, unsigned mode, unsigned flags) {
	static unsigned minus_two = -2;
	unsigned existed = access(fname, F_OK) == 0;

	qdb_config.file = fname;
	qdb_config.mode = flags & QH_RDONLY ? 0644 : 0664;
	qdb_config.type = DB_HASH;

	aux_hdp.flags = 0;
	aux_hdp.phd = mode
		? qdb_open("phd", "ul", "u", flags)
		: qdb_open("phd", "u", "s", flags | QH_AINDEX);
	strlcpy(aux_hdp.fname, fname, BUFSIZ);
	if (existed) {
		size_t len;
		qdb_smeta_t *smeta = qdb_getc(aux_hdp.phd, &len, &minus_two, sizeof(minus_two));
		mode = !strcmp(smeta->key, "ul");
		aux_hdp.flags |= QH_NOT_NEW;
	}
	if (mode) {
		aux_hdp.hd[0] = qdb_open("hd", "u", "u", QH_SEC | QH_DUP);
		aux_hdp.hd[1] = qdb_open("rhd", "u", "u", QH_SEC | QH_DUP);
		qdb_assoc(aux_hdp.hd[0], aux_hdp.phd, assoc_hd);
		qdb_assoc(aux_hdp.hd[1], aux_hdp.phd, m1_assoc_rhd);
	} else {
		aux_hdp.hd[0] = qdb_open("hd", "u", "s", QH_SEC);
		qdb_config.type = DB_BTREE;
		aux_hdp.hd[1] = qdb_open("rhd", "s", "u", QH_SEC | QH_DUP);
		qdb_assoc(aux_hdp.hd[0], aux_hdp.phd, assoc_hd);
		qdb_assoc(aux_hdp.hd[1], aux_hdp.phd, m0_assoc_rhd);
	}
	return mode;
}

static inline void hdpair_close(struct hdpair *pair, unsigned nochange) {
	qdb_close(pair->phd, 0);
	qdb_close(pair->hd[0], 0);
	qdb_close(pair->hd[1], 0);
	if (!(pair->flags & QH_NOT_NEW) && nochange)
		unlink(pair->fname);
}

int
main(int argc, char *argv[])
{
	static char *optstr = "xla:q:p:d:g:m:rR:L:?";
	char *fname = argv[argc - 1], ch;
	unsigned nochange = 1;

	if (argc < 2) {
		usage(*argv);
		return EXIT_FAILURE;
	}

	qdb_init();
	qdb_reg("hdp", sizeof(struct hdpair));

	ahds = qdb_open(NULL, "u", "hdp", QH_AINDEX);
	qhds = qdb_open(NULL, "u", "hdp", QH_AINDEX);

	while ((ch = getopt(argc, argv, optstr)) != -1)
		switch (ch) {
		case 'm':
			switch (mode = strtoul(optarg, NULL, 10)) {
				case 0:
				case 1: break;
				default:
					fprintf(stderr, "Invalid mode\n");
					return EXIT_FAILURE;
			}
			break;
		case 'a':
			gen_open(optarg, 0, QH_RDONLY);
			qdb_put(ahds, NULL, &aux_hdp);
			ahds_n++;
			break;
		case 'q':
			gen_open(optarg, 0, QH_RDONLY);
			qdb_put(qhds, NULL, &aux_hdp);
			qhds_n++;
			break;
		case 'x':
			bail = 1;
			break;
		case 'p':
		case 'd':
			/* TODO m1 can be inferred */
			nochange = 0;
		case 'l':
		case 'L':
		case 'R':
		case 'g':
		case 'r': break;
		default: usage(*argv); return EXIT_FAILURE;
		case '?': usage(*argv); return EXIT_SUCCESS;
		}

	optind = 1;
	mode = gen_open(fname, mode, nochange ? QH_RDONLY : 0);
	gen = m_gen[mode];
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

	hdpair_close(&prim, nochange);

	unsigned key;
	qdb_cur_t c = qdb_iter(ahds, NULL);

	while (qdb_next(&key, &aux_hdp, &c))
		hdpair_close(&aux_hdp, 1);

	c = qdb_iter(qhds, NULL);

	while (qdb_next(&key, &aux_hdp, &c))
		hdpair_close(&aux_hdp, 1);
}
