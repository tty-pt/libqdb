#include "./include/qhash.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

DB_TXN *txnid;

typedef void del_t(unsigned hd, void *key);
typedef int gen_vdel_t(unsigned hd, void *key, void *value);
typedef void print_t(void *value);
typedef size_t measure_t(void *value);
typedef void gen_put_t(void *key, void *value);
typedef int gen_cond_t(unsigned qhds_n, unsigned is_value);

struct hdpair {
	unsigned phd, hd[2], flags;
	char fname[BUFSIZ];
} aux_hdp, prim;

typedef struct {
	gen_vdel_t *vdel;
	gen_put_t *put;
	gen_cond_t *cond;
	qdb_type_t *value, *key;
} gen_t;

qdb_type_t qdb_hdpair = {
	.len = sizeof(struct hdpair),
};

char value_buf[BUFSIZ], key_buf[BUFSIZ], *col;

unsigned qhds, ahds, qhds_n = 0, ahds_n = 0;

unsigned mode = 0, reverse = 0, tmprev_q, tmprev_a, bail = 0;

int m0_vdel(unsigned hd, void *key, void *value) {
	return 1;
}

int m1_vdel(unsigned hd, void *key, void *value) {
	if (!col)
		return 1;

	qdb_rem(hd, key, value);
	return 0;
}

void m0_put() {
	if (col)
		qdb_put(prim.phd, key_buf, value_buf);
	else {
		unsigned id = qdb_new(prim.phd, value_buf);
		printf("%u\n", id);
	}
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

gen_t m1_gen[2] = {{
	.key = &qdb_unsigned, .value = &qdb_unsigned,
	.vdel = m1_vdel,
	.put = m1_put, .cond = m1_cond,
}, {
	.key = &qdb_unsigned, .value = &qdb_unsigned,
	.vdel = m1_vdel,
	.put = m1_put, .cond = m1_cond,
}}, m0_gen[2] = {{
	.key = &qdb_string, .value = &qdb_unsigned,
	.vdel = m0_vdel,
	.put = m0_put, .cond = m0_cond,
}, {
	.key = &qdb_unsigned, .value = &qdb_string,
	.vdel = m0_vdel,
	.put = m0_put, .cond = m0_cond,
}}, gen, *gen_r;

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

int u_get(unsigned hd, void *value, void *key) {
	return qdb_get(aux_hdp.hd[0], value, key);
}

static inline void *rec_query(unsigned qhds, char *tbuf, char *buf, unsigned tmprev) {
	qdb_cur_t c2 = qdb_iter(qhds, NULL);
	unsigned aux;
	char *aux2;

	while (qdb_next(&aux, &aux_hdp, &c2)) {
		tmprev = !tmprev;
		/* fprintf(stderr, "req_query %u %u %u %u %s", qhds, tmprev, aux_hdp.hd[tmprev], * (unsigned *) buf, buf); */
		if (qdb_get(aux_hdp.hd[tmprev], tbuf, buf)) {
			/* fprintf(stderr, "?\n"); */
			qdb_fin(&c2);
			return NULL;
		}
		/* fprintf(stderr, "!\n"); */
		aux2 = buf;
		buf = tbuf;
		tbuf = aux2;
	}

	return buf;
}

inline static char *_gen_lookup(char *buf, unsigned *tmprev_r, char *str, unsigned qhds, unsigned qhds_n, unsigned is_value) {
	unsigned cond = gen.cond(qhds_n, is_value);
	char *ret = NULL;

	if (cond)
		strcpy(buf, str);
	else {
		unsigned ret = strtoul(str, NULL, 10);
		memcpy(buf, &ret, sizeof(unsigned));
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

	if (!str) {
		gen = gen_r[tmprev_q];
		return NULL;
	}

	gen = gen_r[reverse];

	col = strchr(str, ':');
	if (col) {
		*col = '\0';
		col++;
		_gen_lookup(value_buf, &tmprev_a, col, ahds, ahds_n, 1);
		other_buf = key_buf;
	}

	ret = _gen_lookup(other_buf, &tmprev_q, str, qhds, qhds_n, other_buf == value_buf);
	gen = gen_r[tmprev_q];
	return ret;
}

static inline void gen_del() {
	char *iter_key = gen_lookup(optarg);

	if (gen.vdel(prim.hd[!tmprev_q], value_buf, key_buf))
		qdb_del(prim.hd[!tmprev_q], value_buf);
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
		putchar(' ');
		if (u_get(aux_hdp.hd[0], alt_buf, key_buf)) {
			s_print("-1");
			continue;
		}
		s_print(alt_buf);
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

	c = qdb_iter(prim.hd[!tmprev_q], iter_key);

	while (qdb_next((unsigned *) key_buf, value_buf, &c))
		if (!assoc_exists(key_buf))
			continue;
		else if ((--count) <= rand) {
			qdb_fin(&c);
			break;
		}

	if (mode)
		gen.key->print(key_buf);
	else {
		u_print(key_buf);
		putchar(' ');
		gen.key->print(value_buf);
	}

	assoc_print();
	printf("\n");
}

static inline void _gen_get() {
	u_print(key_buf);
	putchar(' ');
	gen.key->print(value_buf);
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

	c = qdb_iter(prim.hd[!tmprev_q], iter_key);

	while (qdb_next((unsigned *) key_buf, value_buf, &c))
		if (assoc_exists(key_buf))
			_gen_get();
}

static void gen_list() {
	qdb_cur_t c;
	unsigned cond;

	gen_lookup(NULL);
	cond = gen.cond(qhds_n, 1);

	c = qdb_iter(prim.hd[!tmprev_q], NULL);

	while (qdb_next((unsigned *) key_buf, value_buf, &c)) {
		rec_query(qhds, key_buf, value_buf, !cond);
		_gen_get();
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

		while (qdb_next(&aux, &aux_hdp, &c2))
			if (qdb_get(aux_hdp.hd[!tmprev_q], key_buf, value_buf)) {
				u_print(key_buf);
				putchar(' ');
				gen.key->print(value_buf);
				assoc_print();
				putchar('\n');
			}
	}
}

void m0_assoc_rhd(void **data, uint32_t *len, void *key, void *value) {
	*len = strlen(value) + 1;
	*data = value;
	if (* (unsigned *) key == (unsigned) -1)
		*data = key;
}

void assoc_hd(void **data, uint32_t *len, void *key, void *value) {
	*len = sizeof(unsigned);
	*data = key;
}

void m1_assoc_rhd(void **data, uint32_t *len, void *key, void *value) {
	*len = sizeof(unsigned);
	*data = ((unsigned *) value);
}

unsigned gen_open(char *fname, unsigned mode, unsigned flags) {
	static unsigned minus_two = -2;
	unsigned existed = access(fname, F_OK) == 0;

	hash_config.file = fname;
	hash_config.mode = flags & QH_RDONLY ? 0644 : 0664;
	hash_config.flags = flags;
	hash_config.type = DB_BTREE;

	aux_hdp.flags = flags;
	aux_hdp.phd = mode
		? qdb_init("phd", &qdb_unsigned, &qdb_string)
		: qdb_init("phd", &qdb_unsigned_pair, &qdb_unsigned);
	strlcpy(aux_hdp.fname, fname, BUFSIZ);
	if (existed) {
		qdb_get(aux_hdp.phd, &mode, &minus_two);
		aux_hdp.flags |= 1;
	}
	flags &= ~QH_RDONLY;
	hash_config.mode = flags & QH_RDONLY ? 0644 : 0664;
	hash_config.flags = flags;
	hash_config.type = DB_HASH;
	if (mode) {
		aux_hdp.hd[0] = qdb_ainit("hd"); // needed for dupes
		aux_hdp.hd[1] = qdb_ainit("rhd");
		qdb_assoc(aux_hdp.hd[0], aux_hdp.phd, assoc_hd);
		qdb_assoc(aux_hdp.hd[1], aux_hdp.phd, m1_assoc_rhd);
	} else {
		aux_hdp.hd[0] = qdb_init("hd", &qdb_unsigned, &qdb_string); // not really needed but here for consistency
		aux_hdp.hd[1] = qdb_ainit("rhd");
		qdb_assoc(aux_hdp.hd[0], aux_hdp.phd, assoc_hd);
		qdb_assoc(aux_hdp.hd[1], aux_hdp.phd, m0_assoc_rhd);
	}
	if (!existed)
		qdb_put(aux_hdp.phd, &minus_two, &mode);
	return mode;
}

static inline void hdpair_close(struct hdpair *pair, unsigned nochange) {
	qdb_close(pair->phd, 0);
	qdb_close(pair->hd[0], 0);
	qdb_close(pair->hd[1], 0);
	if (!(pair->flags & 1) && nochange)
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

	ahds = qdb_linit(NULL, &qdb_hdpair);
	qhds = qdb_linit(NULL, &qdb_hdpair);

	gen_r = m0_gen;

	while ((ch = getopt(argc, argv, optstr)) != -1)
		switch (ch) {
		case 'm':
			switch (mode = strtoul(optarg, NULL, 10)) {
				case 0: break;
				case 1: gen_r = m1_gen; break;
				default:
					fprintf(stderr, "Invalid mode\n");
					return EXIT_FAILURE;
			}
			break;
		case 'a':
			gen_open(optarg, 0, QH_RDONLY);
			qdb_new(ahds, &aux_hdp);
			ahds_n++;
			break;
		case 'q':
			gen_open(optarg, 0, QH_RDONLY);
			qdb_new(qhds, &aux_hdp);
			qhds_n++;
			break;
		case 'x':
			bail = 1;
			break;
		case 'p':
		case 'd':
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
	if ((mode = gen_open(fname, mode, nochange ? QH_RDONLY : 0)))
		gen_r = m1_gen;
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
