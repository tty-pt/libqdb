#include "include/qhash.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

typedef struct hash_cursor gen_iter_t(unsigned hd, void *key);
typedef void gen_del_t(unsigned hd, void *key);
typedef int gen_vdel_t(unsigned hd, void *key, void *value);
typedef void print_t(void *value);
typedef void gen_put_t(void *key, void *value);
typedef int gen_get_t(unsigned hd, void *value, void *key);
typedef int gen_cond_t(unsigned qhds_n, unsigned is_value);

int u_gen_get(unsigned hd, void *value, void *key) {
	return uhash_get(hd, value, * (unsigned *) key);
}

int s_gen_pget(unsigned hd, void *value, void *key) {
	return shash_pget(hd, value, key);
}

int u_gen_pget(unsigned hd, void *value, void *key) {
	return uhash_pget(hd, value, * (unsigned *) key);
}

struct hdpair {
	unsigned phd, hd[2];
} aux_hdp, prim;

typedef struct {
	print_t *value_print;
	gen_iter_t *iter;
	gen_del_t *del;
	gen_vdel_t *vdel;
	gen_put_t *put;
	gen_get_t *get;
	gen_cond_t *cond;
} gen_t;

char value_buf[BUFSIZ], key_buf[BUFSIZ], *col;

unsigned qhds, ahds, qhds_n = 0, ahds_n = 0;

unsigned mode = 0, reverse = 0, tmprev_q, tmprev_a;

void u_print(void *value) {
	printf("%u", * (unsigned *) value);
}

void s_print(void *value) {
	printf("%s", (char *) value);
}

struct hash_cursor u_iter(unsigned hd, void *key) {
	return hash_iter(hd, key, sizeof(unsigned));
}

struct hash_cursor s_iter(unsigned hd, void *key) {
	return hash_iter(hd, key, key ? strlen(key) + 1 : 0);
}

void m0_u_del(unsigned hd, void *key) {
	lhash_del(prim.phd, * (unsigned *) key);
}

void u_del(unsigned hd, void *key) {
	uhash_del(hd, * (unsigned *) key);
}

void s_del(unsigned hd, void *key) {
	shash_del(hd, key);
}

int m0_vdel(unsigned hd, void *key, void *value) {
	return 1;
}

int m1_vdel(unsigned hd, void *key, void *value) {
	if (!col)
		return 1;

	ahash_remove(hd, * (unsigned *) key, * (unsigned *) value);
	return 0;
}

void m0_put() {
	if (col)
		lhash_put(prim.phd, * (unsigned *) key_buf, value_buf);
	else {
		unsigned id = lhash_new(prim.phd, value_buf);
		printf("%u\n", id);
	}
}

void m1_put(void *key, void *value) {
	unsigned values[2] = { * (unsigned *) key, * (unsigned *) value };
	hash_put(prim.phd, values, sizeof(values), &values[1], sizeof(unsigned));
}

int m0_cond(unsigned qhds_n, unsigned is_value) {
	return (!reverse) == is_value;
}

int m1_cond(unsigned qhds_n, unsigned is_value) {
	return qhds_n & 1;
}

gen_t m1_gen[2] = {{
	.value_print = u_print, .iter = u_iter, .del = u_del, .vdel = m1_vdel,
	.put = m1_put, .get = u_gen_get, .cond = m1_cond,
}, {
	.value_print = u_print, .iter = u_iter, .del = u_del, .vdel = m1_vdel,
	.put = m1_put, .get = u_gen_pget, .cond = m1_cond,
}}, m0_gen[2] = {{
	.value_print = s_print, .iter = s_iter, .del = s_del, .vdel = m0_vdel,
	.put = m0_put, .get = s_gen_pget, .cond = m0_cond,
}, {
	.value_print = s_print, .iter = u_iter, .del = m0_u_del, .vdel = m0_vdel,
	.put = m0_put, .get = u_gen_get, .cond = m0_cond,
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
	fprintf(stderr, "    Modes (default is 0):\n");
	fprintf(stderr, "         0           index mode (1 string : 1 id)\n");
	fprintf(stderr, "         1           associative mode (n id : n id)\n");
}

int u_get(unsigned hd, void *value, void *key) {
	return uhash_get(aux_hdp.hd[0], value, * (unsigned *) key);
}

static inline void *rec_query(unsigned qhds, char *tbuf, char *buf, unsigned tmprev) {
	struct hash_cursor c2 = lhash_iter(qhds);
	unsigned aux;

	while (lhash_next(&aux, &aux_hdp, &c2)) {
		tmprev = !tmprev;
		if (m0_gen[!tmprev].get(aux_hdp.hd[tmprev], tbuf, buf)) {
			hash_fin(&c2);
			return NULL;
		}
		buf = tbuf;
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
		gen.del(prim.hd[!tmprev_q], value_buf);
}

static inline int assoc_exists() {
	static char alt_buf[BUFSIZ];
	unsigned aux;
	struct hash_cursor c2 = lhash_iter(ahds);

	while (lhash_next(&aux, &aux_hdp, &c2))
		if (u_get(aux_hdp.hd[0], alt_buf, key_buf)) {
			hash_fin(&c2);
			return 0;
		}

	return 1;
}

static inline void assoc_print() {
	static char alt_buf[BUFSIZ];
	unsigned aux;
	struct hash_cursor c2 = lhash_iter(ahds);

	while (lhash_next(&aux, &aux_hdp, &c2)) {
		putchar(' ');
		if (u_get(aux_hdp.hd[0], alt_buf, key_buf)) {
			s_print("-1");
			continue;
		}
		s_print(alt_buf);
	}
}

static inline void gen_rand() {
	unsigned count = 0, rand;
	struct hash_cursor c;
	char *iter_key = gen_lookup(mode ? optarg : NULL);

	c = gen.iter(prim.hd[!tmprev_q], iter_key);

	while (lhash_next((unsigned *) key_buf, value_buf, &c))
		if (assoc_exists())
			count ++;

	if (count == 0) {
		printf("-1\n");
		return;
	}

	rand = random() % count;

	c = gen.iter(prim.hd[!tmprev_q], iter_key);

	while (lhash_next((unsigned *) key_buf, value_buf, &c))
		if (!assoc_exists())
			continue;
		else if ((--count) <= rand) {
			hash_fin(&c);
			break;
		}

	if (mode)
		gen.value_print(key_buf);
	else {
		u_print(key_buf);
		putchar(' ');
		gen.value_print(value_buf);
	}

	assoc_print();
	printf("\n");
}

static inline void _gen_get() {
	u_print(key_buf);
	putchar(' ');
	gen.value_print(value_buf);
	assoc_print();
	printf("\n");
}

static void gen_get(char *str) {
	char *iter_key = gen_lookup(str);
	struct hash_cursor c;

	if (!iter_key) {
		printf("-1\n");
		return;
	}

	c = gen.iter(prim.hd[!tmprev_q], iter_key);

	while (lhash_next((unsigned *) key_buf, value_buf, &c))
		if (assoc_exists())
			_gen_get();
}

static void gen_list() {
	struct hash_cursor c;
	unsigned cond;

	gen_lookup(NULL);
	cond = gen.cond(qhds_n, 1);

	c = gen.iter(prim.hd[!tmprev_q], NULL);

	while (lhash_next((unsigned *) key_buf, value_buf, &c)) {
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
	struct hash_cursor c;
	gen_lookup(NULL);

	if (!qhds_n) {
		fprintf(stderr, "list missing needs a corresponding database\n");
		return;
	}

	c = hash_iter(prim.hd[!tmprev_q], NULL, 0);
	while (lhash_next((unsigned *) key_buf, value_buf, &c)) {
		struct hash_cursor c2 = lhash_iter(qhds);

		while (lhash_next(&aux, &aux_hdp, &c2))
			if (lhash_get(aux_hdp.hd[tmprev_q], value_buf, * (unsigned *) key_buf)) {
				u_print(key_buf);
				putchar(' ');
				gen.value_print(value_buf);
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

unsigned gen_open(char *fname, unsigned mode) {
	static int fmode = 0664;
	unsigned existed = access(fname, F_OK) == 0;

	aux_hdp.phd = lhash_cinit(0, fname, "phd", fmode);
	if (existed)
		lhash_get(aux_hdp.phd, &mode, -2);
	if (mode) {
		aux_hdp.hd[0] = ahash_cinit(fname, "hd", fmode); // needed for dupes
		aux_hdp.hd[1] = ahash_cinit(fname, "rhd", fmode);
		hash_assoc(aux_hdp.hd[0], aux_hdp.phd, assoc_hd);
		hash_assoc(aux_hdp.hd[1], aux_hdp.phd, m1_assoc_rhd);
	} else {
		aux_hdp.hd[0] = hash_cinit(fname, "hd", fmode, 0); // not really needed but here for consistency
		aux_hdp.hd[1] = hash_cinit(fname, "rhd", fmode, QH_DUP);
		hash_assoc(aux_hdp.hd[0], aux_hdp.phd, assoc_hd);
		hash_assoc(aux_hdp.hd[1], aux_hdp.phd, m0_assoc_rhd);
	}
	if (!existed)
		uhash_put(aux_hdp.phd, -2, &mode, sizeof(mode));
	return mode;
}

int
main(int argc, char *argv[])
{
	static char *optstr = "la:q:p:d:g:m:rR:L:?";
	char *fname = argv[argc - 1], ch;
	int fmode = 0444;

	if (argc < 2) {
		usage(*argv);
		return EXIT_FAILURE;
	}

	ahds = lhash_init(sizeof(struct hdpair));
	qhds = lhash_init(sizeof(struct hdpair));

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
			gen_open(optarg, 0);
			lhash_new(ahds, &aux_hdp);
			ahds_n++;
			break;
		case 'q':
			gen_open(optarg, 0);
			lhash_new(qhds, &aux_hdp);
			qhds_n++;
			break;
		case 'p':
		case 'd':
			fmode = 0644;
		case 'l':
		case 'L':
		case 'R':
		case 'g':
		case 'r': break;
		default: usage(*argv); return EXIT_FAILURE;
		case '?': usage(*argv); return EXIT_SUCCESS;
		}

	optind = 1;
	if ((mode = gen_open(fname, mode)))
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

	lhash_close(prim.phd);

	hash_close(prim.hd[0]);
	hash_close(prim.hd[1]);
}
