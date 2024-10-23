#include "include/qhash.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

// mode1 has always unsigned key / values
// mode0 has unsigned and string. It depends on reverse mode
// both are always associated with mode0

struct hdpair {
	unsigned hd, rhd;
} aux_hdp;

typedef struct hash_cursor gen_iter_t(unsigned hd, void *key);
typedef int gen_get_t(unsigned hd, void *value, void *key);
typedef char *gen_lookup_t(unsigned hd, char *str);
typedef void print_t(void *value);

print_t *key_print, *value_print, *aux_print;
gen_iter_t *gen_iter, *gen_iter_aux, *gen_iter_next;
gen_get_t *alt_get, *alt_get_aux;
gen_lookup_t *gen_lookup;

unsigned iqhds, qhds, iahds, ahds;

unsigned hd, rhd, qhd, qrhd, ahd, arhd, ihd, irhd, iqrhd, iqhd;
unsigned tmp_id, mode = 0, reverse = 0, aux, ign;
char a_buf[BUFSIZ], b_buf[BUFSIZ], alt_buf[BUFSIZ], *col, *key_ptr, *value_ptr;
struct hash_cursor c, c2;

void
usage(char *prog)
{
	fprintf(stderr, "Usage: %s [-mqa ARG] [[-rl] [-Rpdg ARG] ...] db_path", prog);
	fprintf(stderr, "    Options:\n");
	fprintf(stderr, "        -r           reverse operation\n");
	fprintf(stderr, "        -l           list all values (not reversed in mode 1)\n");
	fprintf(stderr, "        -L           list missing values\n");
	fprintf(stderr, "        -q other_db  db to use string lookups and printing\n");
	fprintf(stderr, "        -a other_db  db to use for reversed string lookups and printing\n");
	fprintf(stderr, "        -R KEY       get random value of key (key is ignored in mode 0)\n");
	fprintf(stderr, "        -p KEY[:VAL] put a key/value pair (not reversed in mode 1)\n");
	fprintf(stderr, "        -d KEY[:VAL] delete key/value pair(s) (not reversed in mode 1 with only KEY)\n");
	fprintf(stderr, "        -g KEY       get value(s) of a key\n");
	fprintf(stderr, "        -m 0-1       select mode of operation\n");
	fprintf(stderr, "    Modes (default is 0):\n");
	fprintf(stderr, "         0           index mode (1 string : 1 id)\n");
	fprintf(stderr, "         1           associative mode (n id : n id)\n");
}

void u_print(void *value) {
	printf("%u", * (unsigned *) value);
}

void s_print(void *value) {
	printf("%s", (char *) value);
}

int u_get(unsigned hd, void *value, void *key) {
	return uhash_get(aux_hdp.hd, value, * (unsigned *) key);
}

int su_get(unsigned hd, void *value, void *key) {
	return shash_get(aux_hdp.hd, value, key);
}

struct hash_cursor u_iter(unsigned hd, void *key) {
	return hash_iter(hd, key, sizeof(unsigned));
}

struct hash_cursor s_iter(unsigned hd, void *key) {
	return hash_iter(hd, key, key ? strlen(key) + 1 : 0);
}

char *m1_lookup(unsigned hd, char *str) {
	static unsigned ret;
	ret = -1;

	// we can't really use strings for lookups in m1 databases
	// so another behavior for reverse doesn't make sense

	if (hd == -1 || reverse) {
		ret = strtoul(str, NULL, 10);
		memcpy(a_buf, &ret, sizeof(unsigned));
		return a_buf;
	}

	if (shash_get(hd, a_buf, str))
		return NULL;
	return a_buf;
}

char *m0_lookup(unsigned hd, char *str) {
	static unsigned ret;

	if (hd == -1) {
		if (reverse) {
			strcpy(a_buf, str);
		} else {
			ret = strtoul(str, NULL, 10);
			memcpy(a_buf, &ret, sizeof(unsigned));
		}
		return a_buf;
	}

	if (reverse) {
		ret = strtoul(str, NULL, 10);
		if (uhash_get(iqhd, a_buf, ret))
			return NULL;
		return a_buf;
	}

	if (shash_get(hd, a_buf, str))
		return NULL;
	return b_buf;
}

static inline unsigned ahd_get(unsigned hd, char *arg) {
	unsigned ret;

	if (hd == -1)
		return strtoul(arg, NULL, 10);

	shash_get(hd, &ret, arg);
	return ret;
}

// not affected by reverse
static inline int mode1_delete_one() {
	col = strchr(optarg, ':');
	if (!col)
		return 0;

	*col = '\0';

	tmp_id = ahd_get(qhd, optarg);

	col++;
	ign = ahd_get(arhd, col);

	ahash_remove(rhd, tmp_id, ign);
	ahash_remove(hd, ign, tmp_id);
	return 1;
}

static inline void mode1_delete() {
	if (mode1_delete_one())
		return;

	tmp_id = ahd_get(iqrhd, optarg);

	c = hash_iter(irhd, &tmp_id, sizeof(tmp_id));

	while (lhash_next(&ign, &tmp_id, &c))
		hash_cdel(&c);
}

static inline void mode0_delete() {
	if (reverse) {
		shash_get(rhd, &tmp_id, optarg);
		lhash_del(hd, tmp_id);
		return;
	}

	tmp_id = strtoul(optarg, NULL, 10);
	lhash_get(hd, a_buf, tmp_id);
	shash_del(rhd, a_buf);
	lhash_del(hd, tmp_id);
}

static inline unsigned assoc_print(unsigned *count, char *alt_key) {
	unsigned didnt;
	c2 = lhash_iter(iahds);
	while (lhash_next(&aux, &aux_hdp, &c2)) {
		putchar(' ');
		didnt = 1;
		if (alt_get(aux_hdp.hd, alt_buf, alt_key)) {
			s_print("-1");
			continue;
		}
		else if (didnt) {
			didnt = 0;
			(*count)++;
		}
		s_print(alt_buf);
	}
	return didnt;
}

static inline void any_rand() {
	unsigned count = 0, rand;
	char *alt_key = mode ? gen_lookup(iqrhd, optarg) : NULL;

	c = gen_iter(ihd, alt_key);

	while (lhash_next((unsigned *) a_buf, b_buf, &c))
		count ++;

	if (count == 0) {
		printf("-1\n");
		return;
	}

	rand = random() % count;

	c = gen_iter(ihd, alt_key);

	while (lhash_next((unsigned *) b_buf, a_buf, &c)) {
		count --;
		if (count <= rand) {
			hash_fin(&c);
			break;
		}
	}

	key_print(value_ptr);
	putchar(' ');
	value_print(key_ptr);
	assoc_print(&count, value_ptr);
	printf("\n");
}

static void gen_get(char *str) {
	unsigned count = 0, didnt = 0;
	char *alt_key = str ? gen_lookup(iqrhd, str) : a_buf;
	if (str && !alt_key) {
		printf(" -1\n");
		return;
	}
	c = gen_iter(ihd, str ? alt_key : NULL);

	while (lhash_next((unsigned *) a_buf, b_buf, &c)) {
		key_print(key_ptr);
		putchar(' ');
		value_print(value_ptr);
		didnt = assoc_print(&count, alt_key);
		printf("\n");
	}

	if (count || didnt)
		return;

	printf(" -1\n");
}

// not affected by reverse
static inline void mode1_put() {
	col = strchr(optarg, ':');
	if (!col) {
		fprintf(stderr, "putting format in mode 1 is key:value\n");
		return;
	}

	*col = '\0';

	tmp_id = ahd_get(qrhd, optarg);

	col ++;
	ign = ahd_get(arhd, col);

	ahash_add(hd, tmp_id, ign);
	ahash_add(rhd, ign, tmp_id);
}

static inline void mode0_put() {
	if (reverse) {
		fprintf(stderr, "reverse putting in mode 0 is not supported\n");
		exit(EXIT_FAILURE);
	}

	col = strchr(optarg, ':');
	if (col) {
		tmp_id = strtoul(optarg, NULL, 10);
		*col = '\0';
		col++;
		lhash_put(hd, tmp_id, col);
	} else {
		tmp_id = lhash_new(hd, optarg);
		col = optarg;
	}

	suhash_put(rhd, col, tmp_id);
	printf("%u\n", tmp_id);
}

static inline void any_list_missing() {
	unsigned count = 0;

	if (iqhd == -1) {
		fprintf(stderr, "list missing needs a corresponding database\n");
		return;
	}

	c = hash_iter(iqhd, NULL, 0);
	while (lhash_next(&ign, b_buf, &c))
		if (uhash_get(ihd, a_buf, ign)) {
			printf("%u %s", ign, b_buf);
			assoc_print(&count, (char *) &ign);
			putchar('\n');
		}
}

int
main(int argc, char *argv[])
{
	static char *optstr = "la:q:p:d:g:m:rR:L:?";
	char *fname = argv[argc - 1], ch, *col;
	int fmode = 0664;

	if (argc < 2) {
		usage(*argv);
		return EXIT_FAILURE;
	}

	ahds = lhash_init(sizeof(struct hdpair));
	iahds = lhash_init(sizeof(struct hdpair));
	qhds = lhash_init(sizeof(struct hdpair));
	iqhds = lhash_init(sizeof(struct hdpair));
	ahd = qhd = qrhd = arhd = iqrhd = -1;

	gen_iter = u_iter;
	key_print = u_print;
	value_print = s_print;
	gen_lookup = m0_lookup;
	alt_get = u_get;
	gen_iter_next = s_iter;

	while ((ch = getopt(argc, argv, optstr)) != -1)
		switch (ch) {
		case 'm':
			switch (mode = strtoul(optarg, NULL, 10)) {
				case 0: break;
				case 1:
					key_print = value_print = u_print;
					gen_lookup = m1_lookup;
					gen_iter_next = u_iter;
					break;
				default:
					fprintf(stderr, "Invalid mode\n");
					return EXIT_FAILURE;
			}
			break;
		case 'a':
			ahd = lhash_cinit(0, optarg, "hd", fmode);
			arhd = hash_cinit(optarg, "rhd", fmode, QH_DUP);
			aux_hdp.hd = ahd;
			aux_hdp.rhd = arhd;
			lhash_new(ahds, &aux_hdp);
			lhash_new(iahds, &aux_hdp);
			break;
		case 'q':
			qhd = lhash_cinit(0, optarg, "hd", fmode);
			qrhd = hash_cinit(optarg, "rhd", fmode, QH_DUP);
			aux_hdp.hd = qhd;
			aux_hdp.rhd = iqhd;
			lhash_new(qhds, &aux_hdp);
			lhash_new(iqhds, &aux_hdp);
			break;
		case 'p':
		case 'd':
		case 'l':
		case 'L':
		case 'R':
		case 'g':
		case 'r': break;
		default: usage(*argv); return EXIT_FAILURE;
		case '?': usage(*argv); return EXIT_SUCCESS;
		}

	optind = 1;

	if (mode) {
		hd = ahash_cinit(fname, "hd", fmode);
		rhd = ahash_cinit(fname, "rhd", fmode);
	} else {
		hd = lhash_cinit(0, fname, "hd", fmode);
		rhd = hash_cinit(fname, "rhd", fmode, QH_DUP);
	}

	srandom(time(NULL));

	iqrhd = qrhd; ihd = hd; iqhd = qhd;
	key_ptr = a_buf; value_ptr = b_buf;

	while ((ch = getopt(argc, argv, optstr)) != -1) {
		switch (ch) {
		case 'R': any_rand(); break;
		case 'L': any_list_missing(); break;
		case 'l': gen_get(NULL); break;
		case 'p': if (mode) mode1_put(); else mode0_put(); break;
		case 'd': if (mode) mode1_delete(); else mode0_delete(); break;
		case 'g': gen_get(optarg); break;
		case 'r':
			reverse = !reverse;

			col = value_ptr;
			value_ptr = key_ptr;
			key_ptr = col;

			gen_iter_aux = gen_iter;
			gen_iter = gen_iter_next;
			gen_iter_next = gen_iter_aux;

			if (reverse) {
				iqrhd = arhd; ihd = rhd; iqhd = ahd;
				iqhds = ahds; iahds = qhds;
			} else {
				iqrhd = qrhd; ihd = hd; iqhd = qhd;
				iqhds = qhds; iahds = ahds;
			}

			break;
		}
	}

	if (!mode)
		lhash_close(hd);
	else
		hash_close(hd);

	hash_close(rhd);
}
