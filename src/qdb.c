/* OBSERVATIONS:
 *
 * - The reverse flag is counter-intuitive. When it is on,
 *   we query for (primary) keys, for example.
 */
#include "./../include/qdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <qidm.h>
#include <qmap.h>

unsigned QH_NOT_NEW = 1;

unsigned prim_hd, aux_hd;

char value_buf[BUFSIZ], key_buf[BUFSIZ], *col;

unsigned qhds, ahds, qhds_n = 0, ahds_n = 0;

unsigned reverse = 0, bail = 0, print_keys = 0;

unsigned qdb_get_type;
char *qdb_get_buf;

void
usage(char *prog)
{
	fprintf(stderr, "Usage: %s [-qa ARG] [[-rl] [-Rpdg ARG] ...] file[[:k]:v]", prog);
	fprintf(stderr, "    Options:\n");
	fprintf(stderr, "        -r               reverse operation\n");
	fprintf(stderr, "        -l               list all values\n");
	fprintf(stderr, "        -L               list missing values\n");
	fprintf(stderr, "        -q file[:k[:v]]  db to use string lookups and printing\n");
	fprintf(stderr, "        -a file[:k[:v]]  db to use for reversed string lookups and printing\n");
	fprintf(stderr, "        -R KEY           get random value of key (empty key for any)\n");
	fprintf(stderr, "        -p KEY[:VAL]     put a key/value pair\n");
	fprintf(stderr, "        -d KEY[:VAL]     delete key/value pair(s)\n");
	fprintf(stderr, "        -g KEY           get value(s) of a key\n");
	fprintf(stderr, "        -x               when printing associations, bail on first result\n");
	fprintf(stderr, "        -k               also print keys (for get and rand).\n");
	fprintf(stderr, "    'k' and 'v' are key and value types. Supported values:\n");
	fprintf(stderr, "         u               unsigned\n");
	fprintf(stderr, "         s               string (default value type)\n");
	fprintf(stderr, "         a               key only! unsigned automatic index (default)\n");
	fprintf(stderr, "         2<base-type>    key only! allows duplicates\n");
	fprintf(stderr, "\n");
	fprintf(stderr, "Use '.' as the KEY for all keys!\n");
}

static inline const char *
qdb_type(unsigned phd, enum qmap_mbr t, unsigned reverse)
{
	return qmap_type(phd, reverse ? !t : t);
}

static inline void *rec_query(unsigned qhds, char *tbuf, char *buf, unsigned tmprev) {
	tmprev = (qhds_n & 1) == tmprev;
	unsigned c2 = qmap_iter(qhds, NULL);
	ids_t rqs = ids_init();
	unsigned aux, aux_hd;
	char *aux2;

	const char *lktype = qdb_type(prim_hd,
			QMAP_KEY, !reverse);

	while (qmap_next(&aux, &aux_hd, c2)) {
		if (strcmp(qdb_type(aux_hd, QMAP_VALUE, tmprev), lktype))
			tmprev = !tmprev;
		if (strcmp(qdb_type(aux_hd, QMAP_VALUE, tmprev), lktype)) {
			// TODO free idml
			ids_drop(&rqs);
			fprintf(stderr, "Invalid query sequence\n");
			qmap_fin(c2);
			return NULL;
		}
		lktype = qdb_type(aux_hd, QMAP_KEY, tmprev);
		ids_push(&rqs, aux_hd + tmprev);
	}

	while ((aux = ids_pop(&rqs)) != (unsigned) -1) {
		if (qmap_get(aux, tbuf, buf)) {
			ids_drop(&rqs);
			return NULL;
		}
		aux2 = buf;
		buf = tbuf;
		tbuf = aux2;
	};

	ids_drop(&rqs);
	return buf;
}

static inline int gen_cond(int is_value) {
	unsigned c = qmap_iter(qhds, NULL);
	unsigned aux, rev = !reverse, aux_hd;
	const char *type = qdb_type(prim_hd,
			is_value ? QMAP_KEY : QMAP_VALUE,
			rev);

	while (qmap_next(&aux, &aux_hd, c)) {
		rev = !rev;
		type = qdb_type(aux_hd, QMAP_KEY, rev);
	}

	return !strcmp(type, "s");
}

inline static char *
_gen_lookup(char *buf, char *str, unsigned qhds,
		unsigned qhds_n, int is_value)
{
	unsigned cond = gen_cond(is_value);
	char *ret = NULL;

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

	if (!str || !strcmp(str, "."))
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

static inline void gen_del(void) {
	char *key = NULL;
	gen_lookup(optarg);

	if (col)
		key = key_buf;

	qmap_del(prim_hd + !reverse, value_buf, key);
}

static inline int assoc_exists(char *key_buf) {
	if (!ahds_n)
		return 1;

	static char alt_buf[BUFSIZ];
	memcpy(alt_buf, key_buf, sizeof(alt_buf));
	return !!rec_query(ahds, alt_buf, alt_buf, 1);
}

static inline void assoc_print(void) {
	static char pbuf[BUFSIZ];
	static char alt_buf[BUFSIZ];
	char *buf = reverse ? value_buf : key_buf;
	unsigned aux, aux_hd;
	unsigned c2 = qmap_iter(ahds, NULL);
	memset(pbuf, 0, sizeof(pbuf));

	while (qmap_next(&aux, &aux_hd, c2)) {
		putchar(' ');
		if (qmap_get(aux_hd, alt_buf, buf)) {
			printf("-1");
			continue;
		}
		qmap_print(pbuf, aux_hd,
				QMAP_VALUE, alt_buf);
		printf("%s", pbuf);
		if (bail)
			break;
	}
}

static inline void _gen_get(void) {
	char pbuf[BUFSIZ];
	memset(pbuf, 0, sizeof(pbuf));
	if (print_keys) {
		qmap_print(pbuf, prim_hd, QMAP_KEY, key_buf);
		printf("%s ", pbuf);
		qmap_print(pbuf, prim_hd, QMAP_VALUE,
				value_buf);
		printf("%s", pbuf);
	} else {
		qmap_print(pbuf, prim_hd, qdb_get_type,
				qdb_get_buf);
		printf("%s", pbuf);
	}
	assoc_print();
	printf("\n");
}

static inline void gen_rand(void) {
	unsigned count = 0, rand;
	unsigned c;
	char *iter_key = gen_lookup(optarg);

	c = qmap_iter(prim_hd + !reverse, iter_key);

	while (qmap_next((unsigned *) key_buf, value_buf, c))
		if (assoc_exists(key_buf))
			count ++;

	if (count == 0) {
		printf("-1\n");
		return;
	}

	rand = random() % count;
	c = qmap_iter(prim_hd + !reverse, iter_key);

	while (qmap_next((unsigned *) key_buf, value_buf, c))
		if (!assoc_exists(key_buf))
			continue;
		else if ((--count) <= rand) {
			qmap_fin(c);
			break;
		}

	_gen_get();
}

static void gen_get(char *str) {
	char *iter_key = gen_lookup(str);
	unsigned c;
	unsigned nonce = 1;

	qdb_get_buf = value_buf;
	qdb_get_type = reverse ? QMAP_VALUE : QMAP_KEY;

	if (str && strcmp(str, ".") && !iter_key) {
		printf("-1\n");
		return;
	}

	c = qmap_iter(prim_hd + !reverse, iter_key);

	while (qmap_next(key_buf, value_buf, c))
		if (assoc_exists(key_buf)) {
			_gen_get();
			nonce = 0;
		}

	if (nonce)
		printf("-1\n");
}

static void gen_list(void) {
	unsigned c;
	unsigned cond, aux;

	gen_lookup(NULL);
	cond = gen_cond(1);

	c = qmap_iter(prim_hd, NULL);

	qdb_get_type = QMAP_VALUE;
	qdb_get_buf = key_buf;
	aux = print_keys;
	print_keys = 1;
	while (qmap_next(key_buf, value_buf, c)) {
		rec_query(qhds, key_buf, value_buf, !cond);
		_gen_get();
	}
	print_keys = aux;
}

static inline void gen_put(void) {
	unsigned id;
	char *key;
	char pbuf[BUFSIZ];
	memset(pbuf, 0, sizeof(pbuf));

	gen_lookup(optarg);

	key = col ? key_buf : NULL;
	id = qmap_put(prim_hd, key, value_buf);
	qmap_print(pbuf, prim_hd, QMAP_KEY,
			key ? key : (char *) &id);
	printf("%s\n", pbuf);
}

static inline void gen_list_missing(void) {
	unsigned aux;
	unsigned c;
	gen_lookup(NULL);

	if (!qhds_n) {
		fprintf(stderr, "list missing needs a corresponding database\n");
		return;
	}

	c = qmap_iter(prim_hd + !reverse, NULL);
	while (qmap_next(key_buf, value_buf, c)) {
		unsigned c2 = qmap_iter(qhds, NULL);

		while (qmap_next(&aux, &aux_hd, c2))
			if (qmap_get(aux_hd + !reverse,
						key_buf, value_buf))
				_gen_get();
	}
}

unsigned gen_open(char *fname, unsigned flags) {
	char buf[BUFSIZ], *key_type = "s", *value_type = "s";

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
			key_type = "u";
		} else if (*first_col == '2') {
			flags |= QH_DUP;
			first_col++;
			if (*first_col)
				key_type = first_col;
		} else
			key_type = first_col;
	}

	qdb_config.file = buf;
	qdb_config.mode = flags & QH_RDONLY ? 0644 : 0664;
	return qdb_open("", key_type, value_type, flags | QMAP_TWO_WAY);
}

int
main(int argc, char *argv[])
{
	static char *optstr = "kxla:q:p:d:g:rR:L:?";
	char *fname = argv[argc - 1], ch;
	unsigned flags = QH_RDONLY;

	if (argc < 2) {
		usage(*argv);
		return EXIT_FAILURE;
	}

	qmap_init();
	qdb_init();

	ahds = qdb_open(NULL, "u", "u", QH_AINDEX);
	qhds = qdb_open(NULL, "u", "u", QH_AINDEX);

	while ((ch = getopt(argc, argv, optstr)) != -1)
		switch (ch) {
		case 'a':
			aux_hd = gen_open(optarg, QH_RDONLY);
			qmap_put(ahds, NULL, &aux_hd);
			ahds_n++;
			break;
		case 'q':
			aux_hd = gen_open(optarg, QH_RDONLY);
			qmap_put(qhds, NULL, &aux_hd);
			qhds_n++;
			break;
		case 'x':
			bail = 1;
			break;
		case 'k':
			print_keys = 1;
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
	prim_hd = gen_open(fname, flags);
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

	qdb_close(prim_hd, 0);

	unsigned key;
	unsigned c = qmap_iter(ahds, NULL);

	while (qmap_next(&key, &aux_hd, c))
		qdb_close(aux_hd, 0);

	c = qmap_iter(qhds, NULL);

	while (qmap_next(&key, &aux_hd, c))
		qdb_close(aux_hd, 0);
}
