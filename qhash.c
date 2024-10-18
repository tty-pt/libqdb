#include "include/qhash.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

unsigned hd, rhd, qhd, qrhd, ahd, arhd, ihd, iqrhd, iahd, iqhd;
unsigned tmp_id, mode = 0, reverse = 0, ign;
char key_buf[BUFSIZ], value_buf[BUFSIZ], *col;
struct hash_cursor c;

void
usage(char *prog)
{
	fprintf(stderr, "Usage: %s [-mqa ARG] [[-rl] [-Rpdg ARG] ...] db_path", prog);
	fprintf(stderr, "    Options:\n");
	fprintf(stderr, "        -r           reverse operation\n");
	fprintf(stderr, "        -l           list all values (not reversed in mode 1)\n");
	fprintf(stderr, "        -L           list missing values (mode 1)\n");
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

	tmp_id = ahd_get(qrhd, optarg);

	col++;
	ign = ahd_get(arhd, col);

	ahash_remove(hd, tmp_id, ign);
	ahash_remove(rhd, ign, tmp_id);
	return 1;
}

static inline void mode1_delete() {
	if (mode1_delete_one())
		return;

	tmp_id = ahd_get(iqrhd, optarg);

	c = hash_iter(ihd, &tmp_id, sizeof(tmp_id));

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
	lhash_get(hd, key_buf, tmp_id);
	shash_del(rhd, key_buf);
	lhash_del(hd, tmp_id);
}

static inline void any_rand() {
	struct idm_list list = idml_init();
	unsigned count = 0, rand;

	tmp_id = ahd_get(iqrhd, optarg);

	if (mode)
		c = hash_iter(ihd, &tmp_id, sizeof(tmp_id));
	else
		c = hash_iter(ihd, NULL, 0);

	while (lhash_next(&tmp_id, &ign, &c)) {
		idml_push(&list, ign);
		count ++;
	}

	if (count == 0) {
		printf("%u -1\n", tmp_id);
		return;
	}

	rand = random() % count;

	while ((ign = idml_pop(&list)) != ((unsigned) -1)) {
		count --;
		if (count <= rand)
			break;
	}

	if (!mode)
		tmp_id = 0;

	if (iahd != -1) {
		lhash_get(iahd, key_buf, tmp_id);
		printf("%u %u %s\n", tmp_id, ign, key_buf);
		return;
	}

	idml_drop(&list);
	printf("%u %u\n", tmp_id, ign);
}

static inline void mode1_get() {
	unsigned count = 0;
	tmp_id = ahd_get(iqrhd, optarg);

	c = hash_iter(ihd, &tmp_id, sizeof(tmp_id));

	if (iahd != -1) {
		while (lhash_next(&ign, &tmp_id, &c)) {
			lhash_get(iahd, key_buf, tmp_id);
			printf("%u %u %s\n", ign, tmp_id, key_buf);
			count++;
		}
		goto end;
	}

	while (lhash_next(&ign, &tmp_id, &c)) {
		printf("%u %u\n", ign, tmp_id);
		count++;
	}

end:
	if (!count)
		printf("%u -1\n", tmp_id);
}

static inline void mode0_get() {
	if (reverse) {
		if (shash_get(rhd, &tmp_id, optarg)) {
			printf("%u -1\n", tmp_id);
			return;
		}
		printf("%u\n", tmp_id);
		return;
	}

	ign = strtoul(optarg, NULL, 10);
	if (uhash_get(hd, &key_buf, ign)) {
		printf("%u -1\n", ign);
		return;
	}

	printf("%u %s\n", ign, key_buf);
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

	tmp_id = lhash_new(hd, optarg);
	suhash_put(rhd, optarg, tmp_id);
	uhash_put(hd, tmp_id, optarg, strlen(optarg) + 1);
	printf("%u\n", tmp_id);
}

static inline void mode1_list_missing() {
	if (mode != 1) {
		fprintf(stderr, "list missing only possible in mode 1\n");
		return;
	}

	if (iqhd == -1) {
		fprintf(stderr, "list missing needs a corresponding database\n");
		return;
	}

	c = hash_iter(iqhd, NULL, 0);

	while (lhash_next(&ign, key_buf, &c)) {
		if (uhash_get(ihd, key_buf, ign))
			printf("%u %s\n", ign, key_buf);
	}
}

static inline void mode1_list() {
	c = hash_iter(hd, NULL, 0);

	if (qhd != -1) {
		while (lhash_next(&ign, &tmp_id, &c)) {
			lhash_get(qhd, key_buf, ign);
			printf("%u %u %s\n", ign, tmp_id, key_buf);
		}
		return;
	}

	if (ahd == -1) {
		while (lhash_next(&ign, &tmp_id, &c))
			printf("%u %u\n", ign, tmp_id);
		return;
	}

	while (lhash_next(&ign, &tmp_id, &c)) {
		lhash_get(ahd, key_buf, tmp_id);
		printf("%u %u %s\n", ign, tmp_id, key_buf);
	}
}

static inline void mode0_list() { // no reverse
	c = lhash_iter(hd);

	while (lhash_next(&tmp_id, key_buf, &c))
		printf("%u %s\n", tmp_id, key_buf);
}

int
main(int argc, char *argv[])
{
	static char *optstr = "la:q:p:d:g:m:rR:L:?";
	char *fname = argv[argc - 1], ch, *col;
	int fmode = 0644;

	if (argc < 2) {
		usage(*argv);
		return EXIT_FAILURE;
	}

	ahd = qhd = qrhd = arhd = -1;

	while ((ch = getopt(argc, argv, optstr)) != -1)
		switch (ch) {
		case 'm':
			mode = strtoul(optarg, NULL, 10);

			if (mode >= 0 || mode <= 1)
				continue;

			fprintf(stderr, "Invalid mode\n");
			return EXIT_FAILURE;
		case 'a':
			ahd = lhash_cinit(0, optarg, "hd", 0444);
			arhd = hash_cinit(optarg, "rhd", 0444, 0);
			break;
		case 'q':
			qhd = lhash_cinit(0, optarg, "hd", 0444);
			qrhd = hash_cinit(optarg, "rhd", 0444, 0);
			break;
		case 'p':
		case 'd': fmode = 0644;
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
		rhd = hash_cinit(fname, "rhd", fmode, 0);
	}

	srandom(time(NULL));

	iahd = ahd; iqrhd = qrhd; ihd = hd; iqhd = qhd;

	while ((ch = getopt(argc, argv, optstr)) != -1) {
		switch (ch) {
		case 'R': any_rand(); break;
		case 'L': mode1_list_missing(); break;
		case 'l': if (mode) mode1_list(); else mode0_list(); break;
		case 'p': if (mode) mode1_put(); else mode0_put(); break;
		case 'd': if (mode) mode1_delete(); else mode0_delete(); break;
		case 'g': if (mode) mode1_get(); else mode0_get(); break;
		case 'r':
			reverse = !reverse;

			if (reverse) {
				iqrhd = arhd; iahd = qhd; ihd = rhd; iqhd = ahd;
			} else {
				iqrhd = qrhd; iahd = ahd; ihd = hd; iqhd = qhd;
			}

			break;
		}
	}

	if (!mode)
		lhash_close(hd);
	else
		hash_close(hd);

	hash_close(rhd);

	if (ahd != (unsigned) -1)
		lhash_close(ahd);
	if (qhd != (unsigned) -1)
		hash_close(qhd);
}
