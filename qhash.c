#include "include/qhash.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

unsigned hd, rhd;

void
usage(char *prog)
{
	fprintf(stderr, "Usage: %s [-m ARG] [[-rl] [-LRpdg ARG] ...] db_path", prog);
	fprintf(stderr, "    Options:\n");
	fprintf(stderr, "        -l           list values\n");
	fprintf(stderr, "        -L KEY       list values of key (mode 0)\n");
	fprintf(stderr, "        -R KEY       get random value of key (mode 0)\n");
	fprintf(stderr, "        -p [KEY:]VAL put a value\n");
	fprintf(stderr, "        -d [KEY:]VAL delete a value\n");
	fprintf(stderr, "        -g KEY       get a value\n");
	fprintf(stderr, "        -m 0-1       select mode of operation\n");
	fprintf(stderr, "        -r           reverse operation\n");
	fprintf(stderr, "    Modes (default is 0):\n");
	fprintf(stderr, "         0           index mode (1 string : 1 id)\n");
	fprintf(stderr, "         1           associative mode (n id : n id)\n");
	exit(1);
}


int
main(int argc, char *argv[])
{
	static char *optstr = "lL:p:d:g:m:rR:";
	char key_buf[BUFSIZ];
	char *fname = argv[argc - 1], ch, *col;
	unsigned tmp_id, mode = 0, reverse = 0, ign;
	struct hash_cursor c;
	int fmode = 0444;

	if (argc < 2)
		usage(*argv);

	while ((ch = getopt(argc, argv, optstr)) != -1)
		switch (ch) {
		case 'm':
			mode = strtoul(optarg, NULL, 10);

			if (mode >= 0 || mode <= 1)
				continue;

			fprintf(stderr, "Invalid mode\n");
			return EXIT_FAILURE;
		case 'p':
		case 'd':
			fmode = 0644;
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

	while ((ch = getopt(argc, argv, optstr)) != -1) {
		switch (ch) {
		case 'R':
			if (!mode) {
				fprintf(stderr, "getting random value of a key is not valid in mode 0\n");
				break;
			}

			{
				struct idm_list list = idml_init();
				unsigned count = 0, rand;

				tmp_id = strtoul(optarg, NULL, 10);
				c = hash_iter(reverse ? hd : rhd, &tmp_id, sizeof(tmp_id));

				while (lhash_next(&ign, &tmp_id, &c)) {
					idml_push(&list, tmp_id);
					count ++;
				}

				
				if (count == 0) {
					printf("-1\n");
					break;
				}

				rand = random() % count;


				while ((tmp_id = idml_pop(&list)) != ((unsigned) -1)) {
					count --;
					if (count <= rand)
						break;
				}

				while (idml_pop(&list) != ((unsigned) -1));

				printf("%u\n", tmp_id);
			}

			break;

		case 'L':
			if (!mode) {
				fprintf(stderr, "listing values of a key is not valid in mode 0\n");
				break;
			}
			tmp_id = strtoul(optarg, NULL, 10);
			c = hash_iter(reverse ? hd : rhd, &tmp_id, sizeof(tmp_id));

			while (lhash_next(&ign, &tmp_id, &c))
				printf("%u\n", tmp_id);
			break;

		case 'l':
			if (mode) {
				c = hash_iter(reverse ? rhd : hd, NULL, 0);

				while (lhash_next(&ign, &tmp_id, &c))
					printf("%u:%u\n", ign, tmp_id);
			} else {
				c = lhash_iter(hd);

				while (lhash_next(&tmp_id, key_buf, &c))
					printf("%u %s\n", tmp_id, key_buf);
			}

			break;

		case 'p':
			if (mode) {
				ign = strtoul(optarg, NULL, 10);
				col = strchr(optarg, ':');
				if (!col) {
					fprintf(stderr, "putting format in mode 1 is key:value\n");
					break;
				}
				tmp_id = strtoul(col + 1, NULL, 10);
				ahash_add(reverse ? rhd : hd, ign, tmp_id);
				ahash_add(reverse ? hd : rhd, tmp_id, ign);
				break;
			}

			if (reverse) {
				fprintf(stderr, "reverse putting in mode 0 is not supported\n");
				return EXIT_FAILURE;
			}

			tmp_id = lhash_new(hd, optarg);
			suhash_put(rhd, optarg, tmp_id);
			uhash_put(hd, tmp_id, optarg, strlen(optarg) + 1);
			printf("%u\n", tmp_id);
			break;

		case 'd':
			if (mode) {
				ign = strtoul(optarg, NULL, 10);
				col = strchr(optarg, ':');
				if (!col) {
					fprintf(stderr, "deleting format in mode 1 is key:value\n");
					return EXIT_FAILURE;
				}
				tmp_id = strtoul(col + 1, NULL, 10);
				ahash_remove(reverse ? rhd : hd, ign, tmp_id);
				ahash_remove(reverse ? hd : rhd, tmp_id, ign);
				break;
			}
			if (reverse) {
				shash_get(rhd, &tmp_id, optarg);
				lhash_del(hd, tmp_id);
				printf("%u\n", tmp_id);
				break;
			}

			tmp_id = strtoul(optarg, NULL, 10);
			lhash_get(hd, key_buf, tmp_id);
			shash_del(rhd, key_buf);
			lhash_del(hd, tmp_id);
			printf("%s\n", key_buf);
			break;

		case 'g':
			if (mode) {
				ign = strtoul(optarg, NULL, 10);
				uhash_get(reverse ? rhd : hd, &tmp_id, ign);
				printf("%u\n", tmp_id);
				break;
			}

			if (reverse) {
				shash_get(rhd, &tmp_id, optarg);
				printf("%u\n", tmp_id);
				break;
			}

			ign = strtoul(optarg, NULL, 10);
			uhash_get(hd, &key_buf, ign);
			printf("%s\n", key_buf);
			break;

		case 'r':
			reverse = !reverse;
			break;

		case 'm': break;
		default: usage(*argv); return EXIT_FAILURE;
		case '?': usage(*argv); return EXIT_SUCCESS;
		}
	}

	if (!mode)
		lhash_close(hd);
	else
		hash_close(hd);
	hash_close(rhd);
}
