#include "include/qhash.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

unsigned hd, rhd, qhd, ahd, ihd, irhd, iqhd, iahd;

void
usage(char *prog)
{
	fprintf(stderr, "Usage: %s [-m ARG] [[-rl] [-LRpdg ARG] ...] db_path", prog);
	fprintf(stderr, "    Options:\n");
	fprintf(stderr, "        -l           list values\n");
	fprintf(stderr, "        -L KEY       list values of key (mode 0)\n");
	fprintf(stderr, "        -q other_db  associative querying\n");
	fprintf(stderr, "        -a other_db  associative printing\n");
	fprintf(stderr, "        -R KEY       get random value of key (key is ignored in mode 0)\n");
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
	static char *optstr = "lL:a:q:p:d:g:m:rR:";
	char key_buf[BUFSIZ], value_buf[BUFSIZ];
	char *fname = argv[argc - 1], ch, *col;
	unsigned tmp_id, mode = 0, reverse = 0, ign;
	struct hash_cursor c;
	int fmode = 0444;

	if (argc < 2)
		usage(*argv);

	ahd = qhd = -1;

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
			break;
		case 'q':
			qhd = hash_cinit(optarg, "rhd", 0444, 0);
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

	if (mode) {
		hd = ahash_cinit(fname, "hd", fmode);
		rhd = ahash_cinit(fname, "rhd", fmode);
	} else {
		hd = lhash_cinit(0, fname, "hd", fmode);
		rhd = hash_cinit(fname, "rhd", fmode, 0);
	}

	srandom(time(NULL));

	iahd = ahd;
	iqhd = qhd;
	ihd = hd;
	irhd = rhd;

	while ((ch = getopt(argc, argv, optstr)) != -1) {
		switch (ch) {
		case 'R':
			{
				struct idm_list list = idml_init();
				unsigned count = 0, rand;

				if (iqhd == -1)
					tmp_id = strtoul(optarg, NULL, 10);
				else
					shash_get(iqhd, &tmp_id, optarg);

				c = hash_iter(irhd, mode ? &tmp_id : NULL, sizeof(tmp_id));

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

				if (iahd == -1)
					printf("%u\n", tmp_id);
				else {
					lhash_get(iahd, key_buf, tmp_id);
					printf("%u %s\n", tmp_id, key_buf);
				}
			}

			break;

		case 'L':
			if (!mode) {
				fprintf(stderr, "listing values of a key is not valid in mode 0\n");
				break;
			}

			if (iqhd == -1)
				tmp_id = strtoul(optarg, NULL, 10);
			else
				shash_get(iqhd, &tmp_id, optarg);

			c = hash_iter(irhd, &tmp_id, sizeof(tmp_id));

			if (iahd == -1)
				while (lhash_next(&ign, &tmp_id, &c))
					printf("%u\n", tmp_id);
			else
				while (lhash_next(&ign, &tmp_id, &c)) {
					lhash_get(iahd, key_buf, tmp_id);
					printf("%u %s\n", tmp_id, key_buf);
				}
			break;

		case 'l':
			if (mode) {
				c = hash_iter(ihd, NULL, 0);

				if (iqhd == -1) {
					if (iahd == -1)
						while (lhash_next(&ign, &tmp_id, &c))
							printf("%u %u\n", ign, tmp_id);
					else
						while (lhash_next(&ign, &tmp_id, &c)) {
							lhash_get(iahd, key_buf, tmp_id);
							printf("%u %s\n", ign, key_buf);
						}
				} else if (iahd == -1)
					while (lhash_next(&ign, &tmp_id, &c)) {
						lhash_get(iqhd, key_buf, ign);
						printf("%s %u\n", key_buf, tmp_id);
					}
				else
					while (lhash_next(&ign, &tmp_id, &c)) {
						lhash_get(iqhd, key_buf, ign);
						lhash_get(iahd, value_buf, tmp_id);
						printf("%s %s\n", key_buf, value_buf);
					}
			} else {
				c = lhash_iter(hd);

				while (lhash_next(&tmp_id, key_buf, &c))
					printf("%u %s\n", tmp_id, key_buf);
			}

			break;

		case 'p':
			if (mode) {
				col = strchr(optarg, ':');
				if (!col) {
					fprintf(stderr, "putting format in mode 1 is key:value\n");
					break;
				}
				*col = '\0';

				if (iqhd == -1)
					ign = strtoul(optarg, NULL, 10);
				else
					shash_get(iqhd, &ign, optarg);

				tmp_id = strtoul(col + 1, NULL, 10);
				ahash_add(ihd, ign, tmp_id);
				ahash_add(irhd, tmp_id, ign);
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
				col = strchr(optarg, ':');
				if (!col) {
					fprintf(stderr, "deleting format in mode 1 is key:value\n");
					return EXIT_FAILURE;
				}
				*col = '\0';

				if (iqhd == -1)
					ign = strtoul(optarg, NULL, 10);
				else
					shash_get(iqhd, &ign, optarg);

				if (iahd == -1)
					tmp_id = strtoul(col + 1, NULL, 10);
				else
					shash_get(iahd, &tmp_id, col + 1);

				ahash_remove(ihd, ign, tmp_id);
				ahash_remove(irhd, tmp_id, ign);
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
				if (iqhd == -1)
					ign = strtoul(optarg, NULL, 10);
				else
					shash_get(iqhd, &ign, optarg);

				uhash_get(ihd, &tmp_id, ign);

				if (ahd == -1)
					printf("%u\n", tmp_id);
				else {
					lhash_get(iahd, key_buf, tmp_id);
					printf("%u %s\n", tmp_id, key_buf);
				}
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

			if (reverse) {
				iahd = qhd;
				iqhd = ahd;
				ihd = rhd;
				irhd = hd;
			} else {
				iahd = ahd;
				iqhd = qhd;
				ihd = hd;
				irhd = rhd;
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
