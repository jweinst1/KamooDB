#include "kamoodb.h"
#include <time.h>

#define SEC_TO_US(sec) ((sec)*1000000)
#define NS_TO_US(ns)    ((ns)/1000)

static char get_rand_char(void) {
	char f = rand() % 112;
	return f > 0 ? f : 44;
}

static char* get_rand_str(size_t n) {
	char* buf = calloc(1, n + 1 );
	for (int i = 0; i < n; ++i)
	{
		buf[i] = get_rand_char();
	}
	return buf;
}

static char* RAND_STR_ARR[1000000];
static const size_t RAND_ARR_SIZE = sizeof(RAND_STR_ARR) / sizeof(RAND_STR_ARR[0]);

static void fill_rand_arr(void) {
	for (size_t i = 0; i < RAND_ARR_SIZE; ++i)
	{
		RAND_STR_ARR[i] = get_rand_str(30);
	}
}

static uint64_t micro_stamp() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    uint64_t us = SEC_TO_US((uint64_t)ts.tv_sec) + NS_TO_US((uint64_t)ts.tv_nsec);
    return us;
}

int main(int argc, char const *argv[])
{
	struct database db;
	srand(time(NULL));
	fill_rand_arr();
	printf("Will hash and insert %zu keys and values\n", RAND_ARR_SIZE);
	database_open(&db, "bench", NULL);
	uint64_t start = micro_stamp();
	for (size_t i = 0; i < RAND_ARR_SIZE; ++i)
	{
		database_put(&db, RAND_STR_ARR[i], RAND_STR_ARR[i]);
	}
	uint64_t end = micro_stamp();
	printf("Time taken %lluus\n", end - start);
	database_close_and_remove(&db);
	return 0;
}