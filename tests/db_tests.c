#include "kamoodb.h"

//------- tests ---------

static unsigned _failures = 0;

static void check_cond(int cond, const char* condstr, unsigned line) {
	if (!cond) {
		fprintf(stderr, "Failed cond '%s' at line %u\n", condstr, line);
		++_failures;
	}
}

#define CHECKIT(cnd) check_cond(cnd, #cnd, __LINE__)

static void test_djb2_n(void) {
	const char* test1 = "foobarfoobar";
	size_t hash1 = hash_djb2(test1);
	size_t base = DJB2_HASH_BASE;
	hash_djb2_n(test1, 6, &base);
	hash_djb2_n(test1 + 6, 6, &base);
	CHECKIT(hash1 == base);
}

static void test_page_vec(void) {
	struct page_vec pv;
	page_vec_init(&pv);
	CHECKIT(pv.len == 0);
	int times = pv.cap + 2;
	for (int32_t i = 0; i < times; ++i)
	{
		page_vec_push(&pv, i);
	}
	CHECKIT(pv.cap > times);
	page_vec_clear(&pv);
	CHECKIT(pv.len == 0);
	page_vec_deinit(&pv);
	CHECKIT(pv.pages == NULL);
}

static void test_dbfile_rw(void) {
	struct dbfile foo;
	dbfile_open(&foo, "boof", NULL);
	unsigned char buf[3000];
	memset(buf, 255, sizeof(buf));
	dbfile_write(&foo, foo.page_size - 1000, (char*)buf, sizeof(buf));
	memset(buf, 0, sizeof(buf));
	dbfile_read(&foo, foo.page_size - 1000, (char*)buf, sizeof(buf));
	for (int i = 0; i < sizeof(buf); ++i)
	{
		CHECKIT(buf[i] == 255);
	}
	dbfile_close(&foo);
	dbfile_remove(&foo);
	dbfile_path_free(&foo);
}

static void test_dbfile_cmp(void) {
	struct dbfile foo;
	const char* str1 = "abcd";
	const char* str2 = "efgh";
	size_t str1_size = strlen(str1) + 1;
	size_t str2_size = strlen(str2) + 1;
	dbfile_open(&foo, "boof", NULL);
	dbfile_write(&foo, 4, str1, str1_size);
	dbfile_write(&foo, 4 + str1_size, str2, str2_size);
	CHECKIT(dbfile_cmp_null(&foo, 0, 4, "abcd", 5) == 1);
	CHECKIT(dbfile_cmp_null(&foo, 0, 4, "abcd", 8) == 1); // null stops
	CHECKIT(dbfile_cmp_null(&foo, 0, 4, "efgh", 5) == 0);
	CHECKIT(dbfile_cmp_null(&foo, 0, 4 + str1_size, "efgh", 5) == 1);
	// different size compare
	CHECKIT(dbfile_cmp_null(&foo, 0, 4, "abcde", 6) == 0);
	CHECKIT(dbfile_cmp_null(&foo, 0, 4, "abc", 4) == 0);

	unsigned char* big_str = calloc(1, 4000);
	memset(big_str, 'd', 3999);
	dbfile_write(&foo, foo.page_size - 1000, (char*)big_str, 4000);
	CHECKIT(dbfile_cmp_null(&foo, 0, foo.page_size - 1000, (char*)big_str, 4000) == 1);
	free(big_str);
	dbfile_close(&foo);
	dbfile_remove(&foo);
	dbfile_path_free(&foo);
}

static void test_dbfile_hash_null(void) {
	struct dbfile foo;
	const char* str1 = "abcddd";
	const char* str2 = "efgh";
	size_t str1_size = strlen(str1) + 1;
	size_t str2_size = strlen(str2) + 1;
	size_t str1_hash = hash_djb2(str1);
	size_t str2_hash = hash_djb2(str2);
	dbfile_open(&foo, "boof", NULL);
	dbfile_write(&foo, 4, str1, str1_size);
	dbfile_write(&foo, 4 + str1_size, str2, str2_size);
	CHECKIT(dbfile_hash_null(&foo, 0, 4) == str1_hash);
	CHECKIT(dbfile_hash_null(&foo, 0, 4 + str1_size) == str2_hash);
	dbfile_close(&foo);
	dbfile_remove(&foo);
	dbfile_path_free(&foo);
}

static void test_dbfile_grow(void) {
	struct dbfile foo;
	dbfile_open(&foo, "boof", NULL);
	CHECKIT(foo.page_count == 1);
	int32_t new_block = dbfile_grow(&foo, 1);
	CHECKIT(foo.page_count == 2);
	CHECKIT(new_block == (foo.page_count - 1));
	dbfile_close(&foo);
	dbfile_remove(&foo);
	dbfile_path_free(&foo);
}

static void test_database_open_close(void) {
	struct database db;
	CHECKIT(database_open(&db, "boof", NULL));
	CHECKIT(database_get_hashroot(&db) != -1);
	CHECKIT(database_get_spaceroot(&db) != -1);
	CHECKIT(database_get_hash_len(&db) == (1 * hashes_per_block(db.dbf.page_size)));
	database_close_and_remove(&db);
}

static void test_database_add_space(void) {
	struct database db;
	CHECKIT(database_open(&db, "boof", NULL));
	CHECKIT(database_get_hashroot(&db) != -1);
	CHECKIT(database_get_spaceroot(&db) != -1);
	CHECKIT(database_get_space_block_count(&db) == 1);
	CHECKIT(database_add_space_block(&db) != -1);
	CHECKIT(database_get_space_block_count(&db) == 2);
	CHECKIT(database_add_space_block(&db) != -1);
	CHECKIT(database_get_space_block_count(&db) == 3);
	database_close_and_remove(&db);
}

static void test_database_find_space_ptr(void) {
	int32_t result[3];
	size_t page_size = 4096;
	char* page = calloc(1, page_size);
	int32_t* writer = (int32_t*)page;
	writer[0] = -1;
	writer[1] = 1;
	writer[2] = 66;
	writer[3] = 102;
	writer[4] = 200; // size
	CHECKIT(database_find_space_ptr(page, 32, page_size, result) != -1);
	CHECKIT(result[0] == 66);
	CHECKIT(result[1] == 102);
	CHECKIT(result[2] == 32);
	CHECKIT(writer[4] == (200-32));
	free(page);
}

static void test_database_find_space_ptr_zero(void) {
	int32_t result[3];
	size_t page_size = 4096;
	char* page = calloc(1, page_size);
	int32_t* writer = (int32_t*)page;
	writer[0] = -1;
	writer[1] = 1;
	writer[2] = 66;
	writer[3] = 102;
	writer[4] = 32; // size
	writer[5] = 201; // extra byte
	CHECKIT(database_find_space_ptr(page, 32, page_size, result) != -1);
	CHECKIT(result[0] == 66);
	CHECKIT(result[1] == 102);
	CHECKIT(result[2] == 32);
	CHECKIT(writer[1] == 0); // todo len function
	CHECKIT(writer[2] == 201); // extra byte moved
	free(page);
}

static void test_database_allocate(void) {
	struct database db;
	int32_t result1[3];
	int32_t result2[3];
	CHECKIT(database_open(&db, "boof", NULL));
	CHECKIT(database_get_hashroot(&db) != -1);
	CHECKIT(database_get_spaceroot(&db) != -1);
	CHECKIT(database_allocate_storage(&db, 32, result1) == 0);
	CHECKIT(database_allocate_storage(&db, 32, result2) == 0);
	CHECKIT(result1[0] == result2[0]);
	CHECKIT(result1[1] != result2[1]);
	CHECKIT(result1[2] == result2[2]);
	CHECKIT(result1[2] == 32);
	database_close_and_remove(&db);
}

static void test_database_allocate_large(void) {
	struct database db;
	int32_t result1[3];
	int32_t result2[3];
	CHECKIT(database_open(&db, "boof", NULL));
	size_t page_size = db.dbf.page_size;
	CHECKIT(database_get_hashroot(&db) != -1);
	CHECKIT(database_get_spaceroot(&db) != -1);
	CHECKIT(database_allocate_storage(&db, 32, result1) == 0);
	CHECKIT(database_allocate_storage(&db, page_size * 2, result2) == 1);
	CHECKIT(result1[0] != result2[0]);
	CHECKIT(result1[1] == result2[1]); // both will begin in a new block
	CHECKIT(result1[2] != result2[2]);
	CHECKIT(result1[2] == 32);
	database_close_and_remove(&db);
}

static void test_database_deallocate(void) {
	struct database db;
	int32_t result1[3];
	int32_t result2[3];
	CHECKIT(database_open(&db, "boof", NULL));
	int32_t space = database_get_spaceroot(&db);
	char* space_page = dbfile_get_page(&db.dbf, space);
	CHECKIT(space != -1);
	CHECKIT(database_allocate_storage(&db, 32, result1) == 0);
	CHECKIT(database_allocate_storage(&db, 32, result2) == 0);
	int32_t before_len = database_len_get(space_page); // todo length function
	database_deallocate_storage(&db, result1);
	CHECKIT(database_len_get(space_page) == before_len + 1);
	database_deallocate_storage(&db, result2);
	CHECKIT(database_len_get(space_page) == before_len + 2);
	database_close_and_remove(&db);
}

static void test_database_add_hash_block(void) {
	struct database db;
	CHECKIT(database_open(&db, "boof", NULL));
	CHECKIT(database_get_hashroot(&db) != -1);
	CHECKIT(database_get_spaceroot(&db) != -1);
	size_t hash_len1 = database_get_hash_block_count(&db);
	database_add_hash_block(&db, 1);
	database_recomp_hash_len(&db);
	size_t hash_len2 = database_get_hash_block_count(&db);
	CHECKIT(hash_len1 == (hash_len2 - 1));
	database_add_hash_block(&db, 4);
	size_t hash_len3 = database_get_hash_block_count(&db);
	CHECKIT(hash_len2 == (hash_len3 - 4));
	database_close_and_remove(&db);
}

static void test_database_key_cmp(void) {
	struct database db;
	int32_t result1[3];
	const char* keystr = "abcdef";
	size_t keystr_size = strlen(keystr) + 1;
	CHECKIT(database_open(&db, "boof", NULL));
	CHECKIT(database_allocate_storage(&db, keystr_size, result1) == 0);
	dbfile_write_po(&db.dbf, result1[0], result1[1], keystr, keystr_size);
	CHECKIT(database_compare_key(&db, keystr, keystr_size, result1));
	database_close_and_remove(&db);
}

static void test_database_hash_and_probe(void) {
	struct database db;
	int32_t store_ptr1[3];
	int32_t store_ptr2[3];

	const char* keystr1 = "abcdef";
	const char* keystr2 = "abcdefg";
	size_t keystr1_size = strlen(keystr1) + 1;
	size_t keystr2_size = strlen(keystr2) + 1;
	CHECKIT(database_open(&db, "boof", NULL));
	CHECKIT(database_allocate_storage(&db, keystr1_size, store_ptr1) == 0);
	CHECKIT(database_allocate_storage(&db, keystr2_size, store_ptr2) == 0);
	dbfile_write_po(&db.dbf, store_ptr1[0], store_ptr1[1], keystr1, keystr1_size);
	dbfile_write_po(&db.dbf, store_ptr2[0], store_ptr2[1], keystr2, keystr2_size);
	int32_t hashroot = database_get_hashroot(&db);
	int32_t* found1 = database_hash_and_probe(&db, keystr1, keystr1_size, hashroot, NULL, 1);
	CHECKIT(found1 != NULL);
	_write_storage_ptr_hash(found1, store_ptr1);
	int32_t* found2 = database_hash_and_probe(&db, keystr2, keystr2_size, hashroot, NULL, 1);
	CHECKIT(found2 != NULL);
	CHECKIT(found1 != found2);
	_write_storage_ptr_hash(found2, store_ptr2);
	char* hashpage = dbfile_get_page(&db.dbf, hashroot) + HASH_BLOCK_HEADER_SIZE;
	int32_t* reader = (int32_t*)hashpage;
	CHECKIT(reader[0] == store_ptr1[0]);
	CHECKIT(reader[1] == store_ptr1[1]);
	CHECKIT(reader[2] == store_ptr1[2]);
	hashpage += HASHSTORAGE_PTR_SIZE;
	reader = (int32_t*)hashpage;
	CHECKIT(reader[0] == store_ptr2[0]);
	CHECKIT(reader[1] == store_ptr2[1]);
	CHECKIT(reader[2] == store_ptr2[2]);
	database_close_and_remove(&db);
}

static void test_database_put_get_del(void) {
	struct database db;
	const char* key1 = "abcdef";
	const char* key2 = "abc5ef";
	const char* val1 = "abcdefg";
	const char* val2 = "bbbbbb";
	char* key1res = NULL;
	char* key2res = NULL;
	CHECKIT(database_open(&db, "boof", NULL));
	CHECKIT(database_put(&db, key1, val1));
	key1res = database_get(&db, key1);
	CHECKIT(strcmp(key1res, val1) == 0);
	CHECKIT(database_put(&db, key1, val2));
	free(key1res);
	key1res = database_get(&db, key1);
	CHECKIT(strcmp(key1res, val2) == 0);
	CHECKIT(database_put(&db, key2, val2));
	key2res = database_get(&db, key2);
	CHECKIT(strcmp(key2res, key1res) == 0);
	CHECKIT(database_del(&db, key1));
	CHECKIT(database_del(&db, key2));
	CHECKIT(database_get(&db, key1) == NULL);
	CHECKIT(database_get(&db, key2) == NULL);
	free(key1res);
	free(key2res);
	database_close_and_remove(&db);
}

static void test_database_put_get_reopen(void) {
	struct database db;
	const char* key1 = "abcdef";
	const char* val1 = "abcdefg";
	char* key1res = NULL;
	CHECKIT(database_open(&db, "boof", NULL));
	CHECKIT(database_put(&db, key1, val1));
	key1res = database_get(&db, key1);
	CHECKIT(strcmp(key1res, val1) == 0);
	free(key1res);
	database_close(&db);
	memset(&db, 0, sizeof(db));
	CHECKIT(database_open(&db, "boof", NULL));
	key1res = database_get(&db, key1);
	CHECKIT(strcmp(key1res, val1) == 0);
	free(key1res);
	database_close_and_remove(&db);
}

static void test_database_load_factor(void) {
	struct database db;
	const char* key1 = "abcdef";
	const char* key2 = "abc5ef";
	const char* val1 = "abcdefg";
	const char* val2 = "bbbbbb";
	CHECKIT(database_open(&db, "boof", NULL));
	double lf1 =  database_get_load_factor(&db);
	CHECKIT(database_put(&db, key1, val1));
	double lf2 =  database_get_load_factor(&db);
	CHECKIT(database_put(&db, key2, val2));
	double lf3 =  database_get_load_factor(&db);
	CHECKIT(lf2 > lf1);
	CHECKIT(lf3 > lf2);
	database_close_and_remove(&db);
}

static void test_database_expand(void) {
	struct database db;
	const char* key1 = "abcdef";
	const char* key2 = "abc5ef";
	const char* key3 = "foobar";
	const char* val1 = "abcdefg";
	const char* val2 = "bbbbbb";
	const char* val3 = "bbbcbbb";
	char* key1res = NULL;
	char* key2res = NULL;
	char* key3res = NULL;
	CHECKIT(database_open(&db, "boof", NULL));
	int32_t hashroot1 = database_get_hashroot(&db);
	CHECKIT(database_put(&db, key1, val1));
	CHECKIT(database_put(&db, key2, val2));
	key1res = database_get(&db, key1);
	CHECKIT(strcmp(key1res, val1) == 0);
	free(key1res);
	key2res = database_get(&db, key2);
	CHECKIT(strcmp(key2res, val2) == 0);
	free(key2res);
	database_expand(&db, 1);
	CHECKIT(database_put(&db, key3, val3));
	int32_t hashroot2 = database_get_hashroot(&db);
	CHECKIT(hashroot1 != hashroot2);
	key1res = database_get(&db, key1);
	key2res = database_get(&db, key2);
	CHECKIT(strcmp(key1res, val1) == 0);
	CHECKIT(strcmp(key2res, val2) == 0);
	free(key1res);
	free(key2res);
	database_expand(&db, 2);
	key1res = database_get(&db, key1);
	key2res = database_get(&db, key2);
	key3res = database_get(&db, key3);
	CHECKIT(strcmp(key1res, val1) == 0);
	CHECKIT(strcmp(key2res, val2) == 0);
	CHECKIT(strcmp(key3res, val3) == 0);
	free(key1res);
	free(key2res);
	free(key3res);
	database_close_and_remove(&db);
}

static void test_database_put_load_fact_expand(void) {
	struct database db;
	const char* key1 = "abcdef";
	const char* val1 = "abcdefg";
	const char* key2 = "abcddddddef";
	const char* val2 = "abcdefg";
	char* key1res = NULL;
	int32_t factor_lim = 10000000;
	CHECKIT(database_open(&db, "boof", NULL));
	int32_t current_hash_root = database_get_hashroot(&db);
	CHECKIT(factor_lim == database_set_factor_lim(&db, factor_lim));
	CHECKIT(database_put(&db, key1, val1));
	printf("load factor %f\n", database_get_load_factor(&db));
	CHECKIT(database_put(&db, key2, val2));
	// check if expansion occurred
	int32_t second_root = database_get_hashroot(&db);
	CHECKIT(second_root != current_hash_root);
	key1res = database_get(&db, key1);
	CHECKIT(strcmp(key1res, val1) == 0);
	free(key1res);
	database_close_and_remove(&db);
}

int main(int argc, char const *argv[])
{
	test_djb2_n();
	test_page_vec();
	test_dbfile_rw();
	test_dbfile_cmp();
	test_dbfile_hash_null();
	test_dbfile_grow();
	test_database_open_close();
	test_database_add_hash_block();
	test_database_add_space();
	test_database_find_space_ptr();
	test_database_find_space_ptr_zero();
	test_database_allocate();
	test_database_allocate_large();
	test_database_key_cmp();
	test_database_deallocate();
	test_database_hash_and_probe();
	test_database_put_get_del();
	test_database_put_get_reopen();
	test_database_load_factor();
	test_database_expand();
	test_database_put_load_fact_expand();
	return _failures > 0 ? 3 : 0;
}