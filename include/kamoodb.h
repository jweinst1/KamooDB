#ifndef KAMOODB_HEADER
#define KAMOODB_HEADER

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>


static int file_exists(const char* path) {
	struct stat buffer;
	return stat(path, &buffer) == 0;
}

static ssize_t file_size(const char* path) {
	struct stat  sb;
	if (stat(path, &sb) == 0) {
		return sb.st_size;
	}
	return -1;
}

static void create_init_file(const char* path, size_t size) {
	int fd = open(path, O_RDWR | O_CREAT, (mode_t)0600);
	lseek(fd, size-1, SEEK_SET);
	write(fd, "", 1);
	lseek(fd, 0, SEEK_SET);
	close(fd);
} 

static size_t get_page_size(void) {
	return sysconf(_SC_PAGE_SIZE);
}

#define DJB2_HASH_BASE 5381

size_t hash_djb2(const char *str) {
    size_t hash = DJB2_HASH_BASE;
    int c;

    while ((c = *str++))
        hash = ((hash << 5) + hash) + c; 

    return hash;
}

int hash_djb2_n(const char *str, size_t n, size_t* hash) {
	int c;
	while(n-- && (c = *str++))
		*hash = ((*hash << 5) + *hash) + c;
	return c == 0; 
}

static char* str_dupl(const char* src) {
	size_t src_size = strlen(src) + 1;
	char* newstr = malloc(src_size);
	memcpy(newstr, src, src_size);
	return newstr;
}

struct page_vec {
	size_t len;
	size_t cap;
	int32_t* pages;
};

static const size_t PAGE_VEC_DEF_CAPAC = 50;

void page_vec_init(struct page_vec* pvec) {
	pvec->len = 0;
	pvec->cap = PAGE_VEC_DEF_CAPAC;
	pvec->pages = calloc(1, sizeof(int32_t) * pvec->cap);
}

void page_vec_push(struct page_vec* pvec, int32_t page_n) {
	if (pvec->len == pvec->cap) {
		pvec->cap *= 2;
		pvec->pages = realloc(pvec->pages, pvec->cap);
	}
	pvec->pages[pvec->len++] = page_n;
}

void page_vec_clear(struct page_vec* pvec) {
	memset(pvec->pages, 0, sizeof(int32_t) * pvec->len);
	pvec->len = 0;
}

void page_vec_deinit(struct page_vec* pvec) {
	pvec->len = 0;
	pvec->cap = PAGE_VEC_DEF_CAPAC;
	free(pvec->pages);
	pvec->pages = NULL;
}

enum dbstore_type {
	DBSTORE_MEM_MAP
	//DBSTORE_FILE, todo, in future
	//DBSTORE_IN_MEM todo, in future
};

struct dbfile {
	size_t page_size;
	char* filepath;
	size_t file_size;
	size_t page_count;
	size_t page_cap;
	char** pages;
	int fd;
};

struct dbcfg {
	size_t page_size;
	enum dbstore_type ftype;
};

int dbcfg_validate(const struct dbcfg* cfg) {
	if (cfg == NULL) {
		return 1;
	}
	if (cfg->ftype == DBSTORE_MEM_MAP) {
		// If using mmap, the offsets must be a multiple of the os page size
		if (cfg->page_size > 0 && cfg->page_size % get_page_size() != 0) {
			return 0;
		}
	}
	return 1;
}

int dbfile_open(struct dbfile* dbf, const char* path, struct dbcfg* cfg) {
	if(!dbcfg_validate(cfg)) {
		return 0;
	}
	dbf->page_size = cfg != NULL ? cfg->page_size : get_page_size();
	if (!file_exists(path)) {
		create_init_file(path, dbf->page_size * 1); //todo
	}
	ssize_t dbsize = file_size(path);
	int fd = open(path, O_RDWR | O_CREAT, (mode_t)0600);
	if (fd == -1) {
		return 0;
	}
	dbf->filepath = str_dupl(path);
	dbf->page_cap = 10;
	dbf->file_size = dbsize;
	dbf->page_count = dbf->file_size / dbf->page_size;
	dbf->page_cap += dbf->page_count;
	dbf->fd = fd;
	dbf->pages = calloc(1, sizeof(char*) * dbf->page_cap);
	for (size_t i = 0; i < dbf->page_count; ++i){
		char* pagemap = mmap(0, dbf->page_size, PROT_READ | PROT_WRITE, MAP_SHARED, dbf->fd, i * dbf->page_size);
		dbf->pages[i] = pagemap;
	}
	return 1;
}

size_t dbfile_grow(struct dbfile* dbf , size_t n_pages) {
	if ((dbf->page_count + n_pages) > dbf->page_cap) {
		dbf->page_cap += n_pages * 5;
		dbf->pages = realloc(dbf->pages, dbf->page_cap);
	}
	size_t size_increase = n_pages * dbf->page_size;
	dbf->file_size += size_increase;
	lseek(dbf->fd, dbf->file_size-1, SEEK_SET);
	write(dbf->fd, "", 1);
	lseek(dbf->fd, 0, SEEK_SET);
	size_t prev_page_count = dbf->page_count;
	dbf->page_count += n_pages;
	return prev_page_count;
	// wait for pages to need to be mapped into memory lazily 
}

// todo, allow non linear get of page
char* dbfile_get_page(struct dbfile* dbf, size_t n) {
	if (n >= dbf->page_count) {
		dbfile_grow(dbf, (n - dbf->page_count) + 1);
	}
	if (dbf->pages[n] == NULL) {
		char* pagemap = mmap(0, dbf->page_size, PROT_READ | PROT_WRITE, MAP_SHARED, dbf->fd, n * dbf->page_size);
		dbf->pages[n] = pagemap;
	}
	return dbf->pages[n];
}

void dbfile_sync_page(struct dbfile* dbf, size_t n) {
	char* page = dbfile_get_page(dbf, n);
	msync(page, dbf->page_size, MS_SYNC);
}

int dbfile_get_place(struct dbfile* dbf, size_t offset, size_t* result) {
	if (offset >= dbf->file_size) {
		return 0;
	}
	result[0] = offset / dbf->page_size;
	result[1] = offset % dbf->page_size;
	return 1;
}

int dbfile_write(struct dbfile* dbf, size_t offset, const char* data, size_t size) {
	size_t place[2] = {0};
	if (!dbfile_get_place(dbf, offset, place)) {
		return 0;
	}
	size_t cur_page = place[0];
	size_t cur_off = place[1];
	while (size) {
		char* page = dbfile_get_page(dbf, cur_page);
		size_t to_write = dbf->page_size - cur_off;
		memcpy(page + cur_off, data,  to_write > size ? size : to_write );
		size = to_write  > size ? 0 : size - to_write;
		data += to_write;
		cur_off = 0;
		++cur_page; 
	}
	return 1;
}

int dbfile_write_po(struct dbfile* dbf, size_t page, size_t offset, const char* data, size_t size) {
	size_t cur_page = page;
	size_t cur_off = offset;
	while (size) {
		char* page = dbfile_get_page(dbf, cur_page);
		size_t to_write = dbf->page_size - cur_off;
		memcpy(page + cur_off, data,  to_write > size ? size : to_write );
		size = to_write  > size ? 0 : size - to_write;
		data += to_write;
		cur_off = 0;
		++cur_page; 
	}
	return 1;
}

int dbfile_read_po(struct dbfile* dbf, size_t page, size_t offset, char* data, size_t size) {
	size_t cur_page = page;
	size_t cur_off = offset;
	while (size) {
		char* page = dbfile_get_page(dbf, cur_page);
		size_t to_read = dbf->page_size - cur_off;
		memcpy(data, page + cur_off, to_read > size ? size : to_read );
		size = to_read  > size ? 0 : size - to_read;
		data += to_read;
		cur_off = 0;
		++cur_page; 
	}
	return 1;
}

size_t dbfile_hash_null(struct dbfile* dbf, size_t page, size_t offset) {
	size_t cur_page = page;
	size_t cur_off = offset;
	size_t hashbase = DJB2_HASH_BASE;
	int found_null = 0;
	while (!found_null) {
		char* page = dbfile_get_page(dbf, cur_page);
		size_t to_read = dbf->page_size - cur_off;
		found_null = hash_djb2_n(page + cur_off, to_read, &hashbase);
		cur_off = 0;
		++cur_page; 
	}
	return hashbase;
}

int dbfile_read(struct dbfile* dbf, size_t offset, char* data, size_t size) {
	size_t place[2] = {0};
	if (!dbfile_get_place(dbf, offset, place)) {
		return 0;
	}
	size_t cur_page = place[0];
	size_t cur_off = place[1];
	while (size) {
		char* page = dbfile_get_page(dbf, cur_page);
		size_t to_read = dbf->page_size - cur_off;
		memcpy(data, page + cur_off, to_read > size ? size : to_read );
		size = to_read  > size ? 0 : size - to_read;
		data += to_read;
		cur_off = 0;
		++cur_page; 
	}
	return 1;
}

int dbfile_cmp_null(struct dbfile* dbf, size_t page, size_t offset, const char* data, size_t size) {
	size_t cur_page = page;
	size_t cur_off = offset;
	//printf("cur_page %zu cur off %zu\n", cur_page, cur_off);
	while (size) {
		char* page = dbfile_get_page(dbf, cur_page);
		size_t to_read = dbf->page_size - cur_off;
		//printf("to read %zu\n", to_read);
		if (strncmp(data, page + cur_off, to_read > size ? size : to_read ) != 0) {
			return 0;
		}
		size = to_read  > size ? 0 : size - to_read;
		data += to_read;
		cur_off = 0;
		++cur_page; 
	}
	return 1;
}


void dbfile_close(struct dbfile* dbf) {
	for (size_t i = 0; i < dbf->page_count; ++i){
		if(munmap(dbf->pages[i], dbf->page_size) == -1) {
			fprintf(stderr, "Failed to unmap page %zu\n", i);
		}
	}
	close(dbf->fd);
	dbf->fd = -1;
}

void dbfile_path_free(struct dbfile* dbf) {
	if (dbf->filepath != NULL) {
		free(dbf->filepath);
		dbf->filepath = NULL;
	}
}

void dbfile_remove(struct dbfile* dbf) {
	if (dbf->filepath != NULL) {
		remove(dbf->filepath);
	}
}

// storage pointer form
// [page number][offset in page][size]
// number = 4 bytes
// offset in page 4 bytes
// size = 4 bytes

// hash storage pointer form
// [page number][offset in page][size]
// number = 4 bytes
// offset in page 4 bytes
// size = 4 bytes
// hash len = 4 bytes <--- used for rehashes
//
// hash blocks
// * dependent on page size
// [next page][hashslot]
// hashslot = storage pointer 
//
// storage blocks (optional , can just be free blocks)
// [-header-][-list-]
// [next page][len][page number...][page number....]
//
// free blocks
// [-header-][-list-]
// [next page][len][total free space][storageslot...]
// storageslot = storage pointer
//
// db header
// magic string, 4 bytes
// [hashblocks root] = 4 bytes
// [storageblocks root] = 4 bytes (optional)
// [freeblocks root] = 4 bytes
// hash block count (for hash modulo)

static const char MAGIC_SEQ[] = {'k', 'h', 'o', 'm'};
static const size_t STORAGE_PTR_SIZE = sizeof(int32_t) * 3;
static const size_t HASHSTORAGE_PTR_SIZE = sizeof(int32_t) * 3;
static const size_t HASHSTORAGE_PTR_SIZE_INT = HASHSTORAGE_PTR_SIZE / sizeof(int32_t);
static const size_t LEN_BLOCK_HEADER_SIZE = sizeof(int32_t) * 2;
static const size_t HASH_BLOCK_HEADER_SIZE = sizeof(int32_t);
static const size_t DB_HEADER_PAGE_SIZE_OFF = sizeof(MAGIC_SEQ) + (sizeof(int32_t) * 2);
static const size_t DB_HEADER_HASH_LEN_OFF = sizeof(MAGIC_SEQ) + (sizeof(int32_t) * 3);
static const size_t DB_HEADER_ITEM_COUNT_OFF = sizeof(MAGIC_SEQ) + (sizeof(int32_t) * 4);


size_t items_per_block(size_t page_size) {
	size_t slot_port = page_size - LEN_BLOCK_HEADER_SIZE;
	return (slot_port - (slot_port % STORAGE_PTR_SIZE)) / STORAGE_PTR_SIZE;
}

size_t hashes_per_block(size_t page_size) {
	size_t slot_port = page_size - HASH_BLOCK_HEADER_SIZE;
	return (slot_port - (slot_port % HASHSTORAGE_PTR_SIZE)) / HASHSTORAGE_PTR_SIZE;
}

struct database {
	struct dbfile dbf;
};

int _has_magic_seq(const char* page) {
	return page[0] == MAGIC_SEQ[0] &&
	       page[1] == MAGIC_SEQ[1] &&
	       page[2] == MAGIC_SEQ[2] &&
	       page[3] == MAGIC_SEQ[3];
}

void _erase_region(char* ptr, size_t off, size_t size, size_t end) {
	char* temp = malloc(end - (off + size));
	memcpy(temp, ptr + off + size, end - (off + size));
	memcpy(ptr + off, temp, end - (off + size));
	free(temp);
}

void _write_storage_ptr(char* ptr, int32_t page, int32_t off, int32_t size) {
	int32_t* writer = (int32_t*)ptr;
	writer[0] = page;
	writer[1] = off;
	writer[2] = size;
}

void _read_storage_ptr(const char* ptr, int32_t* page, int32_t* off, int32_t* size) {
	int32_t* writer = (int32_t*)ptr;
	*page = writer[0];
	*off = writer[1];
	*size = writer[2];
}

void _write_storage_ptr_w(char* ptr, int32_t* input) {
	int32_t* writer = (int32_t*)ptr;
	writer[0] = input[0];
	writer[1] = input[1];
	writer[2] = input[2];
}

void _write_storage_ptr_hash(int32_t* writer, const int32_t* input) {
	writer[0] = input[0];
	writer[1] = input[1];
	writer[2] = input[2];
}

void _read_storage_ptr_w(const char* ptr, int32_t* output) {
	int32_t* writer = (int32_t*)ptr;
	output[0] = writer[0];
	output[1] = writer[1];
	output[2] = writer[2];
}

void _read_storage_ptr_po(const char* ptr, int32_t size, int32_t* output) {
	int32_t* writer = (int32_t*)ptr;
	output[0] = writer[0];
	output[1] = writer[1];
	output[2] = size;
}

void _shift_storage_ptr(char* ptr, int32_t amount, size_t page_size) {
	int32_t* writer = (int32_t*)ptr;
	writer[0] += (amount / page_size);
	writer[1] += amount;
	writer[2] -= amount;
}

int _has_size_storage_ptr(const char* ptr, int32_t size) {
	int32_t* reader = (int32_t*)ptr;
	return reader[2] >= size;
}

int _is_empty_ins_storage_ptr(const int32_t* reader) {
	return reader[0] == 0;
}

int _is_del_storage_ptr(const int32_t* reader) {
	return reader[0] == -1;
}

void _mark_del_storage_ptr(int32_t* reader) {
	reader[0] = -1;
}

int _is_size_zero_storage_ptr(const char* ptr) {
	int32_t* reader = (int32_t*)ptr;
	return reader[2] == 0;
}

int32_t database_len_get(const char* block) {
	const int32_t* ptr = (const int32_t*)block;
	return ptr[1];
}

void database_len_dec(char* block) {
	int32_t* ptr = (int32_t*)block;
	ptr[1] -= 1;
}

void database_len_init(char* block) {
	int32_t* ptr = (int32_t*)block;
	ptr[0] = -1;
	ptr[1] = 0;
}

void database_hash_init(char* block) {
	int32_t* ptr = (int32_t*)block;
	ptr[0] = -1;
}

int32_t database_get_hashroot(struct database* db) {
	char* header = dbfile_get_page(&db->dbf, 0);
	return *(int32_t*)(header + sizeof(MAGIC_SEQ));
}

void database_set_hashroot(struct database* db, int32_t new_root) {
	char* header = dbfile_get_page(&db->dbf, 0);
	*(int32_t*)(header + sizeof(MAGIC_SEQ)) = new_root;
}

int32_t database_get_spaceroot(struct database* db) {
	char* header = dbfile_get_page(&db->dbf, 0);
	return *(int32_t*)(header + sizeof(MAGIC_SEQ) + sizeof(int32_t));
}

size_t database_get_hash_len(struct database* db) {
	char* header = dbfile_get_page(&db->dbf, 0);
	int32_t hash_block_len = *(int32_t*)(header + DB_HEADER_HASH_LEN_OFF);
	return hash_block_len * hashes_per_block(db->dbf.page_size);
}

int32_t database_get_hash_count(struct database* db) {
	char* header = dbfile_get_page(&db->dbf, 0);
	int32_t hash_block_len = *(int32_t*)(header + DB_HEADER_HASH_LEN_OFF);
	return hash_block_len;
}

int32_t database_get_page_size(struct database* db) {
	char* header = dbfile_get_page(&db->dbf, 0);
	int32_t page_size = *(int32_t*)(header + DB_HEADER_PAGE_SIZE_OFF);
	return page_size;
}

int64_t database_get_item_count(struct database* db) {
	char* header = dbfile_get_page(&db->dbf, 0);
	int64_t item_len = *(int64_t*)(header + DB_HEADER_ITEM_COUNT_OFF);
	return item_len;
}

int64_t database_inc_item_count(struct database* db, int64_t amount) {
	char* header = dbfile_get_page(&db->dbf, 0);
	int64_t* item_len = (int64_t*)(header + DB_HEADER_ITEM_COUNT_OFF);
	*item_len += amount;
	return *item_len;
}

int64_t database_dec_item_count(struct database* db, int64_t amount) {
	char* header = dbfile_get_page(&db->dbf, 0);
	int64_t* item_len = (int64_t*)(header + DB_HEADER_ITEM_COUNT_OFF);
	*item_len -= amount;
	return *item_len;
}

double database_get_load_factor(struct database* db) {
	return (double)database_get_item_count(db) / (double)database_get_hash_len(db);
}

void database_set_hash_count(struct database* db, int32_t new_count) {
	char* header = dbfile_get_page(&db->dbf, 0);
	*(int32_t*)(header + DB_HEADER_HASH_LEN_OFF) = new_count;
}

size_t database_get_hash_block_count(struct database* db) {
	int32_t hash_iter = database_get_hashroot(db);
	size_t total = 0;
	while (hash_iter != -1) {
		char* hash_page = dbfile_get_page(&db->dbf, hash_iter);
		int32_t* header = (int32_t*)hash_page;
		hash_iter = header[0];
		++total;
	}
	return total;
}

int32_t database_get_hash_block(struct database* db, size_t n) {
	int32_t hash_iter = database_get_hashroot(db);
	while (n-- && hash_iter != -1) {
		char* hash_page = dbfile_get_page(&db->dbf, hash_iter);
		int32_t* header = (int32_t*)hash_page;
		hash_iter = header[0];
	}
	return hash_iter;
}

size_t database_get_space_block_count(struct database* db) {
	int32_t space_iter = database_get_spaceroot(db);
	size_t total = 0;
	while (space_iter != -1) {
		char* space_page = dbfile_get_page(&db->dbf, space_iter);
		int32_t* header = (int32_t*)space_page;
		space_iter = header[0];
		++total;
	}
	return total;
}

void database_inc_hash_len(struct database* db) {
	char* header = dbfile_get_page(&db->dbf, 0);
	int32_t* hash_block_len = (int32_t*)(header + DB_HEADER_HASH_LEN_OFF);
	*hash_block_len += 1;
}

void database_recomp_hash_len(struct database* db) {
	size_t new_hash_block_count = database_get_hash_block_count(db);
	char* header = dbfile_get_page(&db->dbf, 0);
	int32_t* hash_block_len = (int32_t*)(header + DB_HEADER_HASH_LEN_OFF);
	*hash_block_len = new_hash_block_count;
}

int32_t database_find_space_ptr(char* block, int32_t min_size, size_t page_size, int32_t* result) {
	char* read_location = block + LEN_BLOCK_HEADER_SIZE;
	int32_t len = ((int32_t*)block)[1]; // todo safe function for this
	for (int32_t i = 0; i < len; ++i)
	{
		if (_has_size_storage_ptr(read_location, min_size)) {
			_read_storage_ptr_po(read_location, min_size, result);
			_shift_storage_ptr(read_location, min_size, page_size);
			if (_is_size_zero_storage_ptr(read_location)) {
				_erase_region(block, read_location - block, STORAGE_PTR_SIZE, page_size);
				database_len_dec(block);
			}
			return i;
		}
		read_location += STORAGE_PTR_SIZE;
	}
	return -1;
}

int32_t database_find_space_block(struct database* db) {
	int32_t space_iter = database_get_spaceroot(db);
	while (space_iter != -1) {
		char* space_page = dbfile_get_page(&db->dbf, space_iter);
		int32_t* header = (int32_t*)space_page;
		if(header[1] < items_per_block(db->dbf.page_size)) {
			return space_iter;
		}
		space_iter = header[0];
	}
	return -1;
}

int32_t database_find_space_storage(struct database* db, int32_t min_size, int32_t* result) {
	int32_t space_iter = database_get_spaceroot(db);
	while (space_iter != -1) {
		char* space_page = dbfile_get_page(&db->dbf, space_iter);
		int32_t* header = (int32_t*)space_page;
		if (database_find_space_ptr(space_page, min_size, db->dbf.page_size, result) != -1) {
			return 0;
		}
		space_iter = header[0];
	}
	return -1;
}

// access into a len block
int32_t* _len_block_read(char* block, int32_t n, int32_t* result) {
	int32_t* header = (int32_t*)block;
	if (n >= header[1]) {
		return NULL;
	}
	char* read_location = block + LEN_BLOCK_HEADER_SIZE + (n * STORAGE_PTR_SIZE);
	_read_storage_ptr_w(read_location, result);
	return (int32_t*)read_location;
}

int32_t* _hash_block_at(char* block, int32_t n) {
	char* read_location = block + HASH_BLOCK_HEADER_SIZE + (n * HASHSTORAGE_PTR_SIZE);
	return (int32_t*)read_location;
}

int32_t* _hash_block_begin(char* block) {
	return (int32_t*)(block + HASH_BLOCK_HEADER_SIZE);
}

int32_t* _hash_block_end(char* block, size_t page_size) {
	return (int32_t*)(block + HASH_BLOCK_HEADER_SIZE + (hashes_per_block(page_size) * HASHSTORAGE_PTR_SIZE));
}

void database_place_ptr_in_len_block(char* block, int32_t page, int32_t off, int32_t size) {
	int32_t* header = (int32_t*)block;
	char* write_location = block + LEN_BLOCK_HEADER_SIZE + (header[1] * STORAGE_PTR_SIZE);
	_write_storage_ptr(write_location, page, off, size);
	header[1] += 1;
}

int32_t database_add_space_block(struct database* db) {
	int32_t new_block = dbfile_grow(&db->dbf, 1);
	database_len_init(dbfile_get_page(&db->dbf, new_block));
	int32_t space_iter = database_get_spaceroot(db);
	char* space_page = dbfile_get_page(&db->dbf, space_iter);
	int32_t* reader = (int32_t*)space_page;
	while (reader[0] != -1) {
		space_iter = reader[0];
		space_page = dbfile_get_page(&db->dbf, space_iter);
		reader = (int32_t*)space_page;
	}
	reader[0] = new_block;
	return new_block;
}

int32_t database_add_hash_block(struct database* db, size_t n_blocks) {
	int32_t hash_iter = database_get_hashroot(db);
	char* hash_page = dbfile_get_page(&db->dbf, hash_iter);
	int32_t* reader = (int32_t*)hash_page;
	while (reader[0] != -1) {
		hash_iter = reader[0];
		hash_page = dbfile_get_page(&db->dbf, hash_iter);
		reader = (int32_t*)hash_page;
	}
	// now at end of list, begin adding
	while(n_blocks--) {
		int32_t new_block = dbfile_grow(&db->dbf, 1);
		database_hash_init(dbfile_get_page(&db->dbf, new_block));
		reader[0] = new_block;
		hash_iter = reader[0];
		hash_page = dbfile_get_page(&db->dbf, hash_iter);
		reader = (int32_t*)hash_page;
	}
	return 1;
}
// makes a linked list of new hash blocks
int32_t database_make_hash_blocks(struct database* db, size_t n_blocks) {
	int32_t hash_block_first = dbfile_grow(&db->dbf, 1);
	database_hash_init(dbfile_get_page(&db->dbf, hash_block_first));
	int32_t hash_iter = hash_block_first;
	char* hash_page = dbfile_get_page(&db->dbf, hash_iter);
	int32_t* reader = (int32_t*)hash_page;
	// now at end of list, begin adding
	while(--n_blocks) {
		int32_t new_block = dbfile_grow(&db->dbf, 1);
		database_hash_init(dbfile_get_page(&db->dbf, new_block));
		reader[0] = new_block;
		hash_iter = reader[0];
		hash_page = dbfile_get_page(&db->dbf, hash_iter);
		reader = (int32_t*)hash_page;
	}
	return hash_block_first;
}

// The number of blocks to increase by must be at least 1
int32_t database_add_storage_blocks(struct database* db, int32_t size) {
	int32_t block_count = (size / db->dbf.page_size) + 1;
	int32_t new_block = dbfile_grow(&db->dbf, block_count);

	int32_t toadd_to = database_find_space_block(db);
	char* adding_space = dbfile_get_page(&db->dbf, toadd_to);
	database_place_ptr_in_len_block(adding_space, new_block, 0, size);
	return new_block;
}

int database_allocate_storage(struct database* db, int32_t size, int32_t* result) {
	int did_inc = 0;
	while (database_find_space_storage(db, size, result) == -1) {
		did_inc = 1;
		printf("added\n");
		database_add_storage_blocks(db, size);
	}
	return did_inc;
}

int database_deallocate_storage(struct database* db, const int32_t* result) {
	if (result[0] <= 0) {
		return 0;
	}

	int32_t toadd_to = -1;
	toadd_to = database_find_space_block(db);
	if (toadd_to == -1) {
		toadd_to = database_add_space_block(db);
	}
	char* adding_space = dbfile_get_page(&db->dbf, toadd_to);
	database_place_ptr_in_len_block(adding_space, result[0], result[1], result[2]);
	return 1;
}

int database_compare_key(struct database* db, const char* key, size_t key_size, const int32_t* store_ptr) {
	return dbfile_cmp_null(&db->dbf, store_ptr[0], store_ptr[1], key, key_size);
}

int32_t* database_hash_and_probe(struct database* db, const char* key, size_t key_size, 
	                            int32_t sblock, int32_t* slot, int put) {
	char* page = dbfile_get_page(&db->dbf, sblock);
	int32_t* place = _hash_block_at(page, slot == NULL ? 0 : *slot);
	if (_is_empty_ins_storage_ptr(place)) {
		return place;
	} else if(_is_del_storage_ptr(place)) {
		if (put) {
			return place;
		}
	} else if (database_compare_key(db, key, key_size, place)) {
		return place;
	}
	int32_t* iter = place;
	int32_t* begin = _hash_block_begin(page);
	int32_t* end = _hash_block_end(page, db->dbf.page_size);
	while (iter != end) {
		if (_is_empty_ins_storage_ptr(iter)) {
			return iter;
		} else if(_is_del_storage_ptr(iter)) {
			if (put) {
				return iter;
			}
		} else if (database_compare_key(db, key, key_size, iter)) {
			return iter;
		}
		iter += HASHSTORAGE_PTR_SIZE_INT;
	}
	iter = begin;
	while (iter != place) {
		if (_is_empty_ins_storage_ptr(iter)) {
			return iter;
		} else if(_is_del_storage_ptr(iter)) {
			if (put) {
				return iter;
			}
		} else if (database_compare_key(db, key, key_size, iter)) {
			return iter;
		}
		iter += HASHSTORAGE_PTR_SIZE_INT;
	}
	return NULL;
}
/**
 * This function just compares if its empty or not because we know there cannot be a duplicate
 * */
int32_t* database_rehash_and_probe(struct database* db, int32_t sblock, int32_t* slot) {
	char* page = dbfile_get_page(&db->dbf, sblock);
	int32_t* place = _hash_block_at(page, slot == NULL ? 0 : *slot);
	if (_is_empty_ins_storage_ptr(place)) {
		return place;
	} 
	int32_t* iter = place;
	int32_t* begin = _hash_block_begin(page);
	int32_t* end = _hash_block_end(page, db->dbf.page_size);
	while (iter != end) {
		if (_is_empty_ins_storage_ptr(iter)) {
			return iter;
		}
		iter += HASHSTORAGE_PTR_SIZE_INT;
	}
	iter = begin;
	while (iter != place) {
		if (_is_empty_ins_storage_ptr(iter)) {
			return iter;
		} 
		iter += HASHSTORAGE_PTR_SIZE_INT;
	}
	return NULL;
}

int32_t database_hashlist_get_n(struct database* db, int32_t hash_list, size_t n) {
	//printf("hash n %zu, hl %d,  pc, %zu\n", n, hash_list, db->dbf.page_count);
	while (n--) {
		char* page = dbfile_get_page(&db->dbf, hash_list);
		int32_t* reader = (int32_t*)page;
		hash_list = reader[0];
	}
	return hash_list;
}

int database_rehash_into(struct database* db, const int32_t* store_ptr, int32_t hash_list, size_t hash_size) {
	if (store_ptr[0] < 1) {
		return 0;
	}
	size_t rehash = dbfile_hash_null(&db->dbf, store_ptr[0], store_ptr[1]);
	size_t hash_slot = rehash % hash_size;
	size_t hash_each_block = hashes_per_block(db->dbf.page_size);
	int32_t hash_place = hash_slot % hash_each_block;
	int32_t into_block = database_hashlist_get_n(db, hash_list, hash_slot / hash_each_block);
	int32_t* cur_spot = database_rehash_and_probe(db, into_block, &hash_place);
	if (cur_spot != NULL) {
		_write_storage_ptr_hash(cur_spot, store_ptr);
		return 1;
	}
	// iterate over other blocks
	int32_t list_place = hash_list;
	while (list_place != -1) {
		int32_t* list_spot = database_rehash_and_probe(db, list_place, NULL);
		if (list_spot != NULL) {
			_write_storage_ptr_hash(list_spot, store_ptr);
			return 1;
		}
		char* list_block = dbfile_get_page(&db->dbf, list_place);
		int32_t* reader = (int32_t*)list_block;
		list_place = reader[0];
	}
	return 0;
}

char* database_adv_to_val(struct database* db, const int32_t* store_ptr, size_t key_size) {
	int32_t adv_ptr[3];
	if (!store_ptr[0])
		return NULL;
	adv_ptr[0] = store_ptr[0] + (key_size / db->dbf.page_size);
	adv_ptr[1] = store_ptr[1] + (key_size % db->dbf.page_size);
	adv_ptr[2] = store_ptr[2] - key_size;
	char* strbuf = malloc(adv_ptr[2]);
	dbfile_read_po(&db->dbf, adv_ptr[0], adv_ptr[1], strbuf, adv_ptr[2]);
	return strbuf;
}

int database_expand(struct database* db, size_t n_blocks) {
	size_t cur_block_count = database_get_hash_block_count(db);
	size_t next_count = (cur_block_count + n_blocks);
	size_t next_len = next_count * hashes_per_block(db->dbf.page_size);
	int32_t new_hash_lists = database_make_hash_blocks(db, next_count);
	int32_t old_hash_root = database_get_hashroot(db);
	int32_t hashiter = old_hash_root;
	while (hashiter != -1) {
		char* page = dbfile_get_page(&db->dbf, hashiter);
		int32_t* begin = _hash_block_begin(page);
		int32_t* end = _hash_block_end(page, db->dbf.page_size);
		int32_t* iter = begin;
		// to do
		while (iter != end) {
			database_rehash_into(db, iter, new_hash_lists, next_len);
			iter += HASHSTORAGE_PTR_SIZE_INT;
		}
		hashiter = ((int32_t*)(page))[0];
	}
	// reset the hash list,
	database_set_hashroot(db, new_hash_lists);

	// turn previous hash list into free blocks
	int32_t old_hash_iter = old_hash_root;
	while (old_hash_iter != -1) {
		int32_t to_free = old_hash_iter;
		char* page = dbfile_get_page(&db->dbf, old_hash_iter);
		old_hash_iter = ((int32_t*)(page))[0];
		int32_t freed_storage[3] = {to_free, 0, db->dbf.page_size};
		database_deallocate_storage(db, freed_storage);
	}
	// now recompute the length
	database_recomp_hash_len(db);
	return 1;
}

int database_put(struct database* db, const char* key, const char* val) {
	int32_t storage_place[3];
	size_t key_hash = hash_djb2(key);
	size_t key_size = strlen(key) + 1;
	size_t val_size = strlen(val) + 1;
	size_t total_size = key_size + val_size;
	char* buff = calloc(1, total_size);
	memcpy(buff, key, key_size);
	memcpy(buff + key_size, val, val_size);
	database_allocate_storage(db, total_size, storage_place);

	dbfile_write_po(&db->dbf, storage_place[0], storage_place[1], buff, total_size);
	free(buff);

	size_t hash_size = database_get_hash_len(db);
	size_t hash_slot = key_hash % hash_size;
	size_t hash_each_block = hashes_per_block(db->dbf.page_size);
	int32_t hash_place = hash_slot % hash_each_block;
	int32_t into_block = database_get_hash_block(db, hash_slot / hash_each_block);
	int32_t* found = database_hash_and_probe(db, key, key_size, into_block, &hash_place, 1);
	if(found != NULL) {
		database_deallocate_storage(db, found);
		_write_storage_ptr_hash(found, storage_place);
		database_inc_item_count(db, 1);
		return 1;
	}
	int32_t block_iter = database_get_hashroot(db);
	while (block_iter != -1) {
		found = database_hash_and_probe(db, key, key_size, into_block, NULL, 1);
		if(found != NULL) {
			database_deallocate_storage(db, found);
			_write_storage_ptr_hash(found, storage_place);
			database_inc_item_count(db, 1);
			return 1;
		}
		char* hash_page = dbfile_get_page(&db->dbf, block_iter);
		int32_t* header = (int32_t*)hash_page;
		block_iter = header[0];
	}
	return 0;
}

char* database_get(struct database* db, const char* key) {
	size_t key_hash = hash_djb2(key);
	size_t key_size = strlen(key) + 1;
	size_t hash_size = database_get_hash_len(db);
	size_t hash_slot = key_hash % hash_size;
	size_t hash_each_block = hashes_per_block(db->dbf.page_size);
	int32_t hash_place = hash_slot % hash_each_block;
	int32_t into_block = database_get_hash_block(db, hash_slot / hash_each_block);

	int32_t* found = database_hash_and_probe(db, key, key_size, into_block, &hash_place, 0);
	if(found != NULL) {
		return database_adv_to_val(db, found, key_size);
	}
	int32_t block_iter = database_get_hashroot(db);
	while (block_iter != -1) {
		found = database_hash_and_probe(db, key, key_size, into_block, NULL, 0);
		if(found != NULL) {
			return database_adv_to_val(db, found, key_size);
		}
		char* hash_page = dbfile_get_page(&db->dbf, block_iter);
		int32_t* header = (int32_t*)hash_page;
		block_iter = header[0];
	}
	return NULL;
}

int database_del(struct database* db, const char* key) {
	size_t key_hash = hash_djb2(key);
	size_t key_size = strlen(key) + 1;
	size_t hash_size = database_get_hash_len(db);
	size_t hash_slot = key_hash % hash_size;
	size_t hash_each_block = hashes_per_block(db->dbf.page_size);
	int32_t hash_place = hash_slot % hash_each_block;
	int32_t into_block = database_get_hash_block(db, hash_slot / hash_each_block);

	int32_t* found = database_hash_and_probe(db, key, key_size, into_block, &hash_place, 0);
	if(found != NULL) {
		database_deallocate_storage(db, found);
		_mark_del_storage_ptr(found);
		database_dec_item_count(db, 1);
		return 1;
	}
	int32_t block_iter = database_get_hashroot(db);
	while (block_iter != -1) {
		found = database_hash_and_probe(db, key, key_size, into_block, NULL, 0);
		if(found != NULL) {
			database_deallocate_storage(db, found);
			_mark_del_storage_ptr(found);
			database_dec_item_count(db, 1);
			return 1;
		}
		char* hash_page = dbfile_get_page(&db->dbf, block_iter);
		int32_t* header = (int32_t*)hash_page;
		block_iter = header[0];
	}
	return 0;
}

void database_init(struct database* db) {
	int32_t roots[2];
	int32_t hash_len = 1;
	int64_t item_count = 0;
	struct dbfile* dbf = &db->dbf;
	char* header = dbfile_get_page(dbf, 0);
	header[0] = MAGIC_SEQ[0];
	header[1] = MAGIC_SEQ[1];
	header[2] = MAGIC_SEQ[2];
	header[3] = MAGIC_SEQ[3];
	header += sizeof(MAGIC_SEQ);
	int32_t hash_root = dbfile_grow(dbf, 1);
	int32_t space_root = dbfile_grow(dbf, 1);
	roots[0] = hash_root; // beginning of hash list
	roots[1] = space_root; // beginning of space heap
	memcpy(header, roots, sizeof(roots));
	header += sizeof(roots);
	int32_t page_size = dbf->page_size;
	memcpy(header, &page_size, sizeof(page_size));
	header += sizeof(page_size);
	memcpy(header, &hash_len, sizeof(hash_len));
	header += sizeof(hash_len);
	memcpy(header, &item_count, sizeof(item_count)); // used for load factor
	// init roots
	char* hash_page = dbfile_get_page(dbf, hash_root);
	char* space_page = dbfile_get_page(dbf, space_root);
	database_hash_init(hash_page);
	database_len_init(space_page);

	database_add_storage_blocks(db, page_size); // todo beginning allocation strategy
}

int database_open(struct database* db, const char* pathfile, struct dbcfg* cfg) {
	if (!dbfile_open(&db->dbf, pathfile, cfg))
		return 0;
	char* header = dbfile_get_page(&db->dbf, 0);
	if (!_has_magic_seq(header)) {
		database_init(db);
	}
	db->dbf.page_size = database_get_page_size(db);
	return 1;
}

void database_close(struct database* db) {
	dbfile_close(&db->dbf);
	dbfile_path_free(&db->dbf);
}

void database_close_and_remove(struct database* db) {
	dbfile_close(&db->dbf);
	dbfile_remove(&db->dbf);
	dbfile_path_free(&db->dbf);
}

#endif // KAMOODB_HEADER