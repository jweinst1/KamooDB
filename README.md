# KamooDB
*A fast, single file key-Value storage library*

## What is KamooDB?

`KamooDB` , or Kamoo for short, is a fast, space effecient, key value database contained in a single file. Kamoo exists as a C library, making it easily embedable inside other applications and programs. Kamoo supports key value operations, such as get, put, update, and delete.  Kamoo maps string keys to string values, each of which can be arbitrary sizes. All keys and values are immutable. No type specific values are supported. 

Unlike other databases and key-value stores, Kamoo functions as a hash table stored on disk and loaded or mapped into memory as needed. It does not use B-trees or LSM-trees to index data. It consists of a series of hash pages that hold pointers to areas of the file that the key and value are stored. This approach is designed to optimize for key-value operations that need to access values as quickly as possible.


## Building

Kamoo currently uses `cmake` as a build system. To build Kamoo, clone the repo, and run the following commands

```
mkdir <build dir> && cd <build dir>
cmake ..
make
```

To run the unit tests, you can run

```
make test
```

## Usage

Kamoo is currently packaged as a single header file called `kamoodb.h` . You can include this in any C or C++ program, or embed it in languages that support running or using C code.

The following C code demonstrates some basic operations that are performed using the C api:

```c
struct database db;
const char* key1 = "abcdef";
const char* val1 = "abcdefg";

char* key1res = NULL;
database_open(&db, "mydbpath", NULL);
database_put(&db, key1, val1);
key1res = database_get(&db, key1);
if (strcmp(key1res, val1) == 0) {
	printf("Found the value %s\n", key1res);
}
free(key1res);
database_del(&db, key1);

if (database_get(&db, key1) == NULL) {
	puts("The key has been deleted!\n");
}
database_close(&db);
```

## Goal

The goal of Kamoo is to provide a single file, light weight, yet fast key value store, that can be used in similar settings to sqlite, but is entirely focused on key-value operations. 

## Design

Kamoo's design centers around an on-disk format that can be sliced into equal sized chunks called pages. These pages have different roles depending on the portion of the database they are used in. Currently, a page must be a size that is a multiple of the page size from calling `sysconf` . The first four bytes of a page represent the "next" page, similar to a singly linked list. These four bytes are always a 32 bit signed integer, indicating the page number. This also means the maximum number of pages in a KamooDB file is 2,147,483,647.

The types of pages in Kamoo are listed below:

* Header: The first page of a Kamoo document is the header page. The header page contains various information about the database file, like the load factor, the hash roots, space block roots, as well as the total number of items stored.
* Hash: A hash page or block is a page that serves as part of the hash table itself. These blocks have a next block 4 byte section at the beginning of the block, and the remainder of the block is used for 12 byte storage pointers
* space: A space page or block is essentially a free list of storage memory within a file. These are blocks that begin with a 4 byte next block pointer, followed by a 32 bit signed integer length.
