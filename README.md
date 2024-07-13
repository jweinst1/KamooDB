# KamooDB
*A fast, single file key-Value storage library*

## Introduction

`KammooDB` , or Kamoo for short, is a fast, space effecient, key value database contained in a single file. Kamoo exists as a C library, making it easily embedable inside other applications and programs. Kamoo supports key value operations, such as get, put, update, and delete.  Kamoo maps string keys to string values. 


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

## Goal

The goal of Kamoo is to provide a single file, light weight, yet fast key value store, that can be used in similar settings to sqlite, but is entirely focused on key-value operations. 
