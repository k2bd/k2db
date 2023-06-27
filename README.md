# k2db

[![test](https://github.com/k2bd/k2db/actions/workflows/ci.yml/badge.svg)](https://github.com/k2bd/k2db/actions/workflows/ci.yml)
[![codecov](https://codecov.io/github/k2bd/k2db/branch/main/graph/badge.svg?token=382UJPD1KY)](https://codecov.io/github/k2bd/k2db)

My solutions to the projects set in [CMU's Intro to Database Systems Course](https://www.youtube.com/playlist?list=PLSE8ODhjZXjbohkNBWQs_otTrBTrjyohi) in Rust.
By the nature of the projects it will also be at least a partial clone of some iteration of [BusTub](https://github.com/cmu-db/bustub), the scaffold database system produced by CMU for that course's project work.

The goal of this repo is for me to learn about database internals and DBMS development by following that CMU course and other resources, and to brush up on Rust development.

## Progress

- [Project 1](https://15445.courses.cs.cmu.edu/fall2019/project1/)
  - Task 1 [Clock Replacer](src/dbms/buffer/replacer/clock_replacer.rs)
  - Task 2 [Buffer Pool Manager](src/dbms/buffer/pool_manager/buffer_pool_manager.rs)
- [Project 2](https://15445.courses.cs.cmu.edu/fall2019/project2/)
  - Task 1 [Hash Table Header Page](src/dbms/storage/page/hash_table/header.rs), [Hash Table Block Page](src/dbms/storage/page/hash_table/block.rs)
  - Task 2 (WIP)


## Dependencies

This project aims to have minimal external main dependencies, with exceptions mainly for things that are necessary to build a DBMS but are out of scope for my learning objectives.
This includes for example the specific hash function used in the hash table implementation (though the hash table implementation is itself in scope and is written from scratch).


## Resources

[The BusTub source repo as of a 2019 commit](https://github.com/cmu-db/bustub/tree/feaf3245bc9e09f4e51e57279f342915f5592674)

[An example solution of the whole project in C++](https://github.com/xiaoshuai98/bustub)


### Some other DBMSs written in Rust

[toyDB](https://github.com/erikgrinaker/toydb), a distributed sql database

[mini-db](https://github.com/kw7oe/mini-db), another relational database system learning project

[OxidSQL](https://github.com/mzinsmeister/OxidSQL), a WIP SQL database
