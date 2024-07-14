

add_executable(db_tests db_tests.c)
target_compile_options(db_tests PRIVATE -fsanitize=address)
target_link_options(db_tests PRIVATE -fsanitize=address)
add_test(db_tests db_tests)

add_executable(db_benchmark db_benchmark.c)