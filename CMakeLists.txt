# Project KamooDB main cmake file

cmake_minimum_required(VERSION 3.6)

project(kamoodb VERSION 0.0.1)

# Disable some windows specific warnings for using more modern c functions
if(MSVC)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
endif(MSVC)

# Options
option(WITH_testing "Allows tests to be run" ON)

if(WITH_testing MATCHES ON)
   enable_testing()
endif(WITH_testing MATCHES ON)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_subdirectory(tests)