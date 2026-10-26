# Not built. scripts/gen_third_party.py configures this project only to read
# the compile flags and link lines CMake computes for the installed folly
# (and with it glog and gflags) and for gtest.
cmake_minimum_required(VERSION 3.20)
project(deps_probe CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(folly CONFIG REQUIRED)
find_package(GTest REQUIRED)

add_executable(folly_probe main.cpp)
target_link_libraries(folly_probe Folly::folly)

add_executable(gtest_probe main.cpp)
target_link_libraries(gtest_probe GTest::gtest)
