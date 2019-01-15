cmake_minimum_required(VERSION 3.13)
project(softdb)

set(CMAKE_CXX_STANDARD 17)

add_executable(softdb main.cpp export.h slice.h status.cpp status.h db.h options.h random.h port_config.h port_stdcxx.h thread_annotations.h atomic_pointer.h)