//
// Created by lingo on 19-1-15.
//

#ifndef SOFTDB_EXPORT_H
#define SOFTDB_EXPORT_H


#if !defined(SOFTDB_EXPORT)

#if defined(SOFTDB_SHARED_LIBRARY)
#if defined(_WIN32)

#if defined(SOFTDB_COMPILE_LIBRARY)
#define SOFTDB_EXPORT __declspec(dllexport)
#else
#define SOFTDB_EXPORT __declspec(dllimport)
#endif  // defined(SOFTDB_COMPILE_LIBRARY)

#else  // defined(_WIN32)
#if defined(SOFTDB_COMPILE_LIBRARY)
#define SOFTDB_EXPORT __attribute__((visibility("default")))
#else
#define SOFTDB_EXPORT
#endif
#endif  // defined(_WIN32)

#else  // defined(SOFTDB_SHARED_LIBRARY)
#define SOFTDB_EXPORT
#endif

#endif  // !defined(SOFTDB_EXPORT)


#endif //SOFTDB_EXPORT_H
