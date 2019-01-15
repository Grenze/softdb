//
// Created by lingo on 19-1-15.
//

#ifndef SOFTDB_PORT_CONFIG_H
#define SOFTDB_PORT_CONFIG_H


// Define to 1 if you have a definition for fdatasync() in <unistd.h>.
#if !defined(HAVE_FUNC_FDATASYNC)
#define HAVE_FUNC_FDATASYNC 0
#endif  // !defined(HAVE_FUNC_FDATASYNC)

// Define to 1 if you have Google CRC32C.
#if !defined(HAVE_CRC32C)
#define HAVE_CRC32C 0
#endif  // !defined(HAVE_CRC32C)

// Define to 1 if you have Google Snappy.
#if !defined(HAVE_SNAPPY)
#define HAVE_SNAPPY 0
#endif  // !defined(HAVE_SNAPPY)

// Define to 1 if your processor stores words with the most significant byte
// first (like Motorola and SPARC, unlike Intel and VAX).
#if !defined(SOFTDB_IS_BIG_ENDIAN)
#define SOFTDB_IS_BIG_ENDIAN 0
#endif  // !defined(SOFTDB_IS_BIG_ENDIAN)


#endif //SOFTDB_PORT_CONFIG_H
