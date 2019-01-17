//
// Created by lingo on 19-1-17.
//

#ifndef SOFTDB_ENV_POSIX_TEST_HELPER_H
#define SOFTDB_ENV_POSIX_TEST_HELPER_H


namespace softdb {

    class EnvPosixTest;

// A helper for the POSIX Env to facilitate testing.
    class EnvPosixTestHelper {
    private:
        friend class EnvPosixTest;

        // Set the maximum number of read-only files that will be opened.
        // Must be called before creating an Env.
        static void SetReadOnlyFDLimit(int limit);

        // Set the maximum number of read-only files that will be mapped via mmap.
        // Must be called before creating an Env.
        static void SetReadOnlyMMapLimit(int limit);
    };

}  // namespace softdb


#endif //SOFTDB_ENV_POSIX_TEST_HELPER_H
