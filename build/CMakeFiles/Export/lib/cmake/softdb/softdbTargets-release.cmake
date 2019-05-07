#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "softdb::softdb" for configuration "Release"
set_property(TARGET softdb::softdb APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(softdb::softdb PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libsoftdb.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS softdb::softdb )
list(APPEND _IMPORT_CHECK_FILES_FOR_softdb::softdb "${_IMPORT_PREFIX}/lib/libsoftdb.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
