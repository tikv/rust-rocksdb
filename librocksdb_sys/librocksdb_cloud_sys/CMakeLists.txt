cmake_minimum_required(VERSION 3.0)
project(cloud)
enable_language(CXX)
enable_language(C)

# Manage absence of rocksdb code dependence

if (NOT ROCKSDB_GIT_REPO)
  set(ROCKSDB_GIT_REPO "https://github.com/pingcap/rocksdb.git")
endif()

if (NOT ROCKSDB_GIT_BRANCH)
  set(ROCKSDB_GIT_BRANCH "6.4.tikv")
endif()

if (NOT DEFINED ROCKSDB_DIR)
  if (GIT_FOUND)
    if (WIN32)
      execute_process(COMMAND $ENV{COMSPEC} /C ${GIT_EXECUTABLE} clone --branch=${ROCKSDB_GIT_BRANCH} ${ROCKSDB_GIT_REPO})
    else()
      execute_process(COMMAND ${GIT_EXECUTABLE} clone --branch=${ROCKSDB_GIT_BRANCH} ${ROCKSDB_GIT_REPO})
    endif()
    set(ROCKSDB_DIR "${CMAKE_BINARY_DIR}/rocksdb")
  endif()
endif()

if (NOT DEFINED ROCKSDB_DIR)
  message(FATAL_ERROR "ROCKSDB_DIR is not defined.")
endif()

# Append rocksdb modules
list(APPEND CMAKE_MODULE_PATH "${ROCKSDB_DIR}/cmake/modules/")
include("cmake_modules/rocksdb_flags.cmake")

include_directories("${ROCKSDB_DIR}")
include_directories("${ROCKSDB_DIR}/include")
include_directories("rocksdb-cloud")
include_directories("rocksdb-cloud/include")

file(GLOB SOURCES rocksdb-cloud/cloud/*.cc)

add_library(cloud STATIC ${SOURCES} $<TARGET_OBJECTS:cloud_env>)

option(WITH_CLOUD_TESTS "Build with tests." ON)
option(WITH_CLOUD_TOOLS "Build with tools." ON)

include_directories(SYSTEM ${ROCKSDB_DIR}/third-party/gtest-1.7.0/fused-src)

if(GIT_FOUND AND EXISTS "${CLOUD_DIR}/.git")
  if(WIN32)
    execute_process(COMMAND $ENV{COMSPEC} /C ${GIT_EXECUTABLE} rev-parse HEAD OUTPUT_VARIABLE GIT_SHA)
  else()
    execute_process(COMMAND ${GIT_EXECUTABLE} rev-parse HEAD OUTPUT_VARIABLE GIT_SHA)
  endif()
else()
  set(GIT_SHA 0)
endif()
string(REGEX REPLACE "[^0-9a-f]+" "" GIT_SHA "${GIT_SHA}")

add_library(cloud_env OBJECT "rocksdb-cloud/cloud/cloud_env.cc")
target_include_directories(cloud_env PRIVATE "rocksdb-cloud/cloud")
target_include_directories(cloud_env PRIVATE "rocksdb-cloud/include")

include(GNUInstallDirs)
install(DIRECTORY include/rocksdb/cloud
  COMPONENT devel
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
install(TARGETS cloud
  COMPONENT devel
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
