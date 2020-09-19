CMAKE_MINIMUM_REQUIRED (VERSION 3.1.3)
PROJECT (libslave)

OPTION (BUILD_STATIC "Force building static library" ON)
OPTION (WITH_TESTING "Enable building the tests framework" OFF)

SET (THREADS_PREFER_PTHREAD_FLAG ON)
FIND_PACKAGE (Threads REQUIRED)

# Build flags
SET (CMAKE_CXX_STANDARD 14)
SET (CMAKE_CXX_STANDARD_REQUIRED TRUE)
SET (CMAKE_CXX_EXTENSIONS FALSE)

IF (CMAKE_CXX_COMPILER_ID STREQUAL "GNU"
 AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.9)
    MESSAGE (FATAL_ERROR "libslave requires GCC version >= 4.9 for proper regex support")
ENDIF ()

IF (CMAKE_CXX_COMPILER_ID STREQUAL "GNU"
 OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # Common options
    ADD_DEFINITIONS (-pipe)
    ADD_DEFINITIONS (-Wall)
ENDIF ()

SET (BOOST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/boost)
SET (MYSQL_SRC ${CMAKE_CURRENT_SOURCE_DIR}/mysql)
SET (MYSQL_BIN ${CMAKE_BINARY_DIR}/mysql)
FILE (MAKE_DIRECTORY ${MYSQL_BIN})

ADD_CUSTOM_TARGET (
        mysql_configure
        WORKING_DIRECTORY ${MYSQL_BIN}
        COMMAND "${CMAKE_COMMAND}"
        -DCMAKE_BUILD_TYPE=Release
        -DDISABLE_SHARED=1
        -DENABLED_PROFILING=0
        -DWITHOUT_SERVER=1
        -DWITH_CLIENT_PROTOCOL_TRACING=0
        -DWITH_DEFAULT_FEATURE_SET=0
        -DWITH_SSL=system
        -DWITH_ZLIB=bundled
        -DWITH_LIBEVENT=bundled
        -DWITH_LZ4=bundled
        -DWITH_PROTOBUF=bundled
        -DWITH_EDITLINE=bundled
        -DDOWNLOAD_BOOST=1
        -DDOWNLOAD_BOOST_TIMEOUT=1200
        -DWITH_BOOST=${BOOST_DIR}
        ${MYSQL_SRC}
)

ADD_CUSTOM_TARGET (
        binlogevents_build
        WORKING_DIRECTORY ${MYSQL_BIN}
        DEPENDS mysql_configure
        COMMAND ${CMAKE_COMMAND} --build . --target binlogevents_static
)

ADD_CUSTOM_TARGET (
        mysql_client_build
        WORKING_DIRECTORY ${MYSQL_BIN}
        DEPENDS binlogevents_build
        COMMAND ${CMAKE_COMMAND} --build . --target perconaserverclient
)

ADD_LIBRARY (binlogevents STATIC IMPORTED)
ADD_DEPENDENCIES (binlogevents binlogevents_build)
SET_PROPERTY (TARGET binlogevents PROPERTY IMPORTED_LOCATION ${MYSQL_BIN}/libbinlogevents/lib/${CMAKE_STATIC_LIBRARY_PREFIX}binlogevents${CMAKE_STATIC_LIBRARY_SUFFIX})

ADD_LIBRARY (libmysql STATIC IMPORTED)
ADD_DEPENDENCIES (libmysql mysql_client_build)
SET_TARGET_PROPERTIES (libmysql PROPERTIES IMPORTED_LOCATION ${MYSQL_BIN}/archive_output_directory/${CMAKE_STATIC_LIBRARY_PREFIX}perconaserverclient${CMAKE_STATIC_LIBRARY_SUFFIX})
SET_TARGET_PROPERTIES (libmysql PROPERTIES INTERFACE_LINK_LIBRARIES binlogevents)

ADD_DEFINITIONS (-DDBUG_OFF)
SET (LINK_TYPE STATIC)
SET (MYSQL_LIBS mysqlclient binlogevents -lssl -lcrypto)
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    LIST (APPEND MYSQL_LIBS ${CMAKE_DL_LIBS})
endif ()
MESSAGE (STATUS "Build ${LINK_TYPE} slave library")

FILE (GLOB HDR "*.h")
INSTALL (FILES ${HDR} DESTINATION include)
AUX_SOURCE_DIRECTORY (${CMAKE_CURRENT_SOURCE_DIR} SRC)

ADD_LIBRARY (slave ${LINK_TYPE} ${SRC})
TARGET_LINK_LIBRARIES (slave PUBLIC libmysql ssl crypto Threads::Threads m rt dl)
TARGET_INCLUDE_DIRECTORIES (slave PUBLIC
        ${MYSQL_BIN}/include
        ${MYSQL_SRC}
        ${MYSQL_SRC}/include
        ${MYSQL_SRC}/libbinlogevents/export
        ${BOOST_DIR}/boost_1_70_0
        )
INSTALL (TARGETS slave DESTINATION lib64)

IF (WITH_TESTING)
    IF (Boost_FOUND)
        ENABLE_TESTING ()
    ENDIF()
    ADD_SUBDIRECTORY (test)
ENDIF()
