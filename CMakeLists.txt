cmake_minimum_required(VERSION 3.16)
project(file_stream LANGUAGES CXX)

# ---------------------------------------------------------------
# Build settings
# ---------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# High performance build
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# ---------------------------------------------------------------
# Boost (header-only, 1.89, installed in /usr/local/include)
# ---------------------------------------------------------------
# We don't call find_package(Boost) anymore.
# Dockerfile copied "boost" into /usr/local/include/boost
set(BOOST_ROOT "/usr/local")
set(BOOST_INCLUDE_DIR "${BOOST_ROOT}/include")

# Make Boost.System explicitly header-only and avoid deprecated stuff
add_compile_definitions(
    BOOST_ERROR_CODE_HEADER_ONLY
    BOOST_SYSTEM_NO_DEPRECATED
)

message(STATUS "Using Boost headers from: ${BOOST_INCLUDE_DIR}")

# ---------------------------------------------------------------
# Find liburing
# ---------------------------------------------------------------
find_library(URING_LIB
    NAMES uring
    PATHS /usr/lib /usr/lib64 /usr/local/lib /opt/lib
)

if(NOT URING_LIB)
    message(FATAL_ERROR "liburing not found. Install liburing-dev / liburing-devel")
else()
    message(STATUS "Found liburing: ${URING_LIB}")
endif()

# ---------------------------------------------------------------
# Compiler definitions for Boost.Asio io_uring + coroutines
# ---------------------------------------------------------------
add_compile_definitions(
    BOOST_ASIO_HAS_IO_URING
    BOOST_ASIO_DISABLE_EPOLL
    BOOST_ASIO_NO_DEPRECATED
    BOOST_ASIO_HAS_CO_AWAIT       # enable co_await support in Asio
)

# ---------------------------------------------------------------
# Build target file_stream_server
# ---------------------------------------------------------------
add_executable(file_stream_server
    file_stream_server.cpp
)

# Tell the target to use the Boost 1.89 headers
target_include_directories(file_stream_server PRIVATE
    ${BOOST_INCLUDE_DIR}
)

# Only link liburing (Boost is header-only here)
target_link_libraries(file_stream_server PRIVATE
    ${URING_LIB}
)

# Optional: optimize aggressively
target_compile_options(file_stream_server PRIVATE
    -O3 -march=native -DNDEBUG -Wall -Wextra -Werror
)

message(STATUS "Build configuration file_stream_server complete.")

# ---------------------------------------------------------------
# Build target file_stream_client
# ---------------------------------------------------------------
add_executable(file_stream_client
    file_stream_client.cpp
)

# Tell the target to use the Boost 1.89 headers
target_include_directories(file_stream_client PRIVATE
    ${BOOST_INCLUDE_DIR}
)

# Only link liburing (Boost is header-only here)
target_link_libraries(file_stream_client PRIVATE
    ${URING_LIB}
)

# Optional: optimize aggressively
target_compile_options(file_stream_client PRIVATE
    -O3 -march=native -DNDEBUG -Wall -Wextra -Werror
)

message(STATUS "Build configuration file_stream_client complete.")

