cmake_minimum_required(VERSION 2.8.12)
include(ExternalProject)

### Core config
set(TARGET_NAME deltatable)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

include_directories(src/include)

# Need a few extra headers from duckdb for parquet reading
include_directories(duckdb/extension/parquet/include)
include_directories(duckdb/third_party/parquet)
include_directories(duckdb/third_party/thrift)

set(EXTENSION_SOURCES
        src/deltatable_extension.cpp
        src/deltatable_functions.cpp
        src/functions/deltatable_scan.cpp)

### Custom config
# TODO: figure out if we really need this?
if(APPLE)
    set(PLATFORM_LIBS m c System resolv "-framework Corefoundation")
elseif(UNIX)
    set(PLATFORM_LIBS m c resolv)
elseif(WIN32)
    set(PLATFORM_LIBS ws2_32 userenv advapi32)
else()
    message(STATUS "UNKNOWN OS")
endif()

### Setup delta-kernel-rs dependency
set(KERNEL_NAME delta_kernel)

# Set default ExternalProject root directory
set_directory_properties(PROPERTIES EP_PREFIX ${CMAKE_BINARY_DIR}/rust)

# Add rust_example as a CMake target
ExternalProject_Add(
        ${KERNEL_NAME}
        DOWNLOAD_COMMAND ""
        CONFIGURE_COMMAND ""
        BUILD_COMMAND cargo build --package deltakernel-ffi --all-features COMMAND cargo build --release --package deltakernel-ffi --all-features
        BINARY_DIR "${CMAKE_SOURCE_DIR}/../delta-kernel-rs"
        BUILD_BYPRODUCTS "${CMAKE_SOURCE_DIR}/../delta-kernel-rs/target/debug/libdeltakernel_ffi.a"
        BUILD_BYPRODUCTS "${CMAKE_SOURCE_DIR}/../delta-kernel-rs/target/release/libdeltakernel_ffi.a"
        BUILD_BYPRODUCTS "${CMAKE_SOURCE_DIR}/../delta-kernel-rs/target/ffi-headers/deltakernel-ffi.h"
        BUILD_BYPRODUCTS "${CMAKE_SOURCE_DIR}/../delta-kernel-rs/target/ffi-headers/deltakernel-ffi.hpp"
        INSTALL_COMMAND ""
        LOG_BUILD ON)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

include_directories(delta-kernel-rs/target/ffi-headers)
include_directories(delta-kernel-rs/target/ffi-headers)

# Hides annoying linker warnings
set(CMAKE_OSX_DEPLOYMENT_TARGET 13.3 CACHE STRING "Minimum OS X deployment version" FORCE)

# Link delta-kernal-rs to static lib
target_link_libraries(${EXTENSION_NAME}
        debug "${CMAKE_SOURCE_DIR}/../delta-kernel-rs/target/debug/libdeltakernel_ffi.a"
        optimized "${CMAKE_SOURCE_DIR}/../delta-kernel-rs/target/release/libdeltakernel_ffi.a"
        ${PLATFORM_LIBS})
add_dependencies(${EXTENSION_NAME} delta_kernel)

# Link delta-kernal-rs to dynamic lib
target_link_libraries(${LOADABLE_EXTENSION_NAME}
        debug "${CMAKE_SOURCE_DIR}/../delta-kernel-rs/target/debug/libdeltakernel_ffi.a"
        optimized "${CMAKE_SOURCE_DIR}/../delta-kernel-rs/target/release/libdeltakernel_ffi.a"
        ${PLATFORM_LIBS})
add_dependencies(${LOADABLE_EXTENSION_NAME} delta_kernel)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")