# set source files
set (server-src-files
     Provider.cpp
     TopicManager.cpp)

set (client-src-files
     Client.cpp
     ServiceHandle.cpp
     TopicHandle.cpp
     Producer.cpp
     Data.cpp
     Metadata.cpp
     Serializer.cpp
     TargetSelector.cpp
     PartitionTargetInfo.cpp
     Validator.cpp
     ThreadPool.cpp
     AsyncRequest.cpp)

set (admin-src-files
     Admin.cpp)

set (dummy-src-files
     dummy/DummyTopicManager.cpp)

set (module-src-files
     BedrockModule.cpp)

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (mofka-pkg "share/cmake/mofka")

# set version fo shared library
set (mofka-vers "${MOFKA_VERSION_MAJOR}.${MOFKA_VERSION_MINOR}")

# client library
add_library (mofka-client ${client-src-files})
target_link_libraries (mofka-client
    PUBLIC thallium PkgConfig::uuid rapidjson bedrock-client PRIVATE spdlog::spdlog fmt::fmt coverage_config)
target_include_directories (mofka-client PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (mofka-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (mofka-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (mofka-client
    PROPERTIES VERSION ${MOFKA_VERSION}
    SOVERSION ${MOFKA_VERSION_MAJOR})

# server library
add_library (mofka-server ${server-src-files} ${dummy-src-files})
target_link_libraries (mofka-server
    PUBLIC thallium PkgConfig::uuid fmt::fmt rapidjson
    PRIVATE mofka-client spdlog::spdlog coverage_config)
target_include_directories (mofka-server PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (mofka-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (mofka-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (mofka-server
    PROPERTIES VERSION ${MOFKA_VERSION}
    SOVERSION ${MOFKA_VERSION_MAJOR})

# bedrock module library
add_library (mofka-bedrock-module ${module-src-files})
target_link_libraries (mofka-bedrock-module mofka-server mofka-client bedrock-server coverage_config)
target_include_directories (mofka-bedrock-module PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (mofka-bedrock-module BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (mofka-bedrock-module BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (mofka-bedrock-module
    PROPERTIES VERSION ${MOFKA_VERSION}
    SOVERSION ${MOFKA_VERSION_MAJOR})

# installation stuff (packaging and install commands)
write_basic_package_version_file (
    "mofka-config-version.cmake"
    VERSION ${MOFKA_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
set (INCLUDE_INSTALL_DIR include/)
set (LIB_INSTALL_DIR lib/)
configure_package_config_file (mofka-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/mofka-config.cmake
  INSTALL_DESTINATION ${LIB_INSTALL_DIR}/mofka/cmake
  PATH_VARS INCLUDE_INSTALL_DIR)

# some bits for the pkg-config file
set (DEST_DIR "${CMAKE_INSTALL_PREFIX}")
set (SERVER_PRIVATE_LIBS "-lmofka-server")
set (CLIENT_PRIVATE_LIBS "-lmofka-client")
configure_file ("mofka-server.pc.in" "mofka-server.pc" @ONLY)
configure_file ("mofka-client.pc.in" "mofka-client.pc" @ONLY)

# configure config.h
configure_file ("Config.h.in" "Config.h" @ONLY)

# "make install" rules
install (TARGETS mofka-server mofka-client
         EXPORT mofka-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
if (${ENABLE_BEDROCK})
    install (TARGETS mofka-bedrock-module
             ARCHIVE DESTINATION lib
             LIBRARY DESTINATION lib)
endif ()
install (EXPORT mofka-targets
         DESTINATION ${mofka-pkg}
         FILE "mofka-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/mofka-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/mofka-config-version.cmake"
         DESTINATION ${mofka-pkg} )
install (DIRECTORY ../include/mofka
         DESTINATION include
         FILES_MATCHING PATTERN "*.h")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/mofka-server.pc"
               "${CMAKE_CURRENT_BINARY_DIR}/mofka-client.pc"
         DESTINATION "lib/pkgconfig/")
