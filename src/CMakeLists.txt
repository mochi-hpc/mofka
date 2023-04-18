# set source files
set (server-src-files
     Provider.cpp
     Backend.cpp)

set (client-src-files
     Client.cpp
     ResourceHandle.cpp
     AsyncRequest.cpp)

set (admin-src-files
     Admin.cpp)

set (dummy-src-files
     dummy/DummyBackend.cpp)

set (module-src-files
     BedrockModule.cpp)

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (alpha-pkg "share/cmake/alpha")

# set version fo shared library
set (alpha-vers "${ALPHA_VERSION_MAJOR}.${ALPHA_VERSION_MINOR}")

# server library
add_library (alpha-server ${server-src-files} ${dummy-src-files})
target_link_libraries (alpha-server
    PUBLIC thallium PkgConfig::uuid
    PRIVATE nlohmann_json::nlohmann_json spdlog::spdlog coverage_config)
target_include_directories (alpha-server PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (alpha-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (alpha-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (alpha-server
    PROPERTIES VERSION ${ALPHA_VERSION}
    SOVERSION ${ALPHA_VERSION_MAJOR})

# client library
add_library (alpha-client ${client-src-files})
target_link_libraries (alpha-client
    PUBLIC thallium PkgConfig::uuid PRIVATE spdlog::spdlog coverage_config)
target_include_directories (alpha-client PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (alpha-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (alpha-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (alpha-client
    PROPERTIES VERSION ${ALPHA_VERSION}
    SOVERSION ${ALPHA_VERSION_MAJOR})

# admin library
add_library (alpha-admin ${admin-src-files})
target_link_libraries (alpha-admin PUBLIC thallium PkgConfig::uuid
    PRIVATE nlohmann_json::nlohmann_json spdlog::spdlog coverage_config)
target_include_directories (alpha-admin PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (alpha-admin BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (alpha-admin BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (alpha-admin
    PROPERTIES VERSION ${ALPHA_VERSION}
    SOVERSION ${ALPHA_VERSION_MAJOR})

if (${ENABLE_BEDROCK})
# bedrock module library
add_library (alpha-bedrock-module ${module-src-files})
target_link_libraries (alpha-bedrock-module alpha-server alpha-admin alpha-client bedrock-server coverage_config)
target_include_directories (alpha-bedrock-module PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (alpha-bedrock-module BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (alpha-bedrock-module BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (alpha-bedrock-module
    PROPERTIES VERSION ${ALPHA_VERSION}
    SOVERSION ${ALPHA_VERSION_MAJOR})
endif ()

# installation stuff (packaging and install commands)
write_basic_package_version_file (
    "alpha-config-version.cmake"
    VERSION ${ALPHA_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
set (INCLUDE_INSTALL_DIR include/)
set (LIB_INSTALL_DIR lib/)
configure_package_config_file (alpha-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/alpha-config.cmake
  INSTALL_DESTINATION ${LIB_INSTALL_DIR}/alpha/cmake
  PATH_VARS INCLUDE_INSTALL_DIR)

# some bits for the pkg-config file
set (DEST_DIR "${CMAKE_INSTALL_PREFIX}")
set (SERVER_PRIVATE_LIBS "-lalpha-server")
set (CLIENT_PRIVATE_LIBS "-lalpha-client")
set (ADMIN_PRIVATE_LIBS  "-lalpha-admin")
configure_file ("alpha-server.pc.in" "alpha-server.pc" @ONLY)
configure_file ("alpha-client.pc.in" "alpha-client.pc" @ONLY)
configure_file ("alpha-admin.pc.in"  "alpha-admin.pc"  @ONLY)

# configure config.h
configure_file ("config.h.in" "config.h" @ONLY)

# "make install" rules
install (TARGETS alpha-admin alpha-server alpha-client
         EXPORT alpha-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
if (${ENABLE_BEDROCK})
    install (TARGETS alpha-bedrock-module
             ARCHIVE DESTINATION lib
             LIBRARY DESTINATION lib)
endif ()
install (EXPORT alpha-targets
         DESTINATION ${alpha-pkg}
         FILE "alpha-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/alpha-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/alpha-config-version.cmake"
         DESTINATION ${alpha-pkg} )
install (DIRECTORY ../include/alpha
         DESTINATION include
         FILES_MATCHING PATTERN "*.h")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/alpha-server.pc"
               "${CMAKE_CURRENT_BINARY_DIR}/alpha-client.pc"
               "${CMAKE_CURRENT_BINARY_DIR}/alpha-admin.pc"
         DESTINATION "lib/pkgconfig/")
