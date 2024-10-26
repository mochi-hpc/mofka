# set source files
set (server-src-files
     Provider.cpp
     DefaultPartitionManager.cpp
     MemoryPartitionManager.cpp)

set (client-src-files
     Client.cpp
     MofkaDriver.cpp
     MofkaTopicHandle.cpp
     Producer.cpp
     BatchProducer.cpp
     MofkaProducer.cpp
     Consumer.cpp
     MofkaConsumer.cpp
     ConsumerHandle.cpp
     Data.cpp
     DataDescriptor.cpp
     Metadata.cpp
     Serializer.cpp
     PartitionSelector.cpp
     Validator.cpp
     EventbridgeValidator.cpp
     ThreadPool.cpp
     PrioPool.cpp)

set (kafka-src-files
     KafkaDriver.cpp
     KafkaTopicHandle.cpp
     KafkaProducer.cpp
     KafkaBatchProducer.cpp
     KafkaProducerBatch.cpp
     KafkaConsumer.cpp
     KafkaEvent.cpp)

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
add_library (mofka::client ALIAS mofka-client)
target_link_libraries (mofka-client
    PUBLIC
    thallium
    PkgConfig::uuid
    bedrock::client
    nlohmann_json::nlohmann_json
    nlohmann_json_schema_validator::validator
    PRIVATE
    yokan::client
    spdlog::spdlog
    fmt::fmt
    coverage_config
    warnings_config)
target_include_directories (mofka-client PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (mofka-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (mofka-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
set_target_properties (mofka-client
    PROPERTIES VERSION ${MOFKA_VERSION}
    SOVERSION ${MOFKA_VERSION_MAJOR})

# optional Kafka driver
if (ENABLE_KAFKA)
    add_library (mofka-kafka-driver ${kafka-src-files})
    add_library (mofka::kafka-driver ALIAS mofka-kafka-driver)
    target_link_libraries (mofka-kafka-driver
         PUBLIC mofka::client
         PRIVATE
             nlohmann_json::nlohmann_json
             PkgConfig::kafka
             coverage_config
             warnings_config)
    target_include_directories (mofka-kafka-driver PUBLIC $<INSTALL_INTERFACE:include>)
    target_include_directories (mofka-kafka-driver BEFORE PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
    target_include_directories (mofka-kafka-driver BEFORE PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
    set_target_properties (mofka-kafka-driver
        PROPERTIES VERSION ${MOFKA_VERSION}
        SOVERSION ${MOFKA_VERSION_MAJOR})
endif ()

# server library
add_library (mofka-server ${server-src-files})
add_library (mofka::server ALIAS mofka-server)
target_link_libraries (mofka-server
    PUBLIC
    thallium
    PkgConfig::uuid
    fmt::fmt
    nlohmann_json::nlohmann_json
    nlohmann_json_schema_validator::validator
    mofka-client
    PRIVATE
    warabi-client
    spdlog::spdlog
    coverage_config
    warnings_config)
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
target_link_libraries (mofka-bedrock-module
    PUBLIC mofka-server mofka-client bedrock::module-api
    PRIVATE coverage_config warnings_config)
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
if (ENABLE_KAFKA)
install (TARGETS mofka-server mofka-client mofka-bedrock-module mofka-kafka-driver
         EXPORT mofka-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
else ()
install (TARGETS mofka-server mofka-client mofka-bedrock-module
         EXPORT mofka-targets
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
         FILES_MATCHING PATTERN "*.hpp")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/mofka-server.pc"
               "${CMAKE_CURRENT_BINARY_DIR}/mofka-client.pc"
         DESTINATION "lib/pkgconfig/")
