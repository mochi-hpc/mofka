add_library (pymofka_client MODULE py-mofka-client.cpp)
target_link_libraries (pymofka_client PRIVATE pybind11::module mofka-client PRIVATE coverage_config)
pybind11_extension (pymofka_client)
pybind11_strip (pymofka_client)

set (PY_MOFKA_TARGETS pymofka_client)

set (PY_VERSION ${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR})

configure_file (mofkactl.sh.in ${CMAKE_BINARY_DIR}/bin/mofkactl USE_SOURCE_PERMISSIONS @ONLY)

install (FILES ${CMAKE_BINARY_DIR}/bin/mofkactl
         DESTINATION bin
         PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE)

install (TARGETS ${PY_MOFKA_TARGETS}
         EXPORT mofka-targets
         ARCHIVE DESTINATION lib/python${PY_VERSION}/site-packages
         LIBRARY DESTINATION lib/python${PY_VERSION}/site-packages)

install (DIRECTORY mochi/mofka
         DESTINATION lib/python${PY_VERSION}/site-packages/mochi)
