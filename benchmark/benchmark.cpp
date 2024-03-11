#include "StringGenerator.hpp"
#include "MetadataGenerator.hpp"
#include "PropertyListSerializer.hpp"

#include <nlohmann/json.hpp>
#include <nlohmann/json-schema.hpp>
#include <iostream>
#include <chrono>
#include <random>
#include <mpi.h>

MOFKA_REGISTER_SERIALIZER(property_list_serializer, PropertyListSerializer);

int main(int argc, char** argv) {

    if(argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config.json>" << std::endl;
        exit(-1);
    }

    MPI_Init(&argc, &argv);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    unsigned seed = 12345 + rank;

    StringGenerator strGenerator{seed};
    MetadataGenerator MetadataGenerator{strGenerator, 10, 8, 16, 8, 16};

    MPI_Finalize();

    return 0;
}
