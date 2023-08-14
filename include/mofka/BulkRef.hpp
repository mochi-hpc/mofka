/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_BULK_REF_HPP
#define MOFKA_BULK_REF_HPP

#include <mofka/ForwardDcl.hpp>

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <memory>
#include <vector>

namespace mofka {

/**
 * The BulkRef struct helps encapsulate information
 * about a segment of memory in a remote process.
 */
struct BulkRef {

    thallium::bulk handle;  /*!< Bulk handle */
    size_t         offset;  /*!< Offset at which the data starts in the bulk handle */
    size_t         size;    /*!< Size of the data */
    std::string    address; /*!< Address of the process where the data is located (empty string for current process) */

    template<typename A>
    void serialize(A& ar) {
        ar(handle);
        ar(offset);
        ar(size);
        ar(address);
    }
};

}

#endif
