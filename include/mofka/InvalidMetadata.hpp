/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_INVALID_METADATA_HPP
#define MOFKA_INVALID_METADATA_HPP

#include <mofka/ForwardDcl.hpp>
#include <mofka/Metadata.hpp>
#include <mofka/Exception.hpp>

#include <exception>
#include <stdexcept>

namespace mofka {

/**
 * @brief The InvalidMetadata class is the exception raised
 * when a Metadata is not valid.
 */
class InvalidMetadata : public Exception {

    public:

    template<typename ... Args>
    InvalidMetadata(Args&&... args)
    : Exception(std::forward<Args>(args)...) {}
};

}

#endif
