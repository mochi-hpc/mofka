/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_SSG_HPP
#define MOFKA_SSG_HPP

#include <mofka/ForwardDcl.hpp>
#include <memory>
#include <string_view>

namespace mofka {

/**
 * @brief SSGFileName is a string_view representing the name
 * of an SSG group file.
 */
struct SSGFileName : public std::string_view {
    template<typename ... Args>
    explicit SSGFileName(Args&&... args)
    : std::string_view(std::forward<Args>(args)...) {}
};

/**
 * @brief The SSGGroupID is a wrapper for an ssg_group_id_t
 * (without having to rely on including SSG).
 */
struct SSGGroupID {
    uint64_t value;
    explicit SSGGroupID(uint64_t v) : value(v) {}
};

}

#endif
