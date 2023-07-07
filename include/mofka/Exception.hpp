/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_EXCEPTION_HPP
#define MOFKA_EXCEPTION_HPP

#include <fmt/format.h>
#include <exception>
#include <stdexcept>

namespace mofka {

class Exception : public std::logic_error {

    public:

    template<typename ... Args>
    Exception(Args&&... args)
    : std::logic_error(fmt::format(std::forward<Args>(args)...)) {}
};

}

#endif
