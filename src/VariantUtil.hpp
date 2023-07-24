/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_VARIANT_UTIL_HPP
#define MOFKA_VARIANT_UTIL_HPP

namespace mofka {

template<class... Ts>
struct Overloaded : Ts... { using Ts::operator()...; };

template<class... Ts>
Overloaded(Ts...) -> Overloaded<Ts...>;

}

#endif
