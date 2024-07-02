#ifndef RNG_H
#define RNG_H

#include <chrono>
#include <random>
#include <thallium.hpp>
#include <mutex>

class RNG {

    std::mt19937    m_rng;
    thallium::mutex m_mtx;

    public:

    using result_type = std::mt19937::result_type;

    RNG(unsigned seed)
    : m_rng{seed} {}

    auto operator()() {
        m_mtx.spin_lock();
        auto val = m_rng();
        m_mtx.unlock();
        return val;
    }

    static constexpr auto min() {
        return std::mt19937::min();
    }

    static constexpr auto max() {
        return std::mt19937::max();
    }
};

#endif
