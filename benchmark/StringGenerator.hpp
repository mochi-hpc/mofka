#ifndef STRING_GENERATOR_H
#define STRING_GENERATOR_H

#include <iostream>
#include <chrono>
#include <random>
#include <string>

/**
 * @brief This class is a helper to generate random strings.
 * The alphabet is provided at construction time. When calling the generate
 * function, the size of the resulting string will be drawn uniformly in
 * [minSize, maxSize]. The content will be drawn uniformly from the generator's
 * alphabet.
 */
class StringGenerator {

    std::mt19937 m_rng;
    std::string  m_alphabet;

    public:

    StringGenerator(unsigned seed, std::string alphabet =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
    : m_rng(seed)
    , m_alphabet(std::move(alphabet)) {}

    std::string generate(size_t minSize, size_t maxSize) {
        std::uniform_int_distribution<size_t> sizeDistribution(minSize, maxSize);
        size_t size = sizeDistribution(m_rng);
        std::uniform_int_distribution<size_t> indexDistribution(0, m_alphabet.size()-1);
        std::string result;
        result.resize(size);
        char* ptr = const_cast<char*>(result.data());
        for(size_t i = 0; i < size; ++i) {
            ptr[i] = m_alphabet[indexDistribution(m_rng)];
        }
        return result;
    }
};

#endif
