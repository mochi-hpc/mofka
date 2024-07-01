#ifndef DATA_GENERATOR_H
#define DATA_GENERATOR_H

#include "StringGenerator.hpp"
#include <mofka/Data.hpp>
#include <nlohmann/json.hpp>
#include <nlohmann/json-schema.hpp>
#include <iostream>
#include <chrono>
#include <random>


/**
 * @brief This class is a helper to generate random Data objects.
 * It is initialized with a reference to a StringGenerator to generate the blocks
 * in the constructor and the values in its generate function.
 */
class DataGenerator {

    using json = nlohmann::json;

    StringGenerator<std::string>& m_strGenerator;
    size_t                        m_minDataSize;
    size_t                        m_maxDataSize;
    size_t                        m_minNumBlocks;
    size_t                        m_maxNumBlocks;

    public:

    DataGenerator(StringGenerator<std::string>& strGen,
                  size_t minDataSize,
                  size_t maxDataSize,
                  size_t minNumBlocks,
                  size_t maxNumBlocks)
    : m_strGenerator(strGen)
    , m_minDataSize(minDataSize)
    , m_maxDataSize(maxDataSize)
    , m_minNumBlocks(minNumBlocks)
    , m_maxNumBlocks(maxNumBlocks) {}

    mofka::Data generate() {
        if(m_maxDataSize == 0) {
            return mofka::Data{};
        }
        std::uniform_int_distribution<size_t> numBlocksDistribution(m_minNumBlocks, m_maxNumBlocks);
        size_t num_blocks = numBlocksDistribution(m_strGenerator.rng());
        std::uniform_int_distribution<size_t> sizeDistribution(m_minDataSize, m_maxDataSize);
        size_t size = sizeDistribution(m_strGenerator.rng());
        num_blocks = std::min(num_blocks, size);
        size_t block_size = size/num_blocks;
        std::vector<std::string> blocks;
        blocks.reserve(num_blocks);
        size_t remaining_size = size;
        while(remaining_size != 0) {
            auto this_block_size = std::min(remaining_size, block_size);
            remaining_size -= this_block_size;
            blocks.push_back(m_strGenerator.generate(this_block_size, this_block_size));
        }
        std::vector<mofka::Data::Segment> segments{blocks.size()};
        for(size_t i = 0; i < blocks.size(); ++i) {
            segments[i] = mofka::Data::Segment{blocks[i].data(), blocks[i].size()};
        }
        return mofka::Data{segments, new decltype(blocks){std::move(blocks)},
                           [](void* ctx) { delete static_cast<decltype(blocks)*>(ctx);}};
    }

};

#endif
