/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __ALPHA_UUID_UTIL_HPP
#define __ALPHA_UUID_UTIL_HPP

#include <uuid/uuid.h>
#include <string>
#include <cstring>

namespace alpha {

/**
 * @brief UUID class (Universally Unique IDentifier).
 */
struct UUID {

    uuid_t m_data;

    /**
     * @brief Constructor, produces a zero-ed UUID.
     */
    UUID() {
        uuid_clear(m_data);
    }

    /**
     * @brief Copy constructor.
     */
    UUID(const UUID& other) = default;

    /**
     * @brief Move constructor.
     */
    UUID(UUID&& other) = default;

    /**
     * @brief Copy-assignment operator.
     */
    UUID& operator=(const UUID& other) = default;

    /**
     * @brief Move-assignment operator.
     */
    UUID& operator=(UUID&& other) = default;

    /**
     * @brief Converts the UUID into a string.
     *
     * @return a readable string representation of the UUID.
     */
    std::string to_string() const {
        std::string result(36, '\0');
        uuid_unparse(this->m_data, const_cast<char*>(result.data()));
        return result;
    }

    /**
     * @brief Makes a UUID from a C-style string.
     *
     * @param str 33-byte null-terminate string
     *
     * @return the corresponding UUID.
     */
    static UUID from_string(const char* str) {
        UUID uuid;
        int ret = uuid_parse(str, uuid.m_data);
        if(ret == -1) {
            throw std::invalid_argument("String argument does not represent a valid UUID");
        }
        return uuid;
    }

    /**
     * @brief Serialization of a UUID into an archive.
     *
     * @tparam Archive
     * @param a
     * @param version
     */
    template<typename Archive>
    void save(Archive& a) const {
        a.write(m_data, sizeof(uuid_t));
    }

    /**
     * @brief Deserialization of a UUID from an archive.
     *
     * @tparam Archive
     * @param a
     * @param version
     */
    template<typename Archive>
    void load(Archive& a) {
        a.read(m_data, sizeof(uuid_t));
    }

    /**
     * @brief Converts the UUID into a string and pass it
     * to the provided stream.
     */
    template<typename T>
    friend T& operator<<(T& stream, const UUID& id) {
        stream << id.to_string();
        return stream;
    }

    /**
     * @brief Generates a random UUID.
     *
     * @return a random UUID.
     */
    static UUID generate() {
        UUID uuid;
        uuid_generate(uuid.m_data);
        return uuid;
    }

    /**
     * @brief randomize the current UUID.
     */
    void randomize() {
        uuid_generate(m_data); 
    }

    /**
     * @brief Compare the UUID with another UUID.
     */
    bool operator==(const UUID& other) const {
        return memcmp(m_data, other.m_data, 16) == 0;
    }

    /**
     * @brief Computes a hash of the UUID.
     *
     * @return a uint64_t hash value.
     */
    uint64_t hash() const {
        const uint64_t* a = reinterpret_cast<const uint64_t*>(m_data);
        const uint64_t* b = reinterpret_cast<const uint64_t*>(m_data+8);
        return *a ^ *b;
    }
};

}

namespace std {


    /**
     * @brief Specialization of std::hash for alpha::UUID
     */
    template<>
    struct hash<alpha::UUID> 
    {
        size_t operator()(const alpha::UUID& id) const
        {
            return id.hash();
        }
    };

}

#endif
