/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_RAPID_JSON_UTIL_H
#define MOFKA_RAPID_JSON_UTIL_H

#include "mofka/Data.hpp"
#include <stack>

namespace mofka {

class StringWrapper {

    public:

    typedef char Ch;

    StringWrapper(std::string& s) : m_str(s) {
        m_str.reserve(4096);
    }

    void Put(char c) {
        m_str.push_back(c);
    }

    void Flush() {
        return;
    }

    private:

    std::string& m_str;
};

template<typename ArchiveType>
class ArchiveWrapper {

    public:

    typedef char Ch;

    ArchiveWrapper(ArchiveType& arch) : m_archive(arch) {}

    void Put(char c) {
        m_archive.write(&c, 1);
    }

    void Flush() {
        return;
    }

    private:

    ArchiveType& m_archive;

};

static inline bool ValidateIsJson(std::string_view json) {
    std::stack<char> brackets;
    bool insideString = false;
    bool escaped = false;

    for (char c : json) {
        if (!escaped) {
            if (c == '"') {
                insideString = !insideString;
            } else if (!insideString) {
                if (c == '{' || c == '[') {
                    brackets.push(c);
                } else if (c == '}' || c == ']') {
                    if (brackets.empty()) {
                        // Extra closing bracket encountered
                        return false;
                    }
                    char top = brackets.top();
                    brackets.pop();

                    if ((c == '}' && top != '{') || (c == ']' && top != '[')) {
                        // Mismatched brackets
                        return false;
                    }
                }
            }
        }

        if (c == '\\') {
            escaped = !escaped;
        } else {
            escaped = false;
        }
    }

    return brackets.empty();
}

}

#endif
