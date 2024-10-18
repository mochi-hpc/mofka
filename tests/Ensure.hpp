#include <functional>
#include <cstdio>
#include <string>

template<typename F>
class deferred {

    public:

        explicit deferred(F&& function)
        : m_function(std::forward<F>(function)) {}

        ~deferred() {
            m_function();
        }

    private:

        F m_function;
};

template<typename F>
inline auto defer(F&& f) {
    return deferred<F>(std::forward<F>(f));
}

#define _UTILITY_ENSURERED_LINENAME_CAT(name, line) name##line
#define _UTILITY_ENSURERED_LINENAME(name, line) _UTILITY_ENSURERED_LINENAME_CAT(name, line)
#define ENSURE(f) \
    const auto& _UTILITY_ENSURERED_LINENAME(EXIT, __LINE__) = ::defer([&]() { f; }); (void)_UTILITY_ENSURERED_LINENAME(EXIT, __LINE__)

struct EnsureFileRemoved {

    std::string m_filename;

    template<typename ... Args>
        EnsureFileRemoved(Args&&... args)
        : m_filename(std::forward<Args>(args)...) {}

    ~EnsureFileRemoved() {
        std::remove(m_filename.c_str());
    }
};
