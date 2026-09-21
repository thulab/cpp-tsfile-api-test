#pragma once
// 测试程序侧兼容层：为 GCC 7.3 等缺少 std::filesystem 的环境提供 POSIX 实现。
// 仅被 cpp-tsfile-api-test 自身引用，不属于 tsfile SDK。
#include <cerrno>
#include <ostream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace fs_compat {

class path {
public:
    path() = default;
    path(const std::string& p) : p_(p) {}
    path(const char* p) : p_(p ? p : "") {}

    path operator/(const path& rhs) const {
        if (p_.empty()) return path(rhs.p_);
        if (rhs.p_.empty()) return path(p_);
        if (p_.back() == '/') return path(p_ + rhs.p_);
        return path(p_ + "/" + rhs.p_);
    }
    path operator/(const std::string& rhs) const { return *this / path(rhs); }
    path operator/(const char* rhs) const { return *this / path(rhs); }

    path parent_path() const {
        if (p_.empty()) return path();
        size_t pos = p_.find_last_of('/');
        if (pos == std::string::npos) return path();
        if (pos == 0) return path("/");
        return path(p_.substr(0, pos));
    }

    bool empty() const { return p_.empty(); }
    const std::string& string() const { return p_; }
    const char* c_str() const { return p_.c_str(); }

private:
    std::string p_;
};

inline std::ostream& operator<<(std::ostream& os, const path& p) {
    return os << p.string();
}

inline bool exists(const path& p) {
    struct stat st;
    return ::stat(p.c_str(), &st) == 0;
}

inline bool is_directory(const path& p) {
    struct stat st;
    return ::stat(p.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

inline bool is_regular_file(const path& p) {
    struct stat st;
    return ::stat(p.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

inline bool remove(const path& p) { return ::remove(p.c_str()) == 0; }

inline bool create_directories(const path& p) {
    const std::string& s = p.string();
    std::string cur;
    for (size_t i = 0; i <= s.size(); ++i) {
        if (i == s.size() || s[i] == '/') {
            if (!cur.empty() && cur != "/" && ::mkdir(cur.c_str(), 0755) != 0 && errno != EEXIST) {
                return false;
            }
            cur += '/';
        } else {
            cur += s[i];
        }
    }
    return true;
}

} // namespace fs_compat
