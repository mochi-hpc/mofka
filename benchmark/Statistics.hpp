#ifndef BENCHMARK_STATISTICS_HPP
#define BENCHMARK_STATISTICS_HPP

#include <iostream>
#include <cstddef>
#include <cmath>
#include <limits>
#include <nlohmann/json.hpp>

class Statistics {

    private:

    size_t num;   // number of samples accumulated
    double avg;   // running average value
    double var;   // running sum
    double min;   // minimum value
    double max;   // maximum value

    public:

    Statistics() : num(0), avg(0.0), var(0.0),
                   min(std::numeric_limits<double>::infinity()),
                   max(-std::numeric_limits<double>::infinity()) {}

    Statistics& operator<<(double value) {
        num++;

        double delta = value - avg;
        avg += delta / num;
        var += delta * (value - avg);

        // Update min and max
        if (value < min) min = value;
        if (value > max) max = value;

        return *this;
    }

    Statistics operator+(const Statistics& other) const {
        Statistics result(*this); // Create a copy of current object

        size_t total_num = result.num + other.num;
        double delta_avg = other.avg - result.avg;
        result.avg = (result.num * result.avg + other.num * other.avg) / total_num;
        result.var = result.var + other.var + delta_avg * delta_avg * result.num * other.num / total_num;
        result.num = total_num;

        if (other.min < result.min) result.min = other.min;
        if (other.max > result.max) result.max = other.max;

        return result;
    }

    size_t count() const {
        return num;
    }

    double minimum() const {
        return min;
    }

    double maximum() const {
        return max;
    }

    double average() const {
        return avg;
    }

    double variance() const {
        if (num <= 1) return 0.0; // Variance is undefined for fewer than 2 samples
        return var / (num - 1);
    }

    nlohmann::json to_json() const {
        nlohmann::json j = nlohmann::json::object();
        j["num"] = count();
        j["avg"] = average();
        j["var"] = variance();
        j["min"] = minimum();
        j["max"] = maximum();
        return j;
    }
};

#endif
