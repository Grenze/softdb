//
// Created by lingo on 19-3-22.
//

#ifndef SOFTDB_HISTOGRAM_H
#define SOFTDB_HISTOGRAM_H


#include <string>

namespace softdb {

    class Histogram {
    public:
        Histogram() { }
        ~Histogram() { }

        void Clear();
        void Add(double value);
        void Merge(const Histogram& other);

        std::string ToString() const;

    private:
        double min_;
        double max_;
        double num_;
        double sum_;
        double sum_squares_;

        enum { kNumBuckets = 154 };
        static const double kBucketLimit[kNumBuckets];
        double buckets_[kNumBuckets];

        double Median() const;
        double Percentile(double p) const;
        double Average() const;
        double StandardDeviation() const;
    };

}  // namespace softdb

#endif //SOFTDB_HISTOGRAM_H
