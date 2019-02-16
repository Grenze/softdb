//
// Created by lingo on 19-2-15.
//

#ifndef SOFTDB_NVM_INTERVAL_H
#define SOFTDB_NVM_INTERVAL_H


#include <iostream>
#include <cassert>

namespace softdb {

template <class Value_>
class Interval_skip_list_interval
{
public:
    typedef Value_ Value;

private:
    Value inf_;
    Value sup_;
public:

    Interval_skip_list_interval(){}
    Interval_skip_list_interval(const Value& inf_,
                                const Value& sup_,
                                bool lb = true,
                                bool rb = true);

    const Value& inf() const {return inf_;}

    const Value& sup() const {return sup_;}


    bool contains(const Value& V) const;

    // true iff this contains (l,r)
    bool contains_interval(const Value& l, const Value& r) const;

    bool operator==(const Interval_skip_list_interval& I) const
    {
        return (inf() == I.inf()) && (sup() == I.sup());
    }

    bool operator!=(const Interval_skip_list_interval& I) const
    {
        return ! (*this == I);
    }
};



template <class V>
std::ostream& operator<<(std::ostream& os,
                         const Interval_skip_list_interval<V>& i)
{
    os << "[" << i.inf() << ", " << i.sup() << "]";
    return os;
}


template <class V>
Interval_skip_list_interval<V>::Interval_skip_list_interval(
        const Value& i,
        const Value& s,
        bool lb, bool rb)
        : inf_(i), sup_(s)
{
    assert( !(inf_ > sup_) );
}


template <class V>
bool
Interval_skip_list_interval<V>::contains_interval(const Value& i,
                                                  const Value& s) const
// true iff this contains (l,r)
{
    return( (inf() <= i) && (sup() >= s) );
}


template <class V>
bool
Interval_skip_list_interval<V>::contains(const Value& v) const
{
    // return true if this contains V, false otherwise
    return v >= inf() && v <= sup();
}

}

#endif //SOFTDB_NVM_INTERVAL_H
