//
// Created by lingo on 19-1-17.
//

#include "softdbtest.h"
#include <cassert>
#include <iostream>
#include <cstdint>
#include <chrono>

#include "../db.h"

using namespace std;

::std::uint64_t NowNanos() {
    return ::std::chrono::duration_cast<::std::chrono::nanoseconds>(
            ::std::chrono::steady_clock::now().time_since_epoch())
            .count();
}
int main(int argc, char** argv) {

    softdb::DB* db;
    softdb::Options options;
    options.create_if_missing = true;
    softdb::Status status = softdb::DB::Open(options, "/tmp/softdb", &db);
    //softdb::Status status = softdb::DB::Open(options, "/dev/shm/softdb", &db);
    assert(status.ok());

    size_t total_insert = 2*500000;

    softdb::Slice s1;
    auto start_time = NowNanos();


    for(int i=0; i<total_insert; i++) {
        s1 = std::to_string(i);
        status = db->Put(softdb::WriteOptions(), s1, std::to_string(i));
        if (!status.ok()) {
            cout<<"put error"<<endl;
            break;
        }
    }

/*
    std::string rep;
    for(int i=0; i<total_insert; i++) {
        s1 = std::to_string(i);
        status = db->Get(softdb::ReadOptions(), s1, &rep);
        if (!status.ok() || rep != s1) {
            cout<<"get error"<<endl;
            break;
        }
    }

    for(int i=0; i<total_insert ;i++) {
        s1 = std::to_string(i);
        status = db->Delete(softdb::WriteOptions(), s1);
        if (!status.ok()) {
            cout<<"delete error"<<endl;
            break;
        }
    }
*/
    auto end_time = NowNanos();
    cout<< "nanosecond: "<<end_time - start_time <<endl;

    delete db;
    //std::cout<<sizeof(void*)<<" "<<sizeof(int); //8 4
    return 1;
}