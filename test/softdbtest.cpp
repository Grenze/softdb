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
    return static_cast<uint64_t>(::std::chrono::duration_cast<::std::chrono::nanoseconds>(
                ::std::chrono::steady_clock::now().time_since_epoch())
                .count());
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
    softdb::Slice s2;


    auto start_time = NowNanos();
    int ll = 1;
    //for (ll = 0; ll < 1000; ll++) {
        for(int i=0; i<total_insert; i++) {
            s1 = std::to_string(i);
            status = db->Put(softdb::WriteOptions(), s1, std::to_string(i+ll));
            if (!status.ok()) {
                cout<<"put error"<<endl;
                break;
            }
        }
    //}


    /*
    for(int i=0; i<total_insert; i++) {
        s1 = std::to_string(i);
        status = db->Put(softdb::WriteOptions(), s1, std::to_string(i+1));
        if (!status.ok()) {
            cout<<"put error"<<endl;
            break;
        }
    }
     */

    auto p1_time = NowNanos();
    cout<< "Phase1 nanosecond: " << p1_time - start_time <<endl;



    softdb::Iterator* it = db->NewIterator(softdb::ReadOptions());
    int check = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        //std::cout<<check<<std::endl;
        //cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
        check++;
    }
    assert(check == total_insert);
    // something wrong here.
    //assert(it->status().ok());  // Check for any errors found during the scan
    delete it;

    std::string rep;
    for(int i=0; i<total_insert; i++) {
        s1 = std::to_string(i);
        s2 = std::to_string(i+ll);
        status = db->Get(softdb::ReadOptions(), s1, &rep);
        if (!status.ok() || rep != s2) {
            cout<<"get error"<<endl;
            break;
        }
    }

    auto p2_time = NowNanos();
    cout<< "Phase2 nanosecond: " << p2_time - p1_time <<endl;

    for(int i=0; i<total_insert ;i++) {
        s1 = std::to_string(i);
        status = db->Delete(softdb::WriteOptions(), s1);
        if (!status.ok()) {
            cout<<"delete error"<<endl;
            break;
        }
    }

    auto p3_time = NowNanos();
    cout<< "Phase3 nanosecond: " << p3_time - p2_time <<endl;

    auto end_time = NowNanos();
    cout<< "Total nanosecond: "<<end_time - start_time <<endl;

    delete db;
    //std::cout<<sizeof(void*)<<" "<<sizeof(int)<<" "<<sizeof(size_t); //8 4 8
    return 1;
}