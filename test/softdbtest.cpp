//
// Created by lingo on 19-1-17.
//

#include "softdbtest.h"
#include <cassert>
#include <iostream>
#include <cstdint>
#include <chrono>

#include "softdb/db.h"

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

    size_t total_insert = 500000;

    softdb::Slice s1;
    softdb::Slice s2;


    softdb::DestroyDB("/tmp/softdb", options);

    auto start_time = NowNanos();
    softdb::Status status = softdb::DB::Open(options, "/tmp/softdb", &db);
    //softdb::Status status = softdb::DB::Open(options, "/dev/shm/softdb", &db);
    assert(status.ok());

    auto open_time = NowNanos();
    cout<< "OpenDB nanosecond: " << open_time - start_time <<endl;


    int ll = 1;
    //for (ll = 0; ll < 1000; ll++) {
        for(int i = 1; i <= total_insert; i++) {
            s1 = std::to_string(i);
            status = db->Put(softdb::WriteOptions(), s1, std::to_string(i+ll));
            if (!status.ok()) {
                cout<<"put error"<<endl;
                break;
            }
        }
    //}


    /*
    for(int i = 1; i <= total_insert; i++) {
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
/*
    it->SeekToFirst();
    softdb::Slice first(it->key().ToString());
    it->SeekToLast();
    softdb::Slice last(it->key().ToString());

    for (it->Seek(last.ToString()); it->Valid() && it->key().ToString() >= first.ToString(); it->Prev()) {
        //cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
        check++;
    }
    assert(check == total_insert);

    check = 0;
    for (it->Seek(first.ToString()); it->Valid() && it->key().ToString() <= last.ToString(); it->Next()) {
        //cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
        check++;
    }
    assert(check == total_insert);

    check = 0;
    for (it->SeekToLast(); it->Valid(); it->Prev()) {
        //cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
        check++;
    }
    assert(check == total_insert);

    check = 0;*/
    //it->Seek("0");
    //std::cout<<it->key().ToString()<<std::endl;//1
    for (it->Seek("0"); it->Valid(); it->Next()) {
        //cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
        check++;
    }
    assert(check == total_insert);
    assert(it->status().ok());  // Check for any errors found during the scan


    auto p2_time = NowNanos();
    cout<< "Phase2 nanosecond: " << p2_time - p1_time <<endl;

    check = 0;
    for (it->SeekToLast(); it->Valid(); it->Prev()) {
        //cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
        check++;
    }
    assert(check == total_insert);
    delete it;

    auto p3_time = NowNanos();
    cout<< "Phase3 nanosecond: " << p3_time - p2_time <<endl;

    std::string rep;
    for(int i = 1; i <= total_insert; i++) {
        s1 = std::to_string(i);
        s2 = std::to_string(i+ll);
        status = db->Get(softdb::ReadOptions(), s1, &rep);
        if (!status.ok() || rep != s2) {
            cout<<"get error"<<endl;
            break;
        }
    }

    auto p4_time = NowNanos();
    cout<< "Phase4 nanosecond: " << p4_time - p3_time <<endl;

    for(int i = 1; i <= total_insert ;i++) {
        s1 = std::to_string(i);
        status = db->Delete(softdb::WriteOptions(), s1);
        if (!status.ok()) {
            cout<<"delete error"<<endl;
            break;
        }
    }

    auto p5_time = NowNanos();
    cout<< "Phase5 nanosecond: " << p5_time - p4_time <<endl;

    auto end_time = NowNanos();
    cout<< "Total nanosecond: "<< end_time - start_time <<endl;

    delete db;
    //std::cout<<sizeof(void*)<<" "<<sizeof(int)<<" "<<sizeof(size_t); //8 4 8
    return 0;
}