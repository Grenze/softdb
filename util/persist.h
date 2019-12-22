//
// Created by lingo on 19-12-22.
//

#ifndef SOFTDB_PERSIST_H
#define SOFTDB_PERSIST_H



#include <cstdlib>
#include <iostream>

#define CPU_FREQ_MHZ 900.026 // cat /proc/cpuinfo

#define CACHE_LINE_SIZE 64

static uint64_t WRITE_LATENCY_IN_NS = 500;

static inline void cpu_pause() {
    __asm__ volatile ("pause" ::: "memory");
}

static inline unsigned long read_tsc() {
    unsigned long var;
    unsigned int hi, lo;
    asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
    var = ((unsigned long long int) hi << 32) | lo;
    return var;
}

inline void mfence() {
    asm volatile("mfence":::"memory");
}

inline void clflush(const char* data, size_t len) {
    if (data == nullptr) return;
    volatile char *ptr = (char *)((unsigned long)data &~(CACHE_LINE_SIZE-1));
    //mfence();
    for (; ptr< const_cast<volatile char*>(data+len); ptr+=CACHE_LINE_SIZE) {
        unsigned long etsc = read_tsc() + (unsigned long)(WRITE_LATENCY_IN_NS*CPU_FREQ_MHZ/1000);
        asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
        while (read_tsc() < etsc)
            cpu_pause();
    }
    //mfence();
}


#endif //SOFTDB_PERSIST_H
