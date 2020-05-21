#include <pthread.h>
#include "MapReduceFramework.h"
#include <iostream>
#include <atomic>

pthread_t* threads;

int numOfThreads;

std::atomic<int> atm(0);


void mapThreadFunc()
{
    int old_val;
    while(atm < numOfThreads - 1)
    {
        old_val = atm.fetch_add(1);
    }
}


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
    numOfThreads = multiThreadLevel;
    threads = (pthread_t*)malloc(sizeof(pthread_t) * multiThreadLevel);
    for(int i = 0; i < multiThreadLevel - 1; i++)
    {

    }
}