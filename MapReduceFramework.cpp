#include <pthread.h>
#include "MapReduceFramework.h"
#include <iostream>
#include <atomic>

using namespace std;

pthread_t* threads;

int numOfThreads;

vector<vector<IntermediatePair>> all_inter_vec;

std::atomic<int> atm(0);

typedef struct Context
{
    int tid;
    const InputVec& inputVec;
    // constructor
    Context(int tid, const InputVec& inputVec):
    //
    tid(tid), inputVec(inputVec){}
    //
}Context;

void mapThreadFunc(const InputVec& inputVec, int tid)
{
    InputPair inputPair;
    IntermediatePair intermediatePair;
    int old_val;
    while(atm < numOfThreads - 1)
    {
        old_val = atm.fetch_add(1);
        inputPair = inputVec.at(old_val);
        void* context = &inputPair;
        emit2(intermediatePair.first, intermediatePair.second, context);
        all_inter_vec.at(tid).push_back(intermediatePair);
    }
}

void shuffleThreadFunc()
{

}

void* threadFunc(void* context)
{
    auto thread_context = (Context*) context;
    if(threads[numOfThreads - 1] != thread_context->tid)
    {
        mapThreadFunc(thread_context->inputVec, thread_context->tid);
    }
    else
    {
        //run shuffle
    }
    return nullptr;
}


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
    numOfThreads = multiThreadLevel;
    threads = (pthread_t*)malloc(sizeof(pthread_t) * multiThreadLevel);
    for(int i = 0; i < multiThreadLevel; i++)
    {
        auto* context = new Context(threads[i], inputVec);
        pthread_create(&threads[i], nullptr, threadFunc, context);
    }
}