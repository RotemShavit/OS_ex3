#include <pthread.h>
#include "MapReduceFramework.h"
#include <iostream>
#include <atomic>

using namespace std;

pthread_t* threads;

pthread_mutex_t* map_mutexes;

int numOfThreads;

vector<vector<IntermediatePair>> all_inter_vec;

atomic<int> atm(0);
atomic<int> numOfProcessed(0);

typedef struct Context
{
    int tid;
    const InputVec& inputVec;
    pthread_mutex_t* mutex;
    // constructor
    Context(int tid, const InputVec& inputVec, pthread_mutex_t* mutex):
    //
    tid(tid), inputVec(inputVec), mutex(mutex){}
    //
}Context;

void mutexLock(pthread_mutex_t* mutex)
{
    if(pthread_mutex_lock(mutex))
    {
        //print error
    }
}

void mutexUnLock(pthread_mutex_t* mutex)
{
    if(pthread_mutex_unlock(mutex))
    {
        //print error
    }
}

void mapThreadFunc(Context* thread_context)
{
    InputPair inputPair;
    IntermediatePair intermediatePair;
    int old_val;
    while(atm < thread_context->inputVec.size())
    {
        old_val = atm.fetch_add(1);
        inputPair = thread_context->inputVec.at(old_val);
        void* context = &inputPair;
        emit2(intermediatePair.first, intermediatePair.second, context);
        mutexLock(thread_context->mutex);
        all_inter_vec.at(thread_context->tid).push_back(intermediatePair);
        mutexUnLock(thread_context->mutex);
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
        mapThreadFunc(thread_context);
    }
    else
    {
        //run shuffle
        shuffleThreadFunc();
    }
    return nullptr;
}


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
    numOfThreads = multiThreadLevel;
    threads = (pthread_t*)malloc(sizeof(pthread_t) * multiThreadLevel);
    map_mutexes = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t) * multiThreadLevel);
    for(int i = 0; i < multiThreadLevel; i++)
    {
        pthread_mutex_init(&map_mutexes[i], nullptr);
        auto* context = new Context(threads[i], inputVec, &map_mutexes[i]);
        pthread_create(&threads[i], nullptr, threadFunc, context);
    }
}