#include <pthread.h>
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include <iostream>
#include <atomic>

using namespace std;

pthread_t* threads;

pthread_mutex_t* map_mutexes;

int numOfThreads;

vector<IntermediatePair>* all_inter_vec;

IntermediateMap interMap;

atomic<int> atm(0);
atomic<int> numOfProcessed(0);

typedef struct Context
{
    int tid;
    const InputVec& inputVec;
    pthread_mutex_t* mutex;
    const MapReduceClient& client;
    // constructor
    Context(int tid, const InputVec& inputVec, pthread_mutex_t* mutex, const MapReduceClient& client):
    //
    tid(tid), inputVec(inputVec), mutex(mutex), client(client){}
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

void emit2 (K2* key, V2* value, void* context)
{
    auto thread_context = (Context*) context;
    IntermediatePair intermediatePair =  IntermediatePair(key, value);
    mutexLock(thread_context->mutex);
    all_inter_vec[thread_context->tid].push_back(intermediatePair);
    mutexUnLock(thread_context->mutex);
}

void mapThreadFunc(Context* thread_context)
{
    InputPair inputPair;
    IntermediatePair intermediatePair;
    int old_val;
    while(atm < thread_context->inputVec.size())
    {
        old_val = atm.fetch_add(1);
        if(old_val < thread_context->inputVec.size())
        {
            inputPair = thread_context->inputVec.at(old_val);
            thread_context->client.map(inputPair.first, inputPair.second, thread_context);
        }
    }
}

void shuffleThreadFunc(Context* thread_context)
{
    // while statement for map phase finished (barrier and atomic variable)
    for(int i = 0; i < numOfThreads - 1; i++)
    {
        while(!all_inter_vec[i].empty())
        {
            mutexLock(&map_mutexes[i]);
            // if key in interMap, add to the specified key vector
            // else add a new key and a new value for it
            auto search = interMap.find(all_inter_vec[i].at(0).first);
            if(search != interMap.end())
            {
                // insert the value to the key vector
            }
            else
            {
                // insert a new key map
            }
            mutexUnLock(&map_mutexes[i]);
        }
    }
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
        shuffleThreadFunc(thread_context);
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
    all_inter_vec = (vector<IntermediatePair>*)malloc(sizeof(vector<IntermediatePair>*) * (multiThreadLevel - 1));
    for(int i = 0; i < multiThreadLevel; i++)
    {
        pthread_mutex_init(&map_mutexes[i], nullptr);
        auto* context = new Context(threads[i], inputVec, &map_mutexes[i], client);
        pthread_create(&threads[i], nullptr, threadFunc, context);
    }
}