#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <atomic>
#include <cstdint>
#include <list>
#include <cstdio>
#include <exception>
#include <pthread.h>
#include "../lock/locker.h"

template <typename T>
class threadpool
{
public:
    struct task
    {
        T *request;
        uint64_t generation;
    };

    /*thread_number是线程池中线程的数量，max_requests是请求队列中最多允许的、等待处理的请求的数量*/
    threadpool(int thread_number = 8, int max_request = 10000);
    ~threadpool();
    // Enqueue work for a connection generation; stale generations are ignored in run().
    bool append_p(T *request, uint64_t generation);
    // Stop accepting work, wake workers, join them. Safe to call once.
    void stop();

private:
    static void *worker(void *arg);
    void run();

private:
    int m_thread_number;
    int m_max_requests;
    pthread_t *m_threads;
    std::list<task> m_workqueue;
    locker m_queuelocker;
    sem m_queuestat;
    std::atomic<bool> m_stop;
    bool m_joined;
};

template <typename T>
threadpool<T>::threadpool(int thread_number, int max_requests)
    : m_thread_number(thread_number), m_max_requests(max_requests), m_threads(NULL),
      m_stop(false), m_joined(false)
{
    if (thread_number <= 0 || max_requests <= 0)
        throw std::exception();
    m_threads = new pthread_t[m_thread_number];
    if (!m_threads)
        throw std::exception();
    for (int i = 0; i < thread_number; ++i)
    {
        if (pthread_create(m_threads + i, NULL, worker, this) != 0)
        {
            delete[] m_threads;
            throw std::exception();
        }
        // Joinable: stop() joins on SIGTERM/SIGINT so in-flight work is not leaked.
    }
}

template <typename T>
threadpool<T>::~threadpool()
{
    stop();
    delete[] m_threads;
}

template <typename T>
bool threadpool<T>::append_p(T *request, uint64_t generation)
{
    m_queuelocker.lock();
    if (m_stop.load(std::memory_order_acquire) ||
        m_workqueue.size() >= static_cast<size_t>(m_max_requests))
    {
        m_queuelocker.unlock();
        return false;
    }
    task t;
    t.request = request;
    t.generation = generation;
    m_workqueue.push_back(t);
    m_queuelocker.unlock();
    m_queuestat.post();
    return true;
}

template <typename T>
void threadpool<T>::stop()
{
    if (m_joined)
    {
        return;
    }

    m_queuelocker.lock();
    m_stop.store(true, std::memory_order_release);
    m_workqueue.clear();
    m_queuelocker.unlock();

    for (int i = 0; i < m_thread_number; ++i)
    {
        m_queuestat.post();
    }
    for (int i = 0; i < m_thread_number; ++i)
    {
        pthread_join(m_threads[i], NULL);
    }
    m_joined = true;
}

template <typename T>
void *threadpool<T>::worker(void *arg)
{
    threadpool *pool = (threadpool *)arg;
    pool->run();
    return pool;
}

template <typename T>
void threadpool<T>::run()
{
    while (true)
    {
        m_queuestat.wait();
        m_queuelocker.lock();
        if (m_stop.load(std::memory_order_acquire) && m_workqueue.empty())
        {
            m_queuelocker.unlock();
            return;
        }
        if (m_workqueue.empty())
        {
            m_queuelocker.unlock();
            continue;
        }
        task t = m_workqueue.front();
        m_workqueue.pop_front();
        m_queuelocker.unlock();
        if (!t.request)
            continue;
        // Stale after timeout/close/fd reuse: do not touch the slot.
        if (!t.request->generation_matches(t.generation))
            continue;
        // Proactor: MySQL is acquired on-demand inside handlers that need DB.
        t.request->process(t.generation);
    }
}
#endif
