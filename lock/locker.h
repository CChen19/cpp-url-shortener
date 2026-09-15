#ifndef LOCKER_H
#define LOCKER_H

#include <exception>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>
#include <errno.h>

class sem
{
public:
    sem()
    {
        if (sem_init(&m_sem, 0, 0) != 0)
        {
            throw std::exception();
        }
    }
    sem(int num)
    {
        if (sem_init(&m_sem, 0, num) != 0)
        {
            throw std::exception();
        }
    }
    ~sem()
    {
        sem_destroy(&m_sem);
    }
    bool wait()
    {
        return sem_wait(&m_sem) == 0;
    }
    // timeout_ms < 0 waits forever; 0 is trywait; >0 is timed wait.
    bool wait(int timeout_ms)
    {
        if (timeout_ms < 0)
        {
            return wait();
        }
        if (timeout_ms == 0)
        {
            return sem_trywait(&m_sem) == 0;
        }

        struct timespec ts;
        if (clock_gettime(CLOCK_REALTIME, &ts) != 0)
        {
            return false;
        }
        ts.tv_sec += timeout_ms / 1000;
        long add_ns = static_cast<long>(timeout_ms % 1000) * 1000000L;
        ts.tv_nsec += add_ns;
        if (ts.tv_nsec >= 1000000000L)
        {
            ts.tv_sec += 1;
            ts.tv_nsec -= 1000000000L;
        }
        while (true)
        {
            int ret = sem_timedwait(&m_sem, &ts);
            if (ret == 0)
            {
                return true;
            }
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }
    }
    bool post()
    {
        return sem_post(&m_sem) == 0;
    }
    bool reset(int num)
    {
        sem_destroy(&m_sem);
        return sem_init(&m_sem, 0, num) == 0;
    }

private:
    sem(const sem &);
    sem &operator=(const sem &);
    sem_t m_sem;
};
class locker
{
public:
    locker()
    {
        if (pthread_mutex_init(&m_mutex, NULL) != 0)
        {
            throw std::exception();
        }
    }
    ~locker()
    {
        pthread_mutex_destroy(&m_mutex);
    }
    bool lock()
    {
        return pthread_mutex_lock(&m_mutex) == 0;
    }
    bool unlock()
    {
        return pthread_mutex_unlock(&m_mutex) == 0;
    }
    pthread_mutex_t *get()
    {
        return &m_mutex;
    }

private:
    locker(const locker &);
    locker &operator=(const locker &);
    pthread_mutex_t m_mutex;
};
class cond
{
public:
    cond()
    {
        if (pthread_cond_init(&m_cond, NULL) != 0)
        {
            throw std::exception();
        }
    }
    ~cond()
    {
        pthread_cond_destroy(&m_cond);
    }
    bool wait(pthread_mutex_t *m_mutex)
    {
        int ret = 0;
        ret = pthread_cond_wait(&m_cond, m_mutex);
        return ret == 0;
    }
    bool timewait(pthread_mutex_t *m_mutex, struct timespec t)
    {
        int ret = 0;
        ret = pthread_cond_timedwait(&m_cond, m_mutex, &t);
        return ret == 0;
    }
    bool signal()
    {
        return pthread_cond_signal(&m_cond) == 0;
    }
    bool broadcast()
    {
        return pthread_cond_broadcast(&m_cond) == 0;
    }

private:
    cond(const cond &);
    cond &operator=(const cond &);
    pthread_cond_t m_cond;
};
#endif
