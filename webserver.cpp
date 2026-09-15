#include "webserver.h"
#include "./analytics/click_event_producer.h"
#include "./observability/structured_logger.h"
#include "./shorturl/short_url_cache.h"

WebServer::WebServer()
    : m_port(0), m_log_write(0), m_close_log(0), m_actormodel(0),
      m_epollfd(-1), users(NULL), m_connPool(NULL), m_sql_num(0),
      m_mysql_port(0), m_mysql_acquire_timeout_ms(50), m_pool(NULL),
      m_thread_num(0), m_listenfd(-1), m_OPT_LINGER(0), m_TRIGMode(0),
      m_LISTENTrigmode(0), m_CONNTrigmode(0), users_timer(NULL)
{
    users = new http_conn[MAX_FD];
    // Value-init so unused slots have sockfd==0; timer() overwrites live ones.
    // Shutdown only closes slots with sockfd > 0 (0 is never a client fd here).
    users_timer = new client_data[MAX_FD]();
    m_pipefd[0] = -1;
    m_pipefd[1] = -1;
}

WebServer::~WebServer()
{
    if (m_pool)
    {
        m_pool->stop();
        delete m_pool;
        m_pool = NULL;
    }
    if (m_epollfd >= 0)
    {
        close(m_epollfd);
        m_epollfd = -1;
    }
    if (m_listenfd >= 0)
    {
        close(m_listenfd);
        m_listenfd = -1;
    }
    if (m_pipefd[1] >= 0)
    {
        close(m_pipefd[1]);
        m_pipefd[1] = -1;
    }
    if (m_pipefd[0] >= 0)
    {
        close(m_pipefd[0]);
        m_pipefd[0] = -1;
    }
    delete[] users;
    delete[] users_timer;
}

void WebServer::init(const Config &cfg)
{
    m_port = cfg.port;
    m_user = cfg.mysql_user;
    m_passWord = cfg.mysql_password;
    m_databaseName = cfg.mysql_database;
    m_sql_num = cfg.mysql_pool_size;
    m_mysql_host = cfg.mysql_host;
    m_mysql_port = cfg.mysql_port;
    m_mysql_acquire_timeout_ms = cfg.mysql_acquire_timeout_ms;
    m_thread_num = cfg.thread_num;
    m_log_write = cfg.log_async ? 1 : 0;
    m_OPT_LINGER = cfg.opt_linger ? 1 : 0;
    m_TRIGMode = cfg.trig_mode;
    m_close_log = cfg.close_log;
    // Single I/O model: main-thread socket I/O (former "proactor"). Reactor
    // busy-waited on improv and raced the event loop; refuse to keep that fork.
    if (cfg.actor_model == 1)
    {
        fprintf(stderr,
                "actor_model=1 (reactor) is disabled; coercing to 0 (main-thread I/O)\n");
        m_actormodel = 0;
    }
    else
    {
        m_actormodel = cfg.actor_model;
    }
}

void WebServer::trig_mode()
{
    //LT + LT
    if (0 == m_TRIGMode)
    {
        m_LISTENTrigmode = 0;
        m_CONNTrigmode = 0;
    }
    //LT + ET
    else if (1 == m_TRIGMode)
    {
        m_LISTENTrigmode = 0;
        m_CONNTrigmode = 1;
    }
    //ET + LT
    else if (2 == m_TRIGMode)
    {
        m_LISTENTrigmode = 1;
        m_CONNTrigmode = 0;
    }
    //ET + ET
    else if (3 == m_TRIGMode)
    {
        m_LISTENTrigmode = 1;
        m_CONNTrigmode = 1;
    }
}

void WebServer::log_write()
{
    if (0 == m_close_log)
    {
        if (1 == m_log_write)
            Log::get_instance()->init("./ServerLog", m_close_log, 2000, 800000, 800);
        else
            Log::get_instance()->init("./ServerLog", m_close_log, 2000, 800000, 0);
    }
}

void WebServer::sql_pool()
{
    m_connPool = connection_pool::GetInstance();
    m_connPool->init(m_mysql_host, m_user, m_passWord, m_databaseName, m_mysql_port, m_sql_num, m_close_log);
    m_connPool->set_acquire_timeout_ms(m_mysql_acquire_timeout_ms);
}

void WebServer::thread_pool()
{
    m_pool = new threadpool<http_conn>(m_thread_num);
}

void WebServer::eventListen()
{
    m_listenfd = socket(PF_INET, SOCK_STREAM, 0);
    assert(m_listenfd >= 0);

    if (0 == m_OPT_LINGER)
    {
        struct linger tmp = {0, 1};
        setsockopt(m_listenfd, SOL_SOCKET, SO_LINGER, &tmp, sizeof(tmp));
    }
    else if (1 == m_OPT_LINGER)
    {
        struct linger tmp = {1, 1};
        setsockopt(m_listenfd, SOL_SOCKET, SO_LINGER, &tmp, sizeof(tmp));
    }

    int ret = 0;
    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(m_port);

    int flag = 1;
    setsockopt(m_listenfd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));
    ret = bind(m_listenfd, (struct sockaddr *)&address, sizeof(address));
    assert(ret >= 0);
    ret = listen(m_listenfd, 5);
    assert(ret >= 0);

    utils.init(TIMESLOT);

    //epoll创建内核事件表
    epoll_event events[MAX_EVENT_NUMBER];
    m_epollfd = epoll_create(5);
    assert(m_epollfd != -1);

    utils.addfd(m_epollfd, m_listenfd, false, m_LISTENTrigmode);
    http_conn::m_epollfd = m_epollfd;

    ret = socketpair(PF_UNIX, SOCK_STREAM, 0, m_pipefd);
    assert(ret != -1);
    utils.setnonblocking(m_pipefd[1]);
    utils.addfd(m_epollfd, m_pipefd[0], false, 0);

    utils.addsig(SIGPIPE, SIG_IGN);
    utils.addsig(SIGALRM, utils.sig_handler, false);
    utils.addsig(SIGTERM, utils.sig_handler, false);
    utils.addsig(SIGINT, utils.sig_handler, false);

    alarm(TIMESLOT);

    Utils::u_pipefd = m_pipefd;
    Utils::u_epollfd = m_epollfd;
}

void WebServer::timer(int connfd, struct sockaddr_in client_address)
{
    users[connfd].init(connfd, client_address, m_CONNTrigmode, m_close_log);

    users_timer[connfd].address = client_address;
    users_timer[connfd].sockfd = connfd;
    users_timer[connfd].conn = &users[connfd];
    util_timer *timer = new util_timer;
    timer->user_data = &users_timer[connfd];
    timer->cb_func = cb_func;
    time_t cur = time(NULL);
    timer->expire = cur + 3 * TIMESLOT;
    users_timer[connfd].timer = timer;
    utils.m_timer_lst.add_timer(timer);
}

void WebServer::adjust_timer(util_timer *timer)
{
    time_t cur = time(NULL);
    timer->expire = cur + 3 * TIMESLOT;
    utils.m_timer_lst.adjust_timer(timer);

    LOG_INFO("%s", "adjust timer once");
}

void WebServer::deal_timer(util_timer *timer, int sockfd)
{
    // Event-loop closer: invalidate+epoll_del+close via cb_func (bumps generation).
    if (timer)
    {
        timer->cb_func(&users_timer[sockfd]);
        utils.m_timer_lst.del_timer(timer);
    }
    else
    {
        cb_func(&users_timer[sockfd]);
    }
    users_timer[sockfd].timer = NULL;

    LOG_INFO("close fd %d", users_timer[sockfd].sockfd);
}

bool WebServer::dealclientdata()
{
    struct sockaddr_in client_address;
    socklen_t client_addrlength = sizeof(client_address);
    if (0 == m_LISTENTrigmode)
    {
        int connfd = accept(m_listenfd, (struct sockaddr *)&client_address, &client_addrlength);
        if (connfd < 0)
        {
            LOG_ERROR("%s:errno is:%d", "accept error", errno);
            return false;
        }
        if (http_conn::m_user_count >= MAX_FD)
        {
            utils.show_error(connfd, "Internal server busy");
            LOG_ERROR("%s", "Internal server busy");
            return false;
        }
        timer(connfd, client_address);
    }

    else
    {
        while (1)
        {
            int connfd = accept(m_listenfd, (struct sockaddr *)&client_address, &client_addrlength);
            if (connfd < 0)
            {
                LOG_ERROR("%s:errno is:%d", "accept error", errno);
                break;
            }
            if (http_conn::m_user_count >= MAX_FD)
            {
                utils.show_error(connfd, "Internal server busy");
                LOG_ERROR("%s", "Internal server busy");
                break;
            }
            timer(connfd, client_address);
        }
        return false;
    }
    return true;
}

bool WebServer::dealwithsignal(bool &timeout, bool &stop_server)
{
    int ret = 0;
    int sig;
    char signals[1024];
    ret = recv(m_pipefd[0], signals, sizeof(signals), 0);
    if (ret == -1)
    {
        return false;
    }
    else if (ret == 0)
    {
        return false;
    }
    else
    {
        for (int i = 0; i < ret; ++i)
        {
            switch (signals[i])
            {
            case SIGALRM:
            {
                timeout = true;
                break;
            }
            case SIGTERM:
            case SIGINT:
            {
                stop_server = true;
                break;
            }
            }
        }
    }
    return true;
}

void WebServer::dealwithread(int sockfd)
{
    util_timer *timer = users_timer[sockfd].timer;

    // Main-thread I/O: event loop owns recv; workers only process().
    if (users[sockfd].read_once())
    {
        LOG_INFO("deal with the client(%s)", inet_ntoa(users[sockfd].get_address()->sin_addr));

        const uint64_t gen = users[sockfd].current_generation();
        if (!m_pool->append_p(users + sockfd, gen))
        {
            LOG_ERROR("thread pool queue full on read, rejecting fd %d", sockfd);
            users[sockfd].reject_overload();
            deal_timer(timer, sockfd);
            return;
        }

        if (timer)
        {
            adjust_timer(timer);
        }
    }
    else
    {
        deal_timer(timer, sockfd);
    }
}

void WebServer::dealwithwrite(int sockfd)
{
    util_timer *timer = users_timer[sockfd].timer;
    // Main-thread I/O: event loop owns send.
    if (users[sockfd].write())
    {
        LOG_INFO("send data to the client(%s)", inet_ntoa(users[sockfd].get_address()->sin_addr));

        if (timer)
        {
            adjust_timer(timer);
        }
    }
    else
    {
        deal_timer(timer, sockfd);
    }
}

void WebServer::shutdown()
{
    // 1) Stop accept: remove listen fd from epoll and close it.
    if (m_listenfd >= 0)
    {
        epoll_ctl(m_epollfd, EPOLL_CTL_DEL, m_listenfd, 0);
        close(m_listenfd);
        m_listenfd = -1;
    }

    // 2) Invalidate live slots so in-flight workers bail on generation checks
    //    when they finish their current bounded work (MySQL acquire times out).
    for (int fd = 0; fd < MAX_FD; ++fd)
    {
        if (users_timer[fd].sockfd > 0)
        {
            users[fd].invalidate();
        }
    }

    // 3) Stop enqueue, drop queued tasks, wake + join workers (no detach).
    if (m_pool)
    {
        m_pool->stop();
    }

    // 3b) Stop analytics/log background threads after workers have joined so
    //     no request path enqueues during flush. Bounded timeout; must not hang.
    ClickEventProducer::instance().shutdown();
    StructuredLogger::instance().shutdown();
    ShortUrlCache::instance().shutdown();

    // 4) Close remaining connections on the event-loop thread.
    for (int fd = 0; fd < MAX_FD; ++fd)
    {
        if (users_timer[fd].sockfd > 0)
        {
            deal_timer(users_timer[fd].timer, fd);
        }
    }

    // 5) Destroy MySQL pool after workers have joined.
    if (m_connPool)
    {
        m_connPool->DestroyPool();
    }
}

void WebServer::eventLoop()
{
    bool timeout = false;
    bool stop_server = false;

    while (!stop_server)
    {
        int number = epoll_wait(m_epollfd, events, MAX_EVENT_NUMBER, -1);
        if (number < 0 && errno != EINTR)
        {
            LOG_ERROR("%s", "epoll failure");
            break;
        }

        for (int i = 0; i < number; i++)
        {
            int sockfd = events[i].data.fd;

            if (sockfd == m_listenfd)
            {
                bool flag = dealclientdata();
                if (false == flag)
                    continue;
            }
            else if (events[i].events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR))
            {
                util_timer *timer = users_timer[sockfd].timer;
                deal_timer(timer, sockfd);
            }
            else if ((sockfd == m_pipefd[0]) && (events[i].events & EPOLLIN))
            {
                bool flag = dealwithsignal(timeout, stop_server);
                if (false == flag)
                    LOG_ERROR("%s", "dealclientdata failure");
            }
            else if (events[i].events & EPOLLIN)
            {
                dealwithread(sockfd);
            }
            else if (events[i].events & EPOLLOUT)
            {
                dealwithwrite(sockfd);
            }
        }
        if (timeout)
        {
            utils.timer_handler();

            LOG_INFO("%s", "timer tick");

            timeout = false;
        }
    }

    shutdown();
}
