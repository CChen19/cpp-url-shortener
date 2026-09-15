#include <mysql/mysql.h>
#include <stdio.h>
#include <stdint.h>
#include <string>
#include <string.h>
#include <stdlib.h>
#include <list>
#include <pthread.h>
#include <iostream>
#include "sql_connection_pool.h"

using namespace std;

namespace {
const int kDefaultAcquireTimeoutMs = 50;
}

connection_pool::connection_pool()
{
	m_CurConn = 0;
	m_FreeConn = 0;
	m_MaxConn = 0;
	m_acquire_timeout_ms = kDefaultAcquireTimeoutMs;
	m_test_mode = false;
}

connection_pool *connection_pool::GetInstance()
{
	static connection_pool connPool;
	return &connPool;
}

void connection_pool::set_acquire_timeout_ms(int timeout_ms)
{
	lock.lock();
	m_acquire_timeout_ms = timeout_ms;
	lock.unlock();
}

int connection_pool::acquire_timeout_ms() const
{
	return m_acquire_timeout_ms;
}

//构造初始化
void connection_pool::init(string url, string User, string PassWord, string DBName, int Port, int MaxConn, int close_log)
{
	lock.lock();

	m_url = url;
	m_Port = Port;
	m_User = User;
	m_PassWord = PassWord;
	m_DatabaseName = DBName;
	m_close_log = close_log;
	m_test_mode = false;
	m_CurConn = 0;
	m_FreeConn = 0;
	connList.clear();

	for (int i = 0; i < MaxConn; i++)
	{
		MYSQL *con = NULL;
		con = mysql_init(con);

		if (con == NULL)
		{
			LOG_ERROR("MySQL init error, skipping remaining connections");
			break;
		}
		con = mysql_real_connect(con, url.c_str(), User.c_str(), PassWord.c_str(), DBName.c_str(), Port, NULL, 0);

		if (con == NULL)
		{
			LOG_ERROR("MySQL connect failed (pool will run with %d/%d connections)", m_FreeConn, MaxConn);
			break;
		}
		connList.push_back(con);
		++m_FreeConn;
	}

	m_MaxConn = m_FreeConn;
	if (!reserve.reset(m_FreeConn))
	{
		LOG_ERROR("Failed to reset connection pool semaphore");
	}

	lock.unlock();
}

void connection_pool::init_for_test(int conn_count, int acquire_timeout_ms)
{
	lock.lock();

	// Drop any prior real connections first.
	if (!m_test_mode)
	{
		for (list<MYSQL *>::iterator it = connList.begin(); it != connList.end(); ++it)
		{
			if (*it)
			{
				mysql_close(*it);
			}
		}
	}
	connList.clear();
	m_CurConn = 0;
	m_FreeConn = 0;
	m_MaxConn = 0;
	m_test_mode = true;
	m_acquire_timeout_ms = acquire_timeout_ms;
	m_close_log = 1;

	for (int i = 0; i < conn_count; ++i)
	{
		// Non-null placeholders; never passed to mysql_* while m_test_mode.
		MYSQL *placeholder = reinterpret_cast<MYSQL *>(static_cast<uintptr_t>(i + 1));
		connList.push_back(placeholder);
		++m_FreeConn;
	}
	m_MaxConn = m_FreeConn;
	reserve.reset(m_FreeConn);

	lock.unlock();
}


MYSQL *connection_pool::GetConnection()
{
	lock.lock();
	const int timeout_ms = m_acquire_timeout_ms;
	lock.unlock();
	return GetConnection(timeout_ms);
}

//当有请求时，从数据库连接池中返回一个可用连接，更新使用和空闲连接数
MYSQL *connection_pool::GetConnection(int timeout_ms)
{
	MYSQL *con = NULL;

	lock.lock();
	const int max_conn = m_MaxConn;
	const int wait_ms = timeout_ms;
	lock.unlock();

	// Empty / destroyed pool: fail fast (do not confuse with "all checked out").
	if (max_conn <= 0)
	{
		return NULL;
	}

	if (!reserve.wait(wait_ms))
	{
		return NULL;
	}

	lock.lock();

	if (connList.empty())
	{
		lock.unlock();
		// Lost the race with DestroyPool; restore semaphore count.
		reserve.post();
		return NULL;
	}

	con = connList.front();
	connList.pop_front();

	--m_FreeConn;
	++m_CurConn;

	lock.unlock();
	return con;
}

//释放当前使用的连接
bool connection_pool::ReleaseConnection(MYSQL *con)
{
	if (NULL == con)
		return false;

	lock.lock();

	connList.push_back(con);
	++m_FreeConn;
	--m_CurConn;

	lock.unlock();

	reserve.post();
	return true;
}

//销毁数据库连接池
void connection_pool::DestroyPool()
{

	lock.lock();
	if (connList.size() > 0)
	{
		list<MYSQL *>::iterator it;
		for (it = connList.begin(); it != connList.end(); ++it)
		{
			MYSQL *con = *it;
			if (!m_test_mode && con)
			{
				mysql_close(con);
			}
		}
		m_CurConn = 0;
		m_FreeConn = 0;
		m_MaxConn = 0;
		connList.clear();
	}
	else
	{
		m_CurConn = 0;
		m_FreeConn = 0;
		m_MaxConn = 0;
	}
	m_test_mode = false;
	reserve.reset(0);

	lock.unlock();
}

//当前空闲的连接数
int connection_pool::GetFreeConn()
{
	lock.lock();
	int free_conn = m_FreeConn;
	lock.unlock();
	return free_conn;
}

connection_pool::~connection_pool()
{
	DestroyPool();
}

connectionRAII::connectionRAII(MYSQL **SQL, connection_pool *connPool){
	poolRAII = connPool;
	if (connPool) {
		*SQL = connPool->GetConnection();
	} else {
		*SQL = NULL;
	}
	conRAII = *SQL;
}

connectionRAII::connectionRAII(MYSQL **SQL, connection_pool *connPool, int timeout_ms){
	poolRAII = connPool;
	if (connPool) {
		*SQL = connPool->GetConnection(timeout_ms);
	} else {
		*SQL = NULL;
	}
	conRAII = *SQL;
}

connectionRAII::~connectionRAII(){
	if (poolRAII && conRAII)
		poolRAII->ReleaseConnection(conRAII);
}
