#ifndef _CONNECTION_POOL_
#define _CONNECTION_POOL_

#include <stdio.h>
#include <list>
#include <mysql/mysql.h>
#include <error.h>
#include <string.h>
#include <iostream>
#include <string>
#include "../lock/locker.h"
#include "../log/log.h"

using namespace std;

class connection_pool
{
public:
	MYSQL *GetConnection();				 //获取数据库连接 (uses default acquire timeout)
	MYSQL *GetConnection(int timeout_ms); // timeout_ms < 0 waits forever
	bool ReleaseConnection(MYSQL *conn); //释放连接
	int GetFreeConn();					 //获取连接
	void DestroyPool();					 //销毁所有连接
	void set_acquire_timeout_ms(int timeout_ms);
	int acquire_timeout_ms() const;

	//单例模式
	static connection_pool *GetInstance();

	void init(string url, string User, string PassWord, string DataBaseName, int Port, int MaxConn, int close_log);

	// Test helper: install N non-owned placeholder connections (no live MySQL).
	// DestroyPool / destructor skip mysql_close while test mode is active.
	void init_for_test(int conn_count, int acquire_timeout_ms);

private:
	connection_pool();
	~connection_pool();

	int m_MaxConn;  //最大连接数
	int m_CurConn;  //当前已使用的连接数
	int m_FreeConn; //当前空闲的连接数
	int m_acquire_timeout_ms;
	bool m_test_mode;
	locker lock;
	list<MYSQL *> connList; //连接池
	sem reserve;

public:
	string m_url;			 //主机地址
	string m_Port;		 //数据库端口号
	string m_User;		 //登陆数据库用户名
	string m_PassWord;	 //登陆数据库密码
	string m_DatabaseName; //使用数据库名
	int m_close_log;	//日志开关
};

class connectionRAII{

public:
	connectionRAII(MYSQL **con, connection_pool *connPool);
	connectionRAII(MYSQL **con, connection_pool *connPool, int timeout_ms);
	~connectionRAII();
	
private:
	MYSQL *conRAII;
	connection_pool *poolRAII;
};

#endif
