package DeduplicationByCDC;

import java.util.LinkedList;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * 
 * @author Zhou Jimmy
 * @email zhoujimmy@yeah.net
 * @version 2019年3月21日下午3:46:47 数据库连接工具类
 */
public class RedisUtil {
//	public static LinkedList<Jedis> redisList = null;
	public static JedisPool pool = null;

	// 产生num个redis连接
//	public static void genRedis(int num) {
//		redisList = new LinkedList<Jedis>();
//		for (int i = 0; i < num; i++) {
//			Jedis r = new Jedis("localhost");
//			r.auth("1997997");
//			redisList.add(r);
//		}
//	}

	// 从数据库连接池取出redis连接
	public static Jedis getRedisFromPool() {
		if (pool == null) {
			pool = new JedisPool("127.0.0.1",6379);
		}
		Jedis redis = pool.getResource();
		redis.auth("1997997");
		return redis;

	}

	// 取出数据库连接
//	public static Jedis getRedis() {
//		Jedis redis = null;
//		synchronized (redisList) {
//			while (redisList.size() == 0 || (redis = redisList.removeFirst()) == null)
//				try {
//					redisList.wait();
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//		}
//		return redis;
//	}

	// 归还数据库连接
//	public static void setRedis(Jedis redis) {
//		synchronized (redisList) {
//			redisList.add(redis);
//			redisList.notify();
//		}
//	}

	// 关闭数据库连接
//	public static void closeRedis() {
//		for (Jedis r : redisList)
//			r.close();
//		redisList = null;
//	}
}
