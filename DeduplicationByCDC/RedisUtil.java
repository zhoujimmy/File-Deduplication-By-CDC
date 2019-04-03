package DeduplicationByCDC;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * 
 * @author Zhou Jimmy
 * @email zhoujimmy@yeah.net
 * @version 2019年3月21日下午3:46:47 数据库连接工具类
 */
public class RedisUtil {
	// public static LinkedList<Jedis> redisList = null;
	public static JedisPool pool = null;

	public static String host = null;
	public static Integer port = null;
	public static String password = null;
	public static Integer db = null;
	static {
		Properties p = new Properties();
		FileInputStream fis;
		try {
			fis = new FileInputStream("src\\db.properties");
			p.load(fis);
			fis.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		host = p.getProperty("host");
		port = Integer.parseInt(p.getProperty("port"));
		password = p.getProperty("password");
		db = Integer.parseInt(p.getProperty("db"));
		pool = new JedisPool(host, port);
	}

	// 产生num个redis连接
	// public static void genRedis(int num) {
	// redisList = new LinkedList<Jedis>();
	// for (int i = 0; i < num; i++) {
	// Jedis r = new Jedis("localhost");
	// r.auth("1997997");
	// redisList.add(r);
	// }
	// }

	// 从数据库连接池取出redis连接
	public static Jedis getRedisFromPool(){
		Jedis redis = pool.getResource();
		redis.auth(password);
		redis.select(db);
		return redis;
	}

	// 取出数据库连接
	// public static Jedis getRedis() {
	// Jedis redis = null;
	// synchronized (redisList) {
	// while (redisList.size() == 0 || (redis = redisList.removeFirst()) == null)
	// try {
	// redisList.wait();
	// } catch (InterruptedException e) {
	// e.printStackTrace();
	// }
	// }
	// return redis;
	// }

	// 归还数据库连接
	// public static void setRedis(Jedis redis) {
	// synchronized (redisList) {
	// redisList.add(redis);
	// redisList.notify();
	// }
	// }

	// 关闭数据库连接
	// public static void closeRedis() {
	// for (Jedis r : redisList)
	// r.close();
	// redisList = null;
	// }
}
