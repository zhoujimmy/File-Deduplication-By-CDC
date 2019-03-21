package DeduplicationByCDC;

import java.io.File;

import redis.clients.jedis.Jedis;

/**
 * 
 * @author Zhou Jimmy
 * @email zhoujimmy@yeah.net
 * @version 2019年3月21日下午3:45:39
 * 删除文件Runnable
 */
public class DeleteRunnable implements Runnable {
	// redis数据库的键值
	public String key;
	// 记录实际删除数目
	public int[] record;
	// 代表在deleteNum数组中的位置，2*index为实际删除数，2*index+1为应删除数
	public int index;

	public DeleteRunnable(String key, int[] record, int index) {
		this.key = key;
		this.record = record;
		this.index = index;
	}

	public void run() {
		// 得到数据库连接
		Jedis redis = RedisUtil.getRedis();
		// 对value进行拆分，得到数据库集合
		String[] parts = redis.get(key).split(",");
		// 删除数据库数据
		redis.del(key);
		// 记录应删除数
		record[2 * index + 1] = parts.length;
		// 数据库集合遍历
		for (String part : parts) {
			// 得到文件
			File file = new File(FileUtil.partDir + part);
			// 得到文件引用次数
			String value = redis.get("part-" + part);
			// 如果value为null或者引用次数为1，则删除文件
			if (value == null || new Integer(value) <= 1) {
				redis.del("part-" + part);
				if (file.exists()) {
					file.delete();
					record[2 * index]++;
				}
			}
			// 否则不删除文件，引用次数减一
			else
				redis.set("part-" + part, new Integer(value) - 1 + "");
		}
		// 将数据库连接归还
		RedisUtil.setRedis(redis);
	}

}
