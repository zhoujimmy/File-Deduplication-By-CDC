package DeduplicationByCDC;

import java.io.File;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;

import redis.clients.jedis.Jedis;
import testForLength.SecretCodeUtil;

/**
 * 
 * @author Zhou Jimmy
 * @email zhoujimmy@yeah.net
 * @version 2019年3月21日下午3:47:15
 * 拆分文件Runnable
 */
public class SplitRunnable implements Runnable {
	// 要拆分的文件
	public File file;
	// 拆分的起始位置
	public long start;
	// 拆分长度
	public long size;
	// 拆分的终结位置end = start+size
	public long end;
	// 记录实际拆分数和实际写入数
	public int[] record = null;
	// 加密后的文件名 如file-1231abcdef
	public String secretFileName = null;
	// 代表在deleteNum数组中的位置，2*index为实际删除数，2*index+1为应删除数
	public int index;

	public SplitRunnable(File file, long start, long size, int[] record, String secretFileName, int index) {
		this.file = file;
		this.start = start;
		this.size = size;
		this.end = start + size;
		this.record = record;
		this.secretFileName = secretFileName;
		this.index = index;
	}

	public void run() {
		try {
			// 得到md5加密实例
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			// 得到数据库键值 如file-1231abcdef-0
			String fileKey = secretFileName + "-" + start;
			// secretCodes有序地存储数据指纹，用于恢复文件
			ArrayList<String> secretCodes = new ArrayList<String>();
			// 得到要拆分文件的访问流
			RandomAccessFile rFile = new RandomAccessFile(file, "r");
			// 滑动窗口
			byte[] win = new byte[FileUtil.WIN_SIZE];
			// 从最小长度开始循环读取
			long now = FileUtil.MIN_SIZE + start;
			rFile.seek(now);
			rFile.read(win);
			BigInteger temp = null;
			while (now < end) {
				// 如果匹配到或者达到了上限要求，就开始写入
				if (now - start >= FileUtil.MAX_SIZE || (temp = SecretCodeUtil.md5Int(win, md5))
						.and(new BigInteger(FileUtil.divisor + "")).equals(new BigInteger(FileUtil.remainder + ""))) {
					if (FileUtil.write(start, now - start, rFile, secretCodes, md5))
						record[2 * index]++;
					record[2 * index + 1]++;
					// 写入完成后重设start和now
					start = now;
					now = start + FileUtil.MIN_SIZE;
				}
				// 否则，窗口继续向前移动一个字节
				else
					now++;
				rFile.seek(now);
				rFile.read(win);
			}
			// 当达到了文件的最大长度，进行写入
			if (now >= end) {
				if (FileUtil.write(start, end - start, rFile, secretCodes, md5))
					record[2 * index]++;
				record[2 * index + 1]++;
			}
			// 将key，value写入数据库
			StringBuilder builder = new StringBuilder();
			for (String value : secretCodes)
				builder.append(value + ",");
			Jedis redis = RedisUtil.getRedis();
			redis.set(fileKey, builder.toString());
			RedisUtil.setRedis(redis);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
