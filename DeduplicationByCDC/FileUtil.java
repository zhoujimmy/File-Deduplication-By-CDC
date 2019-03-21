package DeduplicationByCDC;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import testForLength.SecretCodeUtil;
/**
 * 
 * @author Zhou Jimmy
 * @email zhoujimmy@yeah.net
 * @version 2019年3月21日下午3:44:50
 * 文件去重工具类
 */
public class FileUtil {
	// 记录日志
	public static Logger logger = Logger.getLogger(FileUtil.class);
	// 上传、分片、生成文件的目录
	public static String upDir = "C:\\Users\\79806\\Desktop\\上传文件\\";
	public static String partDir = "C:\\Users\\79806\\Desktop\\分片文件\\";
	public static String downDir = "C:\\Users\\79806\\Desktop\\下载文件\\";
	// 基于内容去重，divisor和remainder
	public static Integer divisor = 127;
	public static Integer remainder = 1;
	// 滑动窗口大小
	public static int WIN_SIZE = 64;
	// 数据块大小上下限
	public static Long MAX_SIZE = (long) (10 * 1024);
	public static Long MIN_SIZE = (long) (5 * 1024);
	// 固定长度分块
	// public static int FIXED_SIZE = 5 * 1024;

	// 文件拆分操作
	public static String splitByCDC(String fileName) throws Exception {
		// 判断文件是否存在
		File file = new File(upDir + fileName);
		if (!file.exists())
			throw new RuntimeException("文件不存在");
		// 得到md5实例，用于加密
		MessageDigest md5 = MessageDigest.getInstance("MD5");
		// 通过文件地址加密后的文件名 如file-1231abcdef
		String secretFileName = "file-" + SecretCodeUtil.md5(FileUtil.upDir + file.getName(), md5);

		// [奇数]记录分块数和[偶数]实际写入的文件数
		int[] record = null;

		// 根据文件大小开启多个线程拆分,默认开启3个线程
		long fileLength = file.length();
		int threadNum = 3;
		// 512MB
		if (fileLength > MAX_SIZE * 1024 * 64)
			threadNum = 7;
		// 128MB
		else if (fileLength > MAX_SIZE * 1024 * 16)
			threadNum = 6;
		// 32MB
		else if (fileLength > MAX_SIZE * 1024 * 4)
			threadNum = 5;
		// 8MB
		else if (fileLength > MAX_SIZE * 1024)
			threadNum = 4;
		// 对文件进行切分,partSize表示每部分的大小，r表示取模后的余数
		long partSize = fileLength / threadNum;
		long r = (int) (fileLength % partSize);
		// t1 记录时间
		long t1 = System.currentTimeMillis();
		ThreadPoolExecutor threadPool = null;
		// 判断每部分的大小，进行切分
		// 如果每个线程分到的容量大于128*MAX_SIZE，则每个线程每次处理大小为128*MAX_SIZE
		if (partSize > 128 * MAX_SIZE) {
			partSize = 128 * MAX_SIZE;
			// 得到实际分块数
			int partNum = (int) (fileLength / partSize);
			record = new int[partNum * 2 + 2];
			// 开启线程池
			threadPool = new ThreadPoolExecutor(threadNum, threadNum * 3, 1, TimeUnit.SECONDS,
					new ArrayBlockingQueue<Runnable>(partNum + 1));
			// 创建数据库连接池
			RedisUtil.genRedis(threadNum);
			// 多线程执行拆分任务
			for (int i = 0; i < partNum; i++) {
				threadPool.execute(new SplitRunnable(file, partSize * i, partSize, record, secretFileName, i));
			}
			if (r != 0)
				threadPool.execute(new SplitRunnable(file, partSize * partNum, r, record, secretFileName, partNum));
		}
		// partSize大于MAX_SIZE * 12时，则每个线程每次处理大小为partSize
		else if (MAX_SIZE * 12 < partSize) {
			record = new int[threadNum * 2];
			// 开启线程池
			threadPool = new ThreadPoolExecutor(threadNum, threadNum, 1, TimeUnit.SECONDS,
					new ArrayBlockingQueue<Runnable>(threadNum));
			// 创建数据库连接池
			RedisUtil.genRedis(threadNum);
			r = (int) (fileLength % threadNum);
			for (int i = 0; i < threadNum - 1; i++) {
				threadPool.execute(new SplitRunnable(file, partSize * i, partSize, record, secretFileName, i));
			}
			if (r == 0)
				threadPool.execute(new SplitRunnable(file, partSize * (threadNum - 1), partSize, record, secretFileName,
						threadNum - 1));
			else
				threadPool.execute(new SplitRunnable(file, partSize * (threadNum - 1), partSize + r, record,
						secretFileName, threadNum - 1));
		}
		// 文件太小的话使用单线程
		else {
			threadNum = 1;
			record = new int[threadNum * 2];
			// 开启线程池
			threadPool = new ThreadPoolExecutor(threadNum, threadNum, 1, TimeUnit.SECONDS,
					new ArrayBlockingQueue<Runnable>(threadNum));
			// 创建数据库连接池
			RedisUtil.genRedis(threadNum);
			// 开始任务
			threadPool.execute(new SplitRunnable(file, 0, fileLength, record, secretFileName, 0));

		}
		// main线程等待任务线程完成
		threadPool.shutdown();
		try {
			boolean loop = true;
			// 等待所有任务完成
			do {
				// 阻塞，直到线程池里所有任务结束
				loop = !threadPool.awaitTermination(2, TimeUnit.SECONDS);
			} while (loop);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// 统计实际写入数和分块数
		int writeNum = 0;
		int allNum = 0;
		for (int i = 0; i < record.length; i++) {
			if (i % 2 == 0)
				writeNum += record[i];
			else
				allNum += record[i];
		}
		// 将文件的长度一并写入数据库，便于恢复
		Jedis redis = RedisUtil.getRedis();
		redis.set(secretFileName + "-length", fileLength + "");
		long t2 = System.currentTimeMillis();
		// 去重率
		DecimalFormat df = new DecimalFormat("0.00");
		String dedupPercent = df.format((double) ((allNum - writeNum)) / allNum);
		// 输出信息到日志和控制台
		logger.debug(fileName + "大小为：" + file.length() + "，去重率=" + dedupPercent + ",开启线程数为：" + threadNum + ",分为"
				+ allNum + "块，实际写入" + writeNum + "块数据块" + ",拆分用时：" + (t2 - t1) + "ms");
		// 返回数据用于分析
		return allNum + "," + (t2 - t1) + "," + dedupPercent;
	}

	// 文件合并操作
	public static void mergeByCDC(String fileName) throws Exception {
		// 判断文件是否存在
		File file = new File(upDir + fileName);
		if (!file.exists())
			throw new RuntimeException("文件不存在");
		// 通过文件地址加密后得到文件在数据库的key
		MessageDigest md5 = MessageDigest.getInstance("MD5");
		String secretFileName = "file-" + SecretCodeUtil.md5(FileUtil.upDir + file.getName(), md5);
		// 记录时间
		long t1 = System.currentTimeMillis();
		// 得到数据库的数据集合
		RedisUtil.genRedis(1);
		Jedis redis = RedisUtil.getRedis();
		Set<String> keys = redis.keys(secretFileName + "*");
		// 开启的多线程数量为
		int keySize = keys.size();
		int threadNum = keySize > 7 ? 7 : keySize;
		// 得到文件长度
		long fileLength = new Long(redis.get(secretFileName + "-length"));
		// 创建目标文件，并设置文件长度
		File mergeFile = new File(downDir + fileName);
		if (mergeFile.exists())
			mergeFile.delete();
		mergeFile.createNewFile();
		RandomAccessFile rFile = new RandomAccessFile(mergeFile, "rw");
		rFile.setLength(fileLength);
		rFile.close();
		// 创建线程池
		ThreadPoolExecutor threadPool = new ThreadPoolExecutor(threadNum, threadNum * 3, 1, TimeUnit.SECONDS,
				new ArrayBlockingQueue<Runnable>(keySize));
		// 开启多线程对每个文件块进行恢复
		for (String key : keys) {
			long start = new Long(key.split("-")[2]);
			String[] parts = redis.get(key).split(",");
			File[] files = new File[parts.length];
			for (int i = 0; i < parts.length; i++)
				files[i] = new File(partDir + parts[i]);
			threadPool.execute(new MergeRunnable(mergeFile, start, files));
		}
		RedisUtil.closeRedis();
		// main线程等待任务线程完成
		threadPool.shutdown();
		try {
			boolean loop = true;
			// 等待所有任务完成
			do {
				// 阻塞，直到线程池里所有任务结束
				loop = !threadPool.awaitTermination(2, TimeUnit.SECONDS);
			} while (loop);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long t2 = System.currentTimeMillis();
		// 输出信息到日志和控制台
		logger.debug(fileName + "开启线程数为：" + threadNum + ",合并用时" + (t2 - t1) + "ms");
	}

	// 文件删除操作
	public static void deleteByCDC(String fileName) throws Exception {
		// 通过文件地址加密得到文件的key
		MessageDigest md5 = MessageDigest.getInstance("MD5");
		String secretFileName = "file-" + SecretCodeUtil.md5(FileUtil.upDir + fileName, md5);
		// 记录时间
		long t1 = System.currentTimeMillis();
		// 得到数据库的数据集合
		RedisUtil.genRedis(1);
		Jedis redis = RedisUtil.getRedis();
		Set<String> keys = redis.keys(secretFileName + "*");
		// 开启的多线程数量为
		int keySize = keys.size();
		int threadNum = keySize > 7 ? 7 : keySize;
		// 创建线程池
		ThreadPoolExecutor threadPool = new ThreadPoolExecutor(threadNum, threadNum * 3, 1, TimeUnit.SECONDS,
				new ArrayBlockingQueue<Runnable>(keySize));
		// 创建数据库连接池
		RedisUtil.genRedis(threadNum);
		// 遍历文件块进行删除
		int[] record = new int[keySize * 2];
		int index = 0;
		for (String key : keys)
			threadPool.execute(new DeleteRunnable(key, record, index++));
		// main线程等待任务线程完成
		threadPool.shutdown();
		try {
			boolean loop = true;
			// 等待所有任务完成
			do {
				// 阻塞，直到线程池里所有任务结束
				loop = !threadPool.awaitTermination(2, TimeUnit.SECONDS);
			} while (loop);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long t2 = System.currentTimeMillis();
		int deleteNum = 0;
		int allNum = 0;
		for (int i = 0; i < record.length; i++)
			if (i % 2 == 0)
				deleteNum += record[i];
			else
				allNum += record[i];
		logger.debug(fileName + "总数据块为：" + allNum + ",删除数据块为：" + deleteNum + ",删除用时" + (t2 - t1) + "ms");
	}

	// 文件拆分操作,固定长度分块
	// public static String splitByFixed(String fileName) throws Exception {
	// // 判断文件是否存在
	// File file = new File(upDir + fileName);
	// if (!file.exists())
	// throw new RuntimeException("文件不存在");
	// //
	// MessageDigest md5 = MessageDigest.getInstance("MD5");
	// String secretFileName = "file-" + SecretCodeUtil.md5(FileUtil.upDir +
	// file.getName(), md5);
	//
	// // 记录实际写入的文件数
	// int[] record = null;
	//
	// // 根据文件大小开启多个线程拆分,默认开启两个线程
	// long fileLength = file.length();
	// int threadNum = 3;
	// // 512MB
	// if (fileLength > MAX_SIZE * 1024 * 64)
	// threadNum = 7;
	// // 128MB
	// else if (fileLength > MAX_SIZE * 1024 * 16)
	// threadNum = 6;
	// // 32MB
	// else if (fileLength > MAX_SIZE * 1024 * 4)
	// threadNum = 5;
	// // 8MB
	// else if (fileLength > MAX_SIZE * 1024)
	// threadNum = 4;
	// long t1 = System.currentTimeMillis();
	// // 判断每部分的大小，进行切分
	// ThreadPoolExecutor threadPool = null;
	// // 记录实际写入数
	// int writeNum = 0;
	// // 分块总数
	// int allNum = (int) Math.ceil((double) fileLength / FIXED_SIZE);
	// // 判断每个线程处理的数据块数量，如果大于128，则按照128*FIXED_SIZE先分一次块
	// long maxPartSize = FIXED_SIZE * 256;
	// if (fileLength > threadNum * maxPartSize) {
	// // 开启线程池
	// threadPool = new ThreadPoolExecutor(threadNum, Integer.MAX_VALUE, 1,
	// TimeUnit.SECONDS,
	// new ArrayBlockingQueue<Runnable>(threadNum));
	// // 创建数据库连接池
	// RedisUtil.genRedis(threadNum);
	// // 执行多线程写入
	// int parts = (int) (fileLength / maxPartSize);
	// record = new int[2 * parts + 2];
	// for (int i = 0; i < parts; i++)
	// threadPool
	// .execute(new SplitFixedRunnable(file, i * maxPartSize, maxPartSize, record,
	// secretFileName, i));
	//
	// long r = fileLength % maxPartSize;
	// if (r != 0)
	// threadPool.execute(new SplitFixedRunnable(file, parts * maxPartSize, r,
	// record, secretFileName, parts));
	// }
	// // 否则就一个个处理数据块
	// else {
	// // 开启线程池
	// threadPool = new ThreadPoolExecutor(threadNum, threadNum, 1,
	// TimeUnit.SECONDS,
	// new ArrayBlockingQueue<Runnable>(threadNum * 2));
	// // 创建数据库连接池
	// RedisUtil.genRedis(threadNum);
	// // 执行多线程写入
	// int parts = (int) Math.ceil((double) fileLength / FIXED_SIZE);
	// record = new int[parts * 2];
	// for (int i = 0; i < parts - 1; i++) {
	// threadPool.execute(new SplitFixedRunnable(file, i * FIXED_SIZE, FIXED_SIZE,
	// record, secretFileName, i));
	// }
	// int r = (int) (fileLength / FIXED_SIZE);
	// if (r != 0)
	// threadPool.execute(
	// new SplitFixedRunnable(file, (parts - 1) * FIXED_SIZE, r, record,
	// secretFileName, parts - 1));
	// else
	// threadPool.execute(new SplitFixedRunnable(file, (parts - 1) * FIXED_SIZE,
	// FIXED_SIZE, record,
	// secretFileName, parts - 1));
	// }
	// // main线程等待任务线程完成
	// threadPool.shutdown();
	// try {
	// boolean loop = true;
	// // 等待所有任务完成
	// do {
	// // 阻塞，直到线程池里所有任务结束
	// loop = !threadPool.awaitTermination(2, TimeUnit.SECONDS);
	// } while (loop);
	// } catch (InterruptedException e) {
	// e.printStackTrace();
	// }
	// // 计算实际写入的数据块
	// allNum = 0;
	// for (int i = 0; i < record.length; i++)
	// if (i % 2 == 0)
	// writeNum += record[i];
	// else
	// allNum += record[i];
	// // 将文件的长度一并写入数据库，便于恢复
	// Jedis redis = RedisUtil.getRedis();
	// redis.set(secretFileName + "-length", fileLength + "");
	// long t2 = System.currentTimeMillis();
	// // 输出信息
	// DecimalFormat df = new DecimalFormat("0.00");
	// // 去重率
	// String dedupPercent = df.format((double) ((allNum - writeNum)) / allNum);
	// logger.debug(fileName + "大小为：" + file.length() + "，去重率=" + dedupPercent +
	// ",开启线程数为：" + threadNum + ",分为"
	// + allNum + "块，实际写入" + writeNum + "块数据块" + ",拆分用时：" + (t2 - t1) + "ms");
	// // if (updated)
	// // System.out.println("文件修改后，删除冗余" + deleteNum + "块");
	// return allNum + "," + (t2 - t1) + "," + dedupPercent;
	// }

	// 文件的写入
	public static boolean write(long start, long writeLen, RandomAccessFile rFile, ArrayList<String> secretCodes,
			MessageDigest md5) throws Exception {
		// result用于记录是否文件
		boolean result = false;
		// buf用于写入数据
		byte[] buf = new byte[(int) writeLen];
		rFile.seek(start);
		rFile.read(buf);
		// 对读取到的md5加密，生成secretCode，用在文件名和数据库的key上
		String secretCode = SecretCodeUtil.md5(buf, md5);
		// 记录数据以及起始位置
		secretCodes.add(secretCode);
		Jedis redis = RedisUtil.getRedis();
		String value = redis.get("part-" + secretCode);
		// 若存在，则不用写，引用次数+1即可
		if (value != null)
			redis.set("part-" + secretCode, new Integer(value) + 1 + "");
		// 若不存在，则必须写，引用次数设为1
		else {
			redis.set("part-" + secretCode, "1");
			// 开始输出文件
			FileOutputStream os = new FileOutputStream(partDir + secretCode);
			os.write(buf);
			os.flush();
			os.close();
			result = true;
		}
		RedisUtil.setRedis(redis);
		return result;
	}

	// 清空文件夹
	public static boolean cleanDir(String dirName) {
		boolean result = false;
		File dir = new File(dirName);
		if (dir.exists()) {
			for (File f : dir.listFiles())
				f.delete();
			result = true;
		} else
			dir.mkdirs();
		return result;
	}
}
