package DeduplicationByCDC;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.junit.Test;

import redis.clients.jedis.Jedis;
/**
 * 
 * @author Zhou Jimmy
 * @email zhoujimmy@yeah.net
 * @version 2019年3月21日下午3:47:30
 * 测试用例
 */
public class test {
	public static Logger logger = Logger.getLogger(test.class);

	// 预先清空数据库和文件夹
	public static void clean() {
		Jedis redis = RedisUtil.getRedisFromPool();
		redis.flushDB();
		redis.close();
		System.out.println("数据库清空成功");
		if (FileUtil.cleanDir(FileUtil.partDir) && FileUtil.cleanDir(FileUtil.downDir))
			System.out.println("文件夹清空成功");
	}

	// 测试最小分块距离 1-20kb的性能
	@Test
	public void testForMinsize() throws Exception {
		// 生成文件列表
		ArrayList<String> list = new ArrayList<String>();
		list.add("ChromeSetup_x64_v72.0.3626.96.exe");
		list.add("ChromeSetup_x64_v72.0.3626.96_1.exe");
		// list.add("redis1.flv");
		// list.add("redis2.flv");
		// 记录写入数据块数，时间，去重率
		StringBuilder num = new StringBuilder();
		StringBuilder percent = new StringBuilder();
		StringBuilder time = new StringBuilder();
		num.append("[");
		percent.append("[");
		time.append("[");
		for (int i = 1; i <= 20; i++) {
			// 预先清空文件夹和数据库
			clean();
			long t1 = System.currentTimeMillis();
			FileUtil.MIN_SIZE = (long) (i * 1024);
			FileUtil.MAX_SIZE = (long) ((i + 4) * 1024);
			// 拆分文件
			String[] split = null;
			for (int j = 0; j < list.size(); j++) {
				if (j == 1) {
					split = FileUtil.splitByCDC(list.get(j)).split(",");
					num.append(split[0] + ",");
					time.append(split[1] + ",");
					percent.append(split[2] + ",");
				} else
					FileUtil.splitByCDC(list.get(j));
			}
			// 拼接文件
			// for (String fileName : list)
			// FileUtil.mergeByCDC(fileName);

			long t2 = System.currentTimeMillis();
			logger.debug("MINSIZE=" + i + "kb,MAXSIZE=" + (i + 4) + "KB,耗时=" + (t2 - t1) + "ms");
		}
		num.append("]");
		percent.append("]");
		time.append("]");
		logger.debug("去重数量：" + num.toString());
		logger.debug("去重时间：" + time.toString());
		logger.debug("去重比例：" + percent.toString());
	}

	// 最小分块为5kb时，测试最大分块距离 6-25kb的性能
	@Test
	public void testForMaxsize() throws Exception {
		// 生成文件列表
		ArrayList<String> list = new ArrayList<String>();
		// list.add("ChromeSetup_x64_v72.0.3626.96.exe");
		// list.add("ChromeSetup_x64_v72.0.3626.96_1.exe");
		list.add("redis1.flv");
		list.add("redis2.flv");
		// 记录写入数据块数，时间，去重率
		StringBuilder num = new StringBuilder();
		StringBuilder percent = new StringBuilder();
		StringBuilder time = new StringBuilder();
		num.append("[");
		percent.append("[");
		time.append("[");
		// 固定最小值
		FileUtil.MIN_SIZE = (long) (5 * 1024);
		for (int i = 1; i <= 20; i++) {
			// 预先清空文件夹和数据库
			clean();
			long t1 = System.currentTimeMillis();
			// 最大值不断变化
			FileUtil.MAX_SIZE = FileUtil.MIN_SIZE + i * 1024;
			// 拆分文件
			String[] split = null;
			for (int j = 0; j < list.size(); j++) {
				if (j == 1) {
					split = FileUtil.splitByCDC(list.get(j)).split(",");
					num.append(split[0] + ",");
					time.append(split[1] + ",");
					percent.append(split[2] + ",");
				} else
					FileUtil.splitByCDC(list.get(j));
			}
			// 拼接文件
			// for (String fileName : list)
			// FileUtil.mergeByCDC(fileName);

			long t2 = System.currentTimeMillis();
			logger.debug("MINSIZE=" + 5 + "kb,MAXSIZE=" + (i + 5) + "KB,耗时=" + (t2 - t1) + "ms");
		}
		num.append("]");
		percent.append("]");
		time.append("]");
		logger.debug("去重数量：" + num.toString());
		logger.debug("去重时间：" + time.toString());
		logger.debug("去重比例：" + percent.toString());
	}

	// 测试divisor 从 2^3-1,2^12-1时性能
	@Test
	public void testForDivisor1() throws Exception {
		// 生成文件列表
		ArrayList<String> list = new ArrayList<String>();
		list.add("ChromeSetup_x64_v72.0.3626.96.exe");
		list.add("ChromeSetup_x64_v72.0.3626.96_1.exe");
		// list.add("redis1.flv");
		// list.add("redis2.flv");
		// 间距的5%-100%的数值设置为divisor
		// int divisor = (int) ((FileUtil.MAX_SIZE - FileUtil.MIN_SIZE) / 64);
		int divisor = (2 << 1) - 1;
		// 记录写入数据块数，时间，去重率
		StringBuilder num = new StringBuilder();
		StringBuilder percent = new StringBuilder();
		StringBuilder time = new StringBuilder();
		num.append("[");
		percent.append("[");
		time.append("[");
		for (int i = 1; divisor <= 2 << 11; i++) {
			// 预先清空文件夹和数据库
			clean();
			long t1 = System.currentTimeMillis();
			divisor = (divisor << 1) + 1;
			FileUtil.divisor = divisor;
			// 拆分文件
			String[] split = null;
			for (int j = 0; j < list.size(); j++) {
				if (j == 1) {
					split = FileUtil.splitByCDC(list.get(j)).split(",");
					num.append(split[0] + ",");
					time.append(split[1] + ",");
					percent.append(split[2] + ",");
				} else
					FileUtil.splitByCDC(list.get(j));
			}
			// 拼接文件
			// for (String fileName : list)
			// FileUtil.mergeByCDC(fileName);

			long t2 = System.currentTimeMillis();
			logger.debug("divisor=" + FileUtil.divisor + ",耗时=" + (t2 - t1) + "ms");

		}
		num.append("]");
		percent.append("]");
		time.append("]");
		logger.debug("去重数量：" + num.toString());
		logger.debug("去重时间：" + time.toString());
		logger.debug("去重比例：" + percent.toString());
	}

	// 测试divisor 选取 2^3-1,2^12-1的最大质数时的性能
	@Test
	public void testForDivisor2() throws Exception {
		// 生成文件列表
		ArrayList<String> list = new ArrayList<String>();
		// list.add("ChromeSetup_x64_v72.0.3626.96.exe");
		// list.add("ChromeSetup_x64_v72.0.3626.96_1.exe");
		list.add("redis1.flv");
		list.add("redis2.flv");
		// divisor
		int divisor = (2 << 1) - 1;
		// 记录写入数据块数，时间，去重率
		StringBuilder num = new StringBuilder();
		StringBuilder percent = new StringBuilder();
		StringBuilder time = new StringBuilder();
		num.append("[");
		percent.append("[");
		time.append("[");
		for (int i = 1; divisor <= 2 << 10; i++) {
			// 预先清空文件夹和数据库
			clean();
			long t1 = System.currentTimeMillis();
			divisor = divisor * 2 + 1;
			// 得到最大质数
			FileUtil.divisor = PrimeUtil.getMaxPrime(divisor);
			// 拆分文件
			String[] split = null;
			for (int j = 0; j < list.size(); j++) {
				if (j == 1) {
					split = FileUtil.splitByCDC(list.get(j)).split(",");
					num.append(split[0] + ",");
					time.append(split[1] + ",");
					percent.append(split[2] + ",");
				} else
					FileUtil.splitByCDC(list.get(j));
			}
			// 拼接文件
			// for (String fileName : list)
			// FileUtil.mergeByCDC(fileName);

			long t2 = System.currentTimeMillis();
			logger.debug("divisor=" + FileUtil.divisor + ",耗时=" + (t2 - t1) + "ms");

		}
		num.append("]");
		percent.append("]");
		time.append("]");
		logger.debug("去重数量：" + num.toString());
		logger.debug("去重时间：" + time.toString());
		logger.debug("去重比例：" + percent.toString());
	}

	// 滑动窗口大小设置
	@Test
	public void testForWinsize() throws Exception {
		// 生成文件列表
		ArrayList<String> list = new ArrayList<String>();
		list.add("ChromeSetup_x64_v72.0.3626.96.exe");
		list.add("ChromeSetup_x64_v72.0.3626.96_1.exe");
		// list.add("redis1.flv");
		// list.add("redis2.flv");
		// 记录写入数据块数，时间，去重率
		StringBuilder num = new StringBuilder();
		StringBuilder percent = new StringBuilder();
		StringBuilder time = new StringBuilder();
		num.append("[");
		percent.append("[");
		time.append("[");
		int winsize = 4;
		for (int i = 1; i <= 16; i++) {
			// 预先清空文件夹和数据库
			clean();
			long t1 = System.currentTimeMillis();
			FileUtil.WIN_SIZE = winsize * i;
			// 拆分文件
			String[] split = null;
			for (int j = 0; j < list.size(); j++) {
				if (j == 1) {
					split = FileUtil.splitByCDC(list.get(j)).split(",");
					num.append(split[0] + ",");
					time.append(split[1] + ",");
					percent.append(split[2] + ",");
				} else
					FileUtil.splitByCDC(list.get(j));
			}
			// 拼接文件
			// for (String fileName : list)
			// FileUtil.mergeByCDC(fileName);

			long t2 = System.currentTimeMillis();
			logger.debug("winsize=" + FileUtil.WIN_SIZE + ",耗时=" + (t2 - t1) + "ms");

		}
		num.append("]");
		percent.append("]");
		time.append("]");
		logger.debug("去重数量：" + num.toString());
		logger.debug("去重时间：" + time.toString());
		logger.debug("去重比例：" + percent.toString());
	}

	// 固定长度去重
	// @Test
	// public void testForFixed() throws Exception {
	// // 生成文件列表
	// ArrayList<String> list = new ArrayList<String>();
	// // list.add("ChromeSetup_x64_v72.0.3626.96.exe");
	// // list.add("ChromeSetup_x64_v72.0.3626.96_1.exe");
	// // list.add("redis1.flv");
	// // list.add("redis2.flv");
	// list.add("redis3.flv");
	// list.add("redis4.flv");
	// // 记录写入数据块数，时间，去重率
	// StringBuilder num = new StringBuilder();
	// StringBuilder percent = new StringBuilder();
	// StringBuilder time = new StringBuilder();
	// num.append("[");
	// percent.append("[");
	// time.append("[");
	// // 预先清空文件夹和数据库
	// clean();
	// long t1 = System.currentTimeMillis();
	// // 拆分文件
	// String[] split = null;
	// for (int j = 0; j < list.size(); j++) {
	// FileUtil.FIXED_SIZE = 5 * 1024;
	// if (j == 1) {
	// split = FileUtil.splitByFixed((list.get(j))).split(",");
	// num.append(split[0] + ",");
	// time.append(split[1] + ",");
	// percent.append(split[2] + ",");
	// } else
	// FileUtil.splitByCDC(list.get(j));
	// }
	// // 拼接文件
	// for (String fileName : list)
	// FileUtil.mergeByCDC(fileName);
	//
	// long t2 = System.currentTimeMillis();
	// logger.debug("winsize=" + FileUtil.WIN_SIZE + ",耗时=" + (t2 - t1) + "ms");
	//
	// num.append("]");
	// percent.append("]");
	// time.append("]");
	// logger.debug("去重数量：" + num.toString());
	// logger.debug("去重时间：" + time.toString());
	// logger.debug("去重比例：" + percent.toString());
	// }
}
