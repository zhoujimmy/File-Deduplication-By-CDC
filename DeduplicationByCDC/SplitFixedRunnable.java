//package DeduplicationByCDC;
//
//import java.io.File;
//import java.io.RandomAccessFile;
//import java.math.BigInteger;
//import java.security.MessageDigest;
//import java.util.ArrayList;
//
//import redis.clients.jedis.Jedis;
//import testForLength.SecretCodeUtil;
//
//public class SplitFixedRunnable implements Runnable {
//	public String filename;
//	public File file;
//	public long start;
//	public long size;
//	public long end;
//	public int[] record = null;
//	public String secretFileName = null;
//	public int partNo;
//
//	public SplitFixedRunnable(File file, long start, long size, int[] record, String secretFileName, int partNo) {
//		this.file = file;
//		this.start = start;
//		this.size = size;
//		this.end = start + size;
//		this.record = record;
//		this.secretFileName = secretFileName;
//		this.partNo = partNo;
//	}
//
//	@Override
//	public void run() {
//		try {
//			// 用来设置数据库的key，start分块的位置，如：file-123abc-0
//			MessageDigest md5 = MessageDigest.getInstance("MD5");
//			String fileKey = secretFileName + "-" + start;
//			// secretCodes存储数据指纹的顺序，用于恢复文件
//			ArrayList<String> secretCodes = new ArrayList<String>();
//			RandomAccessFile rFile = new RandomAccessFile(file, "r");
//			while (start + FileUtil.FIXED_SIZE <= end) {
//				if (FileUtil.write(start, FileUtil.FIXED_SIZE, rFile, secretCodes, md5))
//					record[partNo * 2]++;
//				record[partNo * 2 + 1]++;
//				start += FileUtil.FIXED_SIZE;
//				rFile.seek(start);
//			}
//			if (start < end) {
//				if (FileUtil.write(start, end - start, rFile, secretCodes, md5))
//					record[partNo * 2]++;
//				record[partNo * 2 + 1]++;
//			}
//
//			// 将key，value写入数据库
//			StringBuilder builder = new StringBuilder();
//			for (String value : secretCodes)
//				builder.append(value + ",");
//			Jedis redis = RedisUtil.getRedis();
//			redis.set(fileKey, builder.toString());
//			RedisUtil.setRedis(redis);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//
//}
