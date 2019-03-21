package DeduplicationByCDC;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;

/**
 * 
 * @author Zhou Jimmy
 * @email zhoujimmy@yeah.net
 * @version 2019年3月21日下午3:46:05
 * 还原文件Runnable
 */
public class MergeRunnable implements Runnable {
	// 还原后的文件
	public File mergeFile;
	// 开始写入的位置
	public long start;
	// 要写入的文件集合
	public File[] files;

	public MergeRunnable(File mergeFile, long start, File[] files) {
		this.mergeFile = mergeFile;
		this.start = start;
		this.files = files;
	}

	public void run() {
		try {
			// 得到还原后文件的访问流
			RandomAccessFile rFile = new RandomAccessFile(mergeFile, "rw");
			// 对要写入的文件集合遍历，进行写入
			for (File file : files) {
				byte[] buf = new byte[(int) file.length()];
				FileInputStream input = new FileInputStream(file);
				input.read(buf);
				input.close();
				rFile.seek(start);
				rFile.write(buf);
				start += file.length();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
