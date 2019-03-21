package DeduplicationByCDC;
/**
 * 
 * @author Zhou Jimmy
 * @email zhoujimmy@yeah.net
 * @version 2019年3月21日下午3:46:26
 * 质数工具类
 */
public class PrimeUtil {
	// 得到小于max的最大质数
	public static int getMaxPrime(int max) {
		for (int i = max; i > 1; i--) {
			int sqrt = (int) Math.sqrt(max);
			boolean result = true;
			for (int j = sqrt; j > 1; j--) {
				if (i % j == 0) {
					result = false;
					break;
				}
			}
			if (result)
				return i;
		}
		return 0;
	}
}
