# File Deduplication By CDC

来自毕业设计-《云存储数据去重研究》

使用基于内容的数据分块算法对数据文件进行切分。

(其实也实现了固定长度分块算法，但是效果十分不理想，已全部被注释。)

通过本地的三个目录：上传、分片、生成。模拟对文件的去重效果。使用时请自动调整。

public static String upDir = "C:\\Users\\79806\\Desktop\\上传文件\\";

public static String partDir = "C:\\Users\\79806\\Desktop\\分片文件\\";

public static String downDir = "C:\\Users\\79806\\Desktop\\下载文件\\";

本系统只有上传、还原和删除三项功能。并不完善，欢迎大家参考，提出建议。

语言Java，数据库Redis，外加log4j和jedis两个Jar包即可运行项目。

2019.3.21 上传功能代码

2019.3.30 将SrcretCodeUtil上传
