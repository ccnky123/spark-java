package com.ibeifeng.sparkproject.spark.session;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.math.Ordered;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.ISessionAggrStatDAO;
import com.ibeifeng.sparkproject.dao.ISessionDetailDAO;
import com.ibeifeng.sparkproject.dao.ISessionRandomExtractDAO;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.ITop10CategoryDAO;
import com.ibeifeng.sparkproject.dao.ITop10SessionDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.dao.impl.SessionAggrStatDAOImpl;
import com.ibeifeng.sparkproject.domain.SessionAggrStat;
import com.ibeifeng.sparkproject.domain.SessionDetail;
import com.ibeifeng.sparkproject.domain.SessionRandomExtract;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.domain.Top10Category;
import com.ibeifeng.sparkproject.domain.Top10Session;
import com.ibeifeng.sparkproject.test.MockData;
import com.ibeifeng.sparkproject.util.DateUtils;
import com.ibeifeng.sparkproject.util.NumberUtils;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.SparkUtils;
import com.ibeifeng.sparkproject.util.StringUtils;
import com.ibeifeng.sparkproject.util.ValidUtils;

/**
 * 用户访问session分析Spark作业
 * 
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * 
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 * （MockData中一共生产三个模拟表数据。对数据的筛选（清洗）的经典思路是，先对"user_visit_action"表以时间单位过滤一下，然后按照sessionid字段（尽量小）的粒度聚合，聚合后再join上"user_info"表，就可以过滤出1-5的一些信息）
 * 
 * 我们的spark作业如何接受用户创建的任务？
 * 
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param
 * 字段中
 * 
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main函数的args数组中
 * 
 * 这是spark本身提供的特性
 * 
 * @author Administrator
 *
 */

/**
 * 作用：用于抑制编译器产生警告信息。
 * @author geo
 *
 */
@SuppressWarnings("unused")
public class user_visit_session {
	
	public static void main(String[] args) {
		// 构建Spark上下文
		//设置spark作业初始化信息
		SparkConf conf = new SparkConf()
				//设置常量接口constants,提高后期维护方便性
				.setAppName(Constants.SPARK_APP_NAME_SESSION)
//				.set("spark.default.parallelism", "100")
				.set("spark.storage.memoryFraction", "0.5")  
				.set("spark.shuffle.consolidateFiles", "true")
				.set("spark.shuffle.file.buffer", "64")  
				.set("spark.shuffle.memoryFraction", "0.3")    
				.set("spark.reducer.maxSizeInFlight", "24")  
				.set("spark.shuffle.io.maxRetries", "60")  
				.set("spark.shuffle.io.retryWait", "60")   
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.registerKryoClasses(new Class[]{
						CategorySortKey.class,
						IntList.class});   
		SparkUtils.setMaster(conf); 
		
		/**
		 * 比如，获取top10热门品类功能中，二次排序，自定义了一个Key
		 * 那个key是需要在进行shuffle的时候，进行网络传输的，因此也是要求实现序列化的
		 * 启用Kryo机制以后，就会用Kryo去序列化和反序列化CategorySortKey
		 * 所以这里要求，为了获取最佳性能，注册一下我们自定义的类
		 */
		
		JavaSparkContext sc = new JavaSparkContext(conf);
//		sc.checkpointFile("hdfs://");
		//从JavaSparkContext中取出其sc对应的那个sc？
		SQLContext sqlContext = getSQLContext(sc.sc());
		
		// 生成模拟测试数据
		SparkUtils.mockData(sc, sqlContext);  
		
		// 创建需要使用的DAO组件
		//ITaskDAO(接口)根据主键查询任务,getTaskDAO()返回ITaskDAO(接口)的实现类（为什么这里要获取数据库instance要用单例设计模式？因为，当一个数据操作完后才进行下一个操作（相当于是加了锁），否则数据没操作完，有多个实例同时操作数据，会造成数据脏）
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		
		// 首先得查询出来指定的任务，并获取任务的查询参数
//		接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
//		 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
//		 * 参数就封装在main函数的args数组中
//		 * 
//		 * 这是spark本身提供的特性
		//从命令行参数中提取任务id(main函数入口设置String args[])
		long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
		//根据taskid查出来对应的任务信息
		Task task = taskDAO.findById(taskid);
		if(task == null) {
			System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");  
			return;
		}
		
		//把task.getTaskParam()获取到的值序列化成JSONObject（taskParam是封装了命令行参数的对象。到这一步，就相当于拿到了用户通过J2EE平台提交的参数）
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		// 如果要进行session粒度的数据聚合
		/**
		 * actionRDD，就是一个公共RDD
		 * 第一，要用ationRDD，获取到一个公共的sessionid为key的PairRDD
		 * 第二，actionRDD，用在了session聚合环节里面
		 * 
		 * sessionid为key的PairRDD，是确定了，在后面要多次使用的
		 * 1、与通过筛选的sessionid进行join，获取通过筛选的session的明细数据
		 * 2、将这个RDD，直接传入aggregateBySession方法，进行session聚合统计
		 * 
		 * 重构完以后，actionRDD，就只在最开始，使用一次，用来生成以sessionid为key的RDD
		 * 
		 */
		// 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据(spark core/sql/stream其实也是清洗数据用的)
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
		//通过用日期参数对user_visit_action进行初步过滤后，就可以进行聚合操作了
		JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);
		
		/**
		 * 持久化，很简单，就是对RDD调用persist()方法，并传入一个持久化级别
		 * 
		 * 如果是persist(StorageLevel.MEMORY_ONLY())，纯内存，无序列化，那么就可以用cache()方法来替代
		 * StorageLevel.MEMORY_ONLY_SER()，第二选择
		 * StorageLevel.MEMORY_AND_DISK()，第三选择
		 * StorageLevel.MEMORY_AND_DISK_SER()，第四选择
		 * StorageLevel.DISK_ONLY()，第五选择
		 * 
		 * 如果内存充足，要使用双副本高可靠机制
		 * 选择后缀带_2的策略
		 * StorageLevel.MEMORY_ONLY_2()
		 * 
		 */
		sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());
//		sessionid2actionRDD.checkpoint();
		
		// 首先，可以将行为数据，按照session_id进行groupByKey分组
		// 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
		// 与用户信息数据，进行join（"user_info"表和"user_visit_action"表都有user_id字段，通过这个字段来join）
		// 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
		// 到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)> ，拿到这个数据我们又可以进行下一步的继续过滤
		JavaPairRDD<String, String> sessionid2AggrInfoRDD = 
				aggregateBySession(sc, sqlContext, sessionid2actionRDD);
		
		// 接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
		// 相当于我们自己编写的算子，是要访问外面的任务参数对象的
		// 所以，大家记得我们之前说的，匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的
		
		// 重构，同时进行过滤和统计
		//没有完善的spark java api，accumulator（）方法都不知道参数含义。第一个参数是初始值，第二个参数应该是累加内容？
		//Accumulator<String>累加器，<String>的累加器要操作的类型。sc.accumulator("", new SessionAggrStatAccumulator())，这里要累加的逻辑是我们自定义的，一开始初始化为空""，new SessionAggrStatAccumulator()就是我们自己定义累加器要计算的逻辑
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
				"", new SessionAggrStatAccumulator());
		
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
				sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
		filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());
		
		// 生成公共的RDD：通过筛选条件的session的访问明细数据
		
		/**
		 * 重构：sessionid2detailRDD，就是代表了通过筛选的session对应的访问明细数据
		 */
		JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(
				filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
		sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());
		
		/**
		 * 对于Accumulator这种分布式累加计算的变量的使用，有一个重要说明
		 * 
		 * 从Accumulator中，获取数据，插入数据库的时候，一定要，一定要，是在有某一个action操作以后
		 * 再进行。。。
		 * 
		 * 如果没有action的话，那么整个程序根本不会运行。。。
		 * 
		 * 是不是在calculateAndPersisitAggrStat方法之后，运行一个action操作，比如count、take
		 * 不对！！！
		 * 
		 * 必须把能够触发job执行的操作，放在最终写入MySQL方法之前
		 * 
		 * 计算出来的结果，在J2EE中，是怎么显示的，是用两张柱状图显示
		 */
		
		randomExtractSession(sc, task.getTaskid(), 
				filteredSessionid2AggrInfoRDD, sessionid2detailRDD);
		
		/**
		 * 特别说明
		 * 我们知道，要将上一个功能的session聚合统计数据获取到，就必须是在一个action操作触发job之后
		 * 才能从Accumulator中获取数据，否则是获取不到数据的，因为没有job执行，Accumulator的值为空
		 * 所以，我们在这里，将随机抽取的功能的实现代码，放在session聚合统计功能的最终计算和写库之前
		 * 因为随机抽取功能中，有一个countByKey算子，是action操作，会触发job
		 */
		
		// 计算出各个范围的session占比，并写入MySQL
		calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),
				task.getTaskid());
		
		/**
		 * session聚合统计（统计出访问时长和访问步长，各个区间的session数量占总session数量的比例）
		 * 
		 * 如果不进行重构，直接来实现，思路：
		 * 1、actionRDD，映射成<sessionid,Row>的格式
		 * 2、按sessionid聚合，计算出每个session的访问时长和访问步长，生成一个新的RDD
		 * 3、遍历新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中的对应的值
		 * 4、使用自定义Accumulator中的统计值，去计算各个区间的比例
		 * 5、将最后计算出来的结果，写入MySQL对应的表中
		 * 
		 * 普通实现思路的问题：
		 * 1、为什么还要用actionRDD，去映射？其实我们之前在session聚合的时候，映射已经做过了。多此一举
		 * 2、是不是一定要，为了session的聚合这个功能，单独去遍历一遍session？其实没有必要，已经有session数据
		 * 		之前过滤session的时候，其实，就相当于，是在遍历session，那么这里就没有必要再过滤一遍了
		 * 
		 * 重构实现思路：
		 * 1、不要去生成任何新的RDD（处理上亿的数据）
		 * 2、不要去单独遍历一遍session的数据（处理上千万的数据）
		 * 3、可以在进行session聚合的时候，就直接计算出来每个session的访问时长和访问步长
		 * 4、在进行过滤的时候，本来就要遍历所有的聚合session信息，此时，就可以在某个session通过筛选条件后
		 * 		将其访问时长和访问步长，累加到自定义的Accumulator上面去
		 * 5、就是两种截然不同的思考方式，和实现方式，在面对上亿，上千万数据的时候，甚至可以节省时间长达
		 * 		半个小时，或者数个小时
		 * 
		 * 开发Spark大型复杂项目的一些经验准则：
		 * 1、尽量少生成RDD
		 * 2、尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面，实现多个需要做的功能
		 * 3、尽量少对RDD进行shuffle算子操作，比如groupByKey、reduceByKey、sortByKey（map、mapToPair）
		 * 		shuffle操作，会导致大量的磁盘读写，严重降低性能（shuffle要用到带宽和磁盘的I/O）
		 * 		有shuffle的算子，和没有shuffle的算子，甚至性能，会达到几十分钟，甚至数个小时的差别
		 * 		有shfufle的算子，很容易导致数据倾斜，一旦数据倾斜，简直就是性能杀手（完整的解决方案）
		 * 4、无论做什么功能，性能第一(从大数据开发的角度看是这样的，但是不同领域的考虑出发点不同)
		 * 		在传统的J2EE或者.NET后者PHP，软件/系统/网站开发中，我认为是架构和可维护性，可扩展性的重要
		 * 		程度，远远高于了性能，大量的分布式的架构，设计模式，代码的划分，类的划分（高并发网站除外）（通过架构可以解决大部分性能瓶颈问题？）
		 * 
		 * 		在大数据项目中，比如MapReduce、Hive、Spark、Storm，我认为性能的重要程度，远远大于一些代码
		 * 		的规范，和设计模式，代码的划分，类的划分；大数据，大数据，最重要的，就是性能
		 * 		主要就是因为大数据以及大数据项目的特点，决定了，大数据的程序和项目的速度，都比较慢
		 * 		如果不优先考虑性能的话，会导致一个大数据处理程序运行时间长度数个小时，甚至数十个小时
		 * 		此时，对于用户体验，简直就是一场灾难
		 * 		
		 * 		所以，推荐大数据项目，在开发和代码的架构中，优先考虑性能；其次考虑功能代码的划分、解耦合
		 * 
		 * 		我们如果采用第一种实现方案，那么其实就是代码划分（解耦合、可维护）优先，设计优先（这时每个算子单独完成特定功能，不会夹杂很多功能，但是把每个功能都单独写到不同算子里，计算机就要生产新的RDD和执行新的算子，影响性能）
		 * 		如果采用第二种方案，那么其实就是性能优先
		 * 
		 * 		讲了这么多，其实大家不要以为我是在岔开话题，大家不要觉得项目的课程，就是单纯的项目本身以及
		 * 		代码coding最重要，其实项目，我觉得，最重要的，除了技术本身和项目经验以外；非常重要的一点，就是
		 * 		积累了，处理各种问题的经验
		 * 
		 */
		
		// 获取top10热门品类
		List<Tuple2<CategorySortKey, String>> top10CategoryList = 
				getTop10Category(task.getTaskid(), sessionid2detailRDD);
		
		// 获取top10活跃session；JavaSparkContext sc = new JavaSparkContext(conf);为什么要传sc？（原话：将list生成RDD）
		getTop10Session(sc, task.getTaskid(), 
				top10CategoryList, sessionid2detailRDD);
		
		// 关闭Spark上下文
		sc.close(); 
	}

	/**
	 * 获取SQLContext
	 * 如果是在本地测试环境的话，那么就生成SQLContext对象
	 * 如果是在生产环境运行的话，那么就生成HiveContext对象
	 * @param sc SparkContext
	 * @return SQLContext
	 */
	
	//private只能在同一个类中被访问，static可以通过类直接访问
	private static SQLContext getSQLContext(SparkContext sc) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc);
		}
	}
	
	/**
	 * 生成模拟数据（只有本地模式，才会去生成模拟数据）
	 * @param sc 
	 * @param sqlContext
	 */
	private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			MockData.mock(sc, sqlContext);  
		}
	}
	
	/**
	 * 获取指定日期范围内的用户访问行为数据
	 * @param sqlContext SQLContext
	 * @param taskParam 任务参数
	 * @return 行为数据RDD
	 */
	private static JavaRDD<Row> getActionRDDByDateRange(
			SQLContext sqlContext, JSONObject taskParam) {
		//从taskParm里面拿到起始日期和结束日期（不出现hard code，引用Constants接口）
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		//这里写sql的时候做了格式化处理，和只写一行的效果相同
		String sql = 
				"select * "
				+ "from user_visit_action "
				+ "where date>='" + startDate + "' "
				+ "and date<='" + endDate + "'";  
//				+ "and session_id not in('','','')"
		
		//执行sql，这里相当于获取DataFrame
		DataFrame actionDF = sqlContext.sql(sql);
		
		/**
		 * 这里就很有可能发生上面说的问题
		 * 比如说，Spark SQl默认就给第一个stage设置了20个task，但是根据你的数据量以及算法的复杂度
		 * 实际上，你需要1000个task去并行执行
		 * 
		 * 所以说，在这里，就可以对Spark SQL刚刚查询出来的RDD执行repartition重分区操作
		 */
		
//		return actionDF.javaRDD().repartition(1000);
		//通过用户指定的日期参数，到这里为止，已进行了初步的过滤
		return actionDF.javaRDD();
	}
	
	/**
	 * 获取sessionid2到访问行为数据的映射的RDD(将user_visit_action表中的sessionid和它对应的row映射起来)
	 * @param actionRDD 
	 * @return
	 */
	public static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
//		return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
//
//			private static final long serialVersionUID = 1L;
//			
//			@Override
//			public Tuple2<String, Row> call(Row row) throws Exception {
//				return new Tuple2<String, Row>(row.getString(2), row);  
//			}
//			
//		});
		
		return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator)
					throws Exception {
				List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
				
				while(iterator.hasNext()) {
					Row row = iterator.next();
					list.add(new Tuple2<String, Row>(row.getString(2), row));  
				}
				
				return list;
			}
			
		});
	}
	
	/**
	 * 对行为数据按session粒度进行聚合
	 * @param actionRDD 行为数据RDD
	 * @return session粒度聚合数据
	 */
	private static JavaPairRDD<String, String> aggregateBySession(
			JavaSparkContext sc,
			SQLContext sqlContext, 
			JavaPairRDD<String, Row> sessinoid2actionRDD) {
		// 对行为数据按session粒度进行分组:一个sessionid对应一组row
		JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = 
				sessinoid2actionRDD.groupByKey();
		
		// 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
		// 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
		//视频中的代码中，userid2PartAggrInfoRDD本来是sessionid2PartAggrInfoRDD（怪不得找不到，被替换掉了，因为"user_info"表和"user_visit_action"表join后，就使用共同字段user_id就行了）
		JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
				
				//除了Long, String是传入参数（最后一个为传入参数），其余均为返回值
				new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {
					
					private static final long serialVersionUID = 1L;
		
					@Override
					//这里用Iterator，因为一个session id聚合后多条记录（同一个session id的访问用户，当天访问多次，就产生多条访问记录了）
					//call()方法的返回值Tuple2<Long, String>，对应传入参数Long, String；
					public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple)
							throws Exception {
						String sessionid = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();
						
						StringBuffer searchKeywordsBuffer = new StringBuffer("");
						StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
						
						//定义一个userid，将sessionid对应起来（用它将sessioin和userid才能对应起来）
						Long userid = null;
						
						// session的起始和结束时间;利用重构的思想，在遍历session的过程中，把访问时长和访问步长计算出来，避免新增RDD
						Date startTime = null;
						Date endTime = null;
						// session的访问步长
						int stepLength = 0;
						
						// 遍历session所有的访问行为（所有row的集合）
						while(iterator.hasNext()) {
							// 提取每个访问行为的一行数据
							Row row = iterator.next();
							//要将sessionid和userid对应起来，为什么取一次userid就可以了？每个sessionid对应的userid应该是不同的啊？
							//（方法一开始定义传入参数：Tuple2<String, Iterable<Row>> tuple，取出来的String sessionid = tuple._1和Iterator<Row> iterator = tuple._2.iterator()，是针对每个sessionid的Tuple2<String, Iterable<Row>>传入进来的）
							//所以这里，后面再传进来不同的sessionid对应的Tuple2<String, Iterable<Row>>，就是可以实现不同的userid，而相同的userid则保留下来运算
							//因为你想想看，大数据情况下，session有多条（MockData生成），怎么可能函数就传入一次参数的值就结束了呢？
							if(userid == null) {
								userid = row.getLong(1);
							}
							//获取每条数据的searchKeyword字段，MockData类中构造了"user_visit_action"表，每条记录的第6,第7个字段的内容
							String searchKeyword = row.getString(5);
							Long clickCategoryId = row.getLong(6);
							
							// 实际上这里要对数据说明一下
							// 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的（这个就要下到对业务的理解才能转换代码了）
							// 其实，只有搜索行为，是有searchKeyword字段的
							// 只有点击品类的行为，是有clickCategoryId字段的
							// 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的
							
							// 我们决定是否将搜索词或点击品类id拼接到字符串中去
							// 首先要满足：不能是null值
							// 其次，之前的字符串中还没有搜索词或者点击品类id
							
							//执行完下面两个嵌套if语句，相当于是把"user_visit_action"中，按searchKeyword字段和clickCategoryId字段聚合起来了
							if(StringUtils.isNotEmpty(searchKeyword)) {
								//如果searchKeywordsBuffer中不包含searchKeyword，那么就加进去，这样搜索词和点击品类就都加进去StringBuffer中（通过这样，把用户的搜索过的词和点击过的品类就收集起来，对应到他的sessionid上）
								//把搜索词或点击品类统一用逗号分隔开
								if(!searchKeywordsBuffer.toString().contains(searchKeyword)) {
									searchKeywordsBuffer.append(searchKeyword + ",");  
								}
							}
							if(clickCategoryId != null) {
								if(!clickCategoryIdsBuffer.toString().contains(
										String.valueOf(clickCategoryId))) {   
									clickCategoryIdsBuffer.append(clickCategoryId + ",");  
								}
							}
							
							// 计算session开始和结束时间（下面这里的逻辑怎么理解？）（不要总想着代码的细节，你要先知道这段代码的功能和意图，然后才去看代码的具体实现！）
							Date actionTime = DateUtils.parseTime(row.getString(4));
							//每个sessionid的数据，一开始进来的第一条session，开始和结束时间是一样的；后续再传进来相同sessionid下的其他session，actionTime就不同了
							if(startTime == null) {
								startTime = actionTime;
							}
							if(endTime == null) {
								endTime = actionTime;
							}
							
							if(actionTime.before(startTime)) {
								startTime = actionTime;
							}
							if(actionTime.after(endTime)) {
								endTime = actionTime;
							}
							
							// 计算session访问步长。这里逻辑怎么考虑的？(每一条访问记录row，就是一次页面点击，就是总共点击了几次页面，产生几条row)
							stepLength++;
						}
						
						//规整数据，便于批量统一处理。searchKeywordsBuffer.toString()转成String类，StringUtils.trimComma（去除逗号，这个方法命名已经说得很清楚）
						String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
						String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
						
						// 计算session访问时长（秒）；这样就没有单独再进行一次算子，而是把计算浏览时间和访问步长的信息 的计算放到userid2PartAggrInfoRDD算子中
						long visitLength = (endTime.getTime() - startTime.getTime()) / 1000; 
						
						// 大家思考一下
						// 我们返回的数据格式，即使<sessionid,partAggrInfo>（部分聚合数据：partAggrInfo，因为这里只聚合了点击品类和搜索词）
						// 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
						// 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
						// 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合（但这里sessioinid和userid一样吗？）
						// 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
						// 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举
						
						// 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
						// 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
						// 然后再直接将返回的Tuple的key设置成sessionid
						// 最后的数据格式，还是<sessionid,fullAggrInfo>（这里fullAggrInfo就包括user_info的信息进来了）
						
						// 聚合数据，用什么样的格式进行拼接？（信息都拿到了，怎么拼接成一个string类返回）
						// 我们这里统一定义，使用key=value|key=value（这时partAggrInfo包括sessionid，searchKeywords，clickCategoryIds，需要把这三个数据拼接起来）
						String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
								+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
								+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
								+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
								+ Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
								+ Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);    
						//这里return后，是把这个Tuple2<Long, String>(userid, partAggrInfo)返回给一开始定义的函数这里接收public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple)，而不仅仅是跳出iterator的遍历，所以userid每次都能和sessionid对应起来？
						//这里的返回是针对每一个userid的，多个userid当然会返回多次
						return new Tuple2<Long, String>(userid, partAggrInfo);
					}
					
				});
		
		// 查询所有用户数据，并映射成<userid,Row>的格式（在通过之前的(userid, partAggrInfo)join上用户表，就可以得到完整的用户数据）
		//user_info这个表在MockData类里有创建，在mysql的spark_project库里没有，什么关系？不懂
		String sql = "select * from user_info";  
		JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
		//JavaPairRDD<Long, Row> userid2InfoRDD，long为userid，Row为对应用户数据
		JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(
				//匿名内部类new PairFunction<Row, Long, Row>()，除了最后一个元素外，其他是传入call()方法的参数，这里传入参数是Row，返回是Tuple2<Long, Row>，其中Long就是userid=row.getLong(0)
				new PairFunction<Row, Long, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Row> call(Row row) throws Exception {
						//getLong(0)表示mockdata中"user_info"表的第一个字段数据
						return new Tuple2<Long, Row>(row.getLong(0), row);
					}
					
				});
		
		/**
		 * 这里就可以说一下，比较适合采用reduce join转换为map join的方式
		 * 
		 * userid2PartAggrInfoRDD，可能数据量还是比较大，比如，可能在1千万数据
		 * userid2InfoRDD，可能数据量还是比较小的，你的用户数量才10万用户
		 * 
		 */
		
		// 将session粒度聚合数据，与用户信息进行join。
		//userid2PartAggrInfoRDD是userid和对应的session操作，userid2InfoRDD是userid和对应的用户信息。join之后，这里JavaPairRDD<Long, Tuple2<String, Row>> ，其中Long为userid，string为操作信息，row为用户信息
		JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = 
				userid2PartAggrInfoRDD.join(userid2InfoRDD);
		
		// 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(
				//对期待返回的数据格式，进行算子重写（你知道了作者的意图，下面匿名内部类的代码块就解释的清楚具体逻辑了）
				new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(
							Tuple2<Long, Tuple2<String, Row>> tuple)
							throws Exception {
						String partAggrInfo = tuple._2._1;
						Row userInfoRow = tuple._2._2;
						//"\\|"，这里连接符本来就只有|，但是要对其进行转义，所以要写成"\\|"
						String sessionid = StringUtils.getFieldFromConcatString(
								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
						
						int age = userInfoRow.getInt(3);
						String professional = userInfoRow.getString(4);
						String city = userInfoRow.getString(5);
						String sex = userInfoRow.getString(6);
						
						String fullAggrInfo = partAggrInfo + "|"
								+ Constants.FIELD_AGE + "=" + age + "|"
								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
								+ Constants.FIELD_CITY + "=" + city + "|"
								+ Constants.FIELD_SEX + "=" + sex;
						
						return new Tuple2<String, String>(sessionid, fullAggrInfo);
					}
					
				});
		
		/**
		 * reduce join转换为map join
		 */
		
//		List<Tuple2<Long, Row>> userInfos = userid2InfoRDD.collect();
//		final Broadcast<List<Tuple2<Long, Row>>> userInfosBroadcast = sc.broadcast(userInfos);
//		
//		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2PartAggrInfoRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,String>, String, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(Tuple2<Long, String> tuple)
//							throws Exception {
//						// 得到用户信息map
//						List<Tuple2<Long, Row>> userInfos = userInfosBroadcast.value();
//						
//						Map<Long, Row> userInfoMap = new HashMap<Long, Row>();
//						for(Tuple2<Long, Row> userInfo : userInfos) {
//							userInfoMap.put(userInfo._1, userInfo._2);
//						}
//						
//						// 获取到当前用户对应的信息
//						String partAggrInfo = tuple._2;
//						Row userInfoRow = userInfoMap.get(tuple._1);
//						
//						String sessionid = StringUtils.getFieldFromConcatString(
//								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
//						
//						int age = userInfoRow.getInt(3);
//						String professional = userInfoRow.getString(4);
//						String city = userInfoRow.getString(5);
//						String sex = userInfoRow.getString(6);
//						
//						String fullAggrInfo = partAggrInfo + "|"
//								+ Constants.FIELD_AGE + "=" + age + "|"
//								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
//								+ Constants.FIELD_CITY + "=" + city + "|"
//								+ Constants.FIELD_SEX + "=" + sex;
//						
//						return new Tuple2<String, String>(sessionid, fullAggrInfo);
//					}
//					
//				});
		
		/**
		 * sample采样倾斜key单独进行join
		 */
		
//		JavaPairRDD<Long, String> sampledRDD = userid2PartAggrInfoRDD.sample(false, 0.1, 9);
//		
//		JavaPairRDD<Long, Long> mappedSampledRDD = sampledRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,String>, Long, Long>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<Long, Long> call(Tuple2<Long, String> tuple)
//							throws Exception {
//						return new Tuple2<Long, Long>(tuple._1, 1L);
//					}
//					
//				});
//		
//		JavaPairRDD<Long, Long> computedSampledRDD = mappedSampledRDD.reduceByKey(
//				
//				new Function2<Long, Long, Long>() {
//
//					private static final long serialVersionUID = 1L;
//		
//					@Override
//					public Long call(Long v1, Long v2) throws Exception {
//						return v1 + v2;
//					}
//					
//				});
//		
//		JavaPairRDD<Long, Long> reversedSampledRDD = computedSampledRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,Long>, Long, Long>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<Long, Long> call(Tuple2<Long, Long> tuple)
//							throws Exception {
//						return new Tuple2<Long, Long>(tuple._2, tuple._1);
//					}
//					
//				});
//		
//		final Long skewedUserid = reversedSampledRDD.sortByKey(false).take(1).get(0)._2;  
//		
//		JavaPairRDD<Long, String> skewedRDD = userid2PartAggrInfoRDD.filter(
//				
//				new Function<Tuple2<Long,String>, Boolean>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Boolean call(Tuple2<Long, String> tuple) throws Exception {
//						return tuple._1.equals(skewedUserid);
//					}
//					
//				});
//			
//		JavaPairRDD<Long, String> commonRDD = userid2PartAggrInfoRDD.filter(
//				
//				new Function<Tuple2<Long,String>, Boolean>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Boolean call(Tuple2<Long, String> tuple) throws Exception {
//						return !tuple._1.equals(skewedUserid);
//					}
//					
//				});
//		
//		JavaPairRDD<String, Row> skewedUserid2infoRDD = userid2InfoRDD.filter(
//				
//				new Function<Tuple2<Long,Row>, Boolean>() {
//
//					private static final long serialVersionUID = 1L;
//		
//					@Override
//					public Boolean call(Tuple2<Long, Row> tuple) throws Exception {
//						return tuple._1.equals(skewedUserid);
//					}
//					
//				}).flatMapToPair(new PairFlatMapFunction<Tuple2<Long,Row>, String, Row>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Iterable<Tuple2<String, Row>> call(
//							Tuple2<Long, Row> tuple) throws Exception {
//						Random random = new Random();
//						List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
//						
//						for(int i = 0; i < 100; i++) {
//							int prefix = random.nextInt(100);
//							list.add(new Tuple2<String, Row>(prefix + "_" + tuple._1, tuple._2));
//						}
//						
//						return list;
//					}
//					
//				});
//		
//		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD1 = skewedRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,String>, String, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(Tuple2<Long, String> tuple)
//							throws Exception {
//						Random random = new Random();
//						int prefix = random.nextInt(100);
//						return new Tuple2<String, String>(prefix + "_" + tuple._1, tuple._2);
//					}
//					
//				}).join(skewedUserid2infoRDD).mapToPair(
//						
//						new PairFunction<Tuple2<String,Tuple2<String,Row>>, Long, Tuple2<String, Row>>() {
//
//							private static final long serialVersionUID = 1L;
//		
//							@Override
//							public Tuple2<Long, Tuple2<String, Row>> call(
//									Tuple2<String, Tuple2<String, Row>> tuple)
//									throws Exception {
//								long userid = Long.valueOf(tuple._1.split("_")[1]);  
//								return new Tuple2<Long, Tuple2<String, Row>>(userid, tuple._2);  
//							}
//							
//						});
//		
//		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD2 = commonRDD.join(userid2InfoRDD);
//		
//		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD = joinedRDD1.union(joinedRDD2);
//		
//		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = joinedRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(
//							Tuple2<Long, Tuple2<String, Row>> tuple)
//							throws Exception {
//						String partAggrInfo = tuple._2._1;
//						Row userInfoRow = tuple._2._2;
//						
//						String sessionid = StringUtils.getFieldFromConcatString(
//								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
//						
//						int age = userInfoRow.getInt(3);
//						String professional = userInfoRow.getString(4);
//						String city = userInfoRow.getString(5);
//						String sex = userInfoRow.getString(6);
//						
//						String fullAggrInfo = partAggrInfo + "|"
//								+ Constants.FIELD_AGE + "=" + age + "|"
//								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
//								+ Constants.FIELD_CITY + "=" + city + "|"
//								+ Constants.FIELD_SEX + "=" + sex;
//						
//						return new Tuple2<String, String>(sessionid, fullAggrInfo);
//					}
//					
//				});
		
		/**
		 * 使用随机数和扩容表进行join
		 */
		
//		JavaPairRDD<String, Row> expandedRDD = userid2InfoRDD.flatMapToPair(
//				
//				new PairFlatMapFunction<Tuple2<Long,Row>, String, Row>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Iterable<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple)
//							throws Exception {
//						List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
//						
//						for(int i = 0; i < 10; i++) {
//							list.add(new Tuple2<String, Row>(0 + "_" + tuple._1, tuple._2));
//						}
//						
//						return list;
//					}
//					
//				});
//		
//		JavaPairRDD<String, String> mappedRDD = userid2PartAggrInfoRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,String>, String, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(Tuple2<Long, String> tuple)
//							throws Exception {
//						Random random = new Random();
//						int prefix = random.nextInt(10);
//						return new Tuple2<String, String>(prefix + "_" + tuple._1, tuple._2);  
//					}
//					
//				});
//		
//		JavaPairRDD<String, Tuple2<String, Row>> joinedRDD = mappedRDD.join(expandedRDD);
//		
//		JavaPairRDD<String, String> finalRDD = joinedRDD.mapToPair(
//				
//				new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(
//							Tuple2<String, Tuple2<String, Row>> tuple)
//							throws Exception {
//						String partAggrInfo = tuple._2._1;
//						Row userInfoRow = tuple._2._2;
//						
//						String sessionid = StringUtils.getFieldFromConcatString(
//								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
//						
//						int age = userInfoRow.getInt(3);
//						String professional = userInfoRow.getString(4);
//						String city = userInfoRow.getString(5);
//						String sex = userInfoRow.getString(6);
//						
//						String fullAggrInfo = partAggrInfo + "|"
//								+ Constants.FIELD_AGE + "=" + age + "|"
//								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
//								+ Constants.FIELD_CITY + "=" + city + "|"
//								+ Constants.FIELD_SEX + "=" + sex;
//						
//						return new Tuple2<String, String>(sessionid, fullAggrInfo);
//					}
//					
//				});
		
		return sessionid2FullAggrInfoRDD;
	}
	
	/**
	 * 过滤session数据，并进行聚合统计
	 * @param sessionid2AggrInfoRDD
	 * @return 
	 */
	//传进来的任务参数JSONObject taskParam？task_param的字段，以json格式存储的数据
	private static JavaPairRDD<String, String> filterSessionAndAggrStat(
			JavaPairRDD<String, String> sessionid2AggrInfoRDD, 
			final JSONObject taskParam,
			final Accumulator<String> sessionAggrStatAccumulator) {  
		// 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
		// 此外，这里其实大家不要觉得是多此一举
		// 其实我们是给后面的性能优化埋下了一个伏笔
		String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
		String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
		String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
		
		String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				+ (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				+ (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");
		
		if(_parameter.endsWith("\\|")) {
			_parameter = _parameter.substring(0, _parameter.length() - 1);
		}
		
		final String parameter = _parameter;
		
		// 根据筛选参数进行过滤
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
				
				new Function<Tuple2<String,String>, Boolean>() {
			
					private static final long serialVersionUID = 1L;
			
					@Override
					public Boolean call(Tuple2<String, String> tuple) throws Exception {
						// 首先，从tuple中，获取聚合数据
						String aggrInfo = tuple._2;
						
						//其实除了spark特有的接口api外，其他代码的实现就是各种和业务，javase，设计模式，jdbc/json等类的使用；将业务逻辑转换成java代码的能力还是关键
						// 接着，依次按照筛选条件进行过滤
						// 按照年龄范围进行过滤（startAge、endAge）
						//ValidUtils.between这些工具类的代码实现也要过一次
						//如果if后面跟的括号内条件满足（即为ture时），就会return false，函数接收返回值后就不会继续往下执行了
						if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, 
								parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
							return false;
						}
						
						// 按照职业范围进行过滤（professionals）
						// 互联网,IT,软件
						// 互联网
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, 
								parameter, Constants.PARAM_PROFESSIONALS)) {
							return false;
						}
						
						// 按照城市范围进行过滤（cities）
						// 北京,上海,广州,深圳
						// 成都
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, 
								parameter, Constants.PARAM_CITIES)) {
							return false;
						}
						
						// 按照性别进行过滤
						// 男/女
						// 男，女
						if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, 
								parameter, Constants.PARAM_SEX)) {
							return false;
						}
						
						// 按照搜索词进行过滤
						// 我们的session可能搜索了 火锅,蛋糕,烧烤（从所有的aggrInfo中获取搜索词信息Constants.FIELD_SEARCH_KEYWORDS）
						// 我们的筛选条件可能是 火锅,串串香,iphone手机（从所有的筛选参数parameter中获取搜索词Constants.PARAM_KEYWORDS）
						// 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
						// 任何一个搜索词相等，即通过
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, 
								parameter, Constants.PARAM_KEYWORDS)) {
							return false;
						}
						
						// 按照点击品类id进行过滤
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, 
								parameter, Constants.PARAM_CATEGORY_IDS)) {
							return false;
						}
						
						// 如果经过了之前的多个过滤条件之后，程序能够走到这里
						// 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
						// 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
						// 进行相应的累加计数
						
						// 主要走到这一步，那么就是需要计数的session（上面已经过滤一遍了，能留下来的session都符合条件）
						//一开始定义的累加变量：Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());这个new SessionAggrStatAccumulator()就是自定义实现的逻辑
						sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);  
						
						// 计算出session的访问时长和访问步长的范围，并进行相应的累加
						long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
								aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH)); 
						long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
								aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));  
						calculateVisitLength(visitLength); 
						calculateStepLength(stepLength);  
						
						//最后如果用户的筛选条件都符合了，就返回true，到这一步，过滤操作就完成了；如果上面筛选条件有一个不符合，就终止了
						return true;
					}
					
					/**
					 * 计算访问时长范围
					 * @param visitLength
					 */
					private void calculateVisitLength(long visitLength) {
						if(visitLength >=1 && visitLength <= 3) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);  
						} else if(visitLength >=4 && visitLength <= 6) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);  
						} else if(visitLength >=7 && visitLength <= 9) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);  
						} else if(visitLength >=10 && visitLength <= 30) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);  
						} else if(visitLength > 30 && visitLength <= 60) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);  
						} else if(visitLength > 60 && visitLength <= 180) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);  
						} else if(visitLength > 180 && visitLength <= 600) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);  
						} else if(visitLength > 600 && visitLength <= 1800) {  
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);  
						} else if(visitLength > 1800) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);  
						} 
					}
					
					/**
					 * 计算访问步长范围
					 * @param stepLength
					 */
					private void calculateStepLength(long stepLength) {
						if(stepLength >= 1 && stepLength <= 3) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);  
						} else if(stepLength >= 4 && stepLength <= 6) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);  
						} else if(stepLength >= 7 && stepLength <= 9) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);  
						} else if(stepLength >= 10 && stepLength <= 30) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);  
						} else if(stepLength > 30 && stepLength <= 60) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);  
						} else if(stepLength > 60) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);    
						}
					}
					
				});
		//把过滤出来的rdd将其返回
		return filteredSessionid2AggrInfoRDD;
	}
	
	/**
	 * 获取通过筛选条件的（这里只通过时间筛选条件了？其实筛选条件没考虑？）session的访问明细数据RDD
	 * @param sessionid2aggrInfoRDD
	 * @param sessionid2actionRDD
	 * @return
	 */
	//sessionid2aggrInfoRDD就是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
	//通过用日期参数对模拟数据user_visit_action进行初步过滤后，就可以进行聚合操作了，JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);
	private static JavaPairRDD<String, Row> getSessionid2detailRDD(
			JavaPairRDD<String, String> sessionid2aggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD) {
		JavaPairRDD<String, Row> sessionid2detailRDD = sessionid2aggrInfoRDD
				.join(sessionid2actionRDD)
				//传入参数<Tuple2<String,Tuple2<String,Row>>就是<Tuple2<sessionid,Tuple2<(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex),Row>>
				.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {
		
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Row> call(
							Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
						//Tuple2<String, Row>(tuple._1, tuple._2._2)就是通过各种筛选条件之后的访问明细：<sessionid,Row>
						return new Tuple2<String, Row>(tuple._1, tuple._2._2);
					}
					
				});
		return sessionid2detailRDD;
	}
	
	/**
	 * 随机抽取session
	 * @param sessionid2AggrInfoRDD  
	 */
	private static void randomExtractSession(
			JavaSparkContext sc,
			final long taskid,
			JavaPairRDD<String, String> sessionid2AggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD) { 
		/**
		 * 第一步，计算出每天每小时的session数量
		 */
		
		// 获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD，就是time2sessionidRDD最后想实现出来的格式内容；
		JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(
				
				new PairFunction<Tuple2<String,String>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(
							Tuple2<String, String> tuple) throws Exception {
						String aggrInfo = tuple._2;
						
						String startTime = StringUtils.getFieldFromConcatString(
								aggrInfo, "\\|", Constants.FIELD_START_TIME);
						String dateHour = DateUtils.getDateHour(startTime);
						
						return new Tuple2<String, String>(dateHour, aggrInfo);  
					}
					
				});
		
		/**
		 * 思考一下：这里我们不要着急写大量的代码，做项目的时候，一定要用脑子多思考
		 * 
		 * 每天每小时的session数量，然后计算出每天每小时的session抽取索引，遍历每天每小时session
		 * 首先抽取出的session的聚合数据，写入session_random_extract表
		 * 所以第一个RDD的value，应该是session聚合数据
		 * 
		 */
		
		// 得到每天每小时的session数量
		
		/**
		 * 每天每小时的session数量的计算
		 * 是有可能出现数据倾斜的吧，这个是没有疑问的
		 * 比如说大部分小时，一般访问量也就10万；但是，中午12点的时候，高峰期，一个小时1000万
		 * 这个时候，就会发生数据倾斜
		 * 
		 * 我们就用这个countByKey操作，给大家演示第三种和第四种方案
		 * 
		 */
		//（老师原话：spark它自己会搞出这个object)这里Map<String, Object>的object是啥？不应该是Map<String,int>吗？Map<String,int>，即是Map<yyyy-MM-dd_HH,每个时间对应的session数量>？
		Map<String, Object> countMap = time2sessionidRDD.countByKey();
		
		/**
		 * 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引（算出每天每小时要抽取的session总量，然后以这个总量的值为范围进行随机抽取，然后把这些随机值加入到一个list中，构成随机抽取索引值数组）
		 */
		
		// 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
		//<yyyy-MM-dd_HH,count>这里的count是什么？count是指年月日下具体某个小时对应的session总量（Map<String, Object> countMap = time2sessionidRDD.countByKey();）
		//Map<String, Map<String, Long>> dateHourCountMap-------->Map<每天, Map<每个小时, 对应的session数量>>（要知道作者意图，业务意图，才能看懂代码）
		Map<String, Map<String, Long>> dateHourCountMap = 
				new HashMap<String, Map<String, Long>>();
		//count是指年月日下具体某个小时对应的session总量
		//遍历Map<String, Object> countMap
		for(Map.Entry<String, Object> countEntry : countMap.entrySet()) {
			//对时间格式进行拆分，取出Map<String, Object> countMap的键
			String dateHour = countEntry.getKey();			
			String date = dateHour.split("_")[0];
			String hour = dateHour.split("_")[1];  
			//取出Map<String, Object> countMap的值
			long count = Long.valueOf(String.valueOf(countEntry.getValue()));  
			
			//如果不知道内容，用print方法打印一下看是不是自己理解的那样
			//往hourCountMap放数据。获取Map<String, Map<String, Long>> dateHourCountMap下键date对应的值，就是后面这个map---->Map<String, Long>，Map<每个小时, 对应的session数量>
			Map<String, Long> hourCountMap = dateHourCountMap.get(date);
			
			if(hourCountMap == null) {
				hourCountMap = new HashMap<String, Long>();
				dateHourCountMap.put(date, hourCountMap);
			}
			//放入小时和其对应的count（session数量）
			hourCountMap.put(hour, count);
		}
		
		// 开始实现我们的按时间比例随机抽取算法
		
		// 总共要抽取100个session，先按照天数，进行平分（dateHourCountMap.size()可以拿到当前有几天）（size() 方法用于返回在此映射中的键 - 值映射关系的数量）
		int extractNumberPerDay = 100 / dateHourCountMap.size();
		
		
		
		/**
		 * session随机抽取功能
		 * 
		 * 用到了一个比较大的变量，随机抽取索引map
		 * 之前是直接在算子里面使用了这个map，那么根据我们刚才讲的这个原理，每个task都会拷贝一份map副本
		 * 还是比较消耗内存和网络传输性能的
		 * 
		 * 将map做成广播变量
		 * 
		 */
		//这里再定义一个数据格式
		// Map<String, Map<String, List<Integer>>> dateHourExtractMap放置的内容：<date,<hour,(3,5,20,102)>> ， <每天,<每个小时,(session对应的索引)>>   ；(3,5,20,102)就是每条session对应的sessionid？
		Map<String, Map<String, List<Integer>>> dateHourExtractMap = 
				new HashMap<String, Map<String, List<Integer>>>();
		
		Random random = new Random();
		//遍历Map<String, Map<String, Long>> dateHourCountMap；dateHourCountMap这些变量命名都是有要求的，含义清晰，驼峰式写法
		for(Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
			//获取每一天
			String date = dateHourCountEntry.getKey();
			//Map<每个小时, 对应session的总量>
			Map<String, Long> hourCountMap = dateHourCountEntry.getValue();
			
			// 计算出这一天的session总数
			long sessionCount = 0L;
			//遍历hourCountMap的value值，把它们累加（把当天每个小时的加起来就是一天的）
			for(long hourCount : hourCountMap.values()) {
				sessionCount += hourCount;
			}
			//get方法——返回指定键所映射的值,
			Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
			if(hourExtractMap == null) {
				hourExtractMap = new HashMap<String, List<Integer>>();
				dateHourExtractMap.put(date, hourExtractMap);
			}
			
			// 遍历每个小时的session总数
			for(Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
				//获取每个小时
				String hour = hourCountEntry.getKey();
				//获取每个小时对应的session总数
				long count = hourCountEntry.getValue();
				
				// 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
				// 就可以计算出，当前小时需要抽取的session数量（这里面就是涉及比较多的变量，理解起来逻辑不是很复杂，但是变量名要注释清楚）
				int hourExtractNumber = (int)(((double)count / (double)sessionCount) 
						* extractNumberPerDay);
				//如果当前小时要抽取的量，比当前小时的总量还要大的话
				if(hourExtractNumber > count) {
					hourExtractNumber = (int) count;
				}
				
				// 先获取当前小时的存放随机数的list
				List<Integer> extractIndexList = hourExtractMap.get(hour);
				if(extractIndexList == null) {
					extractIndexList = new ArrayList<Integer>();
					hourExtractMap.put(hour, extractIndexList);
				}
				
				// 生成上面计算出来的数量的随机数(可以计算出每个小时内，从0~session总量之间的范围中（session总量就是count），获取指定抽取数量个随机数，作为随机抽取的索引)
				//有个问题，每条待抽取的session怎么和这个随机index关联起来？
				for(int i = 0; i < hourExtractNumber; i++) {
					//hourExtractNumber生产索引的数量；random.nextInt((int) count索引随机取值范围为当前每小时需抽取的session的总量
					int extractIndex = random.nextInt((int) count);
					
					while(extractIndexList.contains(extractIndex)) {
						extractIndex = random.nextInt((int) count);
					}
					extractIndexList.add(extractIndex);
				}
			}
		}
		
		/**
		 * fastutil的使用，很简单，比如List<Integer>的list，对应到fastutil，就是IntList
		 */
		Map<String, Map<String, IntList>> fastutilDateHourExtractMap = 
				new HashMap<String, Map<String, IntList>>();
		
		
		
		for(Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry : 
				dateHourExtractMap.entrySet()) {
			String date = dateHourExtractEntry.getKey();
			Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();
			
			Map<String, IntList> fastutilHourExtractMap = new HashMap<String, IntList>();
			
			for(Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
				String hour = hourExtractEntry.getKey();
				List<Integer> extractList = hourExtractEntry.getValue();
				
				IntList fastutilExtractList = new IntArrayList();
				
				for(int i = 0; i < extractList.size(); i++) {
					fastutilExtractList.add(extractList.get(i));  
				}
				
				fastutilHourExtractMap.put(hour, fastutilExtractList);
			}
			
			fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
		}
		
		/**
		 * 广播变量，很简单
		 * 其实就是SparkContext的broadcast()方法，传入你要广播的变量，即可
		 */		
		
		
		final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast = 
				sc.broadcast(fastutilDateHourExtractMap);
		
		/**
		 * 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
		 */
		
		// 执行groupByKey算子，得到<dateHour,(session aggrInfo)>  ，就是每天每小时对应的session aggrInfo（session aggrInfo是通过session2PartAggrInfo join userInfo得到的完整聚合数据；这整个过程涉及很多变量，RDD，Map和List，要见名知意，否则程序可读性很差）
		JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();
		
		// 我们用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的RDD数据（time2sessionsRDD）
		// 然后呢，会遍历每天每小时的session
		// 如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上（这一点是怎么样实现的？）
		// 那么抽取该session，直接写入MySQL的random_extract_session表
		// 将抽取出来的session id返回回来，形成一个新的JavaRDD<String>（这里调整为这种数据格式JavaPairRDD<String, String> extractSessionidsRDD，是因为后面要和session2actionRDD进行join操作，要这种都是JavaPairRDD且有公共字段的RDD数据才能join）
		// 然后最后一步，是用抽取出来的sessionid，去join它们的访问行为明细数据，写入session表
		JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					//new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>除了最后一个元素外，其余均为传入call()方法的元素，这里传入Tuple2<String,Iterable<String>，对应time2sessionsRDD数据，tuple_.1是date，tuple_.2是每个date对应的各个sessionid2FullAggrInfoRDD的集合
					public Iterable<Tuple2<String, String>> call(
							Tuple2<String, Iterable<String>> tuple)
							throws Exception {
						List<Tuple2<String, String>> extractSessionids = 
								new ArrayList<Tuple2<String, String>>();
						
						//把time2sessionsRDD的第一个元素取出来，就是tuple._1
						String dateHour = tuple._1;
						String date = dateHour.split("_")[0];
						String hour = dateHour.split("_")[1];
						//把time2sessionsRDD的第二个元素sessionid2FullAggrInfoRDD取出来就是每一条sessionAggrInfo
						Iterator<String> iterator = tuple._2.iterator();
						
						/**
						 * 使用广播变量的时候
						 * 直接调用广播变量（Broadcast类型）的value() / getValue() 
						 * 可以获取到之前封装的广播变量
						 */
						Map<String, Map<String, IntList>> dateHourExtractMap = 
								dateHourExtractMapBroadcast.value();
						//拿到了这天这个小时，所要进行随机抽取的索引。（怎么能获取两次对应获取指定键所映射的值？因为变量dateHourExtractMap的数据结构是<date,<hour,(3,5,20,102)>>先获取date的对应值，然后再获取hour对应的值）
						//Map<String, Map<String, List<Integer>>> dateHourExtractMap放置的内容：<date,<hour,(3,5,20,102)>>
						List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);  
						
						ISessionRandomExtractDAO sessionRandomExtractDAO = 
								DAOFactory.getSessionRandomExtractDAO();
						
						//通过这里人为设置一个索引将之前的随机抽取索引值关联起来
						int index = 0;
						while(iterator.hasNext()) {
							String sessionAggrInfo = iterator.next();
							//通过这里人为设置一个索引将之前的随机抽取索引值关联起来（如果之前随机提取的extractIndexList中的索引值包含了我们手工设置的index值，就可以配对上，实现数据提取的随机性）
							if(extractIndexList.contains(index)) {
								String sessionid = StringUtils.getFieldFromConcatString(
										sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
								
								// 将数据写入MySQL中`session_random_extract`表
								SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
								sessionRandomExtract.setTaskid(taskid);  
								sessionRandomExtract.setSessionid(sessionid);  
								sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
										sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));  
								sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
										sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
								sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
										sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
								
								sessionRandomExtractDAO.insert(sessionRandomExtract);  
								
								// 将sessionid加入list（这里做成new Tuple2<String, String>(sessionid, sessionid)是为了实现后面与sessionid2AggrInfoRDD进行join，然后把join后的数据写入`session_detail`表，这个功能模块才算完整）
								extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));  
							}
							//通过这里人为设置一个索引将之前的随机抽取索引值关联起来(索引值每次加1，肯定能保证extractIndexList.contains(index)能返回true？)
							index++;
						}
						//到这里为止，session随机抽取就完成了
						return extractSessionids;
					}
					
				});
		
		/**
		 * 第四步：获取抽取出来的session的明细数据
		 */
		//JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD的内容<sessionid,tuple2<sessionid,row>>
		JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
				extractSessionidsRDD.join(sessionid2actionRDD);
		
//		extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {  
//			
//			private static final long serialVersionUID = 1L;
//			
//			@Override
//			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
//				Row row = tuple._2._2;
//				
//				SessionDetail sessionDetail = new SessionDetail();
//				sessionDetail.setTaskid(taskid);  
//				sessionDetail.setUserid(row.getLong(1));  
//				sessionDetail.setSessionid(row.getString(2));  
//				sessionDetail.setPageid(row.getLong(3));  
//				sessionDetail.setActionTime(row.getString(4));
//				sessionDetail.setSearchKeyword(row.getString(5));  
//				sessionDetail.setClickCategoryId(row.getLong(6));  
//				sessionDetail.setClickProductId(row.getLong(7));   
//				sessionDetail.setOrderCategoryIds(row.getString(8));  
//				sessionDetail.setOrderProductIds(row.getString(9));  
//				sessionDetail.setPayCategoryIds(row.getString(10)); 
//				sessionDetail.setPayProductIds(row.getString(11));  
//				
//				ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
//				sessionDetailDAO.insert(sessionDetail);  
//			}
//		});
		
		extractSessionDetailRDD.foreachPartition(
				
				new VoidFunction<Iterator<Tuple2<String,Tuple2<String,Row>>>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(
							Iterator<Tuple2<String, Tuple2<String, Row>>> iterator) 
							throws Exception {
						List<SessionDetail> sessionDetails = new ArrayList<SessionDetail>();
						
						while(iterator.hasNext()) {
							Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();
							
							Row row = tuple._2._2;
							
							SessionDetail sessionDetail = new SessionDetail();
							sessionDetail.setTaskid(taskid);  
							sessionDetail.setUserid(row.getLong(1));  
							sessionDetail.setSessionid(row.getString(2));  
							sessionDetail.setPageid(row.getLong(3));  
							sessionDetail.setActionTime(row.getString(4));
							sessionDetail.setSearchKeyword(row.getString(5));  
							sessionDetail.setClickCategoryId(row.getLong(6));  
							sessionDetail.setClickProductId(row.getLong(7));   
							sessionDetail.setOrderCategoryIds(row.getString(8));  
							sessionDetail.setOrderProductIds(row.getString(9));  
							sessionDetail.setPayCategoryIds(row.getString(10)); 
							sessionDetail.setPayProductIds(row.getString(11));  
							
							sessionDetails.add(sessionDetail);
						}
						
						ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
						sessionDetailDAO.insertBatch(sessionDetails);
					}
					
				});
	}
	
	/**
	 * 计算各session范围占比，并写入MySQL
	 * @param value（就是SessionAggrStatAccumulator中定义的累加串，new SessionAggrStatAccumulator()中定义的需要操作的累加对象）
	 */
	private static void calculateAndPersistAggrStat(String value, long taskid) {
		// 从Accumulator统计串中获取值
		//session_count是总数，TIME_PERIOD_1s_3s等是之前accumulator统计后的对应访问1-3s的session总数
		long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.SESSION_COUNT));  
		
		long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1s_3s));  
		long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_4s_6s));
		long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_7s_9s));
		long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10s_30s));
		long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30s_60s));
		long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1m_3m));
		long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_3m_10m));
		long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10m_30m));
		long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30m));
		
		long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_1_3));
		long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_4_6));
		long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_7_9));
		long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_10_30));
		long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_30_60));
		long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_60));
		
		// 计算各个访问时长和访问步长的范围
		double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
				(double)visit_length_1s_3s / (double)session_count, 2);  
		double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
				(double)visit_length_4s_6s / (double)session_count, 2);  
		double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
				(double)visit_length_7s_9s / (double)session_count, 2);  
		double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
				(double)visit_length_10s_30s / (double)session_count, 2);  
		double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
				(double)visit_length_30s_60s / (double)session_count, 2);  
		double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
				(double)visit_length_1m_3m / (double)session_count, 2);
		double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
				(double)visit_length_3m_10m / (double)session_count, 2);  
		double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_10m_30m / (double)session_count, 2);
		double visit_length_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_30m / (double)session_count, 2);  
		
		double step_length_1_3_ratio = NumberUtils.formatDouble(
				(double)step_length_1_3 / (double)session_count, 2);  
		double step_length_4_6_ratio = NumberUtils.formatDouble(
				(double)step_length_4_6 / (double)session_count, 2);  
		double step_length_7_9_ratio = NumberUtils.formatDouble(
				(double)step_length_7_9 / (double)session_count, 2);  
		double step_length_10_30_ratio = NumberUtils.formatDouble(
				(double)step_length_10_30 / (double)session_count, 2);  
		double step_length_30_60_ratio = NumberUtils.formatDouble(
				(double)step_length_30_60 / (double)session_count, 2);  
		double step_length_60_ratio = NumberUtils.formatDouble(
				(double)step_length_60 / (double)session_count, 2);  
		
		// 将统计结果封装为Domain对象
		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskid(taskid);
		sessionAggrStat.setSession_count(session_count);  
		sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);  
		sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);  
		sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);  
		sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);  
		sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);  
		sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio); 
		sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);  
		sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio); 
		sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);  
		sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);  
		sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);  
		sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);  
		sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);  
		sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);  
		sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);  
		
		// 调用对应的DAO插入统计结果
		//ISessionAggrStatDAO sessionAggrStatDAO = new SessionAggrStatDAOImpl();  若后面，表现层大量的使用了PersonDAO personDAO=new PersonDAOImpl()，全部改成new PersonDAOJDBCImpl()谈何容易？所以这里引入工场类
		ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
		sessionAggrStatDAO.insert(sessionAggrStat);  
	}
	
	/**
	 * 获取top10热门品类（这个函数的param对不上啊？）
	 * @param filteredSessionid2AggrInfoRDD
	 * @param sessionid2actionRDD
	 */
	private static List<Tuple2<CategorySortKey, String>> getTop10Category(  
			long taskid,  
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		/**
		 * 第一步：获取符合条件的session访问过的所有品类
		 */
		
		// 获取session访问过的所有品类id
		// 访问过：指的是，点击过、下单过、支付过的品类
		//算子flatMapToPair与flatMap的不同在于，flatMapToPair算子处理后返回的是类型是JavaPairRDD<Long, Long>，flatMap处理后返回的是JavaRDD<Long>
		//sessionid2detailRDD，就是代表了通过筛选的session对应的访问明细数据
		//选择算子的依据是什么？map/flatMpa/flatMapToPair
		//map和mapToPair很好判断用谁，但是map和flatMap怎么判断？为什么这里要用flatMap，要压平吗？什么意思啊（下单或者支付可能会对应多个品类的id，以逗号为分隔符，将品类拆分开来，拆开来时使用的是flatMapToPair）
		JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, Long>> call(
							Tuple2<String, Row> tuple) throws Exception {
						//每个sessionid对应的访问明细
						Row row = tuple._2;
						
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						//参考MockData里的"user_visit_action"表，点击过的id
						Long clickCategoryId = row.getLong(6);
						if(clickCategoryId != null) {
							list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));   
						}
						//下过单的品类id
						String orderCategoryIds = row.getString(8);
						if(orderCategoryIds != null) {
							String[] orderCategoryIdsSplited = orderCategoryIds.split(",");  
							for(String orderCategoryId : orderCategoryIdsSplited) {
								list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),
										Long.valueOf(orderCategoryId)));
							}
						}
						//支付过的品类id
						String payCategoryIds = row.getString(10);
						if(payCategoryIds != null) {
							String[] payCategoryIdsSplited = payCategoryIds.split(",");  
							for(String payCategoryId : payCategoryIdsSplited) {
								list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),
										Long.valueOf(payCategoryId)));
							}
						}
						
						return list;
					}
					
				});
		
		/**
		 * 必须要进行去重
		 * 如果不去重的话，会出现重复的categoryid，排序会对重复的categoryid已经countInfo进行排序
		 * 最后很可能会拿到重复的数据
		 */
		//categoryid当然有重复的（代码要和业务结合的很深，否则一些代码真的很难理解）
		categoryidRDD = categoryidRDD.distinct();
		
		/**
		 * 第二步：计算各品类的点击、下单和支付的次数
		 */
		
		// 访问明细中，其中三种访问行为是：点击、下单和支付
		// 分别来计算各品类点击、下单和支付的次数，可以先对访问明细数据进行过滤
		// 分别过滤出点击、下单和支付行为，然后通过map、reduceByKey等算子来进行计算
		
		// 计算各个品类的点击次数
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = 
				getClickCategoryId2CountRDD(sessionid2detailRDD);
		// 计算各个品类的下单次数
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = 
				getOrderCategoryId2CountRDD(sessionid2detailRDD);
		// 计算各个品类的支付次数
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = 
				getPayCategoryId2CountRDD(sessionid2detailRDD);
		
		/**
		 * 第三步：join各品类与它的点击、下单和支付的次数
		 * 
		 * categoryidRDD中，是包含了所有的符合条件的session，访问过的品类id
		 * 
		 * 上面分别计算出来的三份，各品类的点击、下单和支付的次数，可能不是包含所有品类的
		 * 比如，有的品类，就只是被点击过，但是没有人下单和支付
		 * 
		 * 所以，这里，就不能使用join操作，要使用leftOuterJoin操作，就是说，如果categoryidRDD不能
		 * join到自己的某个数据，比如点击、或下单、或支付次数，那么该categoryidRDD还是要保留下来的
		 * 只不过，没有join到的那个数据，就是0了
		 * 
		 */
		JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(
				categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, 
				payCategoryId2CountRDD);
		
		/**
		 * 第四步：自定义二次排序key
		 * （CategorySortKey.java）
		 */
		
		/**
		 * 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
		 */
		JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD.mapToPair(
				
				new PairFunction<Tuple2<Long,String>, CategorySortKey, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<CategorySortKey, String> call(
							//传入的Tuple2<Long, String>，Tuple2<categoryid, value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;>
							Tuple2<Long, String> tuple) throws Exception {
						String countInfo = tuple._2;
						long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
								countInfo, "\\|", Constants.FIELD_CLICK_COUNT));  
						long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
								countInfo, "\\|", Constants.FIELD_ORDER_COUNT));  
						long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
								countInfo, "\\|", Constants.FIELD_PAY_COUNT));  
						//初始化构造函数
						CategorySortKey sortKey = new CategorySortKey(clickCount,
								orderCount, payCount);
						//key是sortKey，按key进行排序；countInfo是各品类的点击，下单，支付信息
						return new Tuple2<CategorySortKey, String>(sortKey, countInfo);  
					}
					
				});
		//这个怎么就实现排序了？Tuple2<CategorySortKey, String>(sortKey, countInfo)，sortKey是一个对象啊，不是一个具体值啊（本来key是数字，数字有默认的比较规则，但是那样直接比较的就不是二次排序了），怎么实现的？
		//自定义了public class CategorySortKey implements Ordered<CategorySortKey>，实现接口Ordered<CategorySortKey>，再把这个接口内部的方法全部自己实现出来，这样就实现自己定的二次排序逻辑，作用到sortByKey算子上
		JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = 
				sortKey2countRDD.sortByKey(false);
		
		/**
		 * 第六步：用take(10)取出top10热门品类，并写入MySQL
		 */
		ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
		
		List<Tuple2<CategorySortKey, String>> top10CategoryList = 
				sortedCategoryCountRDD.take(10);
		
		for(Tuple2<CategorySortKey, String> tuple: top10CategoryList) {
			String countInfo = tuple._2;
			long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_CATEGORY_ID));  
			long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_CLICK_COUNT));  
			long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_ORDER_COUNT));  
			long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_PAY_COUNT));  
			
			Top10Category category = new Top10Category();
			category.setTaskid(taskid); 
			category.setCategoryid(categoryid); 
			category.setClickCount(clickCount);  
			category.setOrderCount(orderCount);
			category.setPayCount(payCount);
			
			top10CategoryDAO.insert(category);  
		}
		
		return top10CategoryList;
	}
	
	/**
	 * 获取各品类点击次数RDD
	 * @param sessionid2detailRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		/**
		 * 说明一下：
		 * 
		 * 这儿，是对完整的数据进行了filter过滤，过滤出来点击行为的数据
		 * 点击行为的数据其实只占总数据的一小部分
		 * 所以过滤以后的RDD，每个partition的数据量，很有可能跟我们之前说的一样，会很不均匀
		 * 而且数据量肯定会变少很多
		 * 
		 * 所以针对这种情况，还是比较合适用一下coalesce算子的，在filter过后去减少partition的数量
		 * 
		 */
		//sessionid2detailRDD是按条件筛选后的访问明细数据，再把索引值为6的字段，且该字段内容不为空的row过滤取出来
		JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(
				
				new Function<Tuple2<String,Row>, Boolean>() {
					
					private static final long serialVersionUID = 1L;
		
					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;  
						//"user_visit_action"表取索引值为6的字段，如果值不为空，返回该字段的值
						return row.get(6) != null ? true : false;
					}
					
				});
//				.coalesce(100);  
		
		/**
		 * 对这个coalesce操作做一个说明
		 * 
		 * 我们在这里用的模式都是local模式，主要是用来测试，所以local模式下，不用去设置分区和并行度的数量
		 * local模式自己本身就是进程内模拟的集群来执行，本身性能就很高
		 * 而且对并行度、partition数量都有一定的内部的优化
		 * 
		 * 这里我们再自己去设置，就有点画蛇添足
		 * 
		 * 但是就是跟大家说明一下，coalesce算子的使用，即可
		 * 
		 */
		//点击品类也可能是多个，为什么用mapToPair而不用faltMapToPair（从业务数据看，一条session只记录一个点击品类，所以用mapToPair;）
		JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
				
				new PairFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Long> call(Tuple2<String, Row> tuple)
							throws Exception {
						long clickCategoryId = tuple._2.getLong(6);
						//这里的1L是指的clickCategoryId不为空的访问量
						return new Tuple2<Long, Long>(clickCategoryId, 1L);
					}
					
				});
		
		/**
		 * 计算各个品类的点击次数
		 * 
		 * 如果某个品类点击了1000万次，其他品类都是10万次，那么也会数据倾斜
		 * 
		 */
		//对其进行reduceByKey。JavaPairRDD<Long, Long>=<clickCategoryId,clickCategoryId对应的总量>
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(
				
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
					
				});
		
		/**
		 * 提升shuffle reduce端并行度
		 */
		
//		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(
//				
//				new Function2<Long, Long, Long>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Long call(Long v1, Long v2) throws Exception {
//						return v1 + v2;
//					}
//					
//				},
//				1000);
		
		/**
		 * 使用随机key实现双重聚合
		 */
		
//		/**
//		 * 第一步，给每个key打上一个随机数
//		 */
//		JavaPairRDD<String, Long> mappedClickCategoryIdRDD = clickCategoryIdRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,Long>, String, Long>() {
//
//					private static final long serialVersionUID = 1L;
//		
//					@Override
//					public Tuple2<String, Long> call(Tuple2<Long, Long> tuple)
//							throws Exception {
//						Random random = new Random();
//						int prefix = random.nextInt(10);
//						return new Tuple2<String, Long>(prefix + "_" + tuple._1, tuple._2);
//					}
//					
//				});
//		
//		/**
//		 * 第二步，执行第一轮局部聚合
//		 */
//		JavaPairRDD<String, Long> firstAggrRDD = mappedClickCategoryIdRDD.reduceByKey(
//				
//				new Function2<Long, Long, Long>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Long call(Long v1, Long v2) throws Exception {
//						return v1 + v2;
//					}
//					
//				});
//		
//		/**
//		 * 第三步，去除掉每个key的前缀
//		 */
//		JavaPairRDD<Long, Long> restoredRDD = firstAggrRDD.mapToPair(
//				
//				new PairFunction<Tuple2<String,Long>, Long, Long>() {
//
//					private static final long serialVersionUID = 1L;
//		
//					@Override
//					public Tuple2<Long, Long> call(Tuple2<String, Long> tuple)
//							throws Exception {
//						long categoryId = Long.valueOf(tuple._1.split("_")[1]);  
//						return new Tuple2<Long, Long>(categoryId, tuple._2);  
//					}
//					
//				});
//		
//		/**
//		 * 第四步，最第二轮全局的聚合
//		 */
//		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = restoredRDD.reduceByKey(
//				
//				new Function2<Long, Long, Long>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Long call(Long v1, Long v2) throws Exception {
//						return v1 + v2;
//					}
//					
//				});
		
		return clickCategoryId2CountRDD;
	}
	
	/**
	 * 获取各品类的下单次数RDD(这里把数个算子功能打包进一个函数里getOrderCategoryId2CountRDD，代码清爽一些，本来是没有最外层的getOrderCategoryId2CountRDD函数，三个算子各自排开)
	 * @param sessionid2detailRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(
				
				new Function<Tuple2<String,Row>, Boolean>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;  
						return row.getString(8) != null ? true : false;
					}
					
				});
		
		//这里用flatMapToPair算子，因为客户可能会对多个品类下单；为什么上面的点击品类也可能是多个，却用mapToPair？（这里就涉及到业务细节，一条session记录的点击品类只有一个，所以用mapToPair；一个session记录却可出现同时对多个品类下单，可以用flatMapToPair）
		//这些都是从实际业务出发的，一条session里面的点击订单记录就一个品类id，而一条session里面的下单或者支付单里面的品类id却可能是多个
		JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, Long>> call(
							Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						String orderCategoryIds = row.getString(8);
						String[] orderCategoryIdsSplited = orderCategoryIds.split(",");  
						
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						
						for(String orderCategoryId : orderCategoryIdsSplited) {
							//遍历每条row的每个orderCategoryIds字段中每个orderCategoryId，然后组成Tuple2<orderCategoryI, 1L>，再把这些Tuple2放进List中返回，构成JavaPairRDD<Long, Long>
							list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));  
						}
						
						return list;
					}
					
				});
		
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(
				
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
					
				});
		
		return orderCategoryId2CountRDD;
	}
	
	/**
	 * 获取各个品类的支付次数RDD
	 * @param sessionid2detailRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(
				
				new Function<Tuple2<String,Row>, Boolean>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;  
						return row.getString(10) != null ? true : false;
					}
					
				});
		
		JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, Long>> call(
							Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						String payCategoryIds = row.getString(10);
						String[] payCategoryIdsSplited = payCategoryIds.split(",");  
						
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						
						for(String payCategoryId : payCategoryIdsSplited) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));  
						}
						
						return list;
					}
					
				});
		
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(
				
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
					
				});
		
		return payCategoryId2CountRDD;
	}
	
	/**
	 * 连接品类RDD与数据RDD
	 * @param categoryidRDD
	 * @param clickCategoryId2CountRDD
	 * @param orderCategoryId2CountRDD
	 * @param payCategoryId2CountRDD
	 * @return
	 */
	private static JavaPairRDD<Long, String> joinCategoryAndData(
			JavaPairRDD<Long, Long> categoryidRDD,
			JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
			JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
			JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
		// 解释一下，如果用leftOuterJoin，就可能出现，右边那个RDD中，join过来时，没有值
		// 所以Tuple中的第二个值用Optional<Long>类型，就代表，可能有值，可能没有值
		//leftOuterJoin:左边表的数据全显示，右边表的数据只显示符合项的数据
		//JavaPairRDD<Long, Long> categoryidRDD指的是筛选过后的点击过、下单过、支付过的品类<categoryid,categoryid>
		//JavaPairRDD<Long, Long> clickCategoryId2CountRDD，各个品类的点击次数<clickCategoryId,count>
		//JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD=< categoryid（clickCategoryId）, Tuple2<clickCategoryId, count>这是一个啥数据情况？get不到啊
		
		//为什么老师一开始就知道join后的类型是JavaPairRDD<Long, Tuple2<Long, Optional<Long>>>这种形式的？
		JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = 
				categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);
		
		JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(
				
				new PairFunction<Tuple2<Long,Tuple2<Long,Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						//拿出来的点击次数是optional类型，就是count是optional类型（optional也是一个类，表示可能有，可能没有的意思）
						Optional<Long> optional = tuple._2._2;
						long clickCount = 0L;
						
						//如果optional存在才获取count的值（如果是orderCategoryId或payCategoryId，这时的针对clickCategoryId的count为空）
						//因为有些categoryid里的是orderCategoryId或payCategoryId，clickCategoryId是空的
						if(optional.isPresent()) {
							clickCount = optional.get();
						}
						
						String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" + 
								Constants.FIELD_CLICK_COUNT + "=" + clickCount;
						//每次返回Tuple2<Long, String>(categoryid, value)，就是categoryid（clickCategoryId）和它对应的count
						//
						return new Tuple2<Long, String>(categoryid, value);  
					}
					
				});
		
		//把上面leftOuterJoin(clickCategoryId2CountRDD)的tmpMapRDD获取出来，再leftOuterJoin(orderCategoryId2CountRDD)
		//(id1,value1)、（id2,value2）、（id3,value3）...
		tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(
				
				new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						String value = tuple._2._1;
						
						Optional<Long> optional = tuple._2._2;
						long orderCount = 0L;
						//左外连接，将categoryid中的orderCategoryId取出来，计算出orderCategoryId的总量count
						if(optional.isPresent()) {
							orderCount = optional.get();
						}
						//返回的这个Tuple2<Long, String>(categoryid, value)就是品类categoryid和它分别对应的点击和下单次数的总量
						//这些结果是不是像自己想的那种，还是打印出来核对比较明显啊（这里面都有很多细小的逻辑）
						value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;  
						
						return new Tuple2<Long, String>(categoryid, value);  
					}
				
				});
		
		tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(
				
				new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						String value = tuple._2._1;
						
						Optional<Long> optional = tuple._2._2;
						long payCount = 0L;
						
						if(optional.isPresent()) {
							payCount = optional.get();
						}
						
						value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;  
						//最后返回了每一个品类categoryid和其对应的点击，下单，支付的对应次数
						return new Tuple2<Long, String>(categoryid, value);  
					}
				
				});
		
		return tmpMapRDD;
	}
	
	/**
	 * 获取top10活跃session
	 * @param taskid
	 * @param sessionid2detailRDD
	 */
	private static void getTop10Session(
			JavaSparkContext sc,
			final long taskid,
			List<Tuple2<CategorySortKey, String>> top10CategoryList,
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		/**
		 * 第一步：将top10热门品类的id，生成一份RDD
		 */
		List<Tuple2<Long, Long>> top10CategoryIdList = 
				new ArrayList<Tuple2<Long, Long>>();
		
		for(Tuple2<CategorySortKey, String> category : top10CategoryList) {
			long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
					category._2, "\\|", Constants.FIELD_CATEGORY_ID));
			top10CategoryIdList.add(new Tuple2<Long, Long>(categoryid, categoryid));  
		}
		
		//光得到List<Tuple2<Long, Long>> top10CategoryIdList这个list还不行，要生成为JavaPairRDD，join上之前的各个session对品类的点击次数，才能取top（n）
		JavaPairRDD<Long, Long> top10CategoryIdRDD = 
				sc.parallelizePairs(top10CategoryIdList);
		
		/**
		 * 第二步：计算top10品类被各session点击的次数
		 */
		//sessionid2detailRDD，就是代表了通过筛选的session对应的访问明细数据
		//JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(filteredSessionid2AggrInfoRDD, sessionid2actionRDD);|
		JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD =
				sessionid2detailRDD.groupByKey();
		//每次用到的关联变量都注释在这里就很快知道
		//JavaPairRDD<Long, String> categoryid2sessionCountRDD=Tuple2<Long, String>(categoryid, value));
		JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailsRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, String>> call(
							Tuple2<String, Iterable<Row>> tuple) throws Exception {
						String sessionid = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();
						
						Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();
						
						// 计算出该session（注意这里的逻辑是针对每条不同session的），对每个品类的点击次数
						while(iterator.hasNext()) {
							Row row = iterator.next();
							//如果点击行为不为空，获取出点击品类的id
							if(row.get(6) != null) {
								long categoryid = row.getLong(6);
								//获取之前该品类的点击次数；如果该品类的点击次数没被获取过，就让这个品类对应的count设置初始值为0
								Long count = categoryCountMap.get(categoryid);
								if(count == null) {
									count = 0L;
								}
								
								count++;
								
								categoryCountMap.put(categoryid, count);
							}
						}
						
						// 返回结果，<categoryid,sessionid,count>格式
						List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();
						
						for(Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
							long categoryid = categoryCountEntry.getKey();
							long count = categoryCountEntry.getValue();
							String value = sessionid + "," + count;
							//返回了过滤后的所有品类（categoryid）及被其某个session点击的次数
							list.add(new Tuple2<Long, String>(categoryid, value));  
						}
						
						return list;
					}
					
				}) ;
		
		// 获取到top10热门品类，被各个session点击的次数
		JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD
				.join(categoryid2sessionCountRDD)
				.mapToPair(new PairFunction<Tuple2<Long,Tuple2<Long,String>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<Long, String>> tuple)
							throws Exception {
						return new Tuple2<Long, String>(tuple._1, tuple._2._2);
					}
					
				});
		
		/**
		 * 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户
		 */
		//获取到top10热门品类，被各个session点击的次数:JavaPairRDD<Long, String> top10CategorySessionCountRDD
		//
		JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD =
				top10CategorySessionCountRDD.groupByKey();
		
		JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<Long,Iterable<String>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String, String>> call(
							Tuple2<Long, Iterable<String>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						Iterator<String> iterator = tuple._2.iterator();
						
						// 定义取topn的排序数组
						String[] top10Sessions = new String[10];   
						
						while(iterator.hasNext()) {
							String sessionCount = iterator.next();
							long count = Long.valueOf(sessionCount.split(",")[1]);  
							
							// 遍历排序数组
							for(int i = 0; i < top10Sessions.length; i++) {
								// 如果当前i位，没有数据，那么直接将i位数据赋值为当前sessionCount
								if(top10Sessions[i] == null) {
									top10Sessions[i] = sessionCount;
									break;
								} else {
									long _count = Long.valueOf(top10Sessions[i].split(",")[1]);  
									
									// 如果sessionCount比i位的sessionCount要大
									if(count > _count) {
										// 从排序数组最后一位开始，到i位，所有数据往后挪一位
										for(int j = 9; j > i; j--) {
											top10Sessions[j] = top10Sessions[j - 1];
										}
										// 将i位赋值为sessionCount
										top10Sessions[i] = sessionCount;
										break;
									}
									
									// 比较小，继续外层for循环
								}
							}
						}
						
						// 将数据写入MySQL表
						List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
						
						for(String sessionCount : top10Sessions) {
							if(sessionCount != null) {
								String sessionid = sessionCount.split(",")[0];
								long count = Long.valueOf(sessionCount.split(",")[1]);  
								
								// 将top10 session插入MySQL表
								Top10Session top10Session = new Top10Session();
								top10Session.setTaskid(taskid);  
								top10Session.setCategoryid(categoryid);  
								top10Session.setSessionid(sessionid);  
								top10Session.setClickCount(count);  
								
								ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
								top10SessionDAO.insert(top10Session);  
								
								// 放入list
								list.add(new Tuple2<String, String>(sessionid, sessionid));
							}
						}
						
						return list;
					}
					
				});
		
		/**
		 * 第四步：获取top10活跃session的明细数据，并写入MySQL
		 */
		JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
				top10SessionRDD.join(sessionid2detailRDD);  
		sessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {  
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				Row row = tuple._2._2;
				
				SessionDetail sessionDetail = new SessionDetail();
				sessionDetail.setTaskid(taskid);  
				sessionDetail.setUserid(row.getLong(1));  
				sessionDetail.setSessionid(row.getString(2));  
				sessionDetail.setPageid(row.getLong(3));  
				sessionDetail.setActionTime(row.getString(4));
				sessionDetail.setSearchKeyword(row.getString(5));  
				sessionDetail.setClickCategoryId(row.getLong(6));  
				sessionDetail.setClickProductId(row.getLong(7));   
				sessionDetail.setOrderCategoryIds(row.getString(8));  
				sessionDetail.setOrderProductIds(row.getString(9));  
				sessionDetail.setPayCategoryIds(row.getString(10)); 
				sessionDetail.setPayProductIds(row.getString(11));  
				
				ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
				sessionDetailDAO.insert(sessionDetail);  
			}
		});
	}
	
}
