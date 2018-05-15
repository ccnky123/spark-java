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
 * customers session analysis Spark job
 * 
 * accept some conditions might be assigned：
 * 
 * 1、Time range: start date ~ end date.
 * 2、sex:man or women
 * 3、Age range
 * 4、Occupation: multiple choice
 * 5、City: multiple choice
 * 6、Multiple search terms, as long as any action in a session has searched for the specified keyword, the session is qualified.
 * 7、Click on the category: multiple categories, as long as any action in the session has clicked on a category, the session is eligible.
 * How does our spark job accept user-created tasks?
 * 
 * the J2EE platform inserts the task information into the MySQL task table after the request for the user creation task, and the task parameter is wrapped in JSON format in task_param.
 * field
 * 
* then the J2EE platform will execute our spark-submit shell script and pass taskid as a parameter to the spark-submit shell script.
* spark-submit shell script, which can receive parameters when executed, and will pass the received parameters to the main function of the spark job.
* the parameters are encapsulated in the args array of main functions.
*
* this is the feature that spark itself provides.
 * 
 * @author Administrator
 *
 */

/**
 * Action: used to suppress a compiler to produce a warning message.
 * @author geo
 *
 */
@SuppressWarnings("unused")
public class user_visit_session {
	
	public static void main(String[] args) {
		// build the Spark context.
// set spark job initialization information.
		SparkConf conf = new SparkConf()
				// set constant interface constants to improve the convenience of later maintenance.
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
		* for example, to get the top10 popular category function, the second order, customize a Key.
		* that key needs to be transmitted over the network during the shuffle, so it is also required to be serialized.
		* when you enable the Kryo mechanism, you will use Kryo to serialize and deserialize CategorySortKey.
		* so here, to get the best performance, register our custom classes.
		* /
		
		JavaSparkContext sc = new JavaSparkContext(conf);
//		sc.checkpointFile("hdfs://");
		SQLContext sqlContext = getSQLContext(sc.sc());
		
		// generate simulated test data.
		SparkUtils.mockData(sc, sqlContext);  
		
		// create DAO components that need to be used.
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		
		// first you have to query the specified task and get the query parameters for the task.
// then the J2EE platform will execute our spark-submit shell script and pass taskid as a parameter to the spark-submit shell script.
// * spark-submit shell script, which can receive parameters when executed, and will pass the received parameters to the main function of the spark job.
// * parameters are encapsulated in the args array of main functions.
/ / *
// * this is the feature that spark itself provides.
// extract the task id from the command line argument (the main function entry set String args[])
		long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
		// the corresponding task information is detected according to taskid.
		Task task = taskDAO.findById(taskid);
		if(task == null) {
			System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");  
			return;
		}
		
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		/ if you want to do session granularity data aggregation.
/**
* actionRDD is a common RDD.
* first, use ationRDD to get a PairRDD with a common sessionid as key.
* second, actionRDD is used in session aggregation.
*
* sessionid is key's PairRDD, which is determined, and is used multiple times in the back.
* 1. Join with the filtered sessionid to obtain the detail data of the session filtered.
* 2. Pass this RDD directly into the aggregateBySession method to perform session aggregation statistics.
*
* after refactoring, actionRDD is used only once, once, to generate RDD with sessionid as key.
*
*/
// first, you need to query the behavior data within the specified date range from the user_visit_action 
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
		//通过用日期参数对user_visit_action进行初步过滤后，就可以进行聚合操作了
		JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);
		
		
		sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());
//		sessionid2actionRDD.checkpoint();
		
		// first, you can group the behavior data in groupByKey according to session_id.
// the granularity of the data at this point is the session granularity, and then the session granularity data can be obtained.
// with user information data, join ("user_info" table and "user_visit_action" table have user_id fields, and join through this field)
// then you can get the data of session granularity, and the data also contains information about the user of the session.
// until here, access to data is < sessionid, (sessionid searchKeywords, clickCategoryIds, age, professional, city, sex) >, to get the data we can continue to filter for the next step
		JavaPairRDD<String, String> sessionid2AggrInfoRDD = 
				aggregateBySession(sc, sqlContext, sessionid2actionRDD);
		
		// then, the data is filtered according to the filtered parameters specified by the user for the aggregate data of session granularity.
// the operator that we write ourselves is to access the external task parameter object.
// so, you remember what we said earlier, anonymous inner classes (operator functions), accessing external objects, using final modification for external objects.
// refactoring, filtering and statistics at the same time.
// there is no perfect spark Java API, and the accumulator () method does not know the parameter meaning.The first parameter is the initial value, and the second argument should be cumulative. Okay?
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
				"", new SessionAggrStatAccumulator());
		
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
				sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
		filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());
		
	// generate public RDD: access detail data by filtering the conditions of session.
/**
* refactoring: sessionid2detailRDD represents the access detail data that corresponds to the selected session.
*/
		JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(
				filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
		sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());
		
	/**
* there is an important explanation for the use of Accumulator, a distributed, cumulative calculation.
*
* from Accumulator, get the data, insert the database, must, must, is after a certain action operation.
* again...
*
* if there is no action, the whole program will not run at all...
*
* if after calculateAndPersisitAggrStat method, run an action operation, such as the count, take
* no!!!
*
* must be able to trigger job execution before finally writing to MySQL.
*
* the calculated results, in J2EE, are shown in two bar charts.
*/
		
		randomExtractSession(sc, task.getTaskid(), 
				filteredSessionid2AggrInfoRDD, sessionid2detailRDD);
		
		/**
* special instructions
* we know that to get the session aggregate statistics for the last function, it must be after an action action has triggered the job.
* the data can be obtained from Accumulator, otherwise the data cannot be obtained, because there is no job execution, the Accumulator value is empty.
* so, here, we'll put the implementation code of the randomly selected function before the final calculation and writing library of the session aggregation statistics function.
* because of the random extraction function, there is a countByKey operator, which is action, which will trigger job.
*/
// calculate the session proportion of each range and write to MySQL.
		calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),
				task.getTaskid());
		
		/**
* session aggregation statistics (statistics of the length of visit and length of visit, the proportion of session in each interval to the total session number)
*
* if you do not refactor, you can directly implement the idea:
* 1, actionRDD, map to <sessionid,Row> format.
* 2. Aggregated by sessionid, calculated the duration and length of each session and generated a new RDD.
* 3. Iterate through the newly generated RDD, and update the corresponding values of the custom Accumulator with each session's access length and access step length.
* 4. Use the statistic values in the Accumulator to calculate the ratio of each interval.
* 5. The final calculated result is written to the table of MySQL.
*
* common implementation thoughts:
* 1, why use actionRDD to map?Actually, we've already done the mapping in session aggregation.redundant
* 2. Do you have to go through the session alone for the purpose of gathering the session?There is no need to have session data.
* when you filter session before, it's like, you're walking through the session, so there's no need to filter it again.
*
* reconstructing implementation ideas:
* 1. Do not generate any new RDD (handling billions of data)
* 2. Do not go through the session data alone (processing tens of millions of data)
* 3. When the session is aggregated, the duration and length of each session are calculated directly.
* 4. When filtering, it is necessary to iterate through all the aggregation session information, and then, after a session is filtered.
* add the length of the visit and the access step to the custom Accumulator.
* 5. It is two very different ways of thinking and implementation, which can even save time in the face of billions and millions of data.
* half an hour, or hours.
*
* some guidelines for developing large and complex projects for Spark:
* 1. Generate RDD as little as possible.
* 2. Minimize the operator operation of RDD. If possible, try to implement multiple functions in an operator.
* 3, as little as possible, the shuffle operator operation of RDD, such as groupByKey, reduceByKey, sortByKey (map, mapToPair)
* shuffle, which can cause a large amount of disk reads and writes, severely reducing performance (the shuffle requires bandwidth and disk I/O).
* there are shuffle operators, and no shuffle operators, or even performance, which can reach dozens of minutes or even hours of difference.
* with the operator of shfufle, it is very easy to cause data skew. Once the data is tilted, it is a performance killer (complete solution).
* 4. No matter what the function is, the performance is first (from the perspective of big data development, but different areas have different starting points)
* in traditional J2EE or.net, PHP, software/system/web development, I think architecture and maintainability and scalability are important.
* degree, far higher than the performance of a large number of distributed architecture, design patterns, code division, the division of classes (except high concurrency website) (through architecture can solve most of the performance bottleneck problem?)
*
* in big data projects, such as MapReduce, Hive, Spark and Storm, I think the performance is far more important than some code.
* specification, design pattern, code division, class division;Big data, big data, and most importantly, performance.
* mainly because of the big data and the characteristics of big data projects, the process of big data and the speed of the project are relatively slow.
* if you don't give priority to performance, a big data handler can run for hours or even dozens of hours.
* this is a disaster for the user experience.
*
* therefore, it is recommended that big data projects should give priority to performance in the framework of development and code.Second, consider the division and decoupling of functional code.
*
* if we adopt the first implementation scheme, then is code division (decoupling, maintainable) priority, design first, then perform specific functions separately for each operator, not with a lot of features, but each function separately to different operator, the computer will produce new RDD and implement a new operator, impact performance)
* if the second option is adopted, performance is a priority.
*
* when we talk about this, we should not think that I am diverging from the topic, and we should not feel that the program is a simple project.
* coding is the most important, in fact, the project, I think, the most important, besides the technology itself and the project experience;Very important point, yes.
* accumulated experience in dealing with various problems.
*
*/
		
		// get top10 favorite categories.
		List<Tuple2<CategorySortKey, String>> top10CategoryList = 
				getTop10Category(task.getTaskid(), sessionid2detailRDD);
		// get top10 active session;JavaSparkContext sc = new JavaSparkContext(conf);Why pass sc?(source: generate RDD from list)
		getTop10Session(sc, task.getTaskid(), 
				top10CategoryList, sessionid2detailRDD);
		
		
		sc.close(); 
	}

	/**
* get SQLContext
* the SQLContext object is generated if it is a local test environment.
* the HiveContext object is generated if it is running in a production environment.
* @ param sc SparkContext
* @ return SQLContext
*/
	
	private static SQLContext getSQLContext(SparkContext sc) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc);
		}
	}
	
	/**
* generate simulated data (only local mode will generate simulated data)
* @ param sc
* @ param sqlContext
*/
	private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			MockData.mock(sc, sqlContext);  
		}
	}
	
	/**
* gets the user access behavior data within the specified date range.
* @ param sqlContext sqlContext
* @param taskParam task parameter.
* @return behavior data RDD.
*/
	private static JavaRDD<Row> getActionRDDByDateRange(
			SQLContext sqlContext, JSONObject taskParam) {
	
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

		String sql = 
				"select * "
				+ "from user_visit_action "
				+ "where date>='" + startDate + "' "
				+ "and date<='" + endDate + "'";  
//				+ "and session_id not in('','','')"
		
		
		DataFrame actionDF = sqlContext.sql(sql);
		
		/**
* there is a good chance that this will happen.
* for example, Spark SQl sets 20 tasks for the first stage by default, but based on your data size and the complexity of the algorithm.
* in fact, you need 1000 tasks to execute in parallel.
*
* so, here, you can perform repartition repartition operations on the RDD that Spark SQL just queried.
*/
// return actionDF. JavaRDD (.) repartition (1000);
// through the user-specified date parameters, the initial filtering has been done.
		return actionDF.javaRDD();
	}
	
	/**
* get the RDD of sessionid2 to the mapping of access behavior data (map the sessionid in the user_visit_action table to its corresponding row)
* @ param actionRDD
* @ return
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
	
	/* *
* aggregate behavior data by session granularity.
* @param actionRDD behavioral data RDD.
* @return session granularity aggregation data.
*/
	private static JavaPairRDD<String, String> aggregateBySession(
			JavaSparkContext sc,
			SQLContext sqlContext, 
			JavaPairRDD<String, Row> sessinoid2actionRDD) {
		JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = 
				sessinoid2actionRDD.groupByKey();
			JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
				
				new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {
					
					private static final long serialVersionUID = 1L;
		
					@Override
					public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple)
							throws Exception {
						String sessionid = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();
						
						StringBuffer searchKeywordsBuffer = new StringBuffer("");
						StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
						
						Long userid = null;
						
						Date startTime = null;
						Date endTime = null;
					
						int stepLength = 0;
						
						while(iterator.hasNext()) {
							Row row = iterator.next();
							if(userid == null) {
								userid = row.getLong(1);
							}
							String searchKeyword = row.getString(5);
							Long clickCategoryId = row.getLong(6);
							
							// actually, here's the data description.
// not every line of access has a searchKeyword clickCategoryId field (this is about to change the code to the business understanding)
// in fact, only search behavior has a searchKeyword field.
// there are clickCategoryId fields only if you click on the category behavior.
// therefore, it is impossible for any line of behavior data to have two fields, so the data is likely to be null.
// we decide whether to concatenate the search word or the category id into a string.
// first, be satisfied: it cannot be null.
// second, there is no search term or category id in the previous string.
							
							if(StringUtils.isNotEmpty(searchKeyword)) {
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
							
							Date actionTime = DateUtils.parseTime(row.getString(4));
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
							
							stepLength++;
						}
						
						String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
						String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
						
						long visitLength = (endTime.getTime() - startTime.getTime()) / 1000; 
						
						// think about it.
// the format of the data we return, even if <sessionid,partAggrInfo> (partial aggregate data: partAggrInfo, because this is only the click category and search term)
// however, when this step is done, we need to aggregate each row of data with the corresponding user information.
// the problem is, if it is aggregated with user information, then key, it should not be sessionid.
// it should be userid to be able to aggregate the user information from the <userid,Row> format (but is sessioinid and userid the same?)
// if we return directly to <sessionid,partAggrInfo>, we have to do the mapToPair operator again.
// map RDD to <userid,partAggrInfo> format, and so on.
// so, we can actually go straight back to the data format, which is <userid,partAggrInfo>.
// then the partAggrInfo is associated with the userInfo when you join the user information.
// then set the key of the returned Tuple to sessionid.
// the final data format, or <sessionid,fullAggrInfo> (here the fullAggrInfo includes the user_info information)
// aggregate data, in what format?(the information is obtained, how to splice it into a string class)
// we have a unified definition here, using key=value|key=value (at this point, partAggrInfo includes sessionid, searchKeywords, clickCategoryIds, which need to be spliced together)String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
								+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
								+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
								+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
								+ Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
								+ Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);    
						return new Tuple2<Long, String>(userid, partAggrInfo);
					}
					
				});
		
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
