import kafka.serializer.StringDecoder
import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
//Google API
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver
import com.google.api.client.googleapis.auth.oauth2.{GoogleAuthorizationCodeFlow, GoogleClientSecrets}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.HttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.FileDataStoreFactory
import com.google.api.services.fusiontables.Fusiontables
//import com.google.api.services.fusiontables.Fusiontables.Query.Sql;
//import com.google.api.services.fusiontables.Fusiontables.Table.Delete;
import java.io.{IOException, InputStreamReader}
import java.util.{Collections, Date}
import com.google.api.services.fusiontables.FusiontablesScopes
import scala.collection.JavaConversions._



object sparkStreaming {
  private val APPLICATION_NAME = "GFT"
  private val DATA_STORE_DIR = new java.io.File(System.getProperty("user.home"), ".store/fusion_tables_sample")
  private var dataStoreFactory: FileDataStoreFactory = _
  private var httpTransport: HttpTransport = _
  private val JSON_FACTORY = JacksonFactory.getDefaultInstance
  var mapIDtoRowID = scala.collection.mutable.Map[Int, Int]()
  private var fusiontables: Fusiontables = _
  val tableId = "1CYZAQQcB4Ivc8STXLE6pa8Dyf3c9oWMHS58Vyfr9"

  /** Connect to the Google API based on a Two Legged OAUTH 2.0 protocol
    * Details of the process are summarised below:
    *   Create a new project in the Google API Developer's console
    *   Create a service account for the project in the Developers Console.
    *   Delegate domain-wide access to the service account.
    *   Now the application prepares to make authorized API calls by using the service account's credentials to request
    *   an access token from the OAuth 2.0 auth server.
    *   Finally, the application can use the access token to call Google APIs.
    * Details of the protocol is available at https://developers.google.com/identity/protocols/OAuth2ServiceAccount
    * */
  private def connectToGFT(): Unit = {
    try {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport()
      dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR)
      val credential = authorize()
      fusiontables = new Fusiontables.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName(APPLICATION_NAME).build()
      createMapForLookup(tableId)
      return
    } catch {
      case e: IOException => System.err.println(e.getMessage)
      case t: Throwable => t.printStackTrace()
    }
    System.exit(1)
  }
  /** Authorizes the installed application to access user's protected data.
    * This function is completely made up of Google API calls. In summary, the application (implicitly authorised by the
    * user) reads a json file containing the credentials of a user authorised to access the API.
    * The json file structure is as follows:
    * {
    *   "installed":{
    *     "client_id":"123456789-gnvsjj6s3joe423j7f6ovc56pd.apps.googleusercontent.com",
    *     "project_id":"abcd-123",
    *     "auth_uri":"https://accounts.google.com/o/oauth2/auth",
    *     "token_uri":"https://accounts.google.com/o/oauth2/token",
    *     "auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs",
    *     "client_secret":"EFUY3nWAjr7wZiKwN8",
    *     "redirect_uris":["urn:ietf:wg:oauth:2.0:oob","http://localhost"]
    *     }
    * }
    * The json file has to be in the local path of the code (in IntelliJ structure, inside the src folder). This file
    * can be obtained from the Google API Developers Console.
    * */
  private def authorize(): Credential = {
    val clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(classOf[FusionTablesSample].getResourceAsStream("/client_secret_410627372154-gnvsjj6s3joefaf7ct423j7f6ovc56pd.apps.googleusercontent.com.json")))
    if (clientSecrets.getDetails.getClientId.startsWith("Enter") ||
      clientSecrets.getDetails.getClientSecret.startsWith("Enter ")) {
      println("Enter Client ID and Secret from https://code.google.com/apis/console/?api=fusiontables " +
        "into fusiontables-cmdline-sample/src/main/resources/client_secrets.json")
      System.exit(1)
    }
    val flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY, clientSecrets, Collections.singleton(FusiontablesScopes.FUSIONTABLES))
      .setDataStoreFactory(dataStoreFactory)
      .build()
    new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver())
      .authorize("410627372154-gnvsjj6s3joefaf7ct423vc56pd.apps.googleusercontent.com")
  }
  /** Create a Map of Weather Station ID to Fusion Table Row IDs
    * On connecting to the remote service for the first time, a map of the OpenWeatherMap station ID to the Fusion Table
    * Row ID is created. This is required because the only way to lookup the table for updates / deletes is through its
    * row id. Rather than add a query every time to find if the row exists, this function, called once before initiating
    * the streaming process, connects to the database table, queries it for the row ids and station ids and creates a
    * Map that can then be looked up to decide whether an insert or update is required.
    *
    * The difficulty with Google Fusion Tables is that Row IDs are allocated by the API and is known only on completion
    * of a successful insert. Therefore, whenever an insert is done, the response received back (REST JSON) containing
    * the Row ID is to be updated in the map.
    *
    * Complete details of the Google API are provided at https://developers.google.com/fusiontables/
    * */
  private def createMapForLookup(tableId: String): Unit = {
    mapIDtoRowID.empty
    val sql = fusiontables.query.sql("SELECT rowid, id FROM " + tableId)
    try {
      val x = sql.execute()
      val listRows = x.getRows
      println(listRows)
      if (listRows != null) {
        for (myArray <- listRows) {
          val x: Int = myArray.get(0).toString.toInt
          val y: Int = myArray.get(1).toString.toInt
          mapIDtoRowID += (y -> x)
        }
        println(mapIDtoRowID)
      }
    } catch {
      case e: IllegalArgumentException =>
    }
  }
  def main(args: Array[String]) {
    connectToGFT()
    StreamingExamples.setStreamingLogLevels() //Set reasonable logging for streaming if user hasn't configured log4j.
    val topics = "test"
    val brokers = "localhost:9092"
    val topicsSet = topics.split(",").toSet
    val sparkConf = new SparkConf().setAppName("KafkaWeatherCalc").setMaster("local") //spark://localhost:7077
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(60))
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    messages.foreachRDD(rdd => {
      if (rdd.isEmpty()) {
        println("Failed to get data from Kafka. Please check that the Kafka producer is streaming data.")
        System.exit(-1)
      }
      val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(rdd.sparkContext)
      val weatherDF = sqlContext.read.json(rdd.map(_._2)).toDF()
      val aggExpressions: Map[String, String] =
        Map(("temp", "mean"), ("humidity", "mean"), ("windspeed", "mean"), ("winddirection", "mean"), ("pressure", "mean"), ("time", "max"))
      //weatherDF.groupBy("id").agg(aggExpressions).foreach(t => println(t.getAs("avg(windspeed)")))
      weatherDF.groupBy("id", "location", "latitude", "longitude").agg(aggExpressions).foreach(t => {
        val id: Int               = t.getAs("id").toString.toInt
        val location: String      = t.getAs("location").toString
        val latitude: Double      = t.getAs("latitude").toString.toDouble
        val longitude: Double     = t.getAs("longitude").toString.toDouble
        val temperature: Double   = t.getAs("avg(temp)").toString.toDouble
        val humidity: Double      = t.getAs("avg(humidity)").toString.toDouble
        val windspeed: Double     = t.getAs("avg(windspeed)").toString.toDouble
        val winddirection: Double = t.getAs("avg(winddirection)").toString.toDouble
        val pressure: Double      = t.getAs("avg(pressure)").toString.toDouble
        val time: Long            = t.getAs("max(time)").toString.toLong
        val date: String          = new Date (time * 1000L).toString
        writeToDB(id, location, latitude, longitude, temperature, humidity, windspeed, winddirection, pressure, date)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
  /**
    * Write the row of data (Update if exists, Insert if not)
    * A row received from the processed batch may correspond to an entry already existing in the table or not. Therefore,
    * every row is first checked for existence in the local Map. If it exists, the table is updated. If not, the data is
    * inserted into the table which also updates the local Map.
    *
    * @param id
    * @param location
    * @param latitude
    * @param longitude
    * @param temperature
    * @param humidity
    * @param windspeed
    * @param winddirection
    * @param pressure
    * @param date
    */
  private def writeToDB(id: Int, location: String, latitude: Double, longitude: Double, temperature: Double,
                        humidity: Double, windspeed: Double, winddirection: Double, pressure: Double, date:String) = {
    //insertData(tableId, id, location, latitude, longitude, temperature, humidity, windspeed, winddirection, pressure, date)
    //updateData(tableId, id: Int, temperature, humidity, windspeed, winddirection, pressure, date)
    if (mapIDtoRowID.contains(id)) {
      println("Update")
      updateData(tableId: String, id: Int, temperature: Double, humidity: Double, windspeed: Double, winddirection: Double,
        pressure: Double, date:String)
    } else {
      println("Insert")
      insertData(tableId, id, location, latitude, longitude, temperature, humidity, windspeed, winddirection, pressure, date)
    }
  }

  /**
    * Insert Data into the Google Fusion Table.
    * This function is called when it a record is not existing in the table.
    * TODO: Simplify the SQL string. It is too complicated right now, like in PHP :(
    *
    * @param tableId
    * @param id
    * @param location
    * @param latitude
    * @param longitude
    * @param temperature
    * @param humidity
    * @param windspeed
    * @param winddirection
    * @param pressure
    * @param date
    */
  private def insertData(tableId: String, id: Int, location: String, latitude: Double, longitude: Double,
                         temperature: Double, humidity: Double, windspeed: Double, winddirection: Double,
                         pressure: Double, date:String): Unit = {
    var sqlString = "INSERT INTO " + tableId
    sqlString += " (id, place, location, windspeed, winddirection, pressure, humidity, temperature, time) VALUES ("
    val values = id.toString + ", '" + location + "', '" + latitude + "," + longitude + "', '" + windspeed.toString + "', '" +
      winddirection.toString + "', '" + pressure.toString + "', '" + humidity.toString + "', '" + temperature.toString + "', '" + date + "')"
    sqlString += values
    println("\n" + sqlString)
    val sql = fusiontables.query().sql(sqlString)
    try {
      val sqlResponse = sql.execute()
      val listRows = sqlResponse.getRows
      for (myArray <- listRows) {
        val rowid: Int = myArray.get(0).toString.toInt
        mapIDtoRowID += (id -> rowid)
        println(rowid)
      }
    } catch {
      case e: IllegalArgumentException => println(e.getMessage)
    }
  }

  /**
    * Update data in the Google Fusion Table.
    * This function is called when a record with the Station ID already exists in the table
    * TODO: Simplify the SQL string. It is too complicated right now, like in PHP :(
    *
    * @param tableId
    * @param id
    * @param temperature
    * @param humidity
    * @param windspeed
    * @param winddirection
    * @param pressure
    * @param date
    */
  private def updateData(tableId: String, id: Int, temperature: Double, humidity: Double, windspeed: Double,
                         winddirection: Double, pressure: Double, date:String): Unit = {
    var sqlString = "UPDATE " + tableId
    var setValues = " SET windspeed = " + windspeed.toString + ", winddirection = " + winddirection.toString
    setValues += ", pressure = " + pressure.toString + ", humidity = " + humidity.toString + ", temperature = " + temperature.toString
    setValues += ", time = '" + date
    sqlString += setValues
    sqlString += "' WHERE ROWID = '"
    sqlString += mapIDtoRowID.apply(id).toString
    sqlString += "'"
    println("\n" + sqlString)

    val sql = fusiontables.query().sql(sqlString)
    try {
      sql.execute()
      println("Updated " + tableId + " " + id.toString)
    } catch {
      case e: IllegalArgumentException => println(e.getMessage)
    }
  }
}
class FusionTablesSample{}
/*
      weatherDF.registerTempTable("weather")
      weatherDF.show()
      val weatherAggregateDF = sqlContext.sql("SELECT id, AVG(temp), AVG(humidity), AVG(winddirection), AVG(windspeed) FROM weather GROUP BY id")
      weatherAggregateDF.show()
      weatherAggregateDF.foreach(t => {
        Check if the id exists in the DB.
        //If it exists, update, else insert
        println(t.get(0))
      })  //Working SQL Code
 */

/**
  * The combineByKey is one of the most complex distributed RDD function. The function takes
  * three functions and a variable.
  * The first function is the Combiner. It generates a tuple: (K, (V,1))
  * The second function is the mergeValue. In this function, the combiner functions per partition is merged
  * using the merge value function for every key. After this phase the output of every (K, (V, 1)) is
  * (K, (total, count)) in every partition.
  * The third function is the mergeCombiners. This function merges all values across the partitions in the executors
  * and sends the data back to the driver. After this phase the output of every (key, (total, count)) per partition
  * is (K, (totalAcrossAllPartitions, countAcrossAllPartitions)).
  * The map converts the (key, tuple) = (key, (totalAcrossAllPartitions,  countAcrossAllPartitions)) to compute the
  * average per key as (key, tuple._1/tuple._2).
  * TODO: See if the inputDStream can be loaded into a dataframe for simple SQL calculations. Much better for
  * tabular data.
  */
/*val result: DStream[(String, Float)] = messages.combineByKey(
  (v) => (v.toInt, 1),
  (acc: (Int, Int), v) => (acc._1 + v.toInt, acc._2 + 1),
  (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2),
  hashPartitioner
).map{ case (key, value) => (key, value._1 / value._2.toFloat) }
result.print()
