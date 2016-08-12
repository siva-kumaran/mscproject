import java.util.{Date, Properties}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import scala.util.parsing.json._
import java.time.Instant
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Cluster, ResultSet, Row, Statement}
import scala.collection.JavaConverters._
import java.util.Calendar
//Permitted 60 calls per minute
/**
  * This program connects to the www.openweathermap.org website at regular intervals using its ReST API and gets weather
  * data. The website does not have any credential verification but requires the registration and the subsequent
  * inclusion of an API Key in the ReST request. More details are available in the website.
  *
  * On obtaining the data in JSON format, it is parsed, restructured into a simpler JSON string with just the fields
  * for the requirement and published to Kafka.
  */
object ScalaProducerExample extends App {
  val nCycleIntervalInSeconds = 10  //Seconds. Constant
  val owmApiKey = "a7a75f7750e675a80cf89cb0b9"
  val apiBB = "-3,56,0,60,100"  /*  bbox [lon-left,lat-bottom,lon-right,lat-top]  */
  val restURL = "http://api.openweathermap.org/data/2.5/box/city?bbox=" + apiBB + "cluster=no&appid=" + owmApiKey
  val kafkaTopic = "test"
  val kafkaBrokers = "localhost:9092"
  val props = new Properties()
  props.put("metadata.broker.list", kafkaBrokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "async")
  //props.put("request.required.acks", "1")
  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)
  val cluster =  Cluster.builder().addContactPoint("localhost").build()
  val session = cluster.connect("weather_raw_data")
  /**Make a REST API call to the Server
    * Core Scala wget/curl equivalent
    * @param url
    * @return
    */
  def get(url: String): String = {
    scala.io.Source.fromURL(url).mkString
  }
  /** Call the passed function every nCycleIntervalInSeconds seconds
    * This is a simple event loop that gets called at regular, timed intervals.
    * @param callback
    */
  def eventTimer(callback: () => Unit) {
    while (true) { callback(); Thread sleep nCycleIntervalInSeconds*1000 }
  }
  /** Acquire, process and publish to the Kafka server.
    * This function, called within the event loop, sends a ReST API request to the server and receives a string in
    * JSON format. In the bounding box feature, a rectangle specified by its bottom-left and top-right corners are
    * passed along with the request and the reply contains weather details for all stations within the bounding box.
    * {
    *   "cod":"200",
    *   "calctime":0.0059,
    *   "cnt":3,
    *   "list":[
    *      {
    *       "id":2640967,
    *       "name":"Omagh",
    *       "coord":{"lon":-7.3, "lat":54.599998},
    *       "main":{
    *           "temp":13.39,
    *           "temp_min":13.387,
    *           "temp_max":13.387,
    *           "pressure":1006.13,
    *           "sea_level":1024.66,
    *           "grnd_level":1006.13,
    *           "humidity":91},
    *           "dt":1465598141,
    *           "wind":{"speed":1.29, "deg":18.0058},
    *           "clouds":{"all":80},
    *           "weather":[ {"id":803, "main":"Clouds", "description":"broken clouds", "icon":"04n" } ]
    *         }
    *       },
    *       {
    *         "id":2962961,
    *         "name":"Letterkenny",
    *         "coord":{"lon":-7.73333,"lat":54.950001},
    *         "main":{
    *             "temp":12.87,
    *             "temp_min":12.87,
    *             "temp_max":12.87,
    *             "pressure":1017.67,
    *             "sea_level":1024.45,
    *             "grnd_level":1017.67,
    *             "humidity":99},
    *             "dt":1465598251,
    *             "wind":{"speed":1.71,"deg":49.5005},
    *             "clouds":{"all":80},
    *             "weather":[ {"id":803, "main":"Clouds", "description":"broken clouds", "icon":"04n"} ]
    *          }
    *        }
    *     ]
    *  }
    * Among the fields returned in the reply, id, name, latitude, longitude, temperature, pressure, humidity, wind
    * speed and direction are restructured into a simple JSON string and written to the Kafka cluster.
    */
  def processCycle() {
    val startTime = System.nanoTime()
    println("Initiating acquisition...")
    val weatherBox: String = get(restURL)
    val wbJSON = JSON.parseFull(weatherBox)
    val listWeatherReports = wbJSON.get.asInstanceOf[Map[String, Any]]
    val cod:String = listWeatherReports.get("cod").get.asInstanceOf[String]
    val calctime:Double = listWeatherReports.get("calctime").get.asInstanceOf[Double]
    val cnt:Double = listWeatherReports.get("cnt").get.asInstanceOf[Double]
    val stations: List[Any] = listWeatherReports.get("list").get.asInstanceOf[List[Any]]
    stations.foreach(f = stationMap => {
      val station: Map[String, Any] = stationMap.asInstanceOf[Map[String, Any]]
      val id: Double = station.get("id").get.asInstanceOf[Double]
      val name: String = station.get("name").get.asInstanceOf[String]
      val location: Map[String, Double] = station.get("coord").get.asInstanceOf[Map[String, Double]]
      val latitude: Double = location.get("lat").get.asInstanceOf[Double]
      val longitude: Double = location.get("lon").get.asInstanceOf[Double]
      val measurements: Map[String, Double] = station.get("main").get.asInstanceOf[Map[String, Double]]
      val temperature: Double = measurements.get("temp").get.asInstanceOf[Double]
      val pressure: Double = measurements.get("pressure").get.asInstanceOf[Double]
      val humidity: Double = measurements.get("humidity").get.asInstanceOf[Double]
      val wind: Map[String, Double] = station.get("wind").get.asInstanceOf[Map[String, Double]]
      val windspeed: Double = wind.get("speed").get.asInstanceOf[Double]
      val winddirection: Double = wind.get("deg").get.asInstanceOf[Double]
      val telegram = Map("time" -> Instant.now().getEpochSecond(), "id" -> id.toInt, "location" -> name, "latitude" -> latitude, "longitude" -> longitude, "temp" -> temperature, "pressure" -> pressure, "humidity" -> humidity, "windspeed" -> windspeed, "winddirection" -> winddirection)
      val packet = JsonUtil.toJson(telegram)
      //println(packet)
      val key = "unitedkingdom" //Meant to signify weather data from UK //id.toString
      val t = new KeyedMessage[String, String](kafkaTopic, key, packet)
      producer.send(t)
      insertIntoCassandra(telegram)
    })
    val finishTime = System.nanoTime()
    val processingTime = (finishTime-startTime)/1000000000.0
    println(cod.toInt)
    println("Retrieval Time at Server: " + calctime)
    println("Number of Weather Stations: " + cnt.toInt)
    println("Client Side Processing time: " + processingTime + " seconds")
    val cTime:  java.util.Date =new java.util.Date(System.currentTimeMillis)
    println("Time: " + cTime)
    println("----------------------")
  }
  def testConnection(): Unit = {
    val cTime:  java.util.Date =new java.util.Date(System.currentTimeMillis);
    println("Time: " + cTime)
    var key = "K1"
    var packet = "14"
    var t = new KeyedMessage[String, String](kafkaTopic, key, packet)
    producer.send(t)
    println(packet)
    key = "K2"
    packet = "15"
    t = new KeyedMessage[String, String](kafkaTopic, key, packet)
    producer.send(t)
    println(packet)
    key = "K1"
    packet = "16"
    t = new KeyedMessage[String, String](kafkaTopic, key, packet)
    producer.send(t)
    println(packet)
    key = "K2"
    packet = "17"
    t = new KeyedMessage[String, String](kafkaTopic, key, packet)
    producer.send(t)
    println(packet)
    val unixTime = System.currentTimeMillis() / 1000L
    val unixTimestamp = Instant.now().getEpochSecond()
    println("unixTime: " + unixTime + ", unixTimestamp: " + unixTimestamp)
  }
  def weatherStub(): Unit = {
    var key = "weather"
    var packet = "{\"windspeed\":7.7,\"pressure\":999.0,\"location\":\"Dundee\",\"latitude\":56.5,\"longitude\":-2.96667,\"id\":2650752,\"humidity\":67.0,\"temp\":12.85,\"winddirection\":280.0,\"time\":1467387581}"
    var t = new KeyedMessage[String, String](kafkaTopic, key, packet)
    producer.send(t)
    packet = "{\"windspeed\":7.7,\"pressure\":999.0,\"location\":\"Saint Andrews\",\"latitude\":56.338711,\"longitude\":-2.79902,\"id\":2638864,\"humidity\":67.0,\"temp\":19.01,\"winddirection\":280.0,\"time\":1467387581}"
    t = new KeyedMessage[String, String](kafkaTopic, key, packet)
    producer.send(t)
    val unixTime = System.currentTimeMillis() / 1000L;
    println(unixTime + ": Sent Data")
  }
  def stubForRickshaw(): Unit = {
    val unixTimestamp = Instant.now().getEpochSecond()
    val r = scala.util.Random
    val key = "weather"
    var x = r.nextDouble() * 20.0
    var packet = "London," + unixTimestamp + "," + x
    var t = new KeyedMessage[String, String](kafkaTopic, key, packet)
    producer.send(t)
    println(packet)
    x = r.nextDouble() * 40.0
    packet = "Edinburgh," + unixTimestamp + "," + x
    t = new KeyedMessage[String, String](kafkaTopic, key, packet)
    producer.send(t)
    println(packet)
    x = r.nextDouble() * 60.0
    packet = "Cardiff," + unixTimestamp + "," + x
    t = new KeyedMessage[String, String](kafkaTopic, key, packet)
    producer.send(t)
    println(packet)
    x = r.nextDouble() * 80.0
    packet = "Belfast," + unixTimestamp + "," + x
    t = new KeyedMessage[String, String](kafkaTopic, key, packet)
    producer.send(t)
    println(packet)
    println("----------------------------------------------------")
  }
  def insertIntoCassandra(data: Map[String, Any]): Unit = {
    /*
    id text,
    date text,
    eventtime timestamp,
    humidity float,
    pressure float,
    temperature float,
    winddirection float,
    windspeed float,
    PRIMARY KEY ((id, date), eventtime)
    Map("time" -> Instant.now().getEpochSecond(), "id" -> id.toInt, "location" -> name, "latitude" -> latitude,
    "longitude" -> longitude, "temp" -> temperature, "pressure" -> pressure, "humidity" -> humidity,
    "windspeed" -> windspeed, "winddirection" -> winddirection)
     */
    val mydate: Calendar = Calendar.getInstance();
    mydate.setTimeInMillis(data.get("time").get.asInstanceOf[Long] * 1000);
    //println(mydate.get(Calendar.DAY_OF_MONTH));
    val statement: Statement = QueryBuilder.insertInto("weather_raw_data", "records_by_day")
      .value("id", data.get("id").get.asInstanceOf[Int].toString)
      .value("date", mydate.get(Calendar.DAY_OF_MONTH).toString)
      .value("eventtime", data.get("time").get.asInstanceOf[Long] * 1000)
      .value("humidity", data.get("humidity").get.asInstanceOf[Double])
      .value("pressure", data.get("pressure").get.asInstanceOf[Double])
      .value("temperature", data.get("temp").get.asInstanceOf[Double])
      .value("winddirection", data.get("winddirection").get.asInstanceOf[Double])
      .value("windspeed", data.get("windspeed").get.asInstanceOf[Double])
    val x = session.execute(statement)
  }
  //eventTimer(processCycle)
  //eventTimer(testConnection)
  //eventTimer(weatherStub)
  eventTimer(stubForRickshaw)
  producer.close()
}
