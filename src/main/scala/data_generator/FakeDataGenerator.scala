package data_generator

import java.time.Instant
import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import play.api.libs.json.Json
import play.api.libs.json.Json._

import scala.concurrent.duration._
import scala.math._
import scala.util.Random

case class Location(latitude: Double, longitude: Double)

case class Data(deviceId: String, var temperature: Int = 0, var location: Location = null, var time: Instant = Instant.now())

case class DataParameters(maxTemperature: Int, minTemperature: Int, deltaTemperature: Int, maxLatitude: Double, minLatitude: Double, maxLongitude: Double, minLongitude: Double, deltaLocation: Double)

object FakeDataGenerator extends App {

    import DataGenerator._

    val system: ActorSystem = ActorSystem("FakeDataGenerator")

    val config = system.settings.config.getConfig("akka.kafka.producer")

    val producerSettings: ProducerSettings[String, String] =
        ProducerSettings(config, new StringSerializer, new StringSerializer)
          .withBootstrapServers("localhost:9092")

    val topicName = "iot-topic"

    // some random chosen values, should come from configuration
    val maxTemperature = 250
    val minTemperature = -20
    val deltaTemperature = 15

    val maxLatitude = 90
    val minLatitude = -66.5 // exclude South Pole
    val maxLongitude = 180
    val minLongitude = 10 // europe

    val deltaLocation = 5

    val dataParameters = DataParameters(maxTemperature, minTemperature, deltaTemperature,
        maxLatitude, minLatitude, maxLongitude, minLongitude, deltaLocation)

    1 to 3 foreach { _ =>
        var dataGen: ActorRef = system.actorOf(DataGenerator.props(topicName, dataParameters, producerSettings))
        dataGen ! Generate
    }

}

object DataGenerator {

    def props(topicName: String, dataParameters: DataParameters, producerSettings: ProducerSettings[String, String]): Props
    = Props(new DataGenerator(topicName, dataParameters, producerSettings))

    final case object Generate

}

class DataGenerator(topicName: String, dp: DataParameters, producerSettings: ProducerSettings[String, String]) extends Actor {

    implicit val locationWrites = writes[Location]
    implicit val dataWrites = writes[Data]

    implicit val materializer: Materializer = ActorMaterializer()(FakeDataGenerator.system)

    import DataGenerator._

    var data: Data = _

    override def receive = {

        case Generate => {

            bootstrapData()

            // TODO verifiy the necessity of back-pressure (because of Source.tick doesn't have)

            Source.tick(0.second, 500.millisecond, "")
              .map(_ => changeData())
              .map(data => stringify(Json.obj("data" -> toJson(data))))
              .map(json =>
                  new ProducerRecord[String, String](topicName, json))
              .toMat(Producer.plainSink(producerSettings))(Keep.right)
              .run()

        }
    }

    def bootstrapData() = {

        data = Data(generateRandomID)

        data.temperature = Random.nextInt(dp.maxTemperature - dp.minTemperature) + dp.minTemperature // from min to max

        val _latitude = bound(randDouble * (abs(dp.maxLatitude) + abs(dp.minLatitude)), dp.minLatitude, dp.maxLatitude)
        val _longitude = bound(randDouble * (abs(dp.maxLongitude) + abs(dp.minLongitude)), dp.minLongitude, dp.maxLongitude)

        data.location = Location(_latitude, _longitude)

    }

    def changeData(): Data = {

        data.temperature = bound(data.temperature + (Random.nextInt(2 * dp.deltaTemperature + 1)
          - dp.deltaTemperature), dp.minTemperature, dp.maxTemperature)

        val _latitude = bound(data.location.latitude + randDouble * dp.deltaLocation, dp.minLatitude, dp.maxLatitude)
        val _longitude = bound(data.location.longitude + randDouble * dp.deltaLocation, dp.minLongitude, dp.maxLongitude)

        data.location = Location(_latitude, _longitude)

        data.time = Instant.now

        data
    }

    def bound[T](value: T, _min: T, _max: T)(implicit n: Numeric[T]): T = {
        import n._
        min(_max, max(_min, value))
    }

    def randDouble = {
        Random.nextDouble() * 2 - 1
    }

    def generateRandomID = {
        UUID.randomUUID().toString
    }

}