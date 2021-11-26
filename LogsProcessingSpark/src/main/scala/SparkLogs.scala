
import org.apache.spark._
import org.apache.log4j._

import java.util.Properties
import scala.collection.Map
import javax.mail._
import javax.mail.internet._

import com.typesafe.config.ConfigFactory


object SparkLogs {
  def parseLine(line:String): String =
  {
    val words = line.split(" ")
    val logType = words(2)
    logType
  }
  def sendEmail(report:Map[String, Long]) =
  {

    val config = ConfigFactory.load("application")

    val from: String = config.getString("MailInput.from")
    val to: String = config.getString("MailInput.to")
    val email: String = config.getString("MailInput.email")
    val password: String = config.getString("MailInput.password")


    val properties = new Properties()
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")
    properties.put("mail.smtp.host", "smtp.gmail.com")
    properties.put("mail.smtp.port", "587")

    val session = Session.getInstance(properties, new Authenticator() {
        override def getPasswordAuthentication(): PasswordAuthentication =
        {
          new PasswordAuthentication(email, password)
        }
    })

    val message = new MimeMessage(session)
    message.setFrom(new InternetAddress(from));
    message.addRecipient(Message.RecipientType.TO,new InternetAddress(to));
    message.setSubject("Spark Report");
    val text = report.toString()
    message.setText(text);
    // Send message
    Transport.send(message);
    System.out.println("message sent successfully....");
  }
  def main(args: Array[String]): Unit =
  {
    val logger = Logger.getLogger("org")

    val sc = new SparkContext("local[*]", "SparkLogs")

    val lines = sc.textFile("./src/main/resources/test.txt")

    val rdd = lines.map(parseLine)
    val count = rdd.countByValue()

    count.foreach(println)
    sendEmail(count)
  }


}
