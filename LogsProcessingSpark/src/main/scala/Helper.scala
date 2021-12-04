import com.typesafe.config.ConfigFactory

import java.text.SimpleDateFormat
import java.util.Properties
import javax.mail.{Authenticator, Message, PasswordAuthentication, Session, Transport}
import javax.mail.internet.{InternetAddress, MimeMessage}

//helper functions for spark processing
object Helper {

  //parse log lines and retrieve the type of logs
  def parseType(line:String): String =
  {
    val words = line.split(" ")
    val logType = words(2)

    logType
  }

  //parse log lines and retrieve the time of logs
  def parseTime(line:String): Long =
  {
    val words = line.split(" ")
    val time = words(0)
    convertTimeToMilli(time)
  }

  //convert time to milliseconds since jan 1 1970
  def convertTimeToMilli(time:String): Long =
  {
    val sdf = new SimpleDateFormat("HH:mm:ss.SSS");
    val theTime = sdf.parse(time)
    theTime.getTime()
  }

  //convert milliseconds since jan 1 1970 back to 23:59:59.999 format
  def convertMilliToTime(time:Long): String =
  {
    val sdf = new SimpleDateFormat("HH:mm:ss.SSS");
    val theTime = sdf.format(time)
    theTime
  }

  //send email to a gmail account stakeholder
  def sendEmail(report:String) =
  {

    val config = ConfigFactory.load("application")
    val from: String = config.getString("MailInput.email")
    val to: String = from
    val email: String = from
    val password: String = config.getString("MailInput.password")


    val properties = new Properties()
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")
    properties.put("mail.smtp.host", "smtp.gmail.com")
    properties.put("mail.smtp.port", "587")
    properties.put("mail.smtp.ssl.trust", "*")

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
    val text = report
    message.setText(text);
    // Send message
    Transport.send(message);
  }
}
