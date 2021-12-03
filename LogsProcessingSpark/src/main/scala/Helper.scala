import com.typesafe.config.ConfigFactory

import java.text.SimpleDateFormat
import java.util.Properties
import javax.mail.{Authenticator, Message, PasswordAuthentication, Session, Transport}
import javax.mail.internet.{InternetAddress, MimeMessage}

object Helper {
  def parseType(line:String): String =
  {
    val words = line.split(" ")
    val logType = words(2)

    logType
  }

  def parseTime(line:String): Long =
  {
    val words = line.split(" ")
    val time = words(0)
    convertTimeToMilli(time)
  }

  def convertTimeToMilli(time:String): Long =
  {
    val sdf = new SimpleDateFormat("HH:mm:ss.SSS");
    val theTime = sdf.parse(time)
    theTime.getTime()
  }
  def convertMilliToTime(time:Long): String =
  {
    val sdf = new SimpleDateFormat("HH:mm:ss.SSS");
    val theTime = sdf.format(time)
    theTime
  }

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
    System.out.println("message sent successfully....");
  }
}
