package scalaws

//aws
import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}

//java
import java.io.{File, FileInputStream, ByteArrayOutputStream}
import java.util.concurrent.{Executors, ExecutorService}

trait ScalAWS{
  lazy val awsKey: String = loadFromAwsSecret._1
  lazy val awsSecret: String = loadFromAwsSecret._2
  lazy val awsCredentials: AWSCredentials = new BasicAWSCredentials(awsKey, awsSecret)
  val executorService: ExecutorService = Executors.newCachedThreadPool
  
  
  /**
   * Shuts down ScalAWS, but waits for threads to finish
  **/
  def shutdown = executorService.shutdown
  
  /**
   * Shuts down ScalAWS, and does not wait for threads to finish
  **/
  def shutdownNow = executorService.shutdownNow
  
  /**
   * Looks for a file called .awssecret in user.home.  The first line should be the awsKey and the second should be the awsSecret
  **/
  def loadFromAwsSecret: (String, String) = {
      val inStream = new FileInputStream(new File(System.getProperty("user.home") + System.getProperty("file.separator") + ".awssecret"))
      val outStream = new ByteArrayOutputStream
      try {
        var reading = true
        while ( reading ) {
          inStream.read() match {
            case -1 => reading = false
            case c => outStream.write(c)
          }
        }
        outStream.flush()
      }
      finally {
        inStream.close()
      }
      val tokens = new String(outStream.toByteArray(), "UTF-8").split("\n")
      (tokens(0), tokens(1))   
  }
}