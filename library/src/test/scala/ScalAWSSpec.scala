package scalaws

//specs2
import org.specs2.mutable._

class ScalAWSSpec extends Specification{
  
  "The ScalAWS trait" should {
    "find the credentials in user.home/.awssecret" in {
      object Test extends ScalAWS
      import Test._
      awsKey.length must be greaterThan(0)
      awsSecret.length must be greaterThan(0)
      awsKey must be equalTo(awsCredentials.getAWSAccessKeyId)
      awsSecret must be equalTo(awsCredentials.getAWSSecretKey)
    }
  }
  
}