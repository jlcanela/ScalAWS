package scalaws.s3

//specs2
import org.specs2.mutable._

//scalaws
import scalaws.ScalAWS

class S3Spec extends Specification{
  
  "An S3 object" should {
    object Tmp extends ScalAWS with S3Component
    import Tmp._
    
    "show the length of a bucket" in {
      S3.length must be greaterThan(0)
    }
    
    "iterate through buckets" in {
      S3.toList.size must be greaterThan(0)
    }
    
    "select a random access bucket" in {
      S3(0) must not (throwA[Exception])
    }
    
    "select a bucket by name" in {
      S3("tm-test-sites").take(1).size must be equalTo(1)
    }
  }
  
}