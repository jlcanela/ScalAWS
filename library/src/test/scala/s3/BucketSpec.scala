package scalaws.s3

//specs2
import org.specs2.mutable._

//scalaws
import scalaws.ScalAWS

//joda time
import org.joda.time.DateTime

class BucketSpec extends Specification{
  
  "A Bucket" should {
    import ImplicitConversions._
    object Tmp extends ScalAWS with S3Component
    import Tmp._
    import S3._
    
    "get a listing of all of the objects in a bucket" in {
      S3("tm-test-sites").take(10).size must be equalTo(10)
    }
    
    "get a listing of objects by prefix" in {
      (for(obj <- "tm-test-sites" / "a") yield obj.key.startsWith("a")).foldLeft(false)(_ || _) must be equalTo(true)
    }
    
    "create and delete a bucket" in {
      val now = new DateTime
      val bucketName = "thisisatestbucket-1234554321"
      val bucket = S3 + bucketName
      S3.find(_.name == bucketName).isDefined must be equalTo(true)
      
      S3 - bucketName
      S3.find(_.name == bucketName).isDefined must be equalTo(false)
    }
    
    "put an object in a bucket retrieve it and delete it" in {
      val bucketName = "thisisatestbucket-1234554321555"
      val bucket = S3 + bucketName
      val file = new java.io.File("library/src/test/resources/sample_file.txt")
      bucket + file
      bucket.exists(file.getName) must be equalTo(true)
      val obj = bucket("sample_file.txt")
      scala.io.Source.fromInputStream(obj.objectContent).mkString must be equalTo("this is a test file")
      obj.objectContent.close
      bucket - file.getName
      bucket.exists(file.getName) must be equalTo(false)
      S3 - bucket
      S3.find(_.name == bucketName).isDefined must be equalTo(false)
    }
    
  }
  
}