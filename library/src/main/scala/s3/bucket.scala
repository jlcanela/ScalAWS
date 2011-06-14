package scalaws.s3

//joda time
import org.joda.time.DateTime

//java
import java.io.File

//aws
import com.amazonaws.services.s3.AmazonS3Client

trait BucketLike extends Iterable[S3ObjectSummary]{
  import ImplicitConversions._
  implicit val s3Client: AmazonS3Client
  val name: String
  
  /**
   * Get an object by key from S3
  **/
  def apply(key: String): S3Object = s3Client.getObject(name, key)
  
  /**
   * Query for objects with the given prefix
  **/
  def /(q: String): Iterator[S3ObjectSummary] = query(q)
  
  /**
   * Add a file to this bucket. Uses the filename as a key
  **/
  def +(file: File): PutObjectResult = s3Client.putObject(name, file.getName, file)
  
  /**
   * Deletes object from bucket.
  **/
  def -(key: String): Unit = s3Client.deleteObject(name, key)
  
  /**
   * True if the object exists
  **/
  def exists(key: String): Boolean = {
    try{
      val obj = this(key)
      obj.objectContent.close
      true
    } catch {
      case _ => false
    } 
  }
  
  /**
   * Iterate through all of the object summaries in the bucket
  **/
  def iterator: Iterator[S3ObjectSummary] = iterator("")
  
  /**
   * Query for objects with the given prefix
  **/
  def query(q: String): Iterator[S3ObjectSummary] = iterator(q)
  
  /**
   * Get listing with a prefix
  **/
  def iterator(prefix: String): Iterator[S3ObjectSummary] = {
    import ImplicitConversions._
    var objectListing = prefix match {
      case "" => s3Client.listObjects(name)
      case _ => s3Client.listObjects(name, prefix)
    }
    
    var objectSummariesIterator = objectListing.getObjectSummaries.iterator
    
    new Iterator[S3ObjectSummary](){
      def hasNext = objectSummariesIterator.hasNext match {
        case true => true
        case false =>
          objectListing = s3Client.listNextBatchOfObjects(objectListing)
          objectSummariesIterator = objectListing.getObjectSummaries.iterator
          objectSummariesIterator.hasNext
      }
      def next = objectSummariesIterator.next
    }
  }
}

case class Bucket(name: String, owner: Owner, creationDate: DateTime)(implicit val s3Client: AmazonS3Client) extends BucketLike