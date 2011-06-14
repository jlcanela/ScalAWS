package scalaws.s3

//joda time
import org.joda.time.DateTime

//aws
import com.amazonaws.services.s3.AmazonS3Client

trait BucketLike extends Iterable[S3ObjectSummary]{
  implicit val s3Client: AmazonS3Client
  val name: String
  
  /**
   * Query for objects with the given prefix
  **/
  def /(q: String): Iterator[S3ObjectSummary] = query(q)
  
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