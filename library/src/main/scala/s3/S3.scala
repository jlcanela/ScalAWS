package scalaws.s3

//scalaws
import scalaws.ScalAWS

//aws
import com.amazonaws.services.s3.AmazonS3Client

//joda
import org.joda.time.DateTime

trait S3Component{ self: ScalAWS =>
  
  object S3 extends Sequence[Bucket]{
    import ImplicitConversions._
    
    implicit val s3Client = new AmazonS3Client(awsCredentials)
    
    /**
     * Iterator to the buckets
    **/ 
    def iterator: Iterator[Bucket] = {
      val iterator = s3Client.listBuckets.iterator
      new Iterator[Bucket]{
        def next = iterator.next
        def hasNext = iterator.hasNext
      }
    }
    
    /**
     * Creates a bucket
    **/
    def createBucket(bucketName: String, region: Region = USStandard): BucketLike = {
      val s3c = s3Client
      s3Client.createBucket(bucketName, region).getName
      new BucketLike(){
        val name = bucketName
        implicit val s3Client = s3c
      }
    }
    
    /**
     * Creates a bucket
    **/
    def createBucket(createBucketRequest: CreateBucketRequest): BucketLike = {
      val s3c = s3Client
      s3Client.createBucket(createBucketRequest).getName
      new BucketLike(){
        val name = createBucketRequest.getBucketName
        implicit val s3Client = s3c
      }
    }
    
    /**
     * Creates a bucket
    **/
    def +(bucketName: String)(implicit region: Region = USStandard): BucketLike = createBucket(bucketName, region)
    
    /**
     * Deletes a bucket
    **/
    def deleteBucket(bucketName: String): Unit = s3Client.deleteBucket(bucketName)
    
    /**
     * Deletes a bucket
    **/
    def deleteBucket(deleteBucketRequest: DeleteBucketRequest): Unit =  s3Client.deleteBucket(deleteBucketRequest)
    
    /**
     * Deletes a bucket
    **/
    def deleteBucket(bucket: BucketLike): Unit = deleteBucket(bucket.name)
    
    /**
     * Deletes a bucket
    **/
    def -(bucketName: String): Unit = deleteBucket(bucketName)
    
    /**
     * Deletes a bucket
    **/
    def -(bucket: BucketLike): Unit = deleteBucket(bucket.name)
    
    /**
     * Number of buckets
    **/
    def length: Int = iterator.size
    
    /**
     * Fetch a bucket
    **/
    def apply(idx: Int): Bucket = {
      var bucket: Bucket = null
      var count = 0
      val i = iterator
      while(count < idx){
        bucket = i.next
      }
      bucket
    }
    
    
    /**
     * Convenience method to gets a bucket by name
    **/
    def apply(bucketName: String): BucketLike = {
      val s3c = s3Client
      new BucketLike(){ val name = bucketName; val s3Client = s3c}
    }
  }
}