package scalaws.s3

//joda time
import org.joda.time.DateTime

//java
import java.io.InputStream

//aws
import com.amazonaws.services.s3.model.{Bucket => AWSBucket, Owner => AWSOwner, S3Object => AWSS3Object, S3ObjectSummary => AWSS3ObjectSummary, ObjectMetadata => AWSObjectMetadata}
import com.amazonaws.services.s3.model.{Region => AWSRegion, CannedAccessControlList => AWSCannedAccessControlList, CreateBucketRequest => AWSCreateBucketRequest}
import com.amazonaws.services.s3.model.{DeleteBucketRequest => AWSDeleteBucketRequest, PutObjectResult => AWSPutObjectResult, PutObjectRequest => AWSPutObjectRequest}
import com.amazonaws.services.s3.AmazonS3Client
import AWSRegion._
import AWSCannedAccessControlList.{PublicReadWrite => AWSPublicReadWrite, PublicRead => AWSPublicRead, Private => AWSPrivate, LogDeliveryWrite => AWSLogDeliveryWrite, BucketOwnerRead => AWSBucketOwnerRead, AuthenticatedRead => AWSAuthenticatedRead, BucketOwnerFullControl => AWSBucketOwnerFullControl}


object ImplicitConversions{
  implicit def awsS3ObjectSummaryToObjectSummary(awsSummary: AWSS3ObjectSummary): S3ObjectSummary = S3ObjectSummary(bucketName = awsSummary.getBucketName, eTag = awsSummary.getETag, key = awsSummary.getKey, lastModified = new DateTime(awsSummary.getLastModified), owner = awsSummary.getOwner, size = awsSummary.getSize, storageClass = awsSummary.getStorageClass)
  implicit def awsOwnerToOwner(awsOwner: AWSOwner): Owner = Owner(
    id = awsOwner.getId, 
    displayName = awsOwner.getDisplayName)
  implicit def stringToBucketLike(str: String)(implicit s3ClientForOperations: AmazonS3Client) = new BucketLike(){ val name = str; val s3Client = s3ClientForOperations}
  implicit def awsBucketToBucket(awsBucket: AWSBucket)(implicit s3Client: AmazonS3Client): Bucket = Bucket(
    name = awsBucket.getName, 
    owner = awsBucket.getOwner, 
    creationDate = new DateTime(awsBucket.getCreationDate))
  implicit def awsRegionToRegion(awsRegion: AWSRegion): Region = {
    awsRegion match {
      case AP_Singapore => Singapore
      case AP_Tokyo => Tokyo
      case EU_Ireland => Ireland
      case US_Standard => USStandard
      case US_West => USWest
    }
  }
  
  implicit def regionToAWSRegion(region: Region): AWSRegion = {
    region match {
      case Singapore => AP_Singapore
      case Tokyo => AP_Tokyo
      case Ireland => EU_Ireland
      case USStandard => US_Standard
      case USWest => US_West
    }
  }
  
  implicit def awsCannedAccessControlListToCannedAccessControlList(awsACL: AWSCannedAccessControlList): CannedAccessControlList = {
    awsACL match {
      case AWSAuthenticatedRead => AuthenticatedRead
      case AWSBucketOwnerFullControl => BucketOwnerFullControl
      case AWSBucketOwnerRead => BucketOwnerRead
      case AWSLogDeliveryWrite => LogDeliveryWrite
      case AWSPrivate => Private
      case AWSPublicRead => PublicRead
      case AWSPublicReadWrite => PublicReadWrite
    }
  }
  
  implicit def cannedAccessControlListToAWSCannedAccessControlList(acl: CannedAccessControlList): AWSCannedAccessControlList = {
    acl match {
      case  AuthenticatedRead => AWSAuthenticatedRead
      case  BucketOwnerFullControl => AWSBucketOwnerFullControl
      case  BucketOwnerRead => AWSBucketOwnerRead
      case  LogDeliveryWrite => AWSLogDeliveryWrite
      case  Private => AWSPrivate
      case  PublicRead => AWSPublicRead
      case  PublicReadWrite => AWSPublicReadWrite
    }
  }
  
  implicit def createBucketRequestToAWSCreateBucketRequest(createBucketRequest: CreateBucketRequest): AWSCreateBucketRequest = {
    new AWSCreateBucketRequest(createBucketRequest.bucketName, createBucketRequest.region).withCannedAcl(createBucketRequest.cannedACL)
  }
  
  implicit def awsCreateBucketRequestToCreateBucketRequest(awsCreateBucketRequest: AWSCreateBucketRequest): CreateBucketRequest = {
    CreateBucketRequest(awsCreateBucketRequest.getBucketName, AWSRegion.fromValue(awsCreateBucketRequest.getRegion), awsCreateBucketRequest.getCannedAcl)
  }
  
  implicit def deleteBucketRequestToAWSDeleteBucketRequest(deleteBucketRequest: DeleteBucketRequest): AWSDeleteBucketRequest = new AWSDeleteBucketRequest(deleteBucketRequest.bucketName)
  implicit def awsDeleteBucketRequestToDeleteBucketRequest(awsDeleteBucketRequest: AWSDeleteBucketRequest): DeleteBucketRequest = DeleteBucketRequest(awsDeleteBucketRequest.getBucketName)
  implicit def awsPutObjectResultToPutObjectResult(awsPutObjectResult: AWSPutObjectResult): PutObjectResult = {
    val versionId = if(awsPutObjectResult.getVersionId != null) Some(awsPutObjectResult.getVersionId) else None
    PutObjectResult(awsPutObjectResult.getETag, versionId)
  }
  
  implicit def putObjectResultToAWSPutObjectResult(putObjectResult: PutObjectResult): AWSPutObjectResult = {
    val awsPutObjectResult = new AWSPutObjectResult
    awsPutObjectResult.setETag(putObjectResult.eTag)
    awsPutObjectResult.setVersionId(putObjectResult.versionId.getOrElse(null))
    awsPutObjectResult
  }
  
  implicit def awsS3ObjectToS3Object(awsS3Object: AWSS3Object): S3Object = S3Object(bucketName = awsS3Object.getBucketName, key = awsS3Object.getKey, objectContent = awsS3Object.getObjectContent, objectMetadata = awsS3Object.getObjectMetadata)
  
  implicit def awsObjectMetadata(awsObjectMetadata: AWSObjectMetadata): ObjectMetadata = {
    val cacheControl = if(awsObjectMetadata.getCacheControl != null) Some(awsObjectMetadata.getCacheControl) else None
    val contentDisposition = if(awsObjectMetadata.getContentDisposition != null) Some(awsObjectMetadata.getContentDisposition) else None
    val contentEncoding = if(awsObjectMetadata.getContentEncoding != null) Some(awsObjectMetadata.getContentEncoding) else None
    val versionId = if(awsObjectMetadata.getVersionId != null) Some(awsObjectMetadata.getVersionId) else None
    val iterator = awsObjectMetadata.getUserMetadata.keySet.iterator
    var userMetadataMap = Map[String, String]()
    while(iterator.hasNext){
      val key = iterator.next
      userMetadataMap = userMetadataMap + (key -> awsObjectMetadata.getUserMetadata.get(key))
    }
    ObjectMetadata(
      contentLength = awsObjectMetadata.getContentLength,
      contentMD5 = awsObjectMetadata.getContentMD5,
      contentType = awsObjectMetadata.getContentType,
      eTag = awsObjectMetadata.getETag,
      lastModified = new DateTime(awsObjectMetadata.getLastModified),
      userMetadata = userMetadataMap,
      cacheControl = cacheControl,
      contentDisposition = contentDisposition,
      contentEncoding = contentEncoding,
      versionId = versionId
    )
  }
  
  /****** Currently there is no setObjectMetaadata method in the AWS Sdk, so commenting this out
    implicit def s3ObjectToAWSS3Object(s3Object: S3Object): AWSS3Object = {
    val awsS3Object = new AWSS3Object
    awsS3Object.setBucketName(s3Object.bucketName)
    awsS3Object.setKey(s3Object.key)
    awsS3Object.setObjectMetadata(s3Object.metadata)
    awsS3Object.setObjectContent(s3Object.setObjectContent)
  }*/
}

case class ObjectMetadata(contentLength: Long, contentMD5: String, contentType: String, eTag: String, lastModified: DateTime, userMetadata: Map[String, String] = Map[String, String](), cacheControl: Option[String] = None, contentDisposition: Option[String] = None, contentEncoding: Option[String] = None, versionId: Option[String] = None)
case class S3ObjectSummary(bucketName: String, eTag: String, key: String, lastModified: DateTime, owner: Owner, size: Long, storageClass: String)
case class S3Object(bucketName: String, key: String, objectContent: InputStream, objectMetadata: ObjectMetadata)
case class Owner(id: String, displayName: String)
case class CreateBucketRequest(bucketName: String, region: Region = USStandard, cannedACL: CannedAccessControlList = Private)
case class DeleteBucketRequest(bucketName: String)
case class PutObjectResult(eTag: String, versionId: Option[String] = None)

sealed trait Region
case object Singapore extends Region
case object Tokyo extends Region
case object Ireland extends Region
case object USStandard extends Region
case object USWest extends Region

sealed trait CannedAccessControlList
case object AuthenticatedRead extends CannedAccessControlList
case object BucketOwnerFullControl extends CannedAccessControlList
case object BucketOwnerRead extends CannedAccessControlList
case object LogDeliveryWrite extends CannedAccessControlList
case object Private extends CannedAccessControlList
case object PublicRead extends CannedAccessControlList
case object PublicReadWrite extends CannedAccessControlList