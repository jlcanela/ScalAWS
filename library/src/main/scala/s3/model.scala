package scalaws.s3

//joda time
import org.joda.time.DateTime

//aws
import com.amazonaws.services.s3.model.{Bucket => AWSBucket, Owner => AWSOwner, S3Object => AWSS3Object, S3ObjectSummary => AWSS3ObjectSummary}
import com.amazonaws.services.s3.model.{Region => AWSRegion, CannedAccessControlList => AWSCannedAccessControlList, CreateBucketRequest => AWSCreateBucketRequest}
import com.amazonaws.services.s3.model.{DeleteBucketRequest => AWSDeleteBucketRequest}
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
}

case class S3ObjectSummary(bucketName: String, eTag: String, key: String, lastModified: DateTime, owner: Owner, size: Long, storageClass: String)
case class Owner(id: String, displayName: String)
case class CreateBucketRequest(bucketName: String, region: Region = USStandard, cannedACL: CannedAccessControlList = Private)
case class DeleteBucketRequest(bucketName: String)

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