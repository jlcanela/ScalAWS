package scalaws.simpledb

//aws
import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import com.amazonaws.services.simpledb.model.{DomainMetadataRequest => AWSDomainMetadataRequest}

case class Domain(name: String)(implicit simpleDBClient: AmazonSimpleDBClient){
  import Implicits._
  
  /**
   * fetches the Attribues for a given item
  **/
  def apply(itemName: String, attributeNames: List[String] = Nil): List[Attribute] = {
    val result = simpleDBClient.getAttributes(GetAttributesRequest(name, itemName, attributeNames))
    result.attributes
  }
  
  /**
   * Lazily fetches metadata on this domain
  **/
  lazy val metadata: DomainMetadata = {
    val result = simpleDBClient.domainMetadata(new AWSDomainMetadataRequest(name))
    DomainMetadata(result.getAttributeNameCount.intValue, result.getAttributeNamesSizeBytes.longValue, result.getItemCount.intValue, result.getItemNamesSizeBytes.longValue, result.getTimestamp.intValue)
  }
  
  /**
   * Acts as though you were thinking of SimpleDB as a Map, so this will
   * Replace any values on SimpleDB
  **/
  def +(itemName: String, attributes: Map[String, String]) = {
    val attrs = for((name, value) <- attributes.iterator.toList) yield ReplaceableAttribute(name, value, true)
    simpleDBClient.putAttributes(PutAttributesRequest(name, itemName, attrs))
  }
}