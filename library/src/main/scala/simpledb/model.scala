package scalaws.simpledb

//aws
import com.amazonaws.services.simpledb.model.{DomainMetadataRequest, SelectRequest => AWSSelectRequest, SelectResult => AWSSelectResult, Item => AWSItem, Attribute => AWSAttribute}
import com.amazonaws.services.simpledb.model.{GetAttributesRequest => AWSGetAttributesRequest, GetAttributesResult => AWSGetAttributesResult}
import com.amazonaws.services.simpledb.AmazonSimpleDBClient

//java
import java.util.{ArrayList, List => JList}

case class Domain(name: String)(implicit simpleDBClient: AmazonSimpleDBClient){
  import Implicits._
  def metadata: DomainMetadata = {
    val result = simpleDBClient.domainMetadata(new DomainMetadataRequest(name))
    DomainMetadata(result.getAttributeNameCount.intValue, result.getAttributeNamesSizeBytes.longValue, result.getItemCount.intValue, result.getItemNamesSizeBytes.longValue, result.getTimestamp.intValue)
  }
  
  def apply(itemName: String, attributeNames: List[String] = Nil): List[Attribute] = {
    val result = simpleDBClient.getAttributes(GetAttributesRequest(name, itemName, attributeNames))
    result.attributes
  }
}

case class DomainMetadata(attributeNameCount: Int, attributeNamesSizeBytes: Long, itemCount: Int, itemNamesSizeBytes: Long, timestamp: Integer)
case class SelectRequest(selectExpression: String, consistentRead: Option[Boolean] = None, nextToken: Option[String] = None)
case class SelectResult(items: List[Item], nextToken: Option[String])
case class Item(name: String, attributes: List[Attribute], alternateNameEncoding: Option[String] = None)
case class Attribute(name: String, value: String, alternateNameEncoding: Option[String] = None, alternateValueEncoding: Option[String] = None)
case class GetAttributesRequest(domainName: String, itemName: String, attributeNames: List[String] = Nil, consistentRead: Option[Boolean] = None)
case class GetAttributesResult(attributes: List[Attribute])

object Implicits{
  
  implicit def awsSelectResultToSelectResult(awsSelectResult: AWSSelectResult): SelectResult = {
    val nextToken = if(awsSelectResult.getNextToken != null) Some(awsSelectResult.getNextToken) else None
    var items: List[Item] = Nil
    val iterator = awsSelectResult.getItems.iterator
    while(iterator.hasNext) items = iterator.next :: items
    SelectResult(items = items, nextToken = nextToken)
  }
  
  implicit def selectResultToAWSSelectResult(selectResult: SelectResult): AWSSelectResult = {
    val awsSelectResult = new AWSSelectResult
    val items = new ArrayList[AWSItem]()
    for(item <- selectResult.items) items.add(item)
    awsSelectResult.setItems(items)
    if(selectResult.nextToken.isDefined) awsSelectResult.setNextToken(selectResult.nextToken.get)
    awsSelectResult
  }
  
  implicit def awsGetAttributesResultToGetAttributesResult(response: AWSGetAttributesResult): GetAttributesResult = {
    var attributes: List[Attribute] = Nil
    val iterator = response.getAttributes.iterator
    while(iterator.hasNext){
      attributes = iterator.next :: attributes
    }
    GetAttributesResult(attributes)
  }
  
  implicit def getAttributesResponseToAWSGetAttributesResult(result: GetAttributesResult): AWSGetAttributesResult = {
    val attributes = new ArrayList[AWSAttribute]()
    for(attribute <- result.attributes) attributes.add(attribute)
    val awsResult = new AWSGetAttributesResult
    awsResult.setAttributes(attributes)
    awsResult
  }
  
  implicit def awsGetAttributesRequestToGetAttributesRequest(awsGetAttributesRequest: AWSGetAttributesRequest): GetAttributesRequest = {
    val consistentRead = if(awsGetAttributesRequest.getConsistentRead != null) Some(awsGetAttributesRequest.getConsistentRead.booleanValue) else None
    val iterator = awsGetAttributesRequest.getAttributeNames.iterator
    var attributeNames: List[String] = Nil
    while(iterator.hasNext) attributeNames = iterator.next :: attributeNames
    GetAttributesRequest(awsGetAttributesRequest.getDomainName, awsGetAttributesRequest.getItemName, attributeNames, consistentRead)
  }
  
  implicit def getAttributeRequestToAWSGetAttributesRequest(getAttributeRequest: GetAttributesRequest): AWSGetAttributesRequest = {
    val awsGetAttributesRequest = new AWSGetAttributesRequest(getAttributeRequest.domainName, getAttributeRequest.itemName)
    val attributeNames: JList[String] = new ArrayList[String]
    for(name <- getAttributeRequest.attributeNames) attributeNames.add(name)
    if(attributeNames.size > 0) awsGetAttributesRequest.setAttributeNames(attributeNames)
    if(getAttributeRequest.consistentRead.isDefined) awsGetAttributesRequest.setConsistentRead(getAttributeRequest.consistentRead.get)
    awsGetAttributesRequest
  }
  
  implicit def awsSelectRequestToSelectRequest(awsSelectRequest: AWSSelectRequest): SelectRequest = {
    val nextToken = if(awsSelectRequest.getNextToken != null) Some(awsSelectRequest.getNextToken) else None
    val consistentRead = if(awsSelectRequest.getConsistentRead != null) Some(awsSelectRequest.getConsistentRead.booleanValue) else None
    SelectRequest(awsSelectRequest.getSelectExpression, consistentRead, nextToken)
  }

  implicit def selectRequestToAWSSelectRequest(selectRequest: SelectRequest): AWSSelectRequest = {
    val awsSelectRequest = new AWSSelectRequest(selectRequest.selectExpression)
    if(selectRequest.nextToken.isDefined) awsSelectRequest.setNextToken(selectRequest.nextToken.get)
    if(selectRequest.consistentRead.isDefined) awsSelectRequest.setConsistentRead(selectRequest.consistentRead.get)
    awsSelectRequest
  }
  
  implicit def awsAttributeToAttribute(awsAttribute: AWSAttribute): Attribute = {
    val alternateNameEncoding = if(awsAttribute.getAlternateNameEncoding != null) Some(awsAttribute.getAlternateNameEncoding) else None
    val alternateValueEncoding = if(awsAttribute.getAlternateValueEncoding != null) Some(awsAttribute.getAlternateValueEncoding) else None
    Attribute(name = awsAttribute.getName, value = awsAttribute.getValue, alternateNameEncoding = alternateNameEncoding, alternateValueEncoding = alternateValueEncoding)
  }
  
  implicit def attributeToAWSAttribute(attribute: Attribute): AWSAttribute = {
    val awsAttribute = new AWSAttribute(attribute.name, attribute.value)
    if(attribute.alternateNameEncoding.isDefined) awsAttribute.setAlternateNameEncoding(attribute.alternateNameEncoding.get)
    if(attribute.alternateValueEncoding.isDefined) awsAttribute.setAlternateValueEncoding(attribute.alternateValueEncoding.get)
    awsAttribute
  }
  
  implicit def itemToAWSItem(item: Item): AWSItem = {
    val attributes: JList[AWSAttribute] = new ArrayList[AWSAttribute]()
    for(attribute <- item.attributes) attributes.add(attribute)
    val awsItem = new AWSItem(item.name, attributes)
    if(item.alternateNameEncoding.isDefined) awsItem.setAlternateNameEncoding(item.alternateNameEncoding.get)
    awsItem
  }
  
  implicit def awsItemToItem(awsItem: AWSItem): Item = {
    val alternateNameEncoding = if(awsItem.getAlternateNameEncoding != null) Some(awsItem.getAlternateNameEncoding) else None
    var attributes: List[Attribute] = Nil
    val iterator = awsItem.getAttributes.iterator
    while(iterator.hasNext){
      attributes = iterator.next :: attributes
    }
    Item(name = awsItem.getName, attributes = attributes, alternateNameEncoding = alternateNameEncoding)
  }
  
}