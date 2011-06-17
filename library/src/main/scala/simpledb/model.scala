package scalaws.simpledb

//aws
import com.amazonaws.services.simpledb.model.{DomainMetadataRequest, SelectRequest => AWSSelectRequest, SelectResult => AWSSelectResult, Item => AWSItem, Attribute => AWSAttribute,  ReplaceableAttribute => AWSReplaceableAttribute}
import com.amazonaws.services.simpledb.model.{CreateDomainRequest => AWSCreateDomainRequest, DeleteDomainRequest => AWSDeleteDomainRequest}
import com.amazonaws.services.simpledb.model.{GetAttributesRequest => AWSGetAttributesRequest, GetAttributesResult => AWSGetAttributesResult, PutAttributesRequest => AWSPutAttributesRequest, UpdateCondition => AWSUpdateCondition}
import com.amazonaws.services.simpledb.AmazonSimpleDBClient

//java
import java.util.{ArrayList, List => JList}

case class DomainMetadata(attributeNameCount: Int, attributeNamesSizeBytes: Long, itemCount: Int, itemNamesSizeBytes: Long, timestamp: Integer)
case class SelectRequest(selectExpression: String, consistentRead: Option[Boolean] = None, nextToken: Option[String] = None)
case class SelectResult(items: List[Item], nextToken: Option[String])
case class Item(name: String, attributes: List[Attribute], alternateNameEncoding: Option[String] = None)
case class Attribute(name: String, value: String, alternateNameEncoding: Option[String] = None, alternateValueEncoding: Option[String] = None)
case class GetAttributesRequest(domainName: String, itemName: String, attributeNames: List[String] = Nil, consistentRead: Option[Boolean] = None)
case class GetAttributesResult(attributes: List[Attribute])
case class PutAttributesRequest(domainName: String, itemName: String, attributes: List[ReplaceableAttribute], expected: Option[UpdateCondition] = None)
case class ReplaceableAttribute(name: String, value: String, replace: Boolean)
case class UpdateCondition(name: String, value: String, exists: Boolean)
case class CreateDomainRequest(domainName: String)
case class DeleteDomainRequest(domainName: String)

object Implicits{
  
  implicit def deleteDomainRequestToAWSDeleteDomainRequest(ddr: DeleteDomainRequest): AWSDeleteDomainRequest = new AWSDeleteDomainRequest(ddr.domainName)
  implicit def awsDeleteDomainRequestToDeleteDomainRequest(awsDdf: AWSDeleteDomainRequest): DeleteDomainRequest = DeleteDomainRequest(awsDdf.getDomainName)
  implicit def createDomainRequestToAWSCreateDomainRequest(cdr: CreateDomainRequest): AWSCreateDomainRequest = new AWSCreateDomainRequest(cdr.domainName)
  implicit def awsCreateDomainRequestToCreateDomainRequest(awsCdf: AWSCreateDomainRequest): CreateDomainRequest = CreateDomainRequest(awsCdf.getDomainName)
  
  implicit def awsUpdateConditionToUpdateCondition(awsUpdateCondition: AWSUpdateCondition): UpdateCondition = {
    UpdateCondition(awsUpdateCondition.getName, awsUpdateCondition.getValue, awsUpdateCondition.getExists.booleanValue)
  }
  
  implicit def updateConditionToAWSUpdateCondition(updateCondition: UpdateCondition): AWSUpdateCondition = {
    new AWSUpdateCondition(updateCondition.name, updateCondition.value, updateCondition.exists)
  }
  
  implicit def replaceableAttributeToAWSReplaceableAttribute(replaceableAttribute: ReplaceableAttribute): AWSReplaceableAttribute = {
    new AWSReplaceableAttribute(replaceableAttribute.name, replaceableAttribute.value, replaceableAttribute.replace)
  }
  
  implicit def awsReplaceableAttributeToReplaceableAttribute(awsAttr: AWSReplaceableAttribute): ReplaceableAttribute = {
    val replace = if(awsAttr.getReplace != null) awsAttr.getReplace.booleanValue else false
    ReplaceableAttribute(awsAttr.getName, awsAttr.value, replace)
  }
  
  implicit def putAttributesRequestToAWSPutAttributesRequest(putRequest: PutAttributesRequest): AWSPutAttributesRequest = {
    val attributes = new ArrayList[AWSReplaceableAttribute]()
    for(a <- putRequest.attributes) attributes.add(a)
    val awsReq = new AWSPutAttributesRequest(putRequest.domainName, putRequest.itemName, attributes)
    if(putRequest.expected.isDefined) awsReq.setExpected(putRequest.expected.get)
    awsReq    
  }
  
  implicit def awsPutAttributesRequestToPutAttributesRequest(awsRequest: AWSPutAttributesRequest): PutAttributesRequest = {
    val iterator = awsRequest.getAttributes.iterator
    var attributes: List[ReplaceableAttribute] = Nil
    while(iterator.hasNext) attributes = iterator.next :: attributes
    PutAttributesRequest(awsRequest.getDomainName, awsRequest.getItemName, attributes)
  }
  
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