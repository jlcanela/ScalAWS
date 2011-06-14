package scalaws.simpledb

//scalaws
import scalaws.ScalAWS

//aws
import com.amazonaws.services.simpledb.AmazonSimpleDBClient

//java
import java.util.concurrent.{Executors, ExecutorService, Callable}

//joda
import org.joda.time.DateTime

trait SimpleDBComponent{ self: ScalAWS =>

  object SimpleDB extends Sequence[Domain]{
    import Implicits._
    
    implicit val simpleDBClient = new AmazonSimpleDBClient(awsCredentials)
    
    /**
     * Iterator for domains
    **/
    def iterator: Iterator[Domain] = {
      new Iterator[Domain](){
        val listDomainsResult = simpleDBClient.listDomains
        val domainsIterator = listDomainsResult.getDomainNames.iterator
        def next = Domain(domainsIterator.next)
        def hasNext = domainsIterator.hasNext
      }
    }
    
    /**
     * This method fetches the attributes for a given itemName.  It searches concurrently in
     * all of the supplied domains. If it finds attributes for the given itemName in multiple
     * domains, all of those attributes will come back in one list
    **/
    def apply(itemName: String, domains: Traversable[Domain]): List[Attribute] = {
      val futures = for(domain <- domains) yield{
        executorService.submit(new Callable[List[Attribute]](){
          def call = domain(itemName)
        })
      }
      futures.toList.map(_.get).foldLeft(List[Attribute]())(_ ::: _)
    }
    
    /**
     * Returns the domain at the given index
    **/
    def apply(idx: Int): Domain = iterator.toList(idx)
    
    /**
     * Returns the number of domains
    **/
    def length: Int = iterator.length
    
    /**
     * Run a select statement
    **/
    def select(selectRequest: SelectRequest): SelectResult = simpleDBClient.select(selectRequest)
    
    /**
     * Run a select statement
    **/
    def select(selectStatement: String): SelectResult = select(SelectRequest(selectStatement, Some(false)))
    
  }
  
}