package scalaws.simpledb

//specs2
import org.specs2.mutable._
import org.specs2.specification._

//scalaws
import scalaws.ScalAWS

class SimpleDBSpec extends Specification{
  
  "An SimpleDB object" should {
    object Tmp extends ScalAWS with SimpleDBComponent
    import Tmp._
    
    trait executorShutdown extends Scope with org.specs2.specification.After {
      def after = executorService.shutdown
    }
    
    "show the length of a Domain" in {
      SimpleDB.length must be greaterThan(0)
    }
    
    "select some records from a Domain" in {
      val SelectResult(items, _) = SimpleDB.select("select * from cuiscores_0")
      items.size must be greaterThan(1)
    }
    
    "select attributes from a Domain" in {
      import SimpleDB._
      val domain = Domain("cuiscores_0")
      domain("http://edinburgh-festivals.com").size must be greaterThan(0)
    }
    
    "create/delete a domain" in {
      import SimpleDB._
      SimpleDB + "thisisatestdomain"
      val domain = Domain("thisisatestdomain")
      SimpleDB - domain
      true
    }
    
    "put some attributes with the + sign" in {
      import SimpleDB._
      SimpleDB + "thisisanothertestdomain"
      val testMap = Map("one" -> "1", "two" -> "2")
      val domain = Domain("thisisanothertestdomain")
      domain + ("testId", testMap)
      Thread.sleep(10000)
      val attributes = domain("testId")
      SimpleDB + "thisisanothertestdomain"
      attributes.size must be equalTo(2)
      val map = (for(Attribute(name, value, _, _) <- attributes) yield (name -> value)).foldLeft(Map[String, String]())(_ + _)
      map must be equalTo(testMap)
    }

    "delete an item from a domain" in {
      import SimpleDB._
      SimpleDB + "thisisanothertestdomainagain"
      val testMap = Map("one" -> "1", "two" -> "2")
      val domain = Domain("thisisanothertestdomainagain")
      domain + ("testId", testMap)
      Thread.sleep(10000)
      val attributes = domain("testId")
      SimpleDB + "thisisanothertestdomainagain"
      attributes.size must be equalTo(2)
      val map = (for(Attribute(name, value, _, _) <- attributes) yield (name -> value)).foldLeft(Map[String, String]())(_ + _)
      map must be equalTo(testMap)

      domain - "testId"
      Thread.sleep(10000)
      val attributesGone = domain("testId")
      attributesGone must be equalTo(Nil)
    }

    "select across multiple Domains" in new executorShutdown{
      import SimpleDB._
      val domains = SimpleDB.filter(_.name.startsWith("one"))
      SimpleDB("http://infoq.com", domains).size must not be equalTo(Nil)
    }
        
  }
  
}