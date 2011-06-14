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
    
    trait executorShutdown extends Scope with After {
      def after = executorService.shutdown
    }
    
    "show the length of a Domain" in {
      SimpleDB.length must be greaterThan(0)
    }
    
    "select some records from a Domain" in {
      val SelectResult(items, _) = SimpleDB.select("select * from oneoffratings_0")
      items.size must be greaterThan(1)
    }
    
    "select attributes from a Domain" in {
      import SimpleDB._
      val domain = Domain("oneoffratings_0")
      domain("http://webtretho.com").size must be greaterThan(0)
    }
    
    "select across multiple Domains" in new executorShutdown{
      import SimpleDB._
      val domains = SimpleDB.filter(_.name.startsWith("one"))
      SimpleDB("http://infoq.com", domains).size must not be equalTo(Nil)
    }
    
  }
  
}