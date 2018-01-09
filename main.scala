import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object P1{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("P1").setMaster("spark://jupiter:31106")
    val sc = new SparkContext()

    // read titles
    // val raw_titles = sc.textFile("/data/titles3.txt")
    val raw_titles = sc.textFile("hdfs://jupiter:31101/data/titles-sorted.txt")
    val titles = raw_titles.zipWithIndex().map { case (k, v) => (v + 1, k) }

    // get root set
    val root_set = titles.filter { case (k, v) => v.contains(args(0))}

    // read links
    // val links = sc.textFile("/data/links3.txt")
    val links = sc.textFile("hdfs://jupiter:31101/data/links-simple-sorted.txt")
    val map_links = links.filter(s=>(s.endsWith(": ")==false)).map(s => (s.split(':')(0).toLong, s.split(':')(1).trim.split(' '))).flatMapValues(x => x).map{case(k,v) => (k, v.toLong)}
  
    // filter links root = from
    val from_links = map_links.join(root_set).map{case(k,(v1,v2)) => (k,v1)}

    // filter links root = to
    val to_links = map_links.map{case(k,v) => (v,k)}.join(root_set).map{case(k,(v1,v2)) => (v1,k)}

    // get base set
    var base_links = from_links.union(to_links).distinct().sortByKey()
    base_links.cache()

    var base_set = from_links.map{case(k,v) => (v,k)}.union(to_links).groupByKey().join(titles).map{case(k,(v1,v2)) => (k,v2)}
    base_set = base_set.union(root_set).distinct().sortByKey()
    base_set.cache()
    

    // hub = from, auth = to
    // hub score = sum of auth, auth score = sum of hub

    // initialize auths and hubs
    var auths = base_set.map{case(k,v) => (k, 1.0)}
    var hubs = base_set.map{case(k,v) => (k, 1.0)}

    // iterating until converge

    val max_iter = 30
    var count = 0
    while(count < max_iter){
       // println(count)

      // compute auth score
      var temp_auths = auths

      auths = base_links.join(hubs).map{case(k,(v1,v2)) => (v1,v2)}.reduceByKey((x, y) => x+y).rightOuterJoin(hubs).map{case(k,(Some(v1),v2)) => (k, v1);case(k,(None,v2)) => (k,0)}
      var auths_sum = auths.map(_._2).sum()
      auths = auths.map{case(k,v) => (k, BigDecimal(v/auths_sum).setScale(5, BigDecimal.RoundingMode.HALF_UP).toDouble)}
      auths.cache()
      
      
      var temp_hubs = hubs
      // compute hub score
      hubs = base_links.map{case(k,v) => (v,k)}.join(auths).map{case(k,(v1,v2)) => (v1,v2)}.reduceByKey((x, y) => x+y).rightOuterJoin(auths).map{case(k,(Some(v1),v2)) => (k, v1);case(k,(None,v2)) => (k,0)}
      var hubs_sum = hubs.map(_._2).sum()
      hubs = hubs.map{case(k,v) => (k, BigDecimal(v/hubs_sum).setScale(5, BigDecimal.RoundingMode.HALF_UP).toDouble)}
      hubs.cache()
      
      // if(auths.subtract(temp_auths).count()==0 && hubs.subtract(temp_hubs).count()==0){break}
      count = count + 1
    }
    
    var auths_output = auths.join(base_set).map{case(k,(v1,v2)) => (v1,v2)}.sortByKey(false)
    
    var hubs_output = hubs.join(base_set).map{case(k,(v1,v2)) => (v1,v2)}.sortByKey(false)
    
    //output
    auths_output.saveAsTextFile("hdfs://jupiter:31101/output/auth")
    hubs_output.saveAsTextFile("hdfs://jupiter:31101/output/hub")

  }
}
