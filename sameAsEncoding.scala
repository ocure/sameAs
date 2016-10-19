import org.apache.spark.graphx.Edge 
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrameWriter
import org.slf4j.impl.StaticLoggerBinder


import scala.collection.mutable.ListBuffer
import org.apache.spark.HashPartitioner

val NB_FRAGMENTS = sc.defaultParallelism
val part =  sc.defaultParallelism 

val directory = "/home/oliv/git/SPARQL-Spark/sameAs/datasets/drugbankFull/"  //Full
val file = "drugbank_dump.nt" //_dump.nt"

// Load the Drugbank data set
val triples0 = sc.textFile(directory+file).map(x=>x.split(" ")).map(t=>(t(0),t(1),t(2)))

// create an RDD made of the owl:sameAs statements
val sameAs = triples0.filter(x=>x._2=="<http://www.w3.org/2002/07/owl#sameAs>").map{case(s,p,o)=>(s,o)}

// create an RDD containing individuals involved in sameAs triples
val sameAsInd = sameAs.flatMap{case(s,o)=>Array(s,o)}.distinct

// create an RDD made of statements where the property is not owl:sameAs
val nonSameAs = triples0.filter{case(s,p,o)=>p!="<http://www.w3.org/2002/07/owl#sameAs>"}.map{case(s,p,o)=>(s,o.replace('\"',' ').trim)}

// create an RDD containing nonSameAs individuals
val nonSameAsInd = nonSameAs.flatMap{case(s,o)=>Array(s,o)}.distinct.subtract(sameAsInd).filter(x=>x.length>0)

// Create non SameAs Dictionary
val nonSameAsDictionary = nonSameAsInd.zipWithUniqueId.map{case(member, id)=> ((id,0),member)}

val nonSameAsLimit = nonSameAsDictionary.count * 2

// provide a unique Long id to all sameAs individuals
val sameAsIndId = sameAsInd.zipWithUniqueId

// create edges of the graph
val sameAsEdges = sameAs.join(sameAsIndId).map{case(s,(o,id))=>(o,id)}.join(sameAsIndId).map{case(o,(idS,idO))=>Edge(idS,idO,null)}

// create sameAs graph
val sameAsGraph = Graph(sameAsIndId.map{case(uri,id)=>(id,uri)}, sameAsEdges)

// Compute connected components of the graph
val connectedComponents = sameAsGraph.connectedComponents

// create an RDD containing the connected component id and an array of all URI in that individual cluster
val sameAsURIConnectedComp = sameAsIndId.map(x=>(x._2,x._1)).join(connectedComponents.vertices.map(x=>x)).map{case(k,(uri,cid))=>(cid,uri)}.groupByKey

val temp = sameAsURIConnectedComp.zipWithIndex.map{case((ccID,entities), id)=>(id+nonSameAsLimit, entities)}

def zipId(map : Iterable[String] ) : List[(Long,String)] = {
  var res = ListBuffer[(Long,String)]()
  var cumul : Long = 0;
  var i = map.iterator
  while (i.hasNext) {
    res.append((cumul,i.next))
    cumul = cumul + 1
  }
  return res.toList
}

val sameAsDictionary = temp.map{case(id, l)=> (id, zipId(l))}.flatMap{case(cid, list) => list.map{case(id, u)=> ((cid.toLong,id.toInt),u)}}

val metadata = sc.parallelize(Array(nonSameAsLimit))

// store dictionaries
nonSameAsDictionary.union(sameAsDictionary).map{case(id,ent)=>id._1+" "+id._2+" "+ent}.saveAsTextFile(directory+"/dct/individuals.dct") 
// toDS.write.parquet(directory+"/dct/individuals.dct")
metadata.map(x=>"sameAsStartsAt "+ x).saveAsTextFile(directory+"/dct/metadata")
