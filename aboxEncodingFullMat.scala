import scala.collection.JavaConversions._
import java.io.File
import scala.collection.mutable.ListBuffer

val directory = "/home/oliv/git/SPARQL-Spark/liteMat++/drugbankExt/"
val file = "drugbankExt.nt"

// Load the Drugbank data set
val triples = sc.textFile(directory+file).map(x=>x.split(" ")).map(t=>(t(0),t(1),t(2)))

// load metadata
val metadata = sc.textFile(directory+"/dct/metadata").map(x=>x.split(" "))
val metadataKV = metadata.map(x=>(x(0),x(1)))

// load the dictionaries
val concepts = sc.textFile(directory+"/dct/concepts.dct").map(x=>x.split(" "))
val conceptsId2URI = concepts.map(x=>(x(1),x(0),x(2)))
conceptsId2URI.persist
val conceptsURI2Id = concepts.map(x=>(x(0),x(1)))
conceptsURI2Id.persist

val properties = sc.textFile(directory+"/dct/properties.dct").map(x=>x.split(" "))
val propertiesId2URI = properties.map(x=>(x(1),"<"+x(0)+">",x(2)))
conceptsId2URI.persist
val propertiesURI2Id = properties.map(x=>("<"+x(0)+">",x(1)))
conceptsURI2Id.persist

val individuals = sc.textFile(directory+"/dct/individuals.dct").map(x=>x.split(" "))
val indivId2URI = individuals.map(x=>((x(0),x(1)),x(2)))
indivId2URI.persist
val indivURI2Id = individuals.map(x=>(x(2),(x(0),x(1))))
indivURI2Id.persist

///////////////////////////
// Encoding Abox dataset 

// remove sameAs statements
val abox0 = triples.filter(x=>x._2!="<http://www.w3.org/2002/07/owl#sameAs>").map(x=>(x._2,(x._1,x._3)))

// encode property elements
val abox1 = abox0.join(propertiesURI2Id).map{case(p,((s,o),idp))=>(s,(idp,o.replace("\"","")))}

// encode subject elements
val abox2 = abox1.join(indivURI2Id).map{case(s,((p,o),ids))=>(o,(ids,p))}

// encode property elements
val abox = abox2.join(indivURI2Id).union(abox2.join(conceptsURI2Id)).map{case(o,((s,p),ido))=>(s,p,ido)}

/////////////////////////////
// sameAs Materilization

// processing sameAs in subject position
val sameAsFirstId = metadataKV.lookup("sameAsStartsAt").get(0).toLong
val aboxMat1 = abox.filter{case(s,p,o)=> s._1.toLong>= sameAsFirstId}

val aboxMat2 = aboxMat1.flatMap{case(s,p,o)=> materializeSubject(s,p,o)}



val aboxMat = abox.map{case(s,p,o)=>(s._1,(p,o))}.join(indivId2URI.map{case((cid,lid),uri)=>(cid,(lid,null))}).map{case(cid,((p,(ocid,olid)),(slid,n)))=>((cid,slid),p,(ocid,olid))}

// encode concept elements (object position)
//val abox2 = abox1.join(conceptsURI2Id).map{case(o,((idp,s),ido))=>(s,(idp,ido))}

aboxMat.map{case(s,p,o)=>s._1+" "+s._2+" "+p+" "+o._1+" "+o._2).saveAsTextFile(directory+"/eaboxMat/")