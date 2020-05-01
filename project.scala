//import requirements
import org.apache.spark.sql.SQLContext 
val sqlCtx = new SQLContext(sc)
import sqlCtx._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.graphframes.GraphFrame
import org.apache.spark.HashPartitioner

//import MAG Data
val magDF = sqlCtx.jsonFile("/user/nr2229/Project/MAG/*")

//MAG DATA and clean mag data
def cleanMAG(): DataFrame ={
    val distinctMagDF = magDF.distinct()
    val magSelectColumns= distinctMagDF.select("authors","fos","id","keywords","lang","n_citation","publisher","references","title","venue","year")
    val magDropNA = magSelectColumns.na.drop()
    val magExplodeFOSDF = magDropNA.select($"authors",explode($"fos").alias("fos"),$"id",$"keywords",$"lang",$"n_citation",$"publisher",$"references",$"title",$"venue",$"year") 
    val magExplodeKeywordsDF = magExplodeFOSDF.select($"authors",$"fos",$"id",explode($"keywords").alias("keywords"),$"lang",$"n_citation",$"publisher",$"references",$"title",$"venue",$"year")
    var magExplodeAuthorsDF = magExplodeKeywordsDF.withColumn("authors_name", magExplodeKeywordsDF("authors.name"))
    val magPaperNADF = magExplodeAuthorsDF.select($"fos", $"id", $"keywords", $"lang", $"n_citation", $"publisher", $"references", $"title", $"venue", $"year", explode($"authors_name").alias("authors_name"))
    val magPaperDF = magPaperNADF.na.drop()
    magPaperDF
}

//mag profiling
val magPaperDF = cleanMAG()
magDF.count
magPaperDF.count
val dfGroupbyFos = magPaperDF.groupBy("fos").count()
val dfGroupbyKeywords = magPaperDF.groupBy("keywords").count()

//import Aminer Data
val aminerDF = sqlCtx.jsonFile("/user/nr2229/Project/Data/Aminer/*")

//AMINER DATA and clean 
def cleanAminer(): DataFrame = {
    val distinctAminerDF = aminerDF.distinct()
    val aminerSelectColumns= distinctAminerDF.select("authors", "id", "keywords", "lang", "n_citation", "title", "venue", "year")
    val aminerDropNA = aminerSelectColumns.na.drop()
    val aminerExplodeKeywordsDF = aminerDropNA.select($"authors", $"id", explode($"keywords").alias("keywords"), $"lang", $"n_citation", $"title", $"venue", $"year")
    val aminerExplodeVenueDF = aminerExplodeKeywordsDF.withColumn("venue_name",aminerExplodeKeywordsDF("venue.raw"))
    var aminerExplodeAuthorsDF = aminerExplodeVenueDF.withColumn("authors_name", aminerExplodeVenueDF("authors.name"))
    val aminerPaperNADF = aminerExplodeAuthorsDF.select($"id", $"keywords", $"lang", $"n_citation", $"title", $"venue_name".alias("venue"), $"year",explode($"authors_name").alias("authors_name"))
    val aminerPaperDF = aminerPaperNADF.na.drop()
    aminerPaperDF
}


//AMINER PROFILING
val aminerPaperDF = cleanAminer()
aminerDF.count
aminerPaperDF.count
val dfGroupbyKeywordsAminer = aminerPaperDF.groupBy("keywords").count()

//Join datasets (aminer and mag )
def joinDatasets(): DataFrame = {
    val missingFields = magPaperDF.schema.toSet.diff(aminerPaperDF.schema.toSet)
    var aminerDF: DataFrame = null
    for (field <- missingFields){ 
    aminerDF = magPaperDF.withColumn(field.name, expr("null")); 
    } 
    val ResearchPaperDF= magPaperDF.unionAll(aminerDF)
    ResearchPaperDF
}

//Number of rows
val ResearchPaperDF = joinDatasets()
ResearchPaperDF.count

//Functions to Analyse the data

//Number of papers in a field of study
def fieldOfStudyCount(): DataFrame = {
    val fosDF = ResearchPaperDF.groupBy("fos").count()
    fosDF
}
val fosDf = fieldOfStudyCount()
//Numeber of research papers over the years
def researchByYear(): DataFrame = {
    val timelineDF = ResearchPaperDF.groupBy("year").count()
    timelineDF
}
val timelineDf = researchByYear()

//Number of research papers based on the year and field of study
def researchByYearField(): DataFrame = {
    val timelineFieldDF = ResearchPaperDF.groupBy("year","fos").count()
    timelineFieldDF
}
val timelineFieldDf = researchByYearField()

//Number of research papers based on a particular field of study
def researchByField(field: String): DataFrame = {
    val selectField = ResearchPaperDF.select("fos").where(ResearchPaperDF("fos")===field)
    //val compsciDF = ResearchPaperDF.groupBy("year","fos").count().where(ResearchPaperDF("fos")==="Computer Science")
    selectField
}
val csDf = researchByField("Computer Science")

//Number of research papers based on a particular field of study in a particular year
def researchByYearSpecificField(field: String): DataFrame = {
    val fieldDF = ResearchPaperDF.groupBy("year","fos").count().where(ResearchPaperDF("fos")===field)
    fieldDF
}
val fieldDf = researchByYearSpecificField("Computer Science")

//Authors analysis

//Numer of papers each author has contributed
def authorPapers(): DataFrame = {
    val author = ResearchPaperDF.groupBy("authors_name").count()
    val authorDF = author.orderBy($"count".desc)
    authorDF
}

val authorDf = authorPapers()

//Number of papers each author contributed to based on field
def authorPapersField(): DataFrame = {
    val authorFieldDF = ResearchPaperDF.groupBy("authors_name","fos").count()
    authorFieldDF
}
val authorFieldDf = authorPapersField()

//Number of citations per a paper 
def paperCitations(): DataFrame = {
    val paperCitationDf =  ResearchPaperDF.select("title","n_citation").sort(desc("n_citation"))
    paperCitationDf
}

val paperCitationDf = paperCitations()

def paperFieldCitations(): DataFrame = {
    val paperCitationFieldDf =  ResearchPaperDF.select("title","n_citation","fos").groupBy("fos","n_citation").count
    paperCitationFieldDf
}
val paperCitationFieldDf = paperFieldCitations()

def popularKeywords(): DataFrame = {
    val popularKeywords = ResearchPaperDF.select("keywords").groupBy("keywords").count.orderBy($"count".desc)
    popularKeywords
}

val popularKeywordsdf = popularKeywords()

//Number of publications from paper venue
def NumberOfPublications_ByVenue(): DataFrame = {
    val NumberOfPublications_ByVenue = ResearchPaperDF.groupBy("venue").count()
    NumberOfPublications_ByVenue
}
val NumberOfPublications_ByVenue_DF = NumberOfPublications_ByVenue()


// Number of publications in different languages
def NumberOfPublications_ByLanguage(): DataFrame = {
    val NumberOfPublications_ByLanguage = ResearchPaperDF.groupBy("lang").count()
    NumberOfPublications_ByLanguage
}
val NumberOfPublications_ByLanguage_DF = NumberOfPublications_ByLanguage()

//Group publication by their volume
def volume_Of_Paper(): DataFrame = {
    val volume_Of_Paper = ResearchPaperDF.withColumn("Volume_ofPaper",$"page_end" - $"page_start")
    val volume_Of_Paper_df = volume_Of_Paper.groupBy($"Volume_ofPaper").count()
    volume_Of_Paper_df
}

val volume_Of_Paper_DF = volume_Of_Paper()


//Group publication by their volume
def volume_Of_Paper_fos(): DataFrame = {
    val volume_Of_Paper_fos = ResearchPaperDF.withColumn("Volume_ofPaper",$"page_end" - $"page_start")
    volume_Of_Paper_fos = volume_Of_Paper_fos.groupBy($"lang").mean("Volume_ofPaper")
    volume_Of_Paper_fos
}

val volume_Of_Paper_fos = volume_Of_Paper_fos()


//page rank for the authors in CS research papers
def pageRankAuthor() ={
    val authorstCS = ResearchPaperDF.select("id","authors_name","fos").where(ResearchPaperDF("fos")==="Computer Science")
    val authorsFinalDF = authorstCS.na.drop()
    val authorsFinal1 = authorsFinalDF.withColumnRenamed("id","id_1").withColumnRenamed("authors_name","author1")
    val authorsFinal2 = authorsFinalDF.withColumnRenamed("id","id_2").withColumnRenamed("authors_name","author2")
    val df_authors = authorsFinal1.join(authorsFinal2, ((authorsFinal1("id_1")===authorsFinal2("id_2")) && (authorsFinal1("author1")!==authorsFinal2("author2"))),"inner")
    val df_authors_final = df_authors.select("author1","id_1","author2").na.drop
    val authorsTuple = df_authors_final.select("author1","author1").rdd.map(x => (x.get(0).toString(), x.get(1).toString()))
    val authorsTupleGroup =  authorsTuple.groupByKey()
    val authors = authorsTupleGroup.partitionBy(new HashPartitioner(10000)).persist()
    var ranks = authors.mapValues(v=>1.0)
    for(i <-0 until 10){
        val contributions = authors.join(ranks).flatMap{
            case (pageId, (pageLinks,rank)) =>
                pageLinks.map(dest => (dest, rank/pageLinks.size))
        }
        ranks = contributions.reduceByKey((x,y)=>x+y).mapValues(v=>0.15 + 0.85*v)
    }
    ranks
}

var authorRanks = pageRankAuthor()

def createAuthorsGraph()={
    val researchDF = ResearchPaperDF.select("id","authors_name","fos").na.drop()
    val researchDF1 = researchDF.withColumnRenamed("id","id1").withColumnRenamed("authors_name","authors1").withColumnRenamed("fos","fos1")
    val researchDF2 = researchDF.withColumnRenamed("id","id2").withColumnRenamed("authors_name","authors2").withColumnRenamed("fos","fos2")
    val df_authors = researchDF1.join(researchDF2, ((researchDF1("id1")===researchDF2("id2")) && (researchDF1("authors1")!==researchDF2("authors2"))),"inner").na.drop().distinct()
    val authorsDf =  df_authors.select("authors1","authors2","fos1")
    val vertices = authorsDf.select("authors1").distinct().na.drop()
    val v = vertices.withColumnRenamed("authors1","id")
    val e = authorsDf.withColumnRenamed("authors1","src").withColumnRenamed("authors2","dst").withColumnRenamed("fos1","relationship")
    val g = GraphFrame(v, e)
    g
}

val g_author_fos = createAuthorsGraph()

//Queries on the graph
val results = g.triangleCount.run()
results.select("id", "count").show()

val result = g.labelPropagation.maxIter(5).run()
result.select("id", "label").show()

val numField = g.edges.filter("relationship = 'Economics' AND relationship = 'Marketing'").count()
val numField = g.edges.filter("relationship = 'Economics' ").count()

// Graph of Volume of paper's published to year
def year_VolumeOfPapers_Graph()={
    var vertexArray = ResearchPaperDF.select("year")
    vertexArray = vertexArray.na.drop()
    vertexArray = vertexArray.distinct()
    vertexArray = vertexArray.withColumnRenamed("year","year_publish")
    var edge_Array = ResearchPaperDF.groupBy("year").count()
    val graph_data = vertexArray.join(edge_Array, ((vertexArray("year_publish")===edge_Array("year"))),"inner")
    var edges = graph_data.select($"year",$"count")
    var vertex = graph_data("year_publish")
    var vertex_withid = vertex.withColumnRenamed("year_publish","id")
    var edges_withReln = graph_data.withColumnRenamed("year_publish","src").withColumnRenamed("year","dst").withColumnRenamed("count","relationship")
    var graph_year_volumeOfPublish = GraphFrame(vertex_withid, edges_withReln)
    graph_year_volumeOfPublish
}

val volumeOfPaper_year = year_VolumeOfPapers_Graph()


//Keywords analysis with papers and authors 
def createKeywordsGraph() = {
    val researchDF = ResearchPaperDF.select("id","authors_name","title","keywords","year","n_citation").na.drop()
    val researchDF1 = researchDF.withColumnRenamed("id","id1").withColumnRenamed("authors_name","authors1").withColumnRenamed("keywords","keywords1").withColumnRenamed("year","year1").withColumnRenamed("n_citation","n_citation1").withColumnRenamed("title","title1")
    val researchDF2 = researchDF.withColumnRenamed("id","id2").withColumnRenamed("authors_name","authors2").withColumnRenamed("keywords","keywords2").withColumnRenamed("year","year2").withColumnRenamed("n_citation","n_citation2").withColumnRenamed("title","title2")
    val df_keywords = researchDF1.join(researchDF2, ((researchDF1("id1")===researchDF2("id2")) && (researchDF1("authors1")!==researchDF2("authors2"))),"inner").na.drop().distinct()
    //show df_keywords
    val authorsDf =  df_keywords.select("authors1","authors2","keywords1","title1","year1","n_citation1")
    val vertices = authorsDf.select("authors1","title1","year1","n_citation1").distinct().na.drop()
    val v = vertices.withColumnRenamed("authors1","id")
    val e = authorsDf.withColumnRenamed("authors1","src").withColumnRenamed("authors2","dst").withColumnRenamed("keywords1","relationship")
    val g = GraphFrame(v, e)
    g
}

val g_author_keyword = createKeywordsGraph()

def createFieldOfStudyGraph() = {
    val researchDF = ResearchPaperDF.select("id","authors_name","title","keywords","year","n_citation","fos").na.drop()
    val researchDF1 = researchDF.withColumnRenamed("id","id1").withColumnRenamed("authors_name","authors1").withColumnRenamed("keywords","keywords1").withColumnRenamed("year","year1").withColumnRenamed("n_citation","n_citation1").withColumnRenamed("title","title1").withColumnRenamed("fos","fos1")
    val researchDF2 = researchDF.withColumnRenamed("id","id2").withColumnRenamed("authors_name","authors2").withColumnRenamed("keywords","keywords2").withColumnRenamed("year","year2").withColumnRenamed("n_citation","n_citation2").withColumnRenamed("title","title2").withColumnRenamed("fos","fos2")
    val df_keywords = researchDF1.join(researchDF2, ((researchDF1("id1")===researchDF2("id2")) && (researchDF1("keywords1")!==researchDF2("keywords2"))),"inner").na.drop().distinct()
    //show df_keywords
    val keywordsDf =  df_keywords.select("keywords1","keywords2","authors1","title1","year1","n_citation1","fos1")
    val vertices = keywordsDf.select("keywords1","title1","year1","n_citation1","authors1").distinct().na.drop()
    val v = vertices.withColumnRenamed("keywords1","id")
    val e = keywordsDf.withColumnRenamed("keywords1","src").withColumnRenamed("keywords2","dst").withColumnRenamed("fos1","relationship")
    val g = GraphFrame(v, e)
    g
}

val g_keyword_fos = createFieldOfStudyGraph()

def createPublisherGraph() = {
    val researchDF = ResearchPaperDF.select("id","keywords","publisher","fos").where(ResearchPaperDF("fos")==="Computer Science").na.drop()
    val researchDF1 = researchDF.withColumnRenamed("id","id1").withColumnRenamed("publisher","publisher1").withColumnRenamed("keywords","keywords1").withColumnRenamed("fos","fos1")
    val researchDF2 = researchDF.withColumnRenamed("id","id2").withColumnRenamed("publisher","publisher2").withColumnRenamed("keywords","keywords2").withColumnRenamed("fos","fos2")
    val df_publisher = researchDF1.join(researchDF2, ((researchDF1("id1")===researchDF2("id2")) && (researchDF1("keywords1")!==researchDF2("keywords2"))),"inner").na.drop().distinct()
    //show df_keywords
    val publisherDf =  df_publisher.select("keywords1","keywords2","publisher1")
    val vertices = publisherDf.select("keywords1").distinct().na.drop()
    val v = vertices.withColumnRenamed("keywords1","id")
    val e = publisherDf.withColumnRenamed("keywords1","src").withColumnRenamed("keywords2","dst").withColumnRenamed("publisher1","relationship")
    val g = GraphFrame(v, e)
    g
}

val g_keyword_publisher = createPublisherGraph()


g_author_fos.vertices.write.parquet("user/nr2229/Project/Outputs/Grpahs/authorfos/vertices")
g_author_fos.edges.write.parquet("user/nr2229/Project/Outputs/Grpahs/authorfos/edges")

g_author_keyword.vertices.write.parquet("user/nr2229/Project/Outputs/Grpahs/authorkeywords/vertices")
g_author_keyword.edges.write.parquet("user/nr2229/Project/Outputs/Grpahs/authorkeywords/edges")

g_keyword_publisher.vertices.write.parquet("user/nr2229/Project/Outputs/Grpahs/keywordpublisher/vertices")
g_keyword_publisher.edges.write.parquet("user/nr2229/Project/Outputs/Grpahs/keywordpublisher/edges")

g_keyword_fos.vertices.write.parquet("user/nr2229/Project/Outputs/Grpahs/keywordfos/vertices")
g_keyword_fos.edges.write.parquet("user/nr2229/Project/Outputs/Grpahs/keywordfos/edges")