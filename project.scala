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
    val distinctMagDF = magDF.distinct()
    val magSelectColumns= distinctMagDF.select("authors","fos","id","keywords","lang","n_citation","publisher","references","title","venue","year")
    val magDropNA = magSelectColumns.na.drop()
    val authorDetails = magDropNA.select("id","authors","fos")
    val authorsDetailsDf = authorDetails.withColumn("authors_name", magDropNA("authors.name"))
    val authorsExplodefos = authorsDetailsDf.select($"id",$"authors_name",explode($"fos").alias("fos"))
    val authorDf = authorsExplodefos.select($"id",explode($"authors_name").alias("authors"),$"fos").na.drop().distinct()
    val df1 = authorDf.withColumnRenamed("id","id_1").withColumnRenamed("authors","authors1").withColumnRenamed("fos","fos1")
    val df2 = authorDf.withColumnRenamed("id","id_2").withColumnRenamed("authors","authors2").withColumnRenamed("fos","fos2")
    val df_authors = df1.join(df2, ((df1("id_1")===df2("id_2")) && (df1("authors1")!==df2("authors2"))),"inner").na.drop().distinct()
    val df =  df_authors.select("authors1","authors2","fos1")
    val vertices = df.select("authors1").distinct().na.drop()
    val v = vertices.withColumnRenamed("authors1","id")
    val e = df.withColumnRenamed("authors1","src").withColumnRenamed("authors2","dst").withColumnRenamed("fos1","relationship")
    val g = GraphFrame(v, e)
    g
}

val g = createAuthorsGraph()
