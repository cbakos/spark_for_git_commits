package DataFrameAssignment

import java.sql.Timestamp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._



/**
  * Note read the comments carefully, as they describe the expected result and may contain hints in how
  * to tackle the exercises. Note that the data that is given in the examples in the comments does
  * reflect the format of the data, but not the result the graders expect (unless stated otherwise).
  */
object DFAssignment {

  /**
    * In this exercise we want to know all the commit SHA's from a list of commit committers. We require these to be
    * in order according to timestamp.
    *
    * | committer      | sha                                      | timestamp            |
    * |----------------|------------------------------------------|----------------------|
    * | Harbar-Inbound | 1d8e15a834a2157fe7af04421c42a893e8a1f23a | 2019-03-10T15:24:16Z |
    * | ...            | ...                                      | ...                  |
    *
    * Hint: try to work out the individual stages of the exercises, which makes it easier to track bugs, and figure out
    * how Spark Dataframes and their operations work. You can also use the `printSchema()` function and `show()`
    * function to take a look at the structure and contents of the Dataframes.
    *
    * @param commits Commit Dataframe, created from the data_raw.json file.
    * @param authors Sequence of String representing the authors from which we want to know their respective commit
    *                SHA's.
    * @return DataFrame of commits from the requested authors, including the commit SHA and the according timestamp.
    */
  def assignment_1(commits: DataFrame, authors: Seq[String]): DataFrame = {
    val columnNames = Seq("commit.committer.name","sha","commit.committer.date")
    commits.filter(commits("commit.committer.name").isin(authors: _*))
      .sort("commit.committer.date").select(columnNames.head, columnNames.tail: _*)
  }

  /**
    * In order to generate weekly dashboards for all projects, we need the data to be partitioned by weeks. As projects
    * can span multiple years in the data set, care must be taken to partition by not only weeks but also by years.
    * The returned DataFrame that is expected is in the following format:
    *
    * | repository | week             | year | count   |
    * |------------|------------------|------|---------|
    * | Maven      | 41               | 2019 | 21      |
    * | .....      | ..               | .... | ..      |
    *
    * @param commits Commit Dataframe, created from the data_raw.json file.
    * @return Dataframe containing 4 columns, Repository name, week number, year and the number fo commits for that
    *         week.
    */
  def assignment_2(commits: DataFrame): DataFrame = {
    val columnNames = Seq("url", "commit.committer.date")
    commits.select(columnNames.head, columnNames.tail: _*)
      .groupBy(substring_index(col("url"), "/", 6).as("b"), weekofyear(col("date")).as("week"), year(col("date")).as("year"))
      .count()
      .groupBy(substring_index(col("b"), "/", -1).as("repository"), col("week"), col("year"))
      .sum("count")
  }

  /**
    * A developer is interested in the age of commits in seconds, although this is something that can always be
    * calculated during runtime, this would require us to pass a Timestamp along with the computation. Therefore we
    * require you to append the inputted DataFrame with an age column of each commit in `seconds`.
    *
    * Hint: Look into SQL functions in for Spark SQL.
    *
    * Expected Dataframe (column) example that is expected:
    *
    * | age    |
    * |--------|
    * | 1231   |
    * | 20     |
    * | ...    |
    *
    * @param commits Commit Dataframe, created from the data_raw.json file.
    * @return the inputted DataFrame appended with an age column.
    */
  def assignment_3(commits: DataFrame, snapShotTimestamp: Timestamp): DataFrame = {
      commits.withColumn("now", lit(snapShotTimestamp))
        .select(commits.col("*"), col("commit.committer.date").as("age"), col("now"))
        .select(commits.col("*"), (datediff(col("now"), col("age")).*(24*60*60))
          .+ (second(col("now"))-second(col("age"))).+((hour(col("now"))- hour(col("age")))
          .*(60*60)).+((minute(col("now"))- minute(col("age"))).*(60)))
  }

  /**
    * To perform analysis on commit behavior the intermediate time of commits is needed. We require that the DataFrame
    * that is put in is appended with an extra column that expresses the number of days there are between the current
    * commit and the previous commit of the user, independent of the branch or repository.
    * If no commit exists before a commit regard the time difference in days should be zero. Make sure to return the
    * commits in chronological order.
    *
    * Hint: Look into Spark sql's Window to have more expressive power in custom aggregations
    *
    * Expected Dataframe example:
    *
    * | $oid                     	| name   	| date                     	| time_diff 	|
    * |--------------------------	|--------	|--------------------------	|-----------	|
    * | 5ce6929e6480fd0d91d3106a 	| GitHub 	| 2019-01-27T07:09:13.000Z 	| 0         	|
    * | 5ce693156480fd0d5edbd708 	| GitHub 	| 2019-03-04T15:21:52.000Z 	| 36        	|
    * | 5ce691b06480fd0fe0972350 	| GitHub 	| 2019-03-06T13:55:25.000Z 	| 2         	|
    * | ...                      	| ...    	| ...                      	| ...       	|
    *
    * @param commits    Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                   `println(commits.schema)`.
    * @param authorName Name of the author for which the result must be generated.
    * @return DataFrame with column expressing days since last commit.
    */
  def assignment_4(commits: DataFrame, authorName: String): DataFrame = {
    val overCategory = Window.partitionBy("name").orderBy("date")
    val columnNames = Seq("_id", "commit.committer.name", "commit.committer.date")
    commits.filter(col("commit.committer.name") === authorName)
      .select(columnNames.head, columnNames.tail: _*)
      .withColumn("previous_commit_times", collect_list("date") over overCategory)
      .withColumn("previous", col("previous_commit_times")
      .apply(size(col("previous_commit_times")).minus(2)))
      .drop(col("previous_commit_times"))
      .withColumn("time_diff_null", datediff(col("date"), col("previous")))
      .drop(col("previous"))
      .withColumn("time_diff", when(col("time_diff_null").isNull, 0).otherwise(col("time_diff_null")))
  }

  /**
    * To get a bit of insight in the spark SQL, and its aggregation functions, you will have to implement a function
    * that returns a DataFrame containing a column `day` (int) and a column `commits_per_day`, based on the commits
    * commit date. Sunday would be 1, Monday 2, etc.
    *
    * Expected Dataframe example:
    *
    * | day | commits_per_day|
    * |-----|----------------|
    * | 0   | 32             |
    * | ... | ...            |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing a `day` column and a `commits_per_day` representing a count of the total number of
    *         commits that that were ever made on that week day.
    */
  def assignment_5(commits: DataFrame): DataFrame = {
    //commits.select(commits.col("_id"), col("commit.committer.date")).withColumn("time_utc", expr("to_utc_timestamp(date, 'Europe/Amsterdam')"))
    commits.select(commits.col("_id"), col("commit.committer.date"))
      .withColumn("time_utc", expr("to_utc_timestamp(date, 'spark.sql.session.timeZone')"))
      .drop("date")
      .select(commits.col("_id"), col("time_utc"), date_format(col("time_utc"), "u").as("dow_number").cast(IntegerType))
      .groupBy(col("dow_number"))
      .count()
      .withColumn("dow_number", when(col("dow_number") === 7, 1).otherwise(col("dow_number")+1))
      .withColumnRenamed("count","commits_per_day").withColumnRenamed("dow_number", "day" )
      .sort("day")
  }

  /**
    * Commits can be uploaded on different days, we want to get insight in difference in commit time of the author and
    * the committer. Append the given dataframe with a column expressing the number of seconds in difference between
    * the two events in the commit data.
    *
    * Expected Dataframe (column) example:
    *
    * | commit_time_diff |
    * |------------------|
    * | 1022             |
    * | 0                |
    * | ...              |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return original Dataframe appended with a column `commit_time_diff` containing the number of seconds time
    *         difference between authorizing and committing.
    */
  def assignment_6(commits: DataFrame): DataFrame = {
    //val columnNames = Seq("*", "commit.author.date", "commit.committer.date")
    commits
      .withColumn("author_time", expr("to_utc_timestamp(commit.author.date, 'spark.sql.session.timeZone')"))
      .withColumn("committer_time", expr("to_utc_timestamp(commit.committer.date, 'spark.sql.session.timeZone')"))
        //.select("author_time", "committer_time")
        //.withColumn("commit_time_diff", )
      .select(commits.col("*"), ((datediff(col("committer_time"), col("author_time")).*(24*60*60)).+ (second(col("committer_time"))-second(col("author_time"))).+((hour(col("committer_time"))- hour(col("author_time"))).*(60*60)).+((minute(col("committer_time"))- minute(col("author_time"))).*(60))).as("commit_time_diff"))
        //.select("commit_time_diff")
    //a.show(3,false)
    //a
  }

  /**
    * Using Dataframes find all the commit SHA's from which a branch was created, including the number of
    * branches that were made. Only take the SHA's into account if they are also contained in the RDD.
    * Note that the returned Dataframe should not contain any SHA's of which no new branches were made, and should not
    * contain a SHA which is not contained in the given Dataframe.
    *
    * Expected Dataframe example:
    *
    * | sha                                      | times_parent |
    * |------------------------------------------|--------------|
    * | 3438abd8e0222f37934ba62b2130c3933b067678 | 2            |
    * | ...                                      | ...          |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing the SHAs of which a new branch was made.
    */
  def assignment_7(commits: DataFrame): DataFrame = {
    val a = commits.select("sha")
    val b = commits.select("parents")
    a.join(b, expr("array_contains(parents.sha, sha)"), "inner")
        .groupBy("sha")
        .count()
        .filter(col("count")>1)
  }
}
