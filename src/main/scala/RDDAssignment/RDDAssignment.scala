package RDDAssignment

import java.util.UUID

import java.math.BigInteger
import java.security.MessageDigest

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import utils.{Commit, File, Stats}

object RDDAssignment {


  /**
    * Reductions are often used in data processing in order to gather more useful data out of raw data. In this case
    * we want to know how many commits a given RDD contains.
    *
    * @param commits RDD containing commit data.
    * @return Long indicating the number of commits in the given RDD.
    */
  def assignment_1(commits: RDD[Commit]): Long = {

    commits.count()

  }

  /**
    * We want to know how often programming languages are used in committed files. We require a RDD containing Tuples
    * of the used file extension, combined with the number of occurrences. If no filename or file extension is used we
    * assume the language to be 'unknown'.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing tuples indicating the programming language (extension) and number of occurrences.
    */
  def assignment_2(commits: RDD[Commit]): RDD[(String, Long)] = {

    commits.flatMap(fileName => fileName.files.map(fileExtensions => fileExtensions.filename match {
      case _ if fileExtensions.filename.get.contains(".") => fileExtensions.filename.get.split("\\.").last
      case _ => "unknown"
    })).map(fileNames => (fileNames, 1)).groupBy(_._1)
      .mapValues(seq => seq.reduce { (x, y) => (x._1, x._2 + y._2) }._2).sortBy(x => x._2, false)
      .mapValues(longValues => longValues.toLong)
  }


  /**
    * Competitive users on Github might be interested in their ranking in number of commits. We require as return a
    * RDD containing Tuples of the rank (zero indexed) of a commit author, a commit authors name and the number of
    * commits made by the commit author. As in general with performance rankings, a higher performance means a better
    * ranking (0 = best). In case of a tie, the lexicographical ordering of the usernames should be used to break the
    * tie.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing commit author names and total count of commits done by the author, in ordered fashion.
    */
  def assignment_3(commits: RDD[Commit]): RDD[(Long, String, Long)] = {

    commits.map(userCommits => (userCommits.commit.author.name, 1)).groupBy(_._1)
      .mapValues(seq => seq.reduce { (x, y) => (x._1, x._2 + y._2) }._2).sortBy(x => (x._2), false)
      .zipWithIndex.map(commitTuple => (commitTuple._2, commitTuple._1._1, commitTuple._1._2.toLong))

  }

  /**
    * Some users are interested in seeing an overall contribution of all their work. For this exercise we an RDD that
    * contains the committer name and the total of their commits. As stats are optional, missing Stat cases should be
    * handles as s"Stat(0, 0, 0)". If an User is given that is not in the dataset, then the username should not occur in
    * the return RDD.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing committer names and an aggregation of the committers Stats.
    */
  def assignment_4(commits: RDD[Commit], users: List[String]): RDD[(String, Stats)] = {

    commits.map(userCommits => (userCommits.commit.author.name, (userCommits.stats.get.total, userCommits.stats.get.additions, userCommits.stats.get.deletions)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3)).map(x => (x._1, Stats(x._2._1, x._2._2, x._2._3)))
      .filter(x => users.contains(x._1))
  }

  /**
    * There are different types of people, those who own repositories, and those who make commits. Although Git blame is
    * excellent in finding these types of people, we want to do it in Spark. We require as output an RDD containing the
    * names of commit authors and repository owners that have either exclusively committed to repositories, or
    * exclusively own repositories in the given RDD. Note that the repository owner is contained within Github urls.
    *
    * @param commits RDD containing commit data.
    * @return RDD of Strings representing the username that have either only committed to repositories or only own
    *         repositories.
    */

  def assignment_5(commits: RDD[Commit]): RDD[String] = {

    commits.map(commitersData => (commitersData.url.split("/")(4).filterNot(_ == commitersData.commit.author.name)))
      .union(commits.map(commitersData => commitersData.commit.author.name.filterNot(_ == commitersData.url.split("/")(4))))
      .distinct

  }

  /**
    * Sometimes developers make mistakes, sometimes they make many. One way of observing mistakes in commits is by
    * looking at so-called revert commits. We define a 'revert streak' as the number of times `Revert` occurs
    * in a commit. Note that for a commit to be eligible for a 'commit streak', its message must start with `Revert`.
    * As an example: `Revert "Revert ...` would be a revert streak of 2, whilst `Oops, Revert Revert little mistake`
    * would not be a 'revert streak' at all.
    * We require as return a RDD containing Tuples of the username of a commit author and a Tuple containing
    * the length of the longest streak of an user and how often said streak has occurred.
    * Note that we are only interested in the longest commit streak of each author (and its frequency).
    *
    * @param commits RDD containing commit data.
    * @return RDD of Tuple type containing a commit author username, and a tuple containing the length of the longest
    *         commit streak as well its frequency
    */
  def assignment_6(commits: RDD[Commit]): RDD[(String, (Int, Int))] = {

    val reg2 = "(^)(\\S+)((\\s+)\\2)+"

    val reg = "(revert)".r

    commits.map(userCommits => (userCommits.commit.author.name, userCommits.commit.message)).filter(x => x._2.startsWith("Revert"))
      //.map(x => (x._1, (reg.findAllMatchIn(x._2.toLowerCase.replaceAll("[^A-Za-z0-9]", " ")).length)))
      .map(x => (x._1, (x._2.toLowerCase.replaceAll("[^A-Za-z0-9]", " ")).split(reg2).length))
      .mapValues(x => (x, 1)).reduceByKey((x, y) => if (x._1 == y._1) (x._1, x._2 + y._2) else if (x._1 > y._1) (x._1, x._2) else (y._1, y._2))

  }

  /**
    * We want to know the number of commits that are made to each repository contained in the given RDD. Besides the
    * number of commits, we also want to know the unique committers that contributed to the repository. Note that from
    * this exercise on, expensive functions like groupBy are no longer allowed to be used. In real life these wide
    * dependency functions are performance killers, but luckily there are better performing alternatives!
    * The automatic graders will check the computation history of the returned RDD's.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing a tuple indicating the repository name, the number of commits made to the repository as
    *         well as the unique committer usernames that committed to the repository.
    */
  def assignment_7(commits: RDD[Commit]): RDD[(String, Long, Iterable[String])] = {
    //repository name
    //num of commits = stats total
    //reponames = blob url
    //usernames = commiter,names


    commits.map(commitersData => (commitersData.url.split("/")(5), (1, (commitersData.commit.committer.name))))
      .reduceByKey((x, y) => ((x._1 + y._1), if (x._2 != y._2) x._2 + "," + y._2 else x._2))
      .map(x => (x._1, x._2._1, x._2._2.split(",").distinct.toIterable))


  }

  /**
    * Return RDD of tuples containing the repository name and all the files that are contained in that repository.
    * Note that the file names must be unique, so if files occur multiple times (for example due to removal, or new
    * additions), the newest File object must be returned. As the files' filenames are an `Option[String]` discard the
    * files that do not have a filename.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing the files in each repository as described above.
    */
  def assignment_8(commits: RDD[Commit]): RDD[(String, Iterable[File])] =
  {

    commits.map(commitersData => (commitersData.url.split("/")(5), (
      commitersData.files.filterNot(_.filename == null).map(f => (f, f.filename, commitersData.commit.author.date)))))
      .flatMap { case (key, values) => values.map((key, _)) }
      .map(x => (x._1, (Iterable(x._2._1), x._2._2, x._2._3)))
      .reduceByKey((x, y) => if (x._2 == y._2) (x._1 ++ y._1, x._2, x._3) else if (x._3.after(y._3)) (x._1, x._2, x._3) else (y._1,  x._2, x._3))
      .map(x => (x._1, x._2._1))

  }
  /**
    * For this assignment you are asked to find all the files of a single repository. This in order to create an
    * overview of each files, do this by creating a tuple containing the file name, all corresponding commit SHA's
    * as well as a Stat object representing the changes made to the file.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing Tuples representing a file name, its corresponding commit SHA's and a Stats object
    *         representing the total aggregation of changes for a file.
    */
  def assignment_9(commits: RDD[Commit], repository: String): RDD[(String, Seq[String], Stats)] = {

    commits.map(commitersData => (commitersData.url.split("/")(5),
      commitersData.files.filterNot(_.filename == null).map(f => (f.filename, f.sha, f.additions, f.deletions))))
      .filter(_._1 == repository)
      .flatMap(x => x._2)
      .map(x => (x._1.get, (x._2, x._3 + x._4, x._3, x._4)))
      .reduceByKey((x,y) =>  (x._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
      .map(y => (y._1, y._2._1.toSeq, Stats(y._2._2, y._2._3, y._2._4)))

  }

  /**
    * We want to generate an overview of the work done by an user per repository. For this we request an RDD containing a
    * tuple containing the committer username, repository name and a `Stats` object. The Stats object containing the
    * total number of additions, deletions and total contribution.
    * Note that as Stats are optional, therefore a type of Option[Stat] is required.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing tuples of committer names, repository names and and Option[Stat] representing additions and
    *         deletions.
    */
  def assignment_10(commits: RDD[Commit]): RDD[(String, String, Option[Stats])] = {
    commits.map(c => ((c.commit.committer.name, c.url.split("/")(5)), c.stats))
      //.filter(a => a._2.isDefined)
      .reduceByKey((a, b) => Option(Stats(a.get.additions+b.get.additions, a.get.deletions + b.get.deletions, a.get.total+b.get.total)))
      .map(c=> (c._1._1, c._1._2, c._2))
  }
}
