import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import slick.jdbc.{GetResult, JdbcBackend, JdbcProfile, MySQLProfile}

import scala.async.Async._
import scala.concurrent._

object DispatcherServer extends App {

  private val config = ConfigFactory.load()

  private val source: MySQLProfile.backend.Database = {
    import slick.jdbc.MySQLProfile.api._
    Database.forConfig("source")
  }

  private val dest = {
    import slick.jdbc.PostgresProfile.api._
    Database.forConfig("dest")
  }

  def copyContests(source: JdbcBackend#DatabaseDef, dest: JdbcBackend#DatabaseDef)(implicit ec: ExecutionContext) = async {
    import com.github.nscala_time.time.Imports._
    case class Contest(id: Int, name: String, polygonID: String, start: DateTime, finish: DateTime, end: DateTime, expose: DateTime)

    val contests = await {
      import slick.jdbc.MySQLProfile.api._

      implicit val getContest = GetResult(r =>
        Contest(r.nextInt(), r.nextString(), r.nextString(), new DateTime(r.nextTimestamp()),
          new DateTime(r.nextTimestamp()), new DateTime(r.nextTimestamp()), new DateTime(r.nextTimestamp()))
      )

      val x = source.run(sql"""select ID, Name, PolygonID, Start, Finish, End, Expose from Contests""".as[Contest])
      x.onComplete(println(_))
      x
    }

    import slick.jdbc.PostgresProfile.api._
    import com.github.tototoshi.slick.PostgresJodaSupport._

    await {
      dest.run(
        DBIO.sequence(contests.map { c =>
          sqlu"""insert into contests (id, name, polygon_id, start_time, freeze_time, end_time, expose_time)
              values (${c.id}, ${c.name}, ${c.polygonID}, ${c.start}, ${c.finish}, ${c.end}, ${c.expose})
              on conflict (id) do update set name = ${c.name}, polygon_id = ${c.polygonID}, start_time = ${c.start},
              freeze_time = ${c.finish}, end_time = ${c.end}, expose_time = ${c.expose}"""
        }).transactionally
      )
    }.map(println(_))
  }

  def copyTeams(source: JdbcBackend#DatabaseDef, dest: JdbcBackend#DatabaseDef)(implicit ec: ExecutionContext) = async {
    case class School(id: Long, name: String)
    case class Team(id: Long, school: Long, num: Long, name: String)
    case class Participant(contest: Long, team: Long)
    case class Login(contest: Long, team: Long, username: String, password: String)

    val schools = await {
      import slick.jdbc.MySQLProfile.api._

      implicit val getSchool = GetResult(r =>
        School(r.nextLong(), r.nextString())
      )

      source.run(sql"""select ID, Name from Schools""".as[School])
    }

    val teams = await {
      import slick.jdbc.MySQLProfile.api._

      implicit val getTeam = GetResult(r =>
        Team(r.nextLong(), r.nextLong(), r.nextLong(), r.nextString()))
      source.run(sql"""select ID, School, Num, Name""".as[Team])
    }

    val participants = await {
      import slick.jdbc.MySQLProfile.api._

      implicit val getParticipant = GetResult(r =>
      Participant(r.nextLong(), r.nextLong()))

      source.run(sql"select Contest, Team from Participants".as[Participant])
    }

    val logins = await {
      import slick.jdbc.MySQLProfile.api._

      implicit val getLogin = GetResult(r =>
      Login(r.nextLong(), r.nextLong(), r.nextString(), r.nextString()))

      source.run(sql"""select Contest, LocalID, Username, Password from Assignments""".as[Login])
    }

    import slick.jdbc.PostgresProfile.api._
    import com.github.tototoshi.slick.PostgresJodaSupport._

    val cleanAll = Seq(
      sqlu"""truncate table schools""",
      sqlu"""truncate table teams""",
      sqlu"""truncate table participants""",
      sqlu"""truncate table logins""",
    )

    val insertSchools = schools.map { c =>
      sqlu"""insert into schools (id, short_name) values (${c.id}, ${c.name})"""
    }
    val insertTeams = teams.map { c =>
      sqlu"""insert into teams (id, school, num, name) values (${c.id}, ${c.school}, ${c.num}, ${c.name})"""
    }
    val insertParticipants = participants.map { c =>
      sqlu"""insert into participants (contest, team) values (${c.contest}, ${c.team})"""
    }
    val insertLogins = logins.map { c =>
      sqlu"""insert into logins (contest, team, username, password) values (${c.contest}, ${c.team}, ${c.username}, ${c.password})"""
    }

    val all = DBIO.sequence(cleanAll ++ insertSchools ++ insertTeams ++ insertParticipants ++ insertLogins)
    await {
      dest.run(all.transactionally)
    }
  }

  def copySubmits(source: JdbcBackend#DatabaseDef, dest: JdbcBackend#DatabaseDef)(implicit ec: ExecutionContext) = async {
    case class Language(id: Long, ext: String, name: String)

    var languages = await {
      import slick.jdbc.MySQLProfile.api._

      implicit val getLanguage = GetResult(r =>
      Language(r.nextLong(), r.nextString(), r.nextString()))

      source.run(sql"""select ID, Ext, Name from Languages where Contest = 1""".as[Language])
    }

    case class Submit(contest: Long, team: Long, problem: String, lang: Long, id: Long, arrived: DateTime, computerID: String, source: Array[Byte])

    val submits = await {
      import slick.jdbc.MySQLProfile.api._
      implicit val getSubmit = GetResult(r =>
        Submit(r.nextLong(), r.nextLong(), r.nextString(), r.nextLong(), r.nextLong(), new DateTime(r.nextTimestamp()), r.nextString(), r.nextBytes())
      )

      source.run(sql"""select Contest, Team, Problem, SrcLang, Id, Arrived, INET_NTOA(Computer) from NewSubmits""".as[Submit])
    }

    val languageMap = languages.map(v => v.copy(ext = v.ext.stripPrefix("."))).groupBy(_.ext).view.mapValues(_.head)

    import slick.jdbc.PostgresProfile.api._
    import com.github.tototoshi.slick.PostgresJodaSupport._

    val insertLanguages = languageMap.view.mapValues { c =>
      sqlu"""insert into languages (id, name, module_id) values (${c.id}, ${c.name}, ${c.ext})"""
    }

    val insertSubmits = submits.map { c =>
      sqlu"""insert into submits (contest, team_id, problem, id, language_id, source, submit_time_absolute, computer_id)
            values (${c.contest}, ${c.team}, ${c.problem}, ${c.id}, ${c.lang}, ${c.source}, ${c.arrived}, ${c.computerID})"""
    }

    val clearAll = Seq(
      sqlu"""truncate table languages""",
      sqlu"""truncate table submits""",
      sqlu"truncate table testings",
      sqlu"truncate table results"
    )

    val all = DBIO.sequence(clearAll ++ insertLanguages ++ insertSubmits)

    await {
      dest.run(all.transactionally)
    }
  }

  import ExecutionContext.Implicits.global

  import scala.concurrent.duration._

  Await.ready(copyContests(source, dest), 30 seconds)
  Await.ready(copyTeams(source, dest), 30 seconds)
  Await.ready(copySubmits(source, dest), 30 seconds)
}