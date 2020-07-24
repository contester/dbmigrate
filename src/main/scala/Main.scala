import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import slick.jdbc.{GetResult, JdbcBackend, JdbcCapabilities, JdbcProfile, MySQLProfile, PositionedParameters, SetParameter}

import scala.async.Async._
import scala.concurrent._
import com.github.tminglei.slickpg._
import play.api.libs.json.{JsValue, Json}
import slick.basic.Capability

trait MyPostgresProfile extends ExPostgresProfile
  with PgArraySupport
  with PgDate2Support
  with PgDateSupportJoda
  with PgRangeSupport
  with PgHStoreSupport
  with PgPlayJsonSupport
  with PgSearchSupport
  with PgNetSupport
  with PgLTreeSupport {
  def pgjson = "jsonb" // jsonb support is in postgres 9.4.0 onward; for 9.3.x use "json"

  // Add back `capabilities.insertOrUpdate` to enable native `upsert` support; for postgres 9.5+
  override protected def computeCapabilities: Set[Capability] =
    super.computeCapabilities + JdbcCapabilities.insertOrUpdate

  override val api = MyAPI

  object MyAPI extends API with ArrayImplicits
    with DateTimeImplicits
    with JsonImplicits
    with NetImplicits
    with LTreeImplicits
    with RangeImplicits
    with HStoreImplicits
    with SearchImplicits
    with SearchAssistants {
    implicit val strListTypeMapper = new SimpleArrayJdbcType[String]("text").to(_.toList)
    implicit val playJsonArrayTypeMapper =
      new AdvancedArrayJdbcType[JsValue](pgjson,
        (s) => utils.SimpleArrayUtils.fromString[JsValue](Json.parse(_))(s).orNull,
        (v) => utils.SimpleArrayUtils.mkString[JsValue](_.toString())(v)
      ).to(_.toList)
  }
}

object MyPostgresProfile extends MyPostgresProfile

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

      source.run(sql"""select ID, Name, PolygonID, Start, Finish, End, Expose from Contests""".as[Contest])
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
    }
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
      source.run(sql"""select ID, School, Num, Name from Teams""".as[Team])
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
      sqlu"""truncate table schools, teams, participants, logins cascade""",
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
      Language(r.nextLong(), r.nextString().stripPrefix("."), r.nextString()))

      source.run(sql"""select ID, Ext, Name from Languages where Contest = 1""".as[Language])
    }

    println("a")

    case class Submit(contest: Long, team: Long, problem: String, lang: Long, id: Long, arrived: DateTime, computerID: String, source: Array[Byte])

    val submits = await {
      import slick.jdbc.MySQLProfile.api._
      implicit val getSubmit = GetResult(r =>
        Submit(r.nextLong(), r.nextLong(), r.nextString(), r.nextLong(), r.nextLong(), new DateTime(r.nextTimestamp()), r.nextString(), r.nextBytes())
      )

      source.run(sql"""select Contest, Team, Problem, SrcLang, Id, Arrived, INET_NTOA(Computer), Source from NewSubmits""".as[Submit])
    }

    case class CustomTest(id: Long, contest: Long, team: Long, language: String, source: Array[Byte], input: Array[Byte], arrived: DateTime)
    println("b")

    val customTests = await {
      import slick.jdbc.MySQLProfile.api._
      implicit val getCust = GetResult(r =>
        CustomTest(r.<<, r.<<, r.<<, r.<<, r.nextBytes(), r.nextBytes(), new DateTime(r.nextTimestamp()))
      )

      source.run(sql"""select ID, Contest, Team, Ext, Source, Input, Arrived from Eval""".as[CustomTest])
    }

    val languageMap = languages.groupBy(_.ext).view.mapValues(_.head)
    println("c")

    import MyPostgresProfile.api._
    import com.github.tototoshi.slick.PostgresJodaSupport._
    implicit object SetByteArray extends SetParameter[Array[Byte]] {
      override def apply(v1: Array[Byte], v2: PositionedParameters): Unit = v2.setBytes(v1)
    }

    val insertLanguages = languages.map { c =>
      sqlu"""insert into languages (id, name, module_id) values (${c.id}, ${c.name}, ${c.ext})"""
    }

    val insertSubmits = submits.map { c =>
      println(c)
      sqlu"""insert into submits (contest, team_id, problem, id, language_id, source, submit_time_absolute, computer_id)
            values (${c.contest}, ${c.team}, ${c.problem}, ${c.id}, ${c.lang}, ${c.source}, ${c.arrived}, inet(${c.computerID}))"""
    }

    val insertCustom = customTests.map { c =>
      var lang = languageMap.apply(c.language.stripPrefix("."))
      sqlu"""insert into custom_test (id, contest, team_id, language_id, source, input, submit_time_absolute) values
            (${c.id}, ${c.contest}, ${c.team}, ${lang.id}, ${c.source}, ${c.input}, ${c.arrived})"""
    }

    val clearAll = Seq(
      sqlu"""truncate table languages, submits, testings, results, custom_test cascade""",
    )

    val all = DBIO.sequence(clearAll ++ insertLanguages ++ insertSubmits ++ insertCustom)

    await {
      dest.run(all.transactionally)
    }
  }

  def fv[T](x: Future[T])(implicit ec: ExecutionContext): Future[T] = {
    x.onComplete{ v =>
      println(v)
    }
    x
  }

  import ExecutionContext.Implicits.global

  import scala.concurrent.duration._

  Await.ready(fv(copyContests(source, dest)), 30 seconds)
  Await.ready(fv(copyTeams(source, dest)), 30 seconds)
  Await.ready(fv(copySubmits(source, dest)), 30 seconds)
}