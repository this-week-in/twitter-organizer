package com.joshlong.twitter.organizer

import org.apache.commons.logging.LogFactory
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.step.tasklet.TaskletStep
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.database.JdbcBatchItemWriter
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder
import org.springframework.batch.item.support.IteratorItemReader
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.social.twitter.api.Twitter
import org.springframework.social.twitter.api.impl.TwitterTemplate
import java.sql.ResultSet
import java.time.Instant
import java.time.ZoneId
import javax.sql.DataSource

fun main(args: Array<String>) {
	runApplication<TwitterListMakerApplication>(*args)
}

/**
 * TODO
 * 1. run thru all the followers on twitter for my account
 * 2. store their information into a DB
 * 3. figure out how to sort them into different categories. maybe use neo4j to identify clusters?
 *
 * Twitter limits us to 15 calls per 15 minutes, so this will be a job that periodically runs through as many records
 * in the DB as possible, enriches their information and then marks those rows as processed. The next time the job
 * spins up it'll attempt to process the unfinished rows. It'll keep going until every follower's info (in
 * a couple of days) has been enriched. Then we can figure out what to do from there.
 *
 * @author <a href="mailto:josh@joshlong.com">Josh Long</a>
 */
@EnableConfigurationProperties(TwitterOrganizerProperties::class)
@SpringBootApplication
@EnableBatchProcessing
class TwitterListMakerApplication {

	@Bean
	fun jdbcTemplate(ds: DataSource) = JdbcTemplate(ds)

	@Bean
	fun twitter(props: TwitterOrganizerProperties) =
			TwitterTemplate(props.clientKey, props.clientKeySecret, props.accessToken, props.accessTokenSecret)
}

@Configuration
class JobConfiguration(val template: JdbcTemplate,
                       val jbf: JobBuilderFactory,
                       val sbf: StepBuilderFactory) {

	private val log = LogFactory.getLog(javaClass)

	companion object {
		const val IMPORT_PROFILE_IDS = "IMPORT_PROFILE_IDS"
	}

	@Bean
	fun startTasklet(): TaskletStep =
			sbf
					.get("is-profile-ids-empty")
					.tasklet({ contribution, _ ->

						log.info("starting the job @ ${Instant.now().atZone(ZoneId.systemDefault())}")

						val import = this.template.queryForObject(
								"SELECT COUNT(*) FROM PROFILE_IDS", Long::class.java) == 0L

						contribution.exitStatus =
								if (import)
									ExitStatus(IMPORT_PROFILE_IDS)
								else
									ExitStatus.COMPLETED

						if (import) this.log.debug("going to import profile IDs.")

						RepeatStatus.FINISHED
					})
					.build()

	@Bean
	fun endTasklet(): TaskletStep =
			sbf
					.get("endTasklet")
					.tasklet({ contribution, _ ->
						log.info("finishing the job @ ${Instant.now().atZone(ZoneId.systemDefault())}")
						RepeatStatus.FINISHED
					})
					.build()

	@Bean
	fun job(importConfig: ImportProfileIdsStepConfiguration,
	        enrichConfig: EnrichProfileStepConfiguration): Job {
		val isProfileIdsEmpty = this.startTasklet()
		val endTasklet = this.endTasklet()
		val importStep = importConfig.importProfileIdsStep()
		val enrichStep = enrichConfig.enrichProfilesStep()

		return jbf
				.get("twitter-organizer")
				.incrementer(RunIdIncrementer())
				.flow(isProfileIdsEmpty).on(IMPORT_PROFILE_IDS).to(importStep)
				.from(importStep).on("*").to(enrichStep)
				.from(isProfileIdsEmpty).on("*").to(enrichStep)
				.from(enrichStep).on("*").to(endTasklet)
				.build()
				.build()
	}
}

@Configuration
class EnrichProfileStepConfiguration(
		val sbf: StepBuilderFactory,
		val ds: DataSource,
		val twitter: Twitter) {

	private val log = LogFactory.getLog(javaClass)


	@Bean
	fun reader1(): JdbcCursorItemReader<Long> =
			JdbcCursorItemReaderBuilder<Long>()
					.name("profile-id-reader")
					.dataSource(ds)
					.sql(" select ID from PROFILE_IDS LIMIT 10 ")
					.dataSource(this.ds)
					.rowMapper({ rs, _ -> rs.getLong("ID") })
					.build()

	@Bean
	fun processor1(): ItemProcessor<Long, Profile> = ItemProcessor {
		this.log.debug("enriching ${Profile::class.java.name} for ID# $it.")
		val profile = twitter.userOperations().getUserProfile(it)
		Profile(id = profile.id,
				following = profile.isFollowing,
				verified = profile.isVerified,
				name = profile.name,
				description = profile.description,
				screenName = profile.screenName)
	}

	@Bean
	fun writer1(): JdbcBatchItemWriter<Profile> =
			JdbcBatchItemWriterBuilder<Profile>()
					.dataSource(ds)
					.itemPreparedStatementSetter { profile, ps ->
						ps.setLong(1, profile.id)
						ps.setBoolean(2, profile.following)
						ps.setBoolean(3, profile.verified)
						ps.setString(4, profile.name)
						ps.setString(5, profile.description)
						ps.setString(6, profile.screenName)
					}
					.sql(
							"""
						replace into PROFILE( id, following, verified, name, description, screen_name )
						values( ?,?,?,?,?,? )
						"""
					)
					.build()

	@Bean
	fun enrichProfilesStep(): Step =
			this.sbf.get("enrich-profile-configuration")
					.chunk<Long, Profile>(100)
					.reader(this.reader1())
					.processor(this.processor1())
					.writer(this.writer1())
					.build()
}

class Profile(
		var id: Long,
		var following: Boolean = false,
		var verified: Boolean = false,
		var name: String,
		var description: String,
		var screenName: String)

class ProfileRowMapper : RowMapper<Profile> {

	override fun mapRow(rs: ResultSet, indx: Int) =
			Profile(
					rs.getLong("id"),
					rs.getBoolean("following"),
					rs.getBoolean("verified"),
					rs.getString("name"),
					rs.getString("description"),
					rs.getString("screen_name"))
}

/**
 * Import all the profile IDs into the staging database table.
 */
@Configuration
class ImportProfileIdsStepConfiguration(
		val twitter: Twitter,
		val ds: DataSource,
		val sbf: StepBuilderFactory) {

	@Bean
	fun reader(): ItemReader<Long> =
			IteratorItemReader<Long>(twitter.friendOperations().friendIds.iterator())

	@Bean
	fun writer(): ItemWriter<Long> =
			JdbcBatchItemWriterBuilder<Long>()
					.sql(" replace into PROFILE_IDS(ID) values(?) ") // MySQL-nese for 'upsert'
					.dataSource(ds)
					.itemPreparedStatementSetter { id, ps -> ps.setLong(1, id) }
					.build()

	@Bean
	fun importProfileIdsStep(): Step =
			sbf.get("fetch-all-twitter-ids")
					.chunk<Long, Long>(10000)
					.reader(this.reader())
					.writer(this.writer())
					.build()
}


/*


@Configuration
class FollowingStepConfiguration(val jbf: JobBuilderFactory, val sbf: StepBuilderFactory, val ds: DataSource) {

	@Bean
	fun followingSqlItemReader() = JdbcCursorItemReaderBuilder<Following>()
			.sql("select * from following")
			.rowMapper(ProfileRowMapper())
			.dataSource(ds)
			.name("followingSqlItemReader")
			.build()

	@Bean
	fun enrichingProcessor(twitter: Twitter) = ItemProcessor<Following, Following> { following ->
		val userProfile = twitter.userOperations().getUserProfile(following.id)
		following.description = userProfile.description
		following.following = userProfile.isFollowing
		following.screenName = userProfile.screenName
		following.verified = userProfile.isVerified
		following.name = userProfile.name
		return@ItemProcessor following
	}

	@Bean
	fun followingSqlItemWriter() = null
}

@Configuration
class TwitterProfileEnrichmentJobConfiguration*/
/*

@Component
class Initializer(val twitter: Twitter,
                  val tt: TransactionTemplate,
                  val jdbc: JdbcTemplate) : ApplicationRunner {


    val log = LoggerFactory.getLogger(this::class.java)
    val username = "@starbuxman"


    override fun run(args: ApplicationArguments?) {


        twitter.friendOperations().friendIds.forEach({ id ->
            val followingSql = """
                        INSERT INTO following(
                            id,
                            following,
                            verified,
                            name,
                            description,
                            screen_name
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                    """
            jdbc.update(followingSql, id, false, false, null, null, null)
        })

    }
}
*/


/*  private fun old() {
        val maxCursorStateIdSql = """
            SELECT cs.cursor_state FROM cursor_state cs WHERE cs.id IN (SELECT max(cs1.id) FROM cursor_state cs1)
        """

        val maxCursorState: List<Long> = jdbc.query(maxCursorStateIdSql, {
            rs, i ->
            rs.getLong("cursor_state")
        })
        log.info("max cursor state: ${maxCursorState}")

        var friends: CursoredList<TwitterProfile>? = null
        if (maxCursorState.size == 1) {
            friends = twitter.friendOperations().getFriendsInCursor(username, maxCursorState.single())
        } else {
            friends = twitter.friendOperations().getFriends(username)
        }
        do {

            tt.execute {
                friends!!.forEach({
                    val followingSql = """
                        INSERT INTO following(
                            id,
                            following,
                            verified,
                            name,
                            description,
                            screen_name
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                    """
                    jdbc.update(followingSql, it.id, it.isFollowing, it.isVerified, it.name, it.description, it.screenName)
                    log.info("""
                        id: ${it.id}
                        isFollowing: ${it.isFollowing}
                        isVerified: ${it.isVerified}
                        name: ${it.name}
                        description: ${it.description}
                        screen-name: ${it.screenName}
                    """.split(System.lineSeparator())
                            .map { it.trim() }
                            .joinToString(System.lineSeparator())
                            .trim())
                })
                val nextCursor = friends!!.nextCursor
                friends = twitter.friendOperations().getFriendsInCursor(username, nextCursor)
                val stateSql = """
                  INSERT INTO cursor_state(cursor_state) VALUES (?)
                """
                jdbc.update(stateSql, nextCursor)
            }

        } while (friends!!.hasNext())
    }*/