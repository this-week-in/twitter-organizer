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
import org.springframework.social.twitter.api.Twitter
import org.springframework.social.twitter.api.impl.TwitterTemplate
import java.time.Instant
import java.time.ZoneId
import javax.sql.DataSource

fun main(args: Array<String>) {
	runApplication<TwitterOrganizer>(*args)
}

/**
 * TODO
 * 1. run thru all the followers on twitter for my account
 * 2. store their information into a DB
 * 3. figure out how to sort them into different categories. maybe use neo4j to identify clusters?
 * 4. unfollow the ones we've flagged for unsubscription.
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
class TwitterOrganizer {

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
	fun determineIfProfileIdsTableIsEmptyStep(): TaskletStep =
			sbf
					.get("is-profile-ids-empty")
					.tasklet({ contribution, _ ->

						log.info("starting the job @ ${Instant.now().atZone(ZoneId.systemDefault())}")

						val import = this.template.queryForObject(
								"SELECT COUNT(*) FROM profile_ids", Long::class.java) == 0L

						contribution.exitStatus =
								if (import)
									ExitStatus(IMPORT_PROFILE_IDS)
								else
									ExitStatus.COMPLETED

						if (import) this.log.debug("going to import profile IDs.")

						RepeatStatus.FINISHED
					})
					.build()

	// todo this should be a bit cleaner.
	// todo it should reflect that 'processed' = enriched.
	@Bean
	fun markProfilesAsProcessedStep(): TaskletStep =
			sbf
					.get("mark-as-enriched")
					.tasklet({ contribution, _ ->
						// select pids.id  from PROFILE_IDS pids where pids.ID in (select p.id from PROFILE p ) ;
						this.log.info("finishing the job @ ${Instant.now().atZone(ZoneId.systemDefault())}")
						this.template.update(
								"""
									UPDATE profile_ids pi SET pi.processed = NOW() WHERE pi.id IN ( SELECT p.id FROM profile p )
									"""
										.trimMargin())

						RepeatStatus.FINISHED
					})
					.build()

	@Bean
	fun job(importConfig: ImportProfileIdsStepConfiguration,
	        enrichConfig: EnrichProfileStepConfiguration,
	        unfollowConfig: UnfollowStepConfiguration): Job {
		val isProfileIdsEmpty = this.determineIfProfileIdsTableIsEmptyStep()
		val endTasklet = this.markProfilesAsProcessedStep()
		val importStep = importConfig.importStep()
		val enrichStep = enrichConfig.enrichProfilesStep()
		val unfollowStep = unfollowConfig.unfollowStep()
		return jbf
				.get("twitter-organizer")
				.incrementer(RunIdIncrementer())
				.flow(isProfileIdsEmpty).on(IMPORT_PROFILE_IDS).to(importStep)
				.from(importStep).on("*").to(enrichStep)
				.from(isProfileIdsEmpty).on("*").to(enrichStep)
				.from(enrichStep).on("*").to(unfollowStep)
				.from(unfollowStep).on("*").to(endTasklet)
				.build()
				.build()
	}
}


@Configuration
class UnfollowStepConfiguration(
		private val sbf: StepBuilderFactory,
		private val ds: DataSource,
		private val twitter: Twitter) {

	private val log = LogFactory.getLog(javaClass)

	@Bean
	fun unfollowReader(): JdbcCursorItemReader<Long> =
			JdbcCursorItemReaderBuilder<Long>()
					.name("select-profiles-to-unfollow")
					.dataSource(ds)
					.sql(" select ID from profile where followed = 0 and followed_date IS NULL LIMIT 100 ")
					.dataSource(this.ds)
					.rowMapper({ rs, _ -> rs.getLong("ID") })
					.build()

	@Bean
	fun unfollowProcessor() = ItemProcessor<Long, Long> { profileId ->
		val friendOperations = twitter.friendOperations()
		val response = friendOperations.unfollow(profileId)
		log.info("unfollowed $profileId; response: $response")
		profileId
	}

	@Bean
	fun unfollowWriter(): JdbcBatchItemWriter<Long> =
			JdbcBatchItemWriterBuilder<Long>()
					.dataSource(ds)
					.itemPreparedStatementSetter { profileId, ps ->
						ps.setLong(1, profileId)
					}
					.sql(""" update profile set followed_date = NOW() where id = ?  """)
					.build()


	@Bean
	fun unfollowStep(): Step =
			this.sbf
					.get("unfollow-profile-step")
					.chunk<Long, Long>(100)
					.reader(this.unfollowReader())
					.processor(this.unfollowProcessor())
					.writer(this.unfollowWriter())
					.build()
}


/**
 * Turn all the IDs in the PROFILE_IDS table into entries in the PROFILE table.
 */
@Configuration
class EnrichProfileStepConfiguration(
		val sbf: StepBuilderFactory,
		val ds: DataSource,
		val twitter: Twitter) {

	private val log = LogFactory.getLog(javaClass)


	@Bean
	fun enrichReader(): JdbcCursorItemReader<Long> =
			JdbcCursorItemReaderBuilder<Long>()
					.name("profile-id-importReader")
					.dataSource(ds)
					.sql(" select ID from profile_ids where processed IS NULL LIMIT 500 ")
					.dataSource(this.ds)
					.rowMapper({ rs, _ -> rs.getLong("ID") })
					.build()

	@Bean
	fun enrichProcessor(): ItemProcessor<Long, Profile> = ItemProcessor {
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
	fun enrichWriter(): JdbcBatchItemWriter<Profile> =
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
						replace into profile( id, following, verified, name, description, screen_name )
						values( ?,?,?,?,?,? )
						"""
					)
					.build()

	@Bean
	fun enrichProfilesStep(): Step =
			this.sbf.get("enrich-profile-configuration")
					.chunk<Long, Profile>(100)
					.reader(this.enrichReader())
					.processor(this.enrichProcessor())
					.writer(this.enrichWriter())
					.build()
}

class Profile(
		var id: Long,
		var following: Boolean = false,
		var verified: Boolean = false,
		var name: String,
		var description: String,
		var screenName: String)

/**
 * Import all the profile IDs into the staging database table.
 */
@Configuration
class ImportProfileIdsStepConfiguration(
		val twitter: Twitter,
		val ds: DataSource,
		val sbf: StepBuilderFactory) {

	@Bean
	fun importReader(): ItemReader<Long> =
			IteratorItemReader<Long>(twitter.friendOperations().friendIds.iterator())

	@Bean
	fun importWriter(): ItemWriter<Long> =
			JdbcBatchItemWriterBuilder<Long>()
					.sql(" replace into profile_ids(ID) values(?) ") // MySQL-nese for 'upsert'
					.dataSource(ds)
					.itemPreparedStatementSetter { id, ps -> ps.setLong(1, id) }
					.build()

	@Bean
	fun importStep(): Step =
			sbf.get("fetch-all-twitter-ids")
					.chunk<Long, Long>(10000)
					.reader(this.importReader())
					.writer(this.importWriter())
					.build()
}