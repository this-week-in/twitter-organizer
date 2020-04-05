package com.joshlong.twitter.organizer

import com.joshlong.twitter.api.TwitterClient
import org.apache.commons.logging.LogFactory
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
import javax.sql.DataSource

fun main(args: Array<String>) {
	runApplication<TwitterOrganizer>(*args)
}

/**
 *
 * 1. this will load all the IDs for all the profiles i'm following on twitter into the following table  &&
 *    this will update the profiles table, setting the 'followed' column to be true for the profile ID (if the ID exists)
 * 2. this will then go through all the profile IDs from the IDs table and ensure that
 *    we have that record in the profiles table.
 * 3. then we follow the profile on twitter itself to ensure that followed
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

}

@Configuration
class JobConfiguration(
		val jbf: JobBuilderFactory,
		val resetProfileIdsStep: ResetProfileIdsStepConfiguration,
		val importProfileIdsStep: ImportProfileIdsStepConfiguration,
		val enrichProfileStep: EnrichProfileStepConfiguration,
		val followStep: FollowStepConfiguration) {

	@Bean
	fun job(): Job = jbf.get("twitter-organizer")
			.start(resetProfileIdsStep.resetProfileIdsStep())
			.next(importProfileIdsStep.importProfileIdsStep())
			.next(enrichProfileStep.enrichProfileStep())
			.next(followStep.followStep())
			.incrementer(RunIdIncrementer())
			.build()
}

@Configuration
class FollowStepConfiguration(
		private val sbf: StepBuilderFactory,
		private val ds: DataSource,
		private val twitterClient: TwitterClient) {

	private val log = LogFactory.getLog(javaClass)

	@Bean
	fun followStepReader(): JdbcCursorItemReader<Long> =
			JdbcCursorItemReaderBuilder<Long>()
					.name("followStepReader")
					.dataSource(ds)
					.sql(" select p.ID from profile p where p.following = 1 and p.id not in (select pids.id from profile_ids pids) ")
					.dataSource(this.ds)
					.rowMapper { rs, _ -> rs.getLong("ID") }
					.build()

	private fun follow(profileId: Long) {
		TODO("need to implement this")
	}

	@Bean
	fun followStepProcessor() = ItemProcessor<Long, Long> { profileId ->
		follow(profileId)
		profileId
	}

	@Bean
	fun followStepWriter(): JdbcBatchItemWriter<Long> =
			JdbcBatchItemWriterBuilder<Long>()
					.dataSource(ds)
					.itemPreparedStatementSetter { profileId, ps ->
						ps.setLong(1, profileId)
					}
					.sql(""" update profile set following = 1 where id = ?  """)
					.build()

	@Bean
	fun followStep(): Step =
			this.sbf
					.get("followStep")
					.chunk<Long, Long>(100)
					.reader(this.followStepReader())
					.processor(this.followStepProcessor())
					.writer(this.followStepWriter())
					.build()
}


@Configuration
class ResetProfileIdsStepConfiguration(val sbf: StepBuilderFactory, val template: JdbcTemplate) {

	@Bean
	fun resetProfileIdsStep(): TaskletStep = sbf
			.get("reset-step")
			.tasklet({ _, _ ->
				template.execute(""" DELETE FROM profile_ids """)
				RepeatStatus.FINISHED
			})
			.build()
}

@Configuration
class ImportProfileIdsStepConfiguration(
		val twitter: TwitterClient,
		val ds: DataSource,
		val sbf: StepBuilderFactory) {

	// todo
	private fun readFriendIds(): Iterator<Long> = emptyList<Long>().iterator()

	@Bean
	fun importProfileIdsReader(): ItemReader<Long> =
			IteratorItemReader(readFriendIds())

	@Bean
	fun importProfileIdsWriter(): ItemWriter<Long> =
			JdbcBatchItemWriterBuilder<Long>()
					.sql(" replace into profile_ids(ID) values(?) ") // MySQL-nese for 'upsert'
					.dataSource(ds)
					.itemPreparedStatementSetter { id, ps -> ps.setLong(1, id) }
					.build()

	@Bean
	fun importProfileIdsStep(): Step =
			sbf.get("importProfileIdsStep")
					.chunk<Long, Long>(10_000)
					.reader(this.importProfileIdsReader())
					.writer(this.importProfileIdsWriter())
					.build()
}


@Configuration
class EnrichProfileStepConfiguration(
		val sbf: StepBuilderFactory,
		val ds: DataSource,
		val twitter: TwitterClient) {

	@Bean
	fun enrichProfileStepReader(): JdbcCursorItemReader<Long> =
			JdbcCursorItemReaderBuilder<Long>()
					.name("enrichProfileStepReader")
					.dataSource(ds)
					.sql(" select pids.ID from profile_ids pids where pids.ID not in (select p.ID from profile p) ")
					.dataSource(this.ds)
					.rowMapper { rs, _ -> rs.getLong("ID") }
					.build()

	private fun loadProfileFor(profileId: Long): Profile {
		TODO("need to load the Profile")
	}

	@Bean
	fun enrichProfileStepProcessor() = ItemProcessor<Long, Profile> { loadProfileFor(it) }

	@Bean
	fun enrichProfileStepWriter() = JdbcBatchItemWriterBuilder<Profile>()
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
	fun enrichProfileStep(): Step =
			this.sbf.get("enrichProfileStep")
					.chunk<Long, Profile>(100)
					.reader(this.enrichProfileStepReader())
					.processor(this.enrichProfileStepProcessor())
					.writer(this.enrichProfileStepWriter())
					.build()
}

class Profile(
		var id: Long,
		var following: Boolean = false,
		var verified: Boolean = false,
		var name: String,
		var description: String,
		var screenName: String)