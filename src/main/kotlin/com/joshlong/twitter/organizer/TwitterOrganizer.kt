package com.joshlong.twitter.organizer

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.event.EventListener
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.stereotype.Component
import javax.sql.DataSource

/**
 * @author <a href="mailto:josh@joshlong.com">Josh Long</a>
 */
fun main(args: Array<String>) {
	runApplication<TwitterOrganizer>(*args)
}

@SpringBootApplication
@EnableBatchProcessing
class TwitterOrganizer {

	@Bean
	fun jdbcTemplate(ds: DataSource) = JdbcTemplate(ds)

}

@Component
class Initializer(private val jdbcTemplate: JdbcTemplate) {

	@EventListener(ApplicationReadyEvent::class)
	fun ready() {

		data class Reservation(val id: Long, val name: String)

		this.jdbcTemplate
				.queryForList("select * from reservation", RowMapper<Reservation> { rs, i ->
					Reservation(rs.getLong("id"), rs.getString("name"))
				})
				.forEach {
					println(it)
				}
	}
}