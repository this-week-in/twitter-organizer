package com.joshlong.twitter.organizer

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.core.JdbcTemplate
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
