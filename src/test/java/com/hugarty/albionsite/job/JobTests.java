package com.hugarty.albionsite.job;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.Sql.ExecutionPhase;
import org.springframework.test.context.jdbc.SqlGroup;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestTemplate;

import com.hugarty.albionsite.job.config.JobConfig;
import com.hugarty.albionsite.job.dto.alliance.AllianceDTO;
import com.hugarty.albionsite.job.item.stepone.FetchAlliancesAndDetachedGuildsItemProcessor;
import com.hugarty.albionsite.job.item.stepthree.BuildAlliancesDailyItemProcessor;
import com.hugarty.albionsite.job.item.steptwo.FetchGuildDailyAndInvalidAllianceItemProcessor;
import com.hugarty.albionsite.job.rest.RestRetryableProvider;

/* 
 * @SqlGroup
 * Use this approuch because ins't possible execute SQL only one time and
 * isn't possible change the TransactionMode because Spring Batch has it's 
 * own transaction that needs the Batch Schema.
 */
@SqlGroup({
		@Sql(
				scripts = { "/create-batch-domain-schema.sql",
				"/insert-invalid-alliance.sql" }, executionPhase = ExecutionPhase.BEFORE_TEST_METHOD),
		@Sql(
				scripts = { "/drop-batch-domain-schema.sql" }, 
				executionPhase = ExecutionPhase.AFTER_TEST_METHOD)
})
@RunWith(SpringRunner.class)
@SpringBatchTest
@ContextConfiguration(classes={JobTests.TestConfig.class, 
		FetchAlliancesAndDetachedGuildsItemProcessor.class,
		FetchGuildDailyAndInvalidAllianceItemProcessor.class,
		BuildAlliancesDailyItemProcessor.class,
		RestRetryableProvider.class })
public class JobTests {

	@Autowired
	private JobLauncherTestUtils jobLauncherTestUtils;

	@MockBean
	private RestTemplate restTemplate;

	@Test
	public void should_work_when_runJob() throws Exception {
		ResponseEntity<AllianceDTO> ok = ResponseEntity.ok(new AllianceDTO());
		when(restTemplate.getForEntity(anyString(), any()))
				.thenReturn((ResponseEntity) ok);

		JobExecution launchJob = jobLauncherTestUtils.launchJob();
		assertEquals(ExitStatus.COMPLETED, launchJob.getExitStatus());
	}

	@Test
	public void should_work_when_tryFetchAllianceData_and_restFail_FewTimes() throws Exception {
		ResponseEntity<AllianceDTO> ok = ResponseEntity.ok(new AllianceDTO());
		ResponseEntity<AllianceDTO> fail = ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
		when(restTemplate.getForEntity(anyString(), any()))
				.thenReturn((ResponseEntity) fail)
				.thenReturn((ResponseEntity) fail)
				.thenReturn((ResponseEntity) fail)
				.thenReturn((ResponseEntity) ok);

		JobExecution launchJob = jobLauncherTestUtils.launchJob();
		assertEquals(ExitStatus.COMPLETED, launchJob.getExitStatus());
	}

	@Test
	public void should_fail_when_tryFetchAllianceData_and_restFail() throws Exception {
		ResponseEntity<AllianceDTO> fail = ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
		when(restTemplate.getForEntity(anyString(), any()))
				.thenReturn((ResponseEntity) fail);

		JobExecution launchJob = jobLauncherTestUtils.launchJob();
		
		assertEquals(ExitStatus.FAILED.getExitCode(), launchJob.getExitStatus().getExitCode());
		verify(restTemplate, atLeast(RestRetryableProvider.MAX_ATTEMPTS)).getForEntity(anyString(), any());
	}

	@Configuration
	@EnableBatchProcessing
	@Import(JobConfig.class)
	static class TestConfig {

		@Bean
		public NamedParameterJdbcTemplate namedParameterJdbcTemplate(DataSource dataSource) {
			return new NamedParameterJdbcTemplate(dataSource);
		}

		@Bean
		public DataSource dataSource() {
			DriverManagerDataSource dataSource = new DriverManagerDataSource();
			dataSource.setDriverClassName("org.h2.Driver");
			dataSource.setUrl("jdbc:h2:mem:db;DB_CLOSE_DELAY=-1");
			dataSource.setUsername("sa");
			dataSource.setPassword("sa");

			return dataSource;
		}
	}
}
