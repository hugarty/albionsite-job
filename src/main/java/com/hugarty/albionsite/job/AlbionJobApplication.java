package com.hugarty.albionsite.job;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication(scanBasePackages = {"com.hugarty.albionsite.job"})
@EnableBatchProcessing
@EnableScheduling
public class AlbionJobApplication {

	private static Logger logger = LoggerFactory.getLogger(AlbionJobApplication.class);
	static ConfigurableApplicationContext applicationContext;

	public static void main(String[] args) {
		applicationContext = SpringApplication.run(AlbionJobApplication.class, args);
	}

	@Scheduled(cron = "0 0 23 * * *")
	public void launchDailyJob () {
		JobLauncher jobLauncher = applicationContext.getBean(JobLauncher.class);
		Job job = applicationContext.getBean("dailyJob", Job.class);
		JobParameters jobParameters = new JobParametersBuilder()
				.addString("localdatetime-irrelevant-param", LocalDateTime.now().toString())
				.toJobParameters();

		try {			
			JobExecution jobExecution = jobLauncher.run(job, jobParameters);
			logger.info("Job Start Time: {}", jobExecution.getStartTime());
			logger.info("Job End Time: {}", jobExecution.getEndTime());
			logger.info("Time execution Millis: {}", jobExecution.getEndTime().getTime() - jobExecution.getStartTime().getTime());
		} catch (JobExecutionAlreadyRunningException 
				| JobRestartException 
				| JobInstanceAlreadyCompleteException
				| JobParametersInvalidException e) {
			e.printStackTrace();
		}
	}
}
