package com.hugarty.albionsite.job;

import java.time.LocalDateTime;

import org.springframework.batch.core.Job;
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

@SpringBootApplication(scanBasePackages = {"com.hugarty.albionsite.job"})
@EnableBatchProcessing
public class AlbionJobApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext run = SpringApplication.run(AlbionJobApplication.class, args);

		JobLauncher jobLauncher = run.getBean(JobLauncher.class);
		Job job = run.getBean(Job.class);

		JobParameters jobParameters = new JobParametersBuilder()
				.addString("paramTeste", LocalDateTime.now().toString())
				.toJobParameters();

		try {

			jobLauncher.run(job, jobParameters);

		} catch (JobExecutionAlreadyRunningException 
				| JobRestartException 
				| JobInstanceAlreadyCompleteException
				| JobParametersInvalidException e) {
			e.printStackTrace();
		}
	}
}
