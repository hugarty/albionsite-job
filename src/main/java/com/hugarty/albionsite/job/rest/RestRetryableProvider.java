package com.hugarty.albionsite.job.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.listener.RetryListenerSupport;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Component
public class RestRetryableProvider {

  private static final String URL_KEY = "URL";
	private static Logger logger = LoggerFactory.getLogger(RestRetryableProvider.class);
  
  // todo - Testar isso, LEMBRAR de adicionar no HEROKU
  
  private final Integer MAX_ATTEMPTS;
  private final Integer DELAY_MILLIS;
  private final RetryTemplate retryTemplate;
  private final RestTemplate restTemplate;

  public RestRetryableProvider(RestTemplate restTemplate, 
      @Value("${albion.job.max.attempts}") Integer MAX_ATTEMPTS,
      @Value("${albion.job.delay.millis}") Integer DELAY_MILLIS) {
    this.restTemplate = restTemplate;
    this.MAX_ATTEMPTS = MAX_ATTEMPTS;
    this.DELAY_MILLIS = DELAY_MILLIS;
    this.retryTemplate = getRetryTemplate();
  }

  private RetryTemplate getRetryTemplate() {
    SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
    retryPolicy.setMaxAttempts(MAX_ATTEMPTS);

    FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
    fixedBackOffPolicy.setBackOffPeriod(DELAY_MILLIS);

    RetryTemplate template = new RetryTemplate();
    template.setRetryPolicy(retryPolicy);
    template.setBackOffPolicy(fixedBackOffPolicy);
    template.setThrowLastExceptionOnExhausted(true);
    template.registerListener(new DefaultListenerSupport());
    return template;
  }

  public <R> R getForEntity(String url, Class<R> clazz) {
    return retryTemplate.execute(retryContext -> {
      retryContext.setAttribute(URL_KEY, url);
      ResponseEntity<R> forEntity = restTemplate.getForEntity(url, clazz);

      if (!HttpStatus.OK.equals(forEntity.getStatusCode())) {
        String message = String.format("Fail to recover information: %s", forEntity.toString());
        throw new RestClientException(message);
      }
      return forEntity.getBody();
    });
  }

  public static class DefaultListenerSupport extends RetryListenerSupport {
    @Override
    public <T, E extends Throwable> void onError(RetryContext context,
      RetryCallback<T, E> callback, Throwable throwable) {
        logger.info("Error on RestRetryable Provider: {} \n {} \n {}", context.getAttribute(URL_KEY), throwable.getClass(), throwable.getMessage());
        super.onError(context, callback, throwable);
    }
  }
}
