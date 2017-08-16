package com.example.repro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.cloud.sleuth.Span.idToHex;

import javax.inject.Inject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.cloud.netflix.feign.EnableFeignClients;
import org.springframework.cloud.netflix.feign.FeignClient;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.example.repro.ReproApplicationTests.ReproApplication;
import com.example.repro.ReproApplicationTests.ReproApplication.ReproClient;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import rx.Observable;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = ReproApplication.class, webEnvironment = WebEnvironment.DEFINED_PORT, properties = {
        "server.port=9009", "feign.hystrix.enabled=true" })
@Slf4j
public class ReproApplicationTests {

    @Inject
    private ReproClient reproClient;

    @Inject
    private Tracer tracer;

    @ToString
    private static class TraceIds {
        String testTraceId;
        String inDoOnErrorTraceId;
        String inMapTraceId;
    }

    @Test
    public void test() {
        TraceIds traceIds = new TraceIds();
        traceIds.testTraceId = idToHex(tracer.createSpan("test").getTraceId());
        log.info("Starting test");
        String result = reproClient.repro() //
                .doOnError(throwable -> {
                    log.error("In doOnError with error {}", throwable.getMessage());
                    traceIds.inDoOnErrorTraceId = idToHex(tracer.getCurrentSpan().getTraceId());
                }) //
                .onErrorReturn(throwable -> new ReproApplication.ReproDto("there was an error")) //
                .map(reproDto -> {
                    log.error("In map");
                    traceIds.inMapTraceId = idToHex(tracer.getCurrentSpan().getTraceId());
                    return reproDto.getMessage();
                }) //
                .toBlocking().first();

        log.info("{}", traceIds);
        assertThat(result).isEqualTo("there was an error");
        // This is what I get : testTraceIds != inDoOnErrorTraceId and inDoOnErrorTraceId == inMapTraceId
        // But I rather expect the following which fails as of today :
        assertThat(traceIds.testTraceId) //
                .isEqualTo(traceIds.inDoOnErrorTraceId) //
                .isEqualTo(traceIds.inMapTraceId);
    }

    @SpringBootApplication
    @RestController("/repro")
    @EnableFeignClients
    public static class ReproApplication {

        @Data
        @AllArgsConstructor
        public static class ReproDto {
            private String message;
        }

        @FeignClient(name = "repro", url = "127.0.0.1:9009")
        public interface ReproClient {
            @RequestMapping(method = RequestMethod.GET, value = "/repro")
            Observable<ReproDto> repro();
        }

        @GetMapping
        public ReproDto repro() throws InterruptedException {
            Thread.sleep(1000);
            return new ReproDto("pheeeew this was long");
        }

        public static void main(String[] args) {
            SpringApplication.run(ReproApplication.class, args);
        }
    }

}
