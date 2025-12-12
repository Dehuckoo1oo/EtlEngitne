package ru.pospelov.etl.engine.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import ru.pospelov.etl.api.controller.JobController;
import ru.pospelov.etl.api.dto.JobRequest;
import ru.pospelov.etl.api.dto.JobResponse;
import ru.pospelov.etl.api.dto.JobRunResponse;
import ru.pospelov.etl.api.exception.JobAlreadyExistsException;
import ru.pospelov.etl.api.exception.JobNotFoundException;
import ru.pospelov.etl.api.service.JobService;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Unit-тест для JobController с мокированием JobService
 */
@WebMvcTest(JobController.class)
class JobControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private JobService jobService;

    private JobRequest sampleRequest;
    private JobResponse sampleResponse;

    @BeforeEach
    void setUp() {
        sampleRequest = createSampleJobRequest("test-job-1");
        sampleResponse = createSampleJobResponse("test-job-1");
    }

    @Test
    void testCreateJob_Success() throws Exception {
        when(jobService.createJob(any(JobRequest.class))).thenReturn(sampleResponse);

        mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(sampleRequest)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value("test-job-1"))
                .andExpect(jsonPath("$.source").value("SELECT * FROM test_table"))
                .andExpect(jsonPath("$.params.extractorType").value("sql"));
    }

    @Test
    void testCreateJob_DuplicateId_ReturnsConflict() throws Exception {
        when(jobService.createJob(any(JobRequest.class)))
                .thenThrow(new JobAlreadyExistsException("test-job-1"));

        mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(sampleRequest)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value(containsString("already exists")));
    }

    @Test
    void testCreateJob_MissingRequiredParams_ReturnsBadRequest() throws Exception {
        when(jobService.createJob(any(JobRequest.class)))
                .thenThrow(new IllegalArgumentException("extractorType parameter is required"));

        JobRequest invalidRequest = new JobRequest();
        invalidRequest.setId("test-job-1");
        invalidRequest.setParams(new HashMap<>());

        mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(invalidRequest)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(containsString("required")));
    }

    @Test
    void testGetJob_Success() throws Exception {
        when(jobService.getJob("test-job-1")).thenReturn(sampleResponse);

        mockMvc.perform(get("/api/jobs/test-job-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value("test-job-1"))
                .andExpect(jsonPath("$.params.extractorType").value("sql"));
    }

    @Test
    void testGetJob_NotFound_ReturnsNotFound() throws Exception {
        when(jobService.getJob("non-existent-job"))
                .thenThrow(new JobNotFoundException("non-existent-job"));

        mockMvc.perform(get("/api/jobs/non-existent-job"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message").value(containsString("not found")));
    }

    @Test
    void testGetAllJobs_Success() throws Exception {
        List<JobResponse> jobs = Arrays.asList(
                createSampleJobResponse("job-1"),
                createSampleJobResponse("job-2")
        );
        when(jobService.getAllJobs()).thenReturn(jobs);

        mockMvc.perform(get("/api/jobs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[*].id", containsInAnyOrder("job-1", "job-2")));
    }

    @Test
    void testUpdateJob_Success() throws Exception {
        JobResponse updatedResponse = createSampleJobResponse("test-job-1");
        updatedResponse.setSource("SELECT * FROM updated_table");

        when(jobService.updateJob(eq("test-job-1"), any(JobRequest.class)))
                .thenReturn(updatedResponse);

        mockMvc.perform(put("/api/jobs/test-job-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(sampleRequest)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value("test-job-1"))
                .andExpect(jsonPath("$.source").value("SELECT * FROM updated_table"));
    }

    @Test
    void testUpdateJob_NotFound_ReturnsNotFound() throws Exception {
        when(jobService.updateJob(eq("non-existent-job"), any(JobRequest.class)))
                .thenThrow(new JobNotFoundException("non-existent-job"));

        mockMvc.perform(put("/api/jobs/non-existent-job")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(sampleRequest)))
                .andExpect(status().isNotFound());
    }

    @Test
    void testDeleteJob_Success() throws Exception {
        mockMvc.perform(delete("/api/jobs/test-job-1"))
                .andExpect(status().isNoContent());
    }

    @Test
    void testDeleteJob_NotFound_ReturnsNotFound() throws Exception {
        org.mockito.Mockito.doThrow(new JobNotFoundException("non-existent-job"))
                .when(jobService).deleteJob("non-existent-job");

        mockMvc.perform(delete("/api/jobs/non-existent-job"))
                .andExpect(status().isNotFound());
    }

    @Test
    void testRunJob_Success() throws Exception {
        JobRunResponse runResponse = JobRunResponse.builder()
                .jobId("test-job-1")
                .status("STARTED")
                .message("Job execution started successfully")
                .startedAt(System.currentTimeMillis())
                .build();

        when(jobService.runJob(eq("test-job-1"), any()))
                .thenReturn(runResponse);

        mockMvc.perform(post("/api/jobs/test-job-1/run"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.jobId").value("test-job-1"))
                .andExpect(jsonPath("$.status").value("STARTED"));
    }

    private JobRequest createSampleJobRequest(String jobId) {
        JobRequest request = new JobRequest();
        request.setId(jobId);
        request.setSource("SELECT * FROM test_table");
        request.setTarget(null);

        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "sql");
        params.put("transformerType", "noop");
        params.put("loaderType", "kafka");
        params.put("topic", "test-topic");
        params.put("format", "avro");
        params.put("threads", 8);
        params.put("streamBatchSize", 50000);

        request.setParams(params);
        return request;
    }

    private JobResponse createSampleJobResponse(String jobId) {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "sql");
        params.put("transformerType", "noop");
        params.put("loaderType", "kafka");
        params.put("topic", "test-topic");

        return JobResponse.builder()
                .id(jobId)
                .source("SELECT * FROM test_table")
                .target(null)
                .params(params)
                .build();
    }
}
