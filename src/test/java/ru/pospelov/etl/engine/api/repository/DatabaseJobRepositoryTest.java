package ru.pospelov.etl.engine.api.repository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.pospelov.etl.engine.api.entity.JobEntity;
import ru.pospelov.etl.engine.api.mapper.JobMapper;
import ru.pospelov.etl.engine.model.EtlJob;

import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit-тесты для DatabaseJobRepository с мокированием зависимостей
 */
@ExtendWith(MockitoExtension.class)
class DatabaseJobRepositoryTest {

    @Mock
    private JpaJobRepository jpaRepository;

    @Mock
    private JobMapper jobMapper;

    @InjectMocks
    private DatabaseJobRepository databaseJobRepository;

    private EtlJob sampleEtlJob;
    private JobEntity sampleJobEntity;

    @BeforeEach
    void setUp() {
        Map<String, Object> params = new HashMap<>();
        params.put("extractorType", "sql");
        params.put("loaderType", "kafka");
        params.put("threads", 4);

        sampleEtlJob = new EtlJob(
                "test-job-1",
                "SELECT * FROM test_table",
                "target_table",
                params
        );

        sampleJobEntity = JobEntity.builder()
                .id("test-job-1")
                .name("test-job-1")
                .source("SELECT * FROM test_table")
                .target("target_table")
                .params("{\"extractorType\":\"sql\",\"loaderType\":\"kafka\",\"threads\":4}")
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .createdBy("system")
                .status("ACTIVE")
                .build();
    }

    @Test
    void save_newJob_shouldCreateNewEntity() {
        // Given
        when(jpaRepository.findById("test-job-1")).thenReturn(Optional.empty());
        when(jobMapper.toEntity(sampleEtlJob)).thenReturn(sampleJobEntity);
        when(jpaRepository.save(sampleJobEntity)).thenReturn(sampleJobEntity);
        when(jobMapper.toEtlJob(sampleJobEntity)).thenReturn(sampleEtlJob);

        // When
        EtlJob result = databaseJobRepository.save(sampleEtlJob);

        // Then
        assertNotNull(result);
        assertEquals("test-job-1", result.getJobId());
        verify(jpaRepository).findById("test-job-1");
        verify(jobMapper).toEntity(sampleEtlJob);
        verify(jpaRepository).save(sampleJobEntity);
        verify(jobMapper).toEtlJob(sampleJobEntity);
    }

    @Test
    void save_existingJob_shouldUpdateEntity() {
        // Given
        JobEntity existingEntity = JobEntity.builder()
                .id("test-job-1")
                .name("test-job-1")
                .source("SELECT * FROM old_table")
                .target("old_target")
                .params("{\"extractorType\":\"kafka\"}")
                .createdAt(LocalDateTime.of(2024, 1, 1, 10, 0))
                .updatedAt(LocalDateTime.of(2024, 1, 1, 10, 0))
                .createdBy("original_user")
                .status("ACTIVE")
                .build();

        when(jpaRepository.findById("test-job-1")).thenReturn(Optional.of(existingEntity));
        when(jobMapper.updateEntity(existingEntity, sampleEtlJob)).thenReturn(sampleJobEntity);
        when(jpaRepository.save(sampleJobEntity)).thenReturn(sampleJobEntity);
        when(jobMapper.toEtlJob(sampleJobEntity)).thenReturn(sampleEtlJob);

        // When
        EtlJob result = databaseJobRepository.save(sampleEtlJob);

        // Then
        assertNotNull(result);
        verify(jpaRepository).findById("test-job-1");
        verify(jobMapper).updateEntity(existingEntity, sampleEtlJob);
        verify(jobMapper, never()).toEntity(any(EtlJob.class));
        verify(jpaRepository).save(sampleJobEntity);
    }

    @Test
    void findById_existingJob_shouldReturnJob() {
        // Given
        when(jpaRepository.findById("test-job-1")).thenReturn(Optional.of(sampleJobEntity));
        when(jobMapper.toEtlJob(sampleJobEntity)).thenReturn(sampleEtlJob);

        // When
        Optional<EtlJob> result = databaseJobRepository.findById("test-job-1");

        // Then
        assertTrue(result.isPresent());
        assertEquals("test-job-1", result.get().getJobId());
        verify(jpaRepository).findById("test-job-1");
        verify(jobMapper).toEtlJob(sampleJobEntity);
    }

    @Test
    void findById_nonExistingJob_shouldReturnEmpty() {
        // Given
        when(jpaRepository.findById("non-existent")).thenReturn(Optional.empty());

        // When
        Optional<EtlJob> result = databaseJobRepository.findById("non-existent");

        // Then
        assertFalse(result.isPresent());
        verify(jpaRepository).findById("non-existent");
        verify(jobMapper, never()).toEtlJob(any());
    }

    @Test
    void findAll_shouldReturnAllJobs() {
        // Given
        JobEntity entity2 = JobEntity.builder()
                .id("test-job-2")
                .name("test-job-2")
                .source("SELECT * FROM table2")
                .target("target2")
                .params("{\"extractorType\":\"kafka\"}")
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .status("ACTIVE")
                .build();

        EtlJob etlJob2 = new EtlJob("test-job-2", "SELECT * FROM table2", "target2", new HashMap<>());

        List<JobEntity> entities = Arrays.asList(sampleJobEntity, entity2);
        when(jpaRepository.findAll()).thenReturn(entities);
        when(jobMapper.toEtlJob(sampleJobEntity)).thenReturn(sampleEtlJob);
        when(jobMapper.toEtlJob(entity2)).thenReturn(etlJob2);

        // When
        List<EtlJob> results = databaseJobRepository.findAll();

        // Then
        assertEquals(2, results.size());
        assertEquals("test-job-1", results.get(0).getJobId());
        assertEquals("test-job-2", results.get(1).getJobId());
        verify(jpaRepository).findAll();
        verify(jobMapper, times(2)).toEtlJob(any());
    }

    @Test
    void findAll_emptyDatabase_shouldReturnEmptyList() {
        // Given
        when(jpaRepository.findAll()).thenReturn(Collections.emptyList());

        // When
        List<EtlJob> results = databaseJobRepository.findAll();

        // Then
        assertTrue(results.isEmpty());
        verify(jpaRepository).findAll();
        verify(jobMapper, never()).toEtlJob(any());
    }

    @Test
    void deleteById_existingJob_shouldReturnTrue() {
        // Given
        when(jpaRepository.existsById("test-job-1")).thenReturn(true);
        doNothing().when(jpaRepository).deleteById("test-job-1");

        // When
        boolean result = databaseJobRepository.deleteById("test-job-1");

        // Then
        assertTrue(result);
        verify(jpaRepository).existsById("test-job-1");
        verify(jpaRepository).deleteById("test-job-1");
    }

    @Test
    void deleteById_nonExistingJob_shouldReturnFalse() {
        // Given
        when(jpaRepository.existsById("non-existent")).thenReturn(false);

        // When
        boolean result = databaseJobRepository.deleteById("non-existent");

        // Then
        assertFalse(result);
        verify(jpaRepository).existsById("non-existent");
        verify(jpaRepository, never()).deleteById(anyString());
    }

    @Test
    void existsById_existingJob_shouldReturnTrue() {
        // Given
        when(jpaRepository.existsById("test-job-1")).thenReturn(true);

        // When
        boolean result = databaseJobRepository.existsById("test-job-1");

        // Then
        assertTrue(result);
        verify(jpaRepository).existsById("test-job-1");
    }

    @Test
    void existsById_nonExistingJob_shouldReturnFalse() {
        // Given
        when(jpaRepository.existsById("non-existent")).thenReturn(false);

        // When
        boolean result = databaseJobRepository.existsById("non-existent");

        // Then
        assertFalse(result);
        verify(jpaRepository).existsById("non-existent");
    }

    @Test
    void findByStatus_shouldFilterByStatus() {
        // Given
        List<JobEntity> entities = Arrays.asList(sampleJobEntity);
        when(jpaRepository.findByStatus("ACTIVE")).thenReturn(entities);
        when(jobMapper.toEtlJob(sampleJobEntity)).thenReturn(sampleEtlJob);

        // When
        List<EtlJob> results = databaseJobRepository.findByStatus("ACTIVE");

        // Then
        assertEquals(1, results.size());
        assertEquals("test-job-1", results.get(0).getJobId());
        verify(jpaRepository).findByStatus("ACTIVE");
        verify(jobMapper).toEtlJob(sampleJobEntity);
    }

    @Test
    void findAllActive_shouldReturnOnlyActiveJobs() {
        // Given
        List<JobEntity> entities = Arrays.asList(sampleJobEntity);
        when(jpaRepository.findAllActive()).thenReturn(entities);
        when(jobMapper.toEtlJob(sampleJobEntity)).thenReturn(sampleEtlJob);

        // When
        List<EtlJob> results = databaseJobRepository.findAllActive();

        // Then
        assertEquals(1, results.size());
        assertEquals("test-job-1", results.get(0).getJobId());
        verify(jpaRepository).findAllActive();
        verify(jobMapper).toEtlJob(sampleJobEntity);
    }

    @Test
    void save_shouldHandleMapperException() {
        // Given
        when(jpaRepository.findById("test-job-1")).thenReturn(Optional.empty());
        when(jobMapper.toEntity(sampleEtlJob)).thenThrow(new IllegalArgumentException("Mapper error"));

        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            databaseJobRepository.save(sampleEtlJob);
        });

        verify(jpaRepository).findById("test-job-1");
        verify(jobMapper).toEntity(sampleEtlJob);
        verify(jpaRepository, never()).save(any());
    }

    @Test
    void findById_shouldHandleMapperException() {
        // Given
        when(jpaRepository.findById("test-job-1")).thenReturn(Optional.of(sampleJobEntity));
        when(jobMapper.toEtlJob(sampleJobEntity)).thenThrow(new IllegalArgumentException("Invalid JSON"));

        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            databaseJobRepository.findById("test-job-1");
        });

        verify(jpaRepository).findById("test-job-1");
        verify(jobMapper).toEtlJob(sampleJobEntity);
    }
}
