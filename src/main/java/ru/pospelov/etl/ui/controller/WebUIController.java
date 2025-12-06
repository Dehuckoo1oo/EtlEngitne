package ru.pospelov.etl.ui.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import ru.pospelov.etl.engine.api.dto.JobResponse;
import ru.pospelov.etl.engine.api.service.JobService;
import ru.pospelov.etl.engine.metrics.EtlJobMetricsSnapshot;
import ru.pospelov.etl.engine.metrics.EtlJobStatus;
import ru.pospelov.etl.engine.metrics.EtlMetricsCollector;

import java.util.List;
import java.util.Optional;

/**
 * MVC контроллер для UI страниц.
 * Отвечает за рендеринг Thymeleaf шаблонов и передачу данных в Model.
 */
@Slf4j
@Controller
@RequiredArgsConstructor
public class WebUIController {

    private final JobService jobService;
    private final EtlMetricsCollector metricsCollector;

    /**
     * Добавляет timestamp для всех запросов (для cache-busting статических ресурсов)
     */
    @ModelAttribute("timestamp")
    public long timestamp() {
        return System.currentTimeMillis();
    }

    /**
     * Главная страница - список всех jobs
     * GET /
     */
    @GetMapping("/")
    public String jobList(Model model) {
        log.debug("Rendering job list page");

        // Получаем список всех jobs
        List<JobResponse> jobs = jobService.getAllJobs();

        // Добавляем статусы для каждого job
        jobs.forEach(job -> {
            Optional<EtlJobStatus> status = metricsCollector.getStatus(job.getId());
            job.getParams().put("status", status.map(Enum::name).orElse("NOT_RUNNING"));
        });

        model.addAttribute("jobs", jobs);
        model.addAttribute("pageTitle", "Jobs | ETL Engine");

        return "job-list";
    }

    /**
     * Страница деталей job с метриками
     * GET /jobs/{id}
     */
    @GetMapping("/jobs/{id}")
    public String jobDetails(@PathVariable String id, Model model) {
        log.debug("Rendering job details page for job: {}", id);

        try {
            // Получаем данные о job
            JobResponse job = jobService.getJob(id);

            // Получаем текущие метрики (если есть)
            Optional<EtlJobMetricsSnapshot> snapshot = metricsCollector.getSnapshot(id);
            Optional<EtlJobStatus> status = metricsCollector.getStatus(id);

            model.addAttribute("job", job);
            model.addAttribute("metrics", snapshot.orElse(null));
            model.addAttribute("status", status.orElse(null));
            model.addAttribute("pageTitle", "Job: " + id + " | ETL Engine");

            return "job-details";

        } catch (Exception e) {
            log.error("Failed to load job details for job: {}", id, e);
            model.addAttribute("errorMessage", "Failed to load job: " + e.getMessage());
            return "redirect:/";
        }
    }

    /**
     * Страница создания нового job
     * GET /jobs/new
     */
    @GetMapping("/jobs/new")
    public String newJobForm(Model model) {
        log.debug("Rendering new job form");

        model.addAttribute("isEditMode", false);
        model.addAttribute("job", null);
        model.addAttribute("pageTitle", "Create Job | ETL Engine");

        return "job-form";
    }

    /**
     * Страница редактирования существующего job
     * GET /jobs/{id}/edit
     */
    @GetMapping("/jobs/{id}/edit")
    public String editJobForm(@PathVariable String id, Model model) {
        log.debug("Rendering edit job form for job: {}", id);

        try {
            JobResponse job = jobService.getJob(id);

            model.addAttribute("isEditMode", true);
            model.addAttribute("job", job);
            model.addAttribute("pageTitle", "Edit Job: " + id + " | ETL Engine");

            return "job-form";

        } catch (Exception e) {
            log.error("Failed to load job for editing: {}", id, e);
            model.addAttribute("errorMessage", "Failed to load job: " + e.getMessage());
            return "redirect:/";
        }
    }
}
