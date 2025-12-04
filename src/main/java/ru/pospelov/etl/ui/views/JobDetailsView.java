package ru.pospelov.etl.ui.views;

import com.vaadin.flow.component.AttachEvent;
import com.vaadin.flow.component.DetachEvent;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.html.Div;
import com.vaadin.flow.component.html.H2;
import com.vaadin.flow.component.html.H3;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.notification.NotificationVariant;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.progressbar.ProgressBar;
import com.vaadin.flow.router.*;
import org.springframework.beans.factory.annotation.Autowired;
import ru.pospelov.etl.engine.api.dto.JobResponse;
import ru.pospelov.etl.engine.api.service.JobService;
import ru.pospelov.etl.engine.metrics.EtlJobMetricsSnapshot;
import ru.pospelov.etl.engine.metrics.EtlJobStatus;
import ru.pospelov.etl.engine.metrics.EtlMetricsCollector;
import ru.pospelov.etl.ui.MainLayout;
import ru.pospelov.etl.ui.services.MetricsBroadcaster;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * View для отображения деталей job'а, его метрик и статуса.
 * Обновляется автоматически через WebSocket Push.
 */
@Route(value = "jobs/details", layout = MainLayout.class)
@PageTitle("Job Details | ETL Engine")
public class JobDetailsView extends VerticalLayout implements HasUrlParameter<String> {

    private final JobService jobService;
    private final EtlMetricsCollector metricsCollector;
    private final MetricsBroadcaster metricsBroadcaster;

    private String jobId;

    // UI компоненты
    private final H2 jobTitle = new H2();
    private final Span statusBadge = new Span();
    private final Div jobInfoDiv = new Div();
    private final Div metricsDiv = new Div();
    private final ProgressBar progressBar = new ProgressBar();
    private final Button runButton = new Button("Run Job", VaadinIcon.PLAY.create());
    private final Button backButton = new Button("Back to List", VaadinIcon.ARROW_LEFT.create());

    @Autowired
    public JobDetailsView(JobService jobService, EtlMetricsCollector metricsCollector,
                         MetricsBroadcaster metricsBroadcaster) {
        this.jobService = jobService;
        this.metricsCollector = metricsCollector;
        this.metricsBroadcaster = metricsBroadcaster;

        setSizeFull();
        setPadding(true);

        configureComponents();
        createLayout();
    }

    /**
     * Настраивает компоненты
     */
    private void configureComponents() {
        runButton.addThemeVariants(ButtonVariant.LUMO_SUCCESS);
        runButton.addClickListener(e -> runJob());

        backButton.addClickListener(e -> navigateToList());

        progressBar.setWidth("100%");
        progressBar.setVisible(false);
    }

    /**
     * Создает layout страницы
     */
    private void createLayout() {
        HorizontalLayout toolbar = new HorizontalLayout(backButton, jobTitle, statusBadge, runButton);
        toolbar.setWidthFull();
        toolbar.setAlignItems(Alignment.CENTER);
        toolbar.expand(jobTitle);

        add(toolbar, progressBar, jobInfoDiv, metricsDiv);
    }

    /**
     * Обрабатывает URL параметр (jobId)
     */
    @Override
    public void setParameter(BeforeEvent event, String parameter) {
        if (parameter == null || parameter.isEmpty()) {
            showNotification("Job ID is required", NotificationVariant.LUMO_ERROR);
            navigateToList();
            return;
        }

        this.jobId = parameter;
        loadJobDetails();
    }

    /**
     * Загружает детали job'а
     */
    private void loadJobDetails() {
        try {
            JobResponse job = jobService.getJob(jobId);

            jobTitle.setText("Job: " + job.getId());

            // Отображаем информацию о джобе
            updateJobInfo(job);

            // Загружаем метрики
            updateMetrics();

        } catch (Exception e) {
            showNotification("Failed to load job: " + e.getMessage(), NotificationVariant.LUMO_ERROR);
            navigateToList();
        }
    }

    /**
     * Обновляет информацию о джобе
     */
    private void updateJobInfo(JobResponse job) {
        jobInfoDiv.removeAll();

        H3 infoTitle = new H3("Job Information");
        Div sourceDiv = new Div(new Span("Source Query: "), new Span(job.getSourceQuery() != null ? job.getSourceQuery() : "N/A"));
        Div targetDiv = new Div(new Span("Target: "), new Span(job.getTarget() != null ? job.getTarget() : "N/A"));

        Map<String, Object> params = job.getParams();
        Div extractorDiv = new Div(new Span("Extractor: "), new Span(params.get("extractorType").toString()));
        Div transformerDiv = new Div(new Span("Transformer: "), new Span(params.get("transformerType").toString()));
        Div loaderDiv = new Div(new Span("Loader: "), new Span(params.get("loaderType").toString()));

        jobInfoDiv.add(infoTitle, sourceDiv, targetDiv, extractorDiv, transformerDiv, loaderDiv);
    }

    /**
     * Обновляет метрики
     */
    private void updateMetrics() {
        Optional<EtlJobMetricsSnapshot> snapshotOpt = metricsCollector.getSnapshot(jobId);

        if (snapshotOpt.isEmpty()) {
            // Нет метрик - job не запущен
            statusBadge.setText("NOT_RUNNING");
            statusBadge.getElement().getThemeList().clear();
            statusBadge.getElement().getThemeList().add("badge");
            progressBar.setVisible(false);
            updateMetricsDisplay(null);
            return;
        }

        EtlJobMetricsSnapshot snapshot = snapshotOpt.get();

        // Обновляем статус
        updateStatus(snapshot.status());

        // Обновляем прогресс бар
        updateProgressBar(snapshot);

        // Обновляем метрики
        updateMetricsDisplay(snapshot);
    }

    /**
     * Обновляет badge со статусом
     */
    private void updateStatus(EtlJobStatus status) {
        statusBadge.setText(status.name());
        statusBadge.getElement().getThemeList().clear();
        statusBadge.getElement().getThemeList().add("badge");

        switch (status) {
            case COMPLETED -> statusBadge.getElement().getThemeList().add("success");
            case FAILED -> statusBadge.getElement().getThemeList().add("error");
            case RUNNING, EXTRACTING, TRANSFORMING, LOADING -> statusBadge.getElement().getThemeList().add("contrast");
            default -> statusBadge.getElement().getThemeList().add("primary");
        }
    }

    /**
     * Обновляет прогресс бар
     */
    private void updateProgressBar(EtlJobMetricsSnapshot snapshot) {
        EtlJobStatus status = snapshot.status();

        if (status == EtlJobStatus.RUNNING || status == EtlJobStatus.EXTRACTING ||
                status == EtlJobStatus.TRANSFORMING || status == EtlJobStatus.LOADING) {
            progressBar.setVisible(true);
            progressBar.setIndeterminate(true);
        } else if (status == EtlJobStatus.COMPLETED) {
            progressBar.setVisible(true);
            progressBar.setIndeterminate(false);
            progressBar.setValue(1.0);
        } else {
            progressBar.setVisible(false);
        }
    }

    /**
     * Обновляет отображение метрик
     */
    private void updateMetricsDisplay(EtlJobMetricsSnapshot snapshot) {
        metricsDiv.removeAll();

        H3 metricsTitle = new H3("Metrics");
        metricsDiv.add(metricsTitle);

        if (snapshot == null) {
            metricsDiv.add(new Span("No metrics available. Job has not been run yet."));
            return;
        }

        // Создаем карточки с метриками
        HorizontalLayout metricsCards = new HorizontalLayout();
        metricsCards.setWidthFull();
        metricsCards.setSpacing(true);

        metricsCards.add(
                createMetricCard("Extracted", String.valueOf(snapshot.extractedRecords()), "primary"),
                createMetricCard("Processed", String.valueOf(snapshot.processedRecords()), "contrast"),
                createMetricCard("Transformed", String.valueOf(snapshot.transformedRecords()), "success"),
                createMetricCard("Loaded", String.valueOf(snapshot.loadedRecords()), "success"),
                createMetricCard("Errors", String.valueOf(snapshot.errorCount()), "error")
        );

        metricsDiv.add(metricsCards);

        // Производительность и длительность
        H3 performanceTitle = new H3("Performance");
        Div throughputDiv = new Div(new Span("Throughput: "), new Span(String.format("%.2f records/sec", snapshot.throughput())));
        Div durationDiv = new Div(new Span("Total Duration: "), new Span(formatDuration(snapshot.totalDuration())));
        Div extractDiv = new Div(new Span("Extract Duration: "), new Span(formatMillis(snapshot.extractDurationMillis())));
        Div transformDiv = new Div(new Span("Transform Duration: "), new Span(formatMillis(snapshot.transformDurationMillis())));
        Div loadDiv = new Div(new Span("Load Duration: "), new Span(formatMillis(snapshot.loadDurationMillis())));

        metricsDiv.add(performanceTitle, throughputDiv, durationDiv, extractDiv, transformDiv, loadDiv);
    }

    /**
     * Создает карточку с метрикой
     */
    private Div createMetricCard(String label, String value, String theme) {
        Div card = new Div();
        card.getStyle()
                .set("border", "1px solid var(--lumo-contrast-10pct)")
                .set("border-radius", "var(--lumo-border-radius)")
                .set("padding", "var(--lumo-space-m)")
                .set("flex", "1");

        Span labelSpan = new Span(label);
        labelSpan.getStyle().set("font-size", "var(--lumo-font-size-s)");

        Span valueSpan = new Span(value);
        valueSpan.getStyle().set("font-size", "var(--lumo-font-size-xl)").set("font-weight", "bold");
        valueSpan.getElement().getThemeList().add("badge " + theme);

        card.add(labelSpan, new Div(), valueSpan);
        return card;
    }

    /**
     * Запускает job
     */
    private void runJob() {
        try {
            jobService.runJob(jobId, null);
            showNotification("Job started successfully", NotificationVariant.LUMO_SUCCESS);
            // Метрики обновятся автоматически
        } catch (Exception e) {
            showNotification("Failed to start job: " + e.getMessage(), NotificationVariant.LUMO_ERROR);
        }
    }

    /**
     * Возвращается к списку jobs
     */
    private void navigateToList() {
        getUI().ifPresent(ui -> ui.navigate(JobListView.class));
    }

    /**
     * Показывает уведомление
     */
    private void showNotification(String message, NotificationVariant variant) {
        Notification notification = Notification.show(message);
        notification.addThemeVariants(variant);
        notification.setPosition(Notification.Position.TOP_CENTER);
    }

    /**
     * Форматирует Duration
     */
    private String formatDuration(Duration duration) {
        if (duration == null) {
            return "N/A";
        }
        long seconds = duration.getSeconds();
        return String.format("%02d:%02d:%02d", seconds / 3600, (seconds % 3600) / 60, seconds % 60);
    }

    /**
     * Форматирует миллисекунды
     */
    private String formatMillis(long millis) {
        return String.format("%.2f sec", millis / 1000.0);
    }

    /**
     * Подписывается на обновления метрик при attach
     */
    @Override
    protected void onAttach(AttachEvent attachEvent) {
        super.onAttach(attachEvent);

        if (jobId != null) {
            // Подписываемся на обновления метрик через WebSocket Push
            metricsBroadcaster.subscribe(jobId, attachEvent.getUI(), this::updateMetrics);
        }
    }

    /**
     * Отписывается от обновлений при detach
     */
    @Override
    protected void onDetach(DetachEvent detachEvent) {
        super.onDetach(detachEvent);

        if (jobId != null) {
            metricsBroadcaster.unsubscribe(jobId, detachEvent.getUI());
        }
    }
}
