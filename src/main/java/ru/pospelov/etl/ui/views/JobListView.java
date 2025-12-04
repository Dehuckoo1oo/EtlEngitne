package ru.pospelov.etl.ui.views;

import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.html.H2;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.notification.NotificationVariant;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.data.renderer.ComponentRenderer;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.router.Route;
import org.springframework.beans.factory.annotation.Autowired;
import ru.pospelov.etl.engine.api.dto.JobResponse;
import ru.pospelov.etl.engine.api.service.JobService;
import ru.pospelov.etl.engine.metrics.EtlJobStatus;
import ru.pospelov.etl.engine.metrics.EtlMetricsCollector;
import ru.pospelov.etl.ui.MainLayout;

import java.util.Optional;

/**
 * View для отображения списка всех ETL job'ов.
 * Главная страница приложения.
 */
@Route(value = "", layout = MainLayout.class)
@PageTitle("Jobs | ETL Engine")
public class JobListView extends VerticalLayout {

    private final JobService jobService;
    private final EtlMetricsCollector metricsCollector;
    private final Grid<JobResponse> grid;

    @Autowired
    public JobListView(JobService jobService, EtlMetricsCollector metricsCollector) {
        this.jobService = jobService;
        this.metricsCollector = metricsCollector;

        setSizeFull();
        setPadding(true);

        H2 title = new H2("ETL Jobs");

        Button createButton = new Button("Create New Job", VaadinIcon.PLUS.create());
        createButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        createButton.addClickListener(e -> getUI().ifPresent(ui -> ui.navigate(JobFormView.class)));

        HorizontalLayout toolbar = new HorizontalLayout(title, createButton);
        toolbar.setWidthFull();
        toolbar.setAlignItems(Alignment.CENTER);
        toolbar.expand(title);

        grid = new Grid<>(JobResponse.class, false);
        configureGrid();

        add(toolbar, grid);
        refreshGrid();
    }

    /**
     * Настраивает колонки Grid
     */
    private void configureGrid() {
        grid.addColumn(JobResponse::getId)
                .setHeader("Job ID")
                .setSortable(true)
                .setAutoWidth(true);

        grid.addColumn(job -> truncate(job.getSourceQuery(), 50))
                .setHeader("Source Query")
                .setAutoWidth(true);

        grid.addColumn(job -> truncate(job.getTarget(), 30))
                .setHeader("Target")
                .setAutoWidth(true);

        grid.addColumn(this::getJobStatusBadge)
                .setHeader("Status")
                .setAutoWidth(true);

        grid.addColumn(new ComponentRenderer<>(this::createActionButtons))
                .setHeader("Actions")
                .setAutoWidth(true);

        grid.setHeight("100%");
    }

    /**
     * Получает статус job'а для отображения
     */
    private String getJobStatusBadge(JobResponse job) {
        Optional<EtlJobStatus> status = metricsCollector.getStatus(job.getId());
        return status.map(s -> s.name()).orElse("NOT_RUNNING");
    }

    /**
     * Создает кнопки действий для каждого job'а
     */
    private HorizontalLayout createActionButtons(JobResponse job) {
        Button viewButton = new Button("View", VaadinIcon.EYE.create());
        viewButton.addThemeVariants(ButtonVariant.LUMO_SMALL);
        viewButton.addClickListener(e -> viewJobDetails(job));

        Button editButton = new Button("Edit", VaadinIcon.EDIT.create());
        editButton.addThemeVariants(ButtonVariant.LUMO_SMALL);
        editButton.addClickListener(e -> editJob(job));

        Button runButton = new Button("Run", VaadinIcon.PLAY.create());
        runButton.addThemeVariants(ButtonVariant.LUMO_SMALL, ButtonVariant.LUMO_SUCCESS);
        runButton.addClickListener(e -> runJob(job));

        Button deleteButton = new Button("Delete", VaadinIcon.TRASH.create());
        deleteButton.addThemeVariants(ButtonVariant.LUMO_SMALL, ButtonVariant.LUMO_ERROR);
        deleteButton.addClickListener(e -> deleteJob(job));

        HorizontalLayout actions = new HorizontalLayout(viewButton, editButton, runButton, deleteButton);
        actions.setSpacing(true);
        return actions;
    }

    /**
     * Обновляет данные в Grid
     */
    private void refreshGrid() {
        grid.setItems(jobService.getAllJobs());
    }

    /**
     * Открывает детали job'а
     */
    private void viewJobDetails(JobResponse job) {
        getUI().ifPresent(ui -> ui.navigate(JobDetailsView.class, job.getId()));
    }

    /**
     * Открывает форму редактирования job'а
     */
    private void editJob(JobResponse job) {
        getUI().ifPresent(ui -> ui.navigate(JobFormView.class, job.getId()));
    }

    /**
     * Запускает job
     */
    private void runJob(JobResponse job) {
        try {
            jobService.runJob(job.getId(), null);
            Notification notification = Notification.show("Job started: " + job.getId());
            notification.addThemeVariants(NotificationVariant.LUMO_SUCCESS);
            notification.setPosition(Notification.Position.TOP_CENTER);
            refreshGrid();
        } catch (Exception e) {
            Notification notification = Notification.show("Failed to start job: " + e.getMessage());
            notification.addThemeVariants(NotificationVariant.LUMO_ERROR);
            notification.setPosition(Notification.Position.TOP_CENTER);
        }
    }

    /**
     * Удаляет job
     */
    private void deleteJob(JobResponse job) {
        try {
            jobService.deleteJob(job.getId());
            Notification notification = Notification.show("Job deleted: " + job.getId());
            notification.addThemeVariants(NotificationVariant.LUMO_SUCCESS);
            notification.setPosition(Notification.Position.TOP_CENTER);
            refreshGrid();
        } catch (Exception e) {
            Notification notification = Notification.show("Failed to delete job: " + e.getMessage());
            notification.addThemeVariants(NotificationVariant.LUMO_ERROR);
            notification.setPosition(Notification.Position.TOP_CENTER);
        }
    }

    /**
     * Обрезает строку до указанной длины
     */
    private String truncate(String text, int maxLength) {
        if (text == null) {
            return "";
        }
        if (text.length() <= maxLength) {
            return text;
        }
        return text.substring(0, maxLength) + "...";
    }
}
