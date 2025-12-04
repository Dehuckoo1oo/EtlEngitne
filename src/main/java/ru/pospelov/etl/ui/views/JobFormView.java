package ru.pospelov.etl.ui.views;

import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.datetimepicker.DateTimePicker;
import com.vaadin.flow.component.formlayout.FormLayout;
import com.vaadin.flow.component.html.H2;
import com.vaadin.flow.component.html.H3;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.notification.NotificationVariant;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.select.Select;
import com.vaadin.flow.component.textfield.IntegerField;
import com.vaadin.flow.component.textfield.TextArea;
import com.vaadin.flow.component.textfield.TextField;
import com.vaadin.flow.router.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;
import ru.pospelov.etl.engine.api.dto.JobRequest;
import ru.pospelov.etl.engine.api.dto.JobResponse;
import ru.pospelov.etl.engine.api.service.JobService;
import ru.pospelov.etl.ui.MainLayout;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * View для создания и редактирования ETL job'ов с динамическими полями.
 */
@Route(value = "jobs/edit", layout = MainLayout.class)
@PageTitle("Edit Job | ETL Engine")
public class JobFormView extends VerticalLayout implements HasUrlParameter<String> {

    private final JobService jobService;
    private final RestTemplate restTemplate = new RestTemplate();

    // Базовые поля
    private final TextField jobIdField = new TextField("Job ID");

    // Типы компонентов
    private final Select<String> extractorTypeSelect = new Select<>();
    private final Select<String> transformerTypeSelect = new Select<>();
    private final Select<String> loaderTypeSelect = new Select<>();

    // SQL Source поля
    private final TextArea sourceQueryField = new TextArea("Source Query (SQL)");

    // SQL Target поля
    private final TextField targetTableField = new TextField("Target Table (SQL)");

    // Kafka Source поля (Extractor)
    private final TextField kafkaSourceTopicField = new TextField("Kafka Topic (Source)");
    private final DateTimePicker startTimestampField = new DateTimePicker("Start Timestamp");
    private final DateTimePicker endTimestampField = new DateTimePicker("End Timestamp");

    // Kafka Target поля (Loader)
    private final TextField kafkaTargetTopicField = new TextField("Kafka Topic (Target)");
    private final TextField partitionColumnField = new TextField("Partition Column (for Kafka)");

    // Общие параметры
    private final IntegerField threadsField = new IntegerField("Threads");
    private final IntegerField streamBatchSizeField = new IntegerField("Stream Batch Size");
    private final Select<String> formatSelect = new Select<>();
    private final Select<String> avroSchemaSelect = new Select<>();
    private final TextField keyColumnField = new TextField("Key Column");

    // SQL-специфичные параметры
    private final IntegerField partitionsField = new IntegerField("Partitions (SQL parallel)");
    private final TextField sqlPartitionColumnField = new TextField("Partition Column (SQL)");

    // Layout контейнеры для динамических секций
    private final VerticalLayout sourceFieldsLayout = new VerticalLayout();
    private final VerticalLayout targetFieldsLayout = new VerticalLayout();
    private final VerticalLayout commonFieldsLayout = new VerticalLayout();

    // Кнопки
    private final Button saveButton = new Button("Save");
    private final Button cancelButton = new Button("Cancel");

    private String currentJobId;
    private boolean isEditMode = false;

    @Autowired
    public JobFormView(JobService jobService) {
        this.jobService = jobService;

        setSizeFull();
        setPadding(true);

        H2 title = new H2("Create/Edit Job");

        configureForm();
        configureButtons();
        setupFieldVisibilityListeners();

        HorizontalLayout buttonsLayout = new HorizontalLayout(saveButton, cancelButton);
        buttonsLayout.setSpacing(true);

        add(title, createMainLayout(), buttonsLayout);
    }

    /**
     * Настраивает поля формы
     */
    private void configureForm() {
        // Настройка селектов типов
        extractorTypeSelect.setLabel("Source Type");
        extractorTypeSelect.setItems("sql", "kafka");
        extractorTypeSelect.setRequiredIndicatorVisible(true);
        extractorTypeSelect.setPlaceholder("Select source type");

        transformerTypeSelect.setLabel("Transformer Type");
        transformerTypeSelect.setItems("noop", "avro");
        transformerTypeSelect.setRequiredIndicatorVisible(true);
        transformerTypeSelect.setValue("noop"); // По умолчанию

        loaderTypeSelect.setLabel("Target Type");
        loaderTypeSelect.setItems("sql", "fast-sql", "kafka");
        loaderTypeSelect.setRequiredIndicatorVisible(true);
        loaderTypeSelect.setPlaceholder("Select target type");

        formatSelect.setLabel("Format");
        formatSelect.setItems("avro", "json", "string");
        formatSelect.setValue("avro");

        avroSchemaSelect.setLabel("Avro Schema (Subject)");
        avroSchemaSelect.setPlaceholder("Select schema from Schema Registry");
        avroSchemaSelect.setEmptySelectionAllowed(true);
        avroSchemaSelect.setEmptySelectionCaption("No schema");
        loadAvroSchemas();

        // Настройка полей
        jobIdField.setRequiredIndicatorVisible(true);
        sourceQueryField.setHeight("150px");
        sourceQueryField.setHelperText("SQL query to extract data");

        kafkaSourceTopicField.setHelperText("Kafka topic to read from");
        kafkaTargetTopicField.setHelperText("Kafka topic to write to");
        targetTableField.setHelperText("SQL table to insert data");

        startTimestampField.setHelperText("Start reading from this timestamp");
        endTimestampField.setHelperText("Stop reading at this timestamp");

        partitionColumnField.setHelperText("Column to use for Kafka partitioning");
        sqlPartitionColumnField.setHelperText("Column to use for SQL parallel extraction");

        // Значения по умолчанию
        threadsField.setValue(4);
        streamBatchSizeField.setValue(50000);

        // Устанавливаем временные метки по умолчанию (последние 24 часа)
        endTimestampField.setValue(LocalDateTime.now());
        startTimestampField.setValue(LocalDateTime.now().minusDays(1));
    }

    /**
     * Загружает список схем из Schema Registry
     */
    private void loadAvroSchemas() {
        try {
            List<String> schemas = restTemplate.exchange(
                    "http://localhost:8080/api/schemas",
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<List<String>>() {}
            ).getBody();

            if (schemas != null && !schemas.isEmpty()) {
                avroSchemaSelect.setItems(schemas);
            }
        } catch (Exception e) {
            avroSchemaSelect.setEnabled(false);
            avroSchemaSelect.setPlaceholder("Schema Registry is not available");
        }
    }

    /**
     * Создает основной layout
     */
    private VerticalLayout createMainLayout() {
        VerticalLayout mainLayout = new VerticalLayout();
        mainLayout.setPadding(false);

        // Базовые поля
        FormLayout baseFields = new FormLayout();
        baseFields.add(jobIdField, 2);
        baseFields.add(extractorTypeSelect);
        baseFields.add(loaderTypeSelect);
        baseFields.add(transformerTypeSelect);

        // Секции для динамических полей
        sourceFieldsLayout.setPadding(false);
        targetFieldsLayout.setPadding(false);
        commonFieldsLayout.setPadding(false);

        mainLayout.add(
            baseFields,
            new H3("Source Configuration"),
            sourceFieldsLayout,
            new H3("Target Configuration"),
            targetFieldsLayout,
            new H3("Common Parameters"),
            commonFieldsLayout
        );

        return mainLayout;
    }

    /**
     * Настраивает слушатели для динамического отображения полей
     */
    private void setupFieldVisibilityListeners() {
        extractorTypeSelect.addValueChangeListener(e -> updateSourceFields());
        loaderTypeSelect.addValueChangeListener(e -> updateTargetFields());

        // Изначально скрываем все
        updateSourceFields();
        updateTargetFields();
        updateCommonFields();
    }

    /**
     * Обновляет видимость полей источника
     */
    private void updateSourceFields() {
        sourceFieldsLayout.removeAll();

        String extractorType = extractorTypeSelect.getValue();
        if (extractorType == null) {
            return;
        }

        FormLayout sourceForm = new FormLayout();
        sourceForm.setResponsiveSteps(
            new FormLayout.ResponsiveStep("0", 1),
            new FormLayout.ResponsiveStep("500px", 2)
        );

        if ("sql".equals(extractorType)) {
            // SQL Source: нужен query, опционально partitionColumn и partitions
            sourceForm.add(sourceQueryField, 2);
            sourceForm.add(sqlPartitionColumnField);
            sourceForm.add(partitionsField);
        } else if ("kafka".equals(extractorType)) {
            // Kafka Source: нужен topic, временные метки, format
            sourceForm.add(kafkaSourceTopicField, 2);
            sourceForm.add(startTimestampField);
            sourceForm.add(endTimestampField);
            sourceForm.add(formatSelect);
            if ("avro".equals(formatSelect.getValue())) {
                sourceForm.add(avroSchemaSelect, 2);
            }
        }

        sourceFieldsLayout.add(sourceForm);
        updateCommonFields();
    }

    /**
     * Обновляет видимость полей назначения
     */
    private void updateTargetFields() {
        targetFieldsLayout.removeAll();

        String loaderType = loaderTypeSelect.getValue();
        if (loaderType == null) {
            return;
        }

        FormLayout targetForm = new FormLayout();
        targetForm.setResponsiveSteps(
            new FormLayout.ResponsiveStep("0", 1),
            new FormLayout.ResponsiveStep("500px", 2)
        );

        if ("sql".equals(loaderType) || "fast-sql".equals(loaderType)) {
            // SQL Target: нужна target table
            targetForm.add(targetTableField, 2);
        } else if ("kafka".equals(loaderType)) {
            // Kafka Target: нужен topic, partitionColumn, format
            targetForm.add(kafkaTargetTopicField, 2);
            targetForm.add(partitionColumnField);
            targetForm.add(keyColumnField);
            targetForm.add(formatSelect);
            if ("avro".equals(formatSelect.getValue())) {
                targetForm.add(avroSchemaSelect, 2);
            }
        }

        targetFieldsLayout.add(targetForm);
        updateCommonFields();
    }

    /**
     * Обновляет общие параметры
     */
    private void updateCommonFields() {
        commonFieldsLayout.removeAll();

        FormLayout commonForm = new FormLayout();
        commonForm.setResponsiveSteps(
            new FormLayout.ResponsiveStep("0", 1),
            new FormLayout.ResponsiveStep("500px", 2)
        );

        commonForm.add(threadsField);
        commonForm.add(streamBatchSizeField);

        commonFieldsLayout.add(commonForm);
    }

    /**
     * Настраивает кнопки
     */
    private void configureButtons() {
        saveButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        saveButton.addClickListener(e -> saveJob());

        cancelButton.addClickListener(e -> navigateToList());
    }

    /**
     * Обрабатывает URL параметр (jobId для редактирования)
     */
    @Override
    public void setParameter(BeforeEvent event, @OptionalParameter String parameter) {
        if (parameter != null && !parameter.isEmpty()) {
            currentJobId = parameter;
            isEditMode = true;
            loadJob(parameter);
        } else {
            isEditMode = false;
            clearForm();
        }
    }

    /**
     * Загружает существующий job для редактирования
     */
    private void loadJob(String jobId) {
        try {
            JobResponse job = jobService.getJob(jobId);

            jobIdField.setValue(job.getId());
            jobIdField.setReadOnly(true);

            Map<String, Object> params = job.getParams();

            // Загружаем типы компонентов
            extractorTypeSelect.setValue((String) params.get("extractorType"));
            transformerTypeSelect.setValue((String) params.get("transformerType"));
            loaderTypeSelect.setValue((String) params.get("loaderType"));

            // Загружаем source-специфичные параметры
            if (job.getSourceQuery() != null) {
                sourceQueryField.setValue(job.getSourceQuery());
            }
            if (params.containsKey("topic")) {
                String topic = (String) params.get("topic");
                if ("kafka".equals(params.get("extractorType"))) {
                    kafkaSourceTopicField.setValue(topic);
                } else {
                    kafkaTargetTopicField.setValue(topic);
                }
            }
            if (params.containsKey("startTimestamp")) {
                long millis = ((Number) params.get("startTimestamp")).longValue();
                startTimestampField.setValue(LocalDateTime.ofInstant(
                    java.time.Instant.ofEpochMilli(millis), ZoneId.systemDefault()));
            }
            if (params.containsKey("endTimestamp")) {
                long millis = ((Number) params.get("endTimestamp")).longValue();
                endTimestampField.setValue(LocalDateTime.ofInstant(
                    java.time.Instant.ofEpochMilli(millis), ZoneId.systemDefault()));
            }

            // Загружаем target-специфичные параметры
            if (job.getTarget() != null) {
                targetTableField.setValue(job.getTarget());
            }

            // Загружаем общие параметры
            if (params.containsKey("threads")) {
                threadsField.setValue(((Number) params.get("threads")).intValue());
            }
            if (params.containsKey("streamBatchSize")) {
                streamBatchSizeField.setValue(((Number) params.get("streamBatchSize")).intValue());
            }
            if (params.containsKey("format")) {
                formatSelect.setValue((String) params.get("format"));
            }
            if (params.containsKey("avroSchema")) {
                avroSchemaSelect.setValue((String) params.get("avroSchema"));
            }
            if (params.containsKey("keyColumn")) {
                keyColumnField.setValue((String) params.get("keyColumn"));
            }
            if (params.containsKey("partitionColumn")) {
                String partCol = (String) params.get("partitionColumn");
                if ("sql".equals(params.get("extractorType"))) {
                    sqlPartitionColumnField.setValue(partCol);
                } else {
                    partitionColumnField.setValue(partCol);
                }
            }
            if (params.containsKey("partitions")) {
                partitionsField.setValue(((Number) params.get("partitions")).intValue());
            }

            // Обновляем видимость полей после загрузки
            updateSourceFields();
            updateTargetFields();

        } catch (Exception e) {
            showNotification("Failed to load job: " + e.getMessage(), NotificationVariant.LUMO_ERROR);
            navigateToList();
        }
    }

    /**
     * Сохраняет job (создает новый или обновляет существующий)
     */
    private void saveJob() {
        try {
            // Валидация
            if (jobIdField.isEmpty() || extractorTypeSelect.isEmpty() ||
                    transformerTypeSelect.isEmpty() || loaderTypeSelect.isEmpty()) {
                showNotification("Please fill all required fields", NotificationVariant.LUMO_ERROR);
                return;
            }

            String extractorType = extractorTypeSelect.getValue();
            String loaderType = loaderTypeSelect.getValue();

            // Проверка специфичных полей
            if ("sql".equals(extractorType) && sourceQueryField.isEmpty()) {
                showNotification("Source Query is required for SQL extraction", NotificationVariant.LUMO_ERROR);
                return;
            }
            if ("kafka".equals(extractorType) && kafkaSourceTopicField.isEmpty()) {
                showNotification("Kafka Topic is required for Kafka extraction", NotificationVariant.LUMO_ERROR);
                return;
            }
            if ("kafka".equals(extractorType) && (startTimestampField.isEmpty() || endTimestampField.isEmpty())) {
                showNotification("Start and End Timestamps are required for Kafka extraction", NotificationVariant.LUMO_ERROR);
                return;
            }
            if (("sql".equals(loaderType) || "fast-sql".equals(loaderType)) && targetTableField.isEmpty()) {
                showNotification("Target Table is required for SQL loading", NotificationVariant.LUMO_ERROR);
                return;
            }
            if ("kafka".equals(loaderType) && kafkaTargetTopicField.isEmpty()) {
                showNotification("Kafka Topic is required for Kafka loading", NotificationVariant.LUMO_ERROR);
                return;
            }

            // Собираем параметры
            Map<String, Object> params = new HashMap<>();
            params.put("extractorType", extractorType);
            params.put("transformerType", transformerTypeSelect.getValue());
            params.put("loaderType", loaderType);
            params.put("threads", threadsField.getValue());
            params.put("streamBatchSize", streamBatchSizeField.getValue());

            // Source-специфичные параметры
            if ("kafka".equals(extractorType)) {
                params.put("topic", kafkaSourceTopicField.getValue());
                params.put("startTimestamp", startTimestampField.getValue()
                    .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
                params.put("endTimestamp", endTimestampField.getValue()
                    .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
                addIfNotEmpty(params, "format", formatSelect.getValue());
                addIfNotEmpty(params, "avroSchema", avroSchemaSelect.getValue());
            }
            if ("sql".equals(extractorType)) {
                addIfNotEmpty(params, "partitionColumn", sqlPartitionColumnField.getValue());
                if (partitionsField.getValue() != null && partitionsField.getValue() > 1) {
                    params.put("partitions", partitionsField.getValue());
                }
            }

            // Target-специфичные параметры
            if ("kafka".equals(loaderType)) {
                params.put("topic", kafkaTargetTopicField.getValue());
                addIfNotEmpty(params, "partitionColumn", partitionColumnField.getValue());
                addIfNotEmpty(params, "keyColumn", keyColumnField.getValue());
                addIfNotEmpty(params, "format", formatSelect.getValue());
                addIfNotEmpty(params, "avroSchema", avroSchemaSelect.getValue());
            }

            // Создаем request
            JobRequest request = new JobRequest();
            request.setId(jobIdField.getValue());

            if ("sql".equals(extractorType)) {
                request.setSourceQuery(sourceQueryField.getValue());
            }
            if ("sql".equals(loaderType) || "fast-sql".equals(loaderType)) {
                request.setTarget(targetTableField.getValue());
            }

            request.setParams(params);

            // Сохраняем
            if (isEditMode) {
                jobService.updateJob(currentJobId, request);
                showNotification("Job updated successfully", NotificationVariant.LUMO_SUCCESS);
            } else {
                jobService.createJob(request);
                showNotification("Job created successfully", NotificationVariant.LUMO_SUCCESS);
            }

            navigateToList();

        } catch (Exception e) {
            showNotification("Failed to save job: " + e.getMessage(), NotificationVariant.LUMO_ERROR);
        }
    }

    /**
     * Добавляет параметр в map если значение не пустое
     */
    private void addIfNotEmpty(Map<String, Object> params, String key, String value) {
        if (value != null && !value.trim().isEmpty()) {
            params.put(key, value);
        }
    }

    /**
     * Очищает форму
     */
    private void clearForm() {
        jobIdField.clear();
        jobIdField.setReadOnly(false);
        sourceQueryField.clear();
        targetTableField.clear();
        kafkaSourceTopicField.clear();
        kafkaTargetTopicField.clear();
        extractorTypeSelect.clear();
        transformerTypeSelect.clear();
        loaderTypeSelect.clear();
        formatSelect.clear();
        avroSchemaSelect.clear();
        keyColumnField.clear();
        partitionColumnField.clear();
        sqlPartitionColumnField.clear();
        partitionsField.clear();
        threadsField.setValue(4);
        streamBatchSizeField.setValue(50000);

        endTimestampField.setValue(LocalDateTime.now());
        startTimestampField.setValue(LocalDateTime.now().minusDays(1));
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
}
