/**
 * ETL Engine - Job Form Page with Dynamic Fields
 */

const JobForm = {
    isEditMode: false,
    jobData: null,

    /**
     * Инициализация формы
     */
    init: async function(isEditMode, jobData) {
        this.isEditMode = isEditMode;
        this.jobData = jobData;

        console.log('Initializing Job Form', { isEditMode, jobData });

        // Загружаем Avro схемы (await для корректной загрузки)
        await this.loadAvroSchemas();

        // Настраиваем слушатели для динамических полей
        this.setupFieldListeners();

        // Настраиваем обработчик отправки формы
        this.setupFormSubmit();

        // Если режим редактирования - заполняем форму (после загрузки схем)
        if (isEditMode && jobData) {
            this.populateForm(jobData);
        }

        // Инициализируем видимость полей
        this.updateFieldVisibility();

        // Устанавливаем временные метки по умолчанию (последние 24 часа)
        if (!isEditMode) {
            this.setDefaultTimestamps();
        }
    },

    /**
     * Загружает список Avro схем из Schema Registry
     */
    loadAvroSchemas: async function() {
        try {
            // Проверяем доступность Schema Registry
            const isAvailable = await ETLEngine.fetch('/api/schemas/health');

            if (!isAvailable) {
                console.warn('Schema Registry is not available');
                this.updateSchemaSelectsWithMessage('Schema Registry не доступен');
                return Promise.resolve();
            }

            const schemas = await ETLEngine.fetch('/api/schemas');

            if (schemas && Array.isArray(schemas) && schemas.length > 0) {
                this.populateSchemaSelect('kafkaSourceSchema', schemas);
                this.populateSchemaSelect('kafkaTargetSchema', schemas);
                console.log(`Loaded ${schemas.length} Avro schemas from Schema Registry`);
            } else {
                console.warn('No Avro schemas found in Schema Registry');
                this.updateSchemaSelectsWithMessage('Нет доступных схем');
            }

            return Promise.resolve();
        } catch (error) {
            console.error('Failed to load Avro schemas:', error);
            this.updateSchemaSelectsWithMessage('Ошибка загрузки схем');
            return Promise.resolve();
        }
    },

    /**
     * Обновляет select схем с информационным сообщением
     */
    updateSchemaSelectsWithMessage: function(message) {
        ['kafkaSourceSchema', 'kafkaTargetSchema'].forEach(selectId => {
            const select = document.getElementById(selectId);
            if (!select) return;

            // Добавляем информационную опцию
            const option = document.createElement('option');
            option.value = '';
            option.textContent = `No schema (${message})`;
            option.disabled = true;
            select.appendChild(option);
        });
    },

    /**
     * Заполняет select схем
     */
    populateSchemaSelect: function(selectId, schemas) {
        const select = document.getElementById(selectId);
        if (!select) return;

        // Очищаем опции кроме первой (No schema)
        while (select.options.length > 1) {
            select.remove(1);
        }

        // Добавляем схемы
        schemas.forEach(schema => {
            const option = document.createElement('option');
            option.value = schema;
            option.textContent = schema;
            select.appendChild(option);
        });
    },

    /**
     * Настраивает слушатели для динамических полей
     */
    setupFieldListeners: function() {
        // Слушатель изменения типа источника
        document.getElementById('extractorType').addEventListener('change', () => {
            this.updateFieldVisibility();
        });

        // Слушатель изменения типа назначения
        document.getElementById('loaderType').addEventListener('change', () => {
            this.updateFieldVisibility();
        });

        // Слушатель изменения формата Kafka источника
        document.getElementById('kafkaSourceFormat').addEventListener('change', (e) => {
            this.toggleSchemaField('kafkaSourceSchemaDiv', e.target.value === 'avro');
        });

        // Слушатель изменения формата Kafka назначения
        document.getElementById('kafkaTargetFormat').addEventListener('change', (e) => {
            this.toggleSchemaField('kafkaTargetSchemaDiv', e.target.value === 'avro');
        });
    },

    /**
     * Обновляет видимость полей в зависимости от выбранных типов
     */
    updateFieldVisibility: function() {
        const extractorType = document.getElementById('extractorType').value;
        const loaderType = document.getElementById('loaderType').value;

        // Source configuration
        this.updateSourceFields(extractorType);

        // Target configuration
        this.updateTargetFields(loaderType);
    },

    /**
     * Обновляет поля источника
     */
    updateSourceFields: function(extractorType) {
        const sourceCard = document.getElementById('sourceConfigCard');
        const sqlFields = document.getElementById('sqlSourceFields');
        const kafkaFields = document.getElementById('kafkaSourceFields');
        const sourceFieldDiv = document.getElementById('sourceFieldDiv');
        const sourceField = document.getElementById('sourceField');
        const sourceFieldHint = document.getElementById('sourceFieldHint');

        if (!extractorType) {
            sourceCard.style.display = 'none';
            sourceFieldDiv.style.display = 'none';
            return;
        }

        sourceCard.style.display = 'block';
        sourceFieldDiv.style.display = 'block';

        if (extractorType === 'sql') {
            sqlFields.style.display = 'block';
            kafkaFields.style.display = 'none';
            // Обновляем hint для SQL
            sourceFieldHint.textContent = 'Leave empty - using SQL query above';
            sourceField.placeholder = 'Auto-filled from SQL query';
            sourceField.required = false;
            sourceField.disabled = true;
            // Делаем SQL query обязательным
            document.getElementById('source').required = true;
            document.getElementById('startTimestamp').required = false;
            document.getElementById('endTimestamp').required = false;
        } else if (extractorType === 'kafka') {
            sqlFields.style.display = 'none';
            kafkaFields.style.display = 'block';
            // Обновляем hint для Kafka
            sourceFieldHint.textContent = 'Kafka topic name to read from';
            sourceField.placeholder = 'topic-name';
            sourceField.required = true;
            sourceField.disabled = false;
            // Делаем Kafka поля обязательными
            document.getElementById('source').required = false;
            document.getElementById('startTimestamp').required = true;
            document.getElementById('endTimestamp').required = true;

            // Показываем/скрываем поле схемы в зависимости от формата
            const format = document.getElementById('kafkaSourceFormat').value;
            this.toggleSchemaField('kafkaSourceSchemaDiv', format === 'avro');
        }
    },

    /**
     * Обновляет поля назначения
     */
    updateTargetFields: function(loaderType) {
        const targetCard = document.getElementById('targetConfigCard');
        const sqlFields = document.getElementById('sqlTargetFields');
        const kafkaFields = document.getElementById('kafkaTargetFields');
        const targetFieldDiv = document.getElementById('targetFieldDiv');
        const targetField = document.getElementById('targetField');
        const targetFieldHint = document.getElementById('targetFieldHint');

        if (!loaderType) {
            targetCard.style.display = 'none';
            targetFieldDiv.style.display = 'none';
            return;
        }

        targetCard.style.display = 'block';
        targetFieldDiv.style.display = 'block';

        if (loaderType === 'sql' || loaderType === 'fast-sql') {
            sqlFields.style.display = 'block';
            kafkaFields.style.display = 'none';
            // Обновляем hint для SQL
            targetFieldHint.textContent = 'SQL table name (schema.table)';
            targetField.placeholder = 'schema.table';
            targetField.required = true;
            targetField.disabled = false;
            // Делаем SQL поля обязательными
            document.getElementById('targetTable').required = true;
        } else if (loaderType === 'kafka') {
            sqlFields.style.display = 'none';
            kafkaFields.style.display = 'block';
            // Обновляем hint для Kafka
            targetFieldHint.textContent = 'Kafka topic name to write to';
            targetField.placeholder = 'topic-name';
            targetField.required = true;
            targetField.disabled = false;
            // Делаем Kafka поля обязательными
            document.getElementById('targetTable').required = false;

            // Показываем/скрываем поле схемы в зависимости от формата
            const format = document.getElementById('kafkaTargetFormat').value;
            this.toggleSchemaField('kafkaTargetSchemaDiv', format === 'avro');
        }
    },

    /**
     * Показывает/скрывает поле выбора схемы
     */
    toggleSchemaField: function(divId, show) {
        const div = document.getElementById(divId);
        if (div) {
            div.style.display = show ? 'block' : 'none';
        }
    },

    /**
     * Настраивает обработчик отправки формы
     */
    setupFormSubmit: function() {
        document.getElementById('jobForm').addEventListener('submit', (e) => {
            e.preventDefault();
            this.submitForm();
        });
    },

    /**
     * Отправляет форму
     */
    submitForm: async function() {
        // Собираем данные формы
        const formData = this.collectFormData();

        // Валидация
        if (!this.validateFormData(formData)) {
            return;
        }

        try {
            let url, method;

            if (this.isEditMode) {
                // PUT для обновления
                url = `/api/jobs/${formData.id}`;
                method = 'PUT';
            } else {
                // POST для создания
                url = '/api/jobs';
                method = 'POST';
            }

            await ETLEngine.fetch(url, {
                method: method,
                body: JSON.stringify(formData)
            });

            ETLEngine.showNotification(
                this.isEditMode ? 'Job updated successfully' : 'Job created successfully',
                'success'
            );

            // Переходим на главную страницу
            setTimeout(() => {
                window.location.href = '/';
            }, 500);

        } catch (error) {
            console.error('Failed to save job:', error);
            ETLEngine.showNotification(
                `Failed to save job: ${error.message}`,
                'error',
                5000
            );
        }
    },

    /**
     * Собирает данные из формы
     */
    collectFormData: function() {
        const extractorType = document.getElementById('extractorType').value;
        const loaderType = document.getElementById('loaderType').value;

        const data = {
            id: document.getElementById('jobId').value.trim(),
            source: null,
            target: null,
            params: {
                extractorType: extractorType,
                transformerType: document.getElementById('transformerType').value,
                loaderType: loaderType,
                threads: parseInt(document.getElementById('threads').value),
                streamBatchSize: parseInt(document.getElementById('streamBatchSize').value)
            }
        };

        // Common параметры (partition, key column)
        const partitionColumn = document.getElementById('partitionColumn').value.trim();
        if (partitionColumn) {
            data.params.partitionColumn = partitionColumn;
        }

        const partitions = parseInt(document.getElementById('partitions').value);
        if (partitions > 1) {
            data.params.partitions = partitions;
        }

        const keyColumn = document.getElementById('keyColumn').value.trim();
        if (keyColumn) {
            data.params.keyColumn = keyColumn;
        }

        // Source-специфичные параметры
        if (extractorType === 'sql') {
            // Для SQL: source = SQL запрос
            data.source = document.getElementById('source').value.trim();

        } else if (extractorType === 'kafka') {
            // Для Kafka extractor: topic в params
            data.params.topic = document.getElementById('sourceField').value.trim();

            // Конвертируем datetime-local в timestamp
            const startDate = new Date(document.getElementById('startTimestamp').value);
            const endDate = new Date(document.getElementById('endTimestamp').value);
            data.params.startTimestamp = startDate.getTime();
            data.params.endTimestamp = endDate.getTime();

            data.params.format = document.getElementById('kafkaSourceFormat').value;

            const schema = document.getElementById('kafkaSourceSchema').value;
            if (schema) {
                data.params.avroSchema = schema;
            }
        }

        // Target-специфичные параметры
        if (loaderType === 'sql' || loaderType === 'fast-sql') {
            // Для SQL: target = SQL таблица
            data.target = document.getElementById('targetField').value.trim();

        } else if (loaderType === 'kafka') {
            // Для Kafka loader: topic в params
            // ВАЖНО: если extractor не Kafka, перезаписываем topic
            // Если extractor тоже Kafka, topic уже установлен выше (одинаковый для source и target)
            if (extractorType !== 'kafka') {
                data.params.topic = document.getElementById('targetField').value.trim();
            }

            data.params.format = document.getElementById('kafkaTargetFormat').value;

            const schema = document.getElementById('kafkaTargetSchema').value;
            if (schema) {
                data.params.avroSchema = schema;
            }
        }

        return data;
    },

    /**
     * Валидирует данные формы
     */
    validateFormData: function(data) {
        // Проверка Job ID
        if (!data.id) {
            ETLEngine.showNotification('Job ID is required', 'error');
            return false;
        }

        // Проверка типов
        if (!data.params.extractorType || !data.params.loaderType || !data.params.transformerType) {
            ETLEngine.showNotification('All types must be selected', 'error');
            return false;
        }

        // Проверка source
        if (data.params.extractorType === 'sql') {
            if (!data.source) {
                ETLEngine.showNotification('SQL query is required', 'error');
                return false;
            }
        } else if (data.params.extractorType === 'kafka') {
            if (!data.params.topic) {
                ETLEngine.showNotification('Kafka topic is required', 'error');
                return false;
            }
            if (!data.params.startTimestamp || !data.params.endTimestamp) {
                ETLEngine.showNotification('Start and End Timestamps are required for Kafka extraction', 'error');
                return false;
            }
        }

        // Проверка target
        if (data.params.loaderType === 'sql' || data.params.loaderType === 'fast-sql') {
            if (!data.target) {
                ETLEngine.showNotification('SQL table is required', 'error');
                return false;
            }
        } else if (data.params.loaderType === 'kafka') {
            if (!data.params.topic) {
                ETLEngine.showNotification('Kafka topic is required', 'error');
                return false;
            }
        }

        return true;
    },

    /**
     * Заполняет форму данными job (режим редактирования)
     */
    populateForm: function(job) {
        console.log('Populating form with job data:', job);

        // Базовые поля уже заполнены через Thymeleaf th:value

        // Заполняем временные метки для Kafka source
        if (job.params.extractorType === 'kafka') {
            if (job.params.startTimestamp) {
                const startDate = new Date(job.params.startTimestamp);
                document.getElementById('startTimestamp').value = this.formatDatetimeLocal(startDate);
            }
            if (job.params.endTimestamp) {
                const endDate = new Date(job.params.endTimestamp);
                document.getElementById('endTimestamp').value = this.formatDatetimeLocal(endDate);
            }

            // Формат и схема для Kafka source
            if (job.params.format) {
                document.getElementById('kafkaSourceFormat').value = job.params.format;
            }
            if (job.params.avroSchema) {
                // Схемы уже загружены, можем установить значение
                const schemaSelect = document.getElementById('kafkaSourceSchema');
                if (schemaSelect) {
                    schemaSelect.value = job.params.avroSchema;
                    if (schemaSelect.value !== job.params.avroSchema) {
                        console.warn(`Schema "${job.params.avroSchema}" not found in Schema Registry`);
                    }
                }
            }
        }

        // Формат и схема для Kafka target
        if (job.params.loaderType === 'kafka') {
            if (job.params.format) {
                document.getElementById('kafkaTargetFormat').value = job.params.format;
            }
            if (job.params.avroSchema) {
                // Схемы уже загружены, можем установить значение
                const schemaSelect = document.getElementById('kafkaTargetSchema');
                if (schemaSelect) {
                    schemaSelect.value = job.params.avroSchema;
                    if (schemaSelect.value !== job.params.avroSchema) {
                        console.warn(`Schema "${job.params.avroSchema}" not found in Schema Registry`);
                    }
                }
            }
        }
    },

    /**
     * Устанавливает временные метки по умолчанию (последние 24 часа)
     */
    setDefaultTimestamps: function() {
        const now = new Date();
        const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);

        document.getElementById('startTimestamp').value = this.formatDatetimeLocal(yesterday);
        document.getElementById('endTimestamp').value = this.formatDatetimeLocal(now);
    },

    /**
     * Форматирует Date в формат datetime-local (YYYY-MM-DDTHH:mm)
     */
    formatDatetimeLocal: function(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');

        return `${year}-${month}-${day}T${hours}:${minutes}`;
    }

};

// Экспортируем в глобальную область
window.JobForm = JobForm;

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    // IS_EDIT_MODE и JOB_DATA передаются из Thymeleaf шаблона
    if (typeof IS_EDIT_MODE !== 'undefined') {
        JobForm.init(IS_EDIT_MODE, JOB_DATA);
    } else {
        console.error('IS_EDIT_MODE is not defined');
    }
});
