/**
 * ETL Engine - Schema-Driven Job Form
 * Dynamically generates form fields based on schema API
 */

const SchemaJobForm = {
    isEditMode: false,
    jobData: null,
    schema: null,
    avroSchemas: [],

    /**
     * Initialize the schema-driven form
     */
    init: async function(isEditMode, jobData) {
        this.isEditMode = isEditMode;
        this.jobData = jobData;

        console.log('Initializing Schema-Driven Job Form', { isEditMode, jobData });

        try {
            // Load component schema from API
            await this.loadComponentSchema();

            // Load Avro schemas for enum fields
            await this.loadAvroSchemas();

            // Setup form listeners
            this.setupFormListeners();

            // If edit mode, populate form
            if (isEditMode && jobData) {
                this.populateForm(jobData);
            }

            // Initialize dynamic fields
            this.updateDynamicFields();

        } catch (error) {
            console.error('Failed to initialize form:', error);
            ETLEngine.showNotification('Failed to load form schema', 'error', 5000);
        }
    },

    /**
     * Load component schema from API
     */
    loadComponentSchema: async function() {
        try {
            this.schema = await ETLEngine.fetch('/api/jobs/schema');
            console.log('Loaded component schema:', this.schema);
        } catch (error) {
            console.error('Failed to load component schema:', error);
            throw error;
        }
    },

    /**
     * Load Avro schemas from Schema Registry
     */
    loadAvroSchemas: async function() {
        try {
            const isAvailable = await ETLEngine.fetch('/api/schemas/health');
            if (!isAvailable) {
                console.warn('Schema Registry is not available');
                return;
            }

            this.avroSchemas = await ETLEngine.fetch('/api/schemas');
            console.log(`Loaded ${this.avroSchemas.length} Avro schemas`);
        } catch (error) {
            console.error('Failed to load Avro schemas:', error);
            // Non-critical error, continue without Avro schemas
        }
    },

    /**
     * Setup form event listeners
     */
    setupFormListeners: function() {
        // Extractor type change
        document.getElementById('extractorType').addEventListener('change', () => {
            this.updateDynamicFields();
        });

        // Transformer type change
        document.getElementById('transformerType').addEventListener('change', () => {
            this.updateDynamicFields();
        });

        // Loader type change
        document.getElementById('loaderType').addEventListener('change', () => {
            this.updateDynamicFields();
        });

        // Form submit
        document.getElementById('jobForm').addEventListener('submit', (e) => {
            e.preventDefault();
            this.submitForm();
        });
    },

    /**
     * Update dynamic fields based on selected types
     */
    updateDynamicFields: function() {
        const extractorType = document.getElementById('extractorType').value;
        const transformerType = document.getElementById('transformerType').value;
        const loaderType = document.getElementById('loaderType').value;

        // Render extractor fields
        if (extractorType && this.schema.extractors[extractorType]) {
            this.renderComponentFields('extractor', extractorType, this.schema.extractors[extractorType]);
        } else {
            this.clearComponentFields('extractor');
        }

        // Render transformer fields
        if (transformerType && this.schema.transformers[transformerType]) {
            this.renderComponentFields('transformer', transformerType, this.schema.transformers[transformerType]);
        } else {
            this.clearComponentFields('transformer');
        }

        // Render loader fields
        if (loaderType && this.schema.loaders[loaderType]) {
            this.renderComponentFields('loader', loaderType, this.schema.loaders[loaderType]);
        } else {
            this.clearComponentFields('loader');
        }
    },

    /**
     * Render fields for a component
     */
    renderComponentFields: function(componentType, selectedType, componentSchema) {
        const containerId = `${componentType}ConfigFields`;
        const container = document.getElementById(containerId);

        if (!container) {
            console.error(`Container not found: ${containerId}`);
            return;
        }

        // Show container
        const card = document.getElementById(`${componentType}ConfigCard`);
        if (card) {
            card.style.display = 'block';
        }

        // Clear existing fields
        container.innerHTML = '';

        // Add title
        const title = document.createElement('h6');
        title.className = 'mb-3';
        title.textContent = `${componentSchema.displayName} Configuration`;
        container.appendChild(title);

        // Create fields container
        const fieldsRow = document.createElement('div');
        fieldsRow.className = 'row g-3';
        container.appendChild(fieldsRow);

        // Render each field
        componentSchema.fields.forEach(field => {
            const fieldDiv = this.createFieldElement(componentType, selectedType, field);
            fieldsRow.appendChild(fieldDiv);
        });
    },

    /**
     * Create a field element based on schema
     */
    createFieldElement: function(componentType, selectedType, fieldSchema) {
        const colDiv = document.createElement('div');
        colDiv.className = this.getFieldColumnClass(fieldSchema);

        const fieldId = `${componentType}_${selectedType}_${fieldSchema.name}`;

        // Label
        const label = document.createElement('label');
        label.className = 'form-label';
        label.htmlFor = fieldId;
        label.textContent = fieldSchema.label;

        if (fieldSchema.required) {
            const requiredSpan = document.createElement('span');
            requiredSpan.className = 'text-danger';
            requiredSpan.textContent = ' *';
            label.appendChild(requiredSpan);
        }

        colDiv.appendChild(label);

        // Input element
        let input;
        if (fieldSchema.type === 'enum') {
            input = this.createEnumField(fieldId, fieldSchema);
        } else if (fieldSchema.type === 'number') {
            input = this.createNumberField(fieldId, fieldSchema);
        } else if (fieldSchema.type === 'text') {
            input = this.createTextField(fieldId, fieldSchema);
        } else if (fieldSchema.type === 'boolean') {
            input = this.createBooleanField(fieldId, fieldSchema);
        } else {
            input = this.createTextField(fieldId, fieldSchema);
        }

        colDiv.appendChild(input);

        // Help text
        if (fieldSchema.description) {
            const helpText = document.createElement('div');
            helpText.className = 'form-text';
            helpText.textContent = fieldSchema.description;
            colDiv.appendChild(helpText);
        }

        return colDiv;
    },

    /**
     * Create enum field (select)
     */
    createEnumField: function(fieldId, fieldSchema) {
        const select = document.createElement('select');
        select.className = 'form-select';
        select.id = fieldId;
        select.name = fieldSchema.name;

        if (fieldSchema.required) {
            select.required = true;
        }

        // Add empty option if not required
        if (!fieldSchema.required) {
            const emptyOption = document.createElement('option');
            emptyOption.value = '';
            emptyOption.textContent = `Select ${fieldSchema.label.toLowerCase()}...`;
            select.appendChild(emptyOption);
        }

        // Add enum values
        if (fieldSchema.enumValues) {
            fieldSchema.enumValues.forEach(value => {
                const option = document.createElement('option');
                option.value = value;
                option.textContent = value;
                select.appendChild(option);
            });
        }

        return select;
    },

    /**
     * Create number field
     */
    createNumberField: function(fieldId, fieldSchema) {
        const input = document.createElement('input');
        input.type = 'number';
        input.className = 'form-control';
        input.id = fieldId;
        input.name = fieldSchema.name;

        if (fieldSchema.required) {
            input.required = true;
        }

        if (fieldSchema.min !== null && fieldSchema.min !== undefined) {
            input.min = fieldSchema.min;
        }

        if (fieldSchema.max !== null && fieldSchema.max !== undefined) {
            input.max = fieldSchema.max;
        }

        // Set recommended default in placeholder
        const recommended = this.getRecommendedValue(fieldSchema.name);
        if (recommended !== null) {
            input.placeholder = `Recommended: ${recommended}`;
        }

        return input;
    },

    /**
     * Create text field
     */
    createTextField: function(fieldId, fieldSchema) {
        const isMultiline = fieldSchema.name === 'sqlQuery';

        if (isMultiline) {
            const textarea = document.createElement('textarea');
            textarea.className = 'form-control font-monospace';
            textarea.id = fieldId;
            textarea.name = fieldSchema.name;
            textarea.rows = 5;

            if (fieldSchema.required) {
                textarea.required = true;
            }

            if (fieldSchema.description) {
                textarea.placeholder = fieldSchema.description;
            }

            return textarea;
        } else {
            const input = document.createElement('input');
            input.type = 'text';
            input.className = 'form-control';
            input.id = fieldId;
            input.name = fieldSchema.name;

            if (fieldSchema.required) {
                input.required = true;
            }

            if (fieldSchema.description) {
                input.placeholder = fieldSchema.description;
            }

            return input;
        }
    },

    /**
     * Create boolean field (checkbox)
     */
    createBooleanField: function(fieldId, fieldSchema) {
        const div = document.createElement('div');
        div.className = 'form-check';

        const input = document.createElement('input');
        input.type = 'checkbox';
        input.className = 'form-check-input';
        input.id = fieldId;
        input.name = fieldSchema.name;

        const label = document.createElement('label');
        label.className = 'form-check-label';
        label.htmlFor = fieldId;
        label.textContent = fieldSchema.description || fieldSchema.label;

        div.appendChild(input);
        div.appendChild(label);

        return div;
    },

    /**
     * Get recommended column class based on field type
     */
    getFieldColumnClass: function(fieldSchema) {
        // Full width for text areas and long text fields
        if (fieldSchema.name === 'sqlQuery') {
            return 'col-md-12';
        }

        // Half width for most fields
        if (fieldSchema.type === 'text') {
            return 'col-md-6';
        }

        // Third width for numbers and enums
        return 'col-md-4';
    },

    /**
     * Get recommended default value for a field
     */
    getRecommendedValue: function(fieldName) {
        const recommendations = {
            'threads': 4,
            'streamBatchSize': 1000,
            'partitions': 1
        };
        return recommendations[fieldName] || null;
    },

    /**
     * Clear component fields
     */
    clearComponentFields: function(componentType) {
        const containerId = `${componentType}ConfigFields`;
        const container = document.getElementById(containerId);

        if (container) {
            container.innerHTML = '';
        }

        const card = document.getElementById(`${componentType}ConfigCard`);
        if (card) {
            card.style.display = 'none';
        }
    },

    /**
     * Populate form with existing job data (edit mode)
     */
    populateForm: function(job) {
        console.log('Populating form with job data:', job);

        // Basic fields are already populated by Thymeleaf

        // Trigger field rendering
        this.updateDynamicFields();

        // Wait a bit for fields to render, then populate values
        setTimeout(() => {
            this.populateComponentFields('extractor', job.params.extractorType, job);
            this.populateComponentFields('transformer', job.params.transformerType, job);
            this.populateComponentFields('loader', job.params.loaderType, job);
        }, 100);
    },

    /**
     * Populate component fields with values
     */
    populateComponentFields: function(componentType, selectedType, job) {
        const params = job.params;

        // Find all fields for this component
        const prefix = `${componentType}_${selectedType}_`;
        const fields = document.querySelectorAll(`[id^="${prefix}"]`);

        fields.forEach(field => {
            const fieldName = field.name;
            let value = params[fieldName];

            // Special handling for timestamps
            if (fieldName === 'startTimestamp' || fieldName === 'endTimestamp') {
                if (value) {
                    value = new Date(value).toISOString().slice(0, 16);
                }
            }

            // Set field value
            if (field.type === 'checkbox') {
                field.checked = !!value;
            } else if (value !== null && value !== undefined) {
                field.value = value;
            }
        });
    },

    /**
     * Submit form
     */
    submitForm: async function() {
        const formData = this.collectFormData();

        if (!this.validateFormData(formData)) {
            return;
        }

        try {
            let url, method;

            if (this.isEditMode) {
                url = `/api/jobs/${formData.id}`;
                method = 'PUT';
            } else {
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
     * Collect form data
     */
    collectFormData: function() {
        const extractorType = document.getElementById('extractorType').value;
        const transformerType = document.getElementById('transformerType').value;
        const loaderType = document.getElementById('loaderType').value;

        const data = {
            id: document.getElementById('jobId').value.trim(),
            source: null,
            target: null,
            params: {
                extractorType: extractorType,
                transformerType: transformerType,
                loaderType: loaderType
            }
        };

        // Collect extractor fields
        this.collectComponentFieldValues(data.params, 'extractor', extractorType);

        // Collect transformer fields
        this.collectComponentFieldValues(data.params, 'transformer', transformerType);

        // Collect loader fields
        this.collectComponentFieldValues(data.params, 'loader', loaderType);

        // Set source and target for backward compatibility
        data.source = this.extractSource(data.params, extractorType);
        data.target = this.extractTarget(data.params, loaderType);

        return data;
    },

    /**
     * Collect component field values
     */
    collectComponentFieldValues: function(params, componentType, selectedType) {
        const prefix = `${componentType}_${selectedType}_`;
        const fields = document.querySelectorAll(`[id^="${prefix}"]`);

        fields.forEach(field => {
            const fieldName = field.name;
            let value;

            if (field.type === 'checkbox') {
                value = field.checked;
            } else if (field.type === 'number') {
                value = field.value ? parseInt(field.value) : null;
            } else if (field.type === 'datetime-local') {
                value = field.value ? new Date(field.value).getTime() : null;
            } else {
                value = field.value || null;
            }

            // Only include non-null values
            if (value !== null && value !== '') {
                params[fieldName] = value;
            }
        });
    },

    /**
     * Extract source for backward compatibility
     */
    extractSource: function(params, extractorType) {
        if (extractorType === 'sql') {
            return params.sqlQuery || null;
        } else if (extractorType === 'kafka') {
            return params.topic || null;
        }
        return null;
    },

    /**
     * Extract target for backward compatibility
     */
    extractTarget: function(params, loaderType) {
        if (loaderType === 'jdbc' || loaderType === 'fast-sql') {
            return params.targetTable || null;
        } else if (loaderType === 'kafka') {
            return params.topic || null;
        }
        return null;
    },

    /**
     * Validate form data
     */
    validateFormData: function(data) {
        if (!data.id) {
            ETLEngine.showNotification('Job ID is required', 'error');
            return false;
        }

        if (!data.params.extractorType || !data.params.transformerType || !data.params.loaderType) {
            ETLEngine.showNotification('All component types must be selected', 'error');
            return false;
        }

        return true;
    }
};

// Export to global scope
window.SchemaJobForm = SchemaJobForm;

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    if (typeof IS_EDIT_MODE !== 'undefined') {
        SchemaJobForm.init(IS_EDIT_MODE, JOB_DATA);
    } else {
        console.error('IS_EDIT_MODE is not defined');
    }
});
