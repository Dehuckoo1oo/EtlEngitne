/**
 * ETL Engine - Job Details Page (Static, no auto-refresh)
 * Metrics are loaded once on page load. Press F5 to refresh.
 */

const JobDetails = {
    jobId: null,

    /**
     * Инициализация страницы
     */
    init: function(jobId) {
        this.jobId = jobId;
        console.log('Initializing Job Details for job:', jobId);

        // Загружаем метрики один раз при загрузке страницы
        this.loadMetrics();
    },

    /**
     * Загрузка метрик через REST API
     */
    loadMetrics: async function() {
        try {
            const response = await fetch(`/api/metrics/${this.jobId}`);

            if (!response.ok) {
                // Если job не найден или метрик нет, показываем сообщение
                if (response.status === 404) {
                    this.showNoMetrics();
                    return;
                }
                throw new Error(`HTTP ${response.status}`);
            }

            const metrics = await response.json();

            // Если получили пустой объект, значит метрик нет
            if (!metrics || Object.keys(metrics).length === 0) {
                this.showNoMetrics();
                return;
            }

            // Обновляем UI
            this.updateMetrics(metrics);
            this.updateStatus(metrics.status);
            this.updateProgressBar(metrics.status);
            this.showMetricsSection();

        } catch (error) {
            console.error('Failed to load metrics:', error);
            this.showNoMetrics();
        }
    },

    /**
     * Обновляет метрики на странице
     */
    updateMetrics: function(metrics) {
        // Обновляем карточки метрик
        this.updateElement('extractedRecords', metrics.extractedRecords || 0);
        this.updateElement('processedRecords', metrics.processedRecords || 0);
        this.updateElement('transformedRecords', metrics.transformedRecords || 0);
        this.updateElement('loadedRecords', metrics.loadedRecords || 0);
        this.updateElement('errorCount', metrics.errorCount || 0);

        // Обновляем performance метрики
        this.updateElement('throughput', (metrics.throughput || 0).toFixed(2) + ' records/sec');
        this.updateElement('totalDuration', ETLEngine.formatMillis(metrics.totalDurationMillis || 0));
        this.updateElement('extractDuration', ETLEngine.formatMillis(metrics.extractDurationMillis || 0));
        this.updateElement('transformDuration', ETLEngine.formatMillis(metrics.transformDurationMillis || 0));
        this.updateElement('loadDuration', ETLEngine.formatMillis(metrics.loadDurationMillis || 0));
    },

    /**
     * Обновляет badge статуса
     */
    updateStatus: function(status) {
        const statusBadge = document.getElementById('statusBadge');
        if (!statusBadge) return;

        statusBadge.textContent = status;

        // Удаляем все классы badge-*
        statusBadge.className = 'badge';

        // Добавляем нужный класс в зависимости от статуса
        switch (status) {
            case 'COMPLETED':
                statusBadge.classList.add('badge-success');
                break;
            case 'FAILED':
                statusBadge.classList.add('badge-error');
                break;
            case 'RUNNING':
            case 'EXTRACTING':
            case 'TRANSFORMING':
            case 'LOADING':
                statusBadge.classList.add('badge-contrast');
                break;
            default:
                statusBadge.classList.add('badge-primary');
        }
    },

    /**
     * Обновляет прогресс-бар
     */
    updateProgressBar: function(status) {
        const container = document.getElementById('progressBarContainer');
        const progressBar = document.getElementById('progressBar');

        if (!container || !progressBar) return;

        const runningStatuses = ['RUNNING', 'EXTRACTING', 'TRANSFORMING', 'LOADING'];

        if (runningStatuses.includes(status)) {
            // Показываем indeterminate progress bar
            container.style.display = 'block';
            progressBar.style.width = '100%';
            progressBar.classList.add('progress-bar-animated', 'progress-bar-striped');
        } else if (status === 'COMPLETED') {
            // Показываем 100% progress bar
            container.style.display = 'block';
            progressBar.style.width = '100%';
            progressBar.classList.remove('progress-bar-animated', 'progress-bar-striped');
            progressBar.classList.add('bg-success');
        } else if (status === 'FAILED') {
            // Показываем красный progress bar
            container.style.display = 'block';
            progressBar.style.width = '100%';
            progressBar.classList.remove('progress-bar-animated', 'progress-bar-striped', 'bg-primary');
            progressBar.classList.add('bg-danger');
        } else {
            // Скрываем progress bar
            container.style.display = 'none';
        }
    },

    /**
     * Показывает секцию метрик
     */
    showMetricsSection: function() {
        const noMetricsMessage = document.getElementById('noMetricsMessage');
        const metricsCards = document.getElementById('metricsCards');
        const performanceSection = document.getElementById('performanceSection');

        if (noMetricsMessage) noMetricsMessage.style.display = 'none';
        if (metricsCards) metricsCards.style.display = 'flex';
        if (performanceSection) performanceSection.style.display = 'block';
    },

    /**
     * Показывает сообщение "нет метрик"
     */
    showNoMetrics: function() {
        const noMetricsMessage = document.getElementById('noMetricsMessage');
        const metricsCards = document.getElementById('metricsCards');
        const performanceSection = document.getElementById('performanceSection');

        if (noMetricsMessage) noMetricsMessage.style.display = 'block';
        if (metricsCards) metricsCards.style.display = 'none';
        if (performanceSection) performanceSection.style.display = 'none';
    },

    /**
     * Обновляет текст элемента
     */
    updateElement: function(elementId, value) {
        const element = document.getElementById(elementId);
        if (element) {
            element.textContent = value;
        }
    },

    /**
     * Форматирует Duration объект в строку HH:MM:SS
     */
    formatDuration: function(duration) {
        if (!duration) return '00:00:00';

        // Duration приходит как строка "PT1H2M3S" (ISO-8601)
        // или как объект { seconds: 123 }
        let totalSeconds = 0;

        if (typeof duration === 'string') {
            // Парсим ISO-8601 duration
            const match = duration.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?/);
            if (match) {
                const hours = parseInt(match[1] || 0);
                const minutes = parseInt(match[2] || 0);
                const seconds = parseFloat(match[3] || 0);
                totalSeconds = hours * 3600 + minutes * 60 + seconds;
            }
        } else if (typeof duration === 'object' && duration.seconds !== undefined) {
            totalSeconds = duration.seconds;
        } else if (typeof duration === 'number') {
            totalSeconds = duration;
        }

        return ETLEngine.formatDuration(Math.floor(totalSeconds));
    },

    /**
     * Запуск job
     */
    runJob: async function() {
        if (!this.jobId) {
            ETLEngine.showNotification('Job ID is not available', 'error');
            return;
        }

        try {
            await ETLEngine.fetch(`/api/jobs/${this.jobId}/run`, {
                method: 'POST',
                body: JSON.stringify({})
            });

            ETLEngine.showNotification(`Job started: ${this.jobId}. Refresh page (F5) to see progress.`, 'success', 5000);

        } catch (error) {
            console.error('Failed to run job:', error);
            ETLEngine.showNotification(
                `Failed to start job: ${error.message}`,
                'error',
                5000
            );
        }
    }
};

// Экспортируем в глобальную область
window.JobDetails = JobDetails;

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    // JOB_ID передается из Thymeleaf шаблона
    if (typeof JOB_ID !== 'undefined') {
        JobDetails.init(JOB_ID);
    } else {
        console.error('JOB_ID is not defined');
    }
});
