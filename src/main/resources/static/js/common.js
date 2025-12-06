/**
 * ETL Engine - Common JavaScript utilities
 */

// Глобальный объект приложения
const ETLEngine = {

    /**
     * Инициализация приложения
     */
    init: function() {
        this.initTheme();
        this.createThemeToggle();
    },

    /**
     * Инициализация темы из localStorage
     */
    initTheme: function() {
        const savedTheme = localStorage.getItem('theme') || 'light';
        document.documentElement.setAttribute('data-theme', savedTheme);
    },

    /**
     * Переключение темы
     */
    toggleTheme: function() {
        const current = document.documentElement.getAttribute('data-theme') || 'light';
        const newTheme = current === 'light' ? 'dark' : 'light';
        document.documentElement.setAttribute('data-theme', newTheme);
        localStorage.setItem('theme', newTheme);

        // Обновляем иконку
        const icon = document.querySelector('.theme-toggle-btn');
        if (icon) {
            icon.textContent = newTheme === 'light' ? '🌙' : '☀️';
        }
    },

    /**
     * Создает кнопку переключения темы
     */
    createThemeToggle: function() {
        const toggle = document.createElement('div');
        toggle.className = 'theme-toggle';

        const button = document.createElement('button');
        button.className = 'theme-toggle-btn';
        button.setAttribute('aria-label', 'Toggle theme');
        button.setAttribute('title', 'Toggle dark/light theme');

        const currentTheme = document.documentElement.getAttribute('data-theme') || 'light';
        button.textContent = currentTheme === 'light' ? '🌙' : '☀️';

        button.addEventListener('click', () => this.toggleTheme());

        toggle.appendChild(button);
        document.body.appendChild(toggle);
    },

    /**
     * Показывает toast уведомление (аналог Vaadin Notification)
     * @param {string} message - Текст сообщения
     * @param {string} type - Тип: 'success', 'error', 'warning', 'info'
     * @param {number} duration - Длительность в мс (по умолчанию 3000)
     */
    showNotification: function(message, type = 'info', duration = 3000) {
        // Создаем контейнер для toast если его нет
        let container = document.querySelector('.toast-container');
        if (!container) {
            container = document.createElement('div');
            container.className = 'toast-container';
            document.body.appendChild(container);
        }

        // Создаем toast элемент
        const toastId = 'toast-' + Date.now();
        const toastHtml = `
            <div id="${toastId}" class="toast align-items-center text-white bg-${this.getBootstrapType(type)} border-0" role="alert" aria-live="assertive" aria-atomic="true">
                <div class="d-flex">
                    <div class="toast-body">
                        ${this.escapeHtml(message)}
                    </div>
                    <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
                </div>
            </div>
        `;

        container.insertAdjacentHTML('beforeend', toastHtml);

        // Инициализируем и показываем toast
        const toastElement = document.getElementById(toastId);
        const toast = new bootstrap.Toast(toastElement, {
            autohide: true,
            delay: duration
        });

        toast.show();

        // Удаляем элемент после скрытия
        toastElement.addEventListener('hidden.bs.toast', function() {
            toastElement.remove();
        });
    },

    /**
     * Конвертирует тип уведомления в Bootstrap класс
     */
    getBootstrapType: function(type) {
        const typeMap = {
            'success': 'success',
            'error': 'danger',
            'warning': 'warning',
            'info': 'primary'
        };
        return typeMap[type] || 'primary';
    },

    /**
     * Экранирует HTML для безопасного вывода
     */
    escapeHtml: function(text) {
        const map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        };
        return text.replace(/[&<>"']/g, m => map[m]);
    },

    /**
     * Выполняет HTTP запрос
     * @param {string} url - URL для запроса
     * @param {object} options - Опции fetch
     * @returns {Promise}
     */
    fetch: async function(url, options = {}) {
        const defaultOptions = {
            headers: {
                'Content-Type': 'application/json'
            }
        };

        const mergedOptions = { ...defaultOptions, ...options };

        try {
            const response = await fetch(url, mergedOptions);

            if (!response.ok) {
                // Пытаемся извлечь сообщение об ошибке из JSON
                const contentType = response.headers.get('content-type');
                let errorMessage = `HTTP error! status: ${response.status}`;

                if (contentType && contentType.includes('application/json')) {
                    try {
                        const errorData = await response.json();
                        errorMessage = errorData.message || errorMessage;
                    } catch (e) {
                        // Если не удалось распарсить JSON, используем дефолтное сообщение
                    }
                } else {
                    const errorText = await response.text();
                    if (errorText) {
                        errorMessage = errorText;
                    }
                }

                throw new Error(errorMessage);
            }

            // Пытаемся распарсить JSON, если не получается возвращаем текст
            const contentType = response.headers.get('content-type');
            if (contentType && contentType.includes('application/json')) {
                return await response.json();
            } else {
                return await response.text();
            }

        } catch (error) {
            console.error('Fetch error:', error);
            throw error;
        }
    },

    /**
     * Форматирует Duration в читаемый вид
     * @param {number} seconds - Количество секунд
     * @returns {string} Форматированная строка (HH:MM:SS)
     */
    formatDuration: function(seconds) {
        if (!seconds || seconds < 0) return '00:00:00';

        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        const secs = seconds % 60;

        return [hours, minutes, secs]
            .map(v => v < 10 ? '0' + v : v)
            .join(':');
    },

    /**
     * Форматирует миллисекунды в секунды
     * @param {number} millis - Миллисекунды
     * @returns {string} Форматированная строка (X.XX sec)
     */
    formatMillis: function(millis) {
        if (!millis || millis < 0) return '0.00 sec';
        return (millis / 1000).toFixed(2) + ' sec';
    },

    /**
     * Обрезает строку до указанной длины
     * @param {string} text - Текст
     * @param {number} maxLength - Максимальная длина
     * @returns {string} Обрезанная строка
     */
    truncate: function(text, maxLength) {
        if (!text) return '';
        if (text.length <= maxLength) return text;
        return text.substring(0, maxLength) + '...';
    }

};

// Экспортируем в глобальную область
window.ETLEngine = ETLEngine;

// Инициализируем приложение при загрузке DOM
document.addEventListener('DOMContentLoaded', function() {
    ETLEngine.init();
});
