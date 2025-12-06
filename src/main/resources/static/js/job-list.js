/**
 * ETL Engine - Job List Page
 * Функционал для списка jobs: запуск и удаление
 */

const JobList = {

    /**
     * Запускает job
     * @param {string} jobId - ID job'а для запуска
     */
    runJob: async function(jobId) {
        if (!jobId) {
            ETLEngine.showNotification('Job ID is required', 'error');
            return;
        }

        try {
            // Отправляем POST запрос на запуск job
            const response = await ETLEngine.fetch(`/api/jobs/${jobId}/run`, {
                method: 'POST',
                body: JSON.stringify({}) // Пустой объект для JobRunRequest
            });

            ETLEngine.showNotification(`Job started: ${jobId}`, 'success');

            // Перезагружаем страницу через 1 секунду чтобы обновить статусы
            setTimeout(() => {
                window.location.reload();
            }, 1000);

        } catch (error) {
            console.error('Failed to run job:', error);
            ETLEngine.showNotification(
                `Failed to start job: ${error.message}`,
                'error',
                5000
            );
        }
    },

    /**
     * Удаляет job с подтверждением
     * @param {string} jobId - ID job'а для удаления
     */
    deleteJob: async function(jobId) {
        if (!jobId) {
            ETLEngine.showNotification('Job ID is required', 'error');
            return;
        }

        // Показываем модальное окно подтверждения
        const confirmed = await this.showConfirmDialog(
            'Delete Job',
            `Are you sure you want to delete job "${jobId}"? This action cannot be undone.`
        );

        if (!confirmed) {
            return;
        }

        try {
            // Отправляем DELETE запрос
            await ETLEngine.fetch(`/api/jobs/${jobId}`, {
                method: 'DELETE'
            });

            ETLEngine.showNotification(`Job deleted: ${jobId}`, 'success');

            // Перезагружаем страницу через 500 мс
            setTimeout(() => {
                window.location.reload();
            }, 500);

        } catch (error) {
            console.error('Failed to delete job:', error);
            ETLEngine.showNotification(
                `Failed to delete job: ${error.message}`,
                'error',
                5000
            );
        }
    },

    /**
     * Показывает модальное окно подтверждения
     * @param {string} title - Заголовок диалога
     * @param {string} message - Текст сообщения
     * @returns {Promise<boolean>} true если пользователь подтвердил
     */
    showConfirmDialog: function(title, message) {
        return new Promise((resolve) => {
            // Создаем модальное окно Bootstrap
            const modalId = 'confirmModal-' + Date.now();
            const modalHtml = `
                <div class="modal fade" id="${modalId}" tabindex="-1" aria-hidden="true">
                    <div class="modal-dialog">
                        <div class="modal-content">
                            <div class="modal-header">
                                <h5 class="modal-title">${ETLEngine.escapeHtml(title)}</h5>
                                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                            </div>
                            <div class="modal-body">
                                ${ETLEngine.escapeHtml(message)}
                            </div>
                            <div class="modal-footer">
                                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                                <button type="button" class="btn btn-danger" id="confirmButton">Delete</button>
                            </div>
                        </div>
                    </div>
                </div>
            `;

            document.body.insertAdjacentHTML('beforeend', modalHtml);

            const modalElement = document.getElementById(modalId);
            const modal = new bootstrap.Modal(modalElement);

            // Обработчик кнопки подтверждения
            document.getElementById('confirmButton').addEventListener('click', () => {
                modal.hide();
                resolve(true);
            });

            // Обработчик закрытия модального окна
            modalElement.addEventListener('hidden.bs.modal', () => {
                modalElement.remove();
                resolve(false);
            });

            modal.show();
        });
    }

};

// Экспортируем в глобальную область
window.JobList = JobList;

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    console.log('Job List page initialized');
});
