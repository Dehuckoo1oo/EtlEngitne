package ru.pospelov.etl.ui;

import com.vaadin.flow.component.page.AppShellConfigurator;
import com.vaadin.flow.component.page.Push;
import com.vaadin.flow.shared.communication.PushMode;

/**
 * Конфигурация Vaadin приложения.
 * Здесь задаются глобальные настройки: Push, PWA и т.д.
 */
@Push(PushMode.AUTOMATIC)
public class AppShell implements AppShellConfigurator {
}
