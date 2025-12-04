package ru.pospelov.etl.ui;

import com.vaadin.flow.component.applayout.AppLayout;
import com.vaadin.flow.component.applayout.DrawerToggle;
import com.vaadin.flow.component.html.H1;
import com.vaadin.flow.component.orderedlayout.FlexComponent;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.router.RouterLink;
import com.vaadin.flow.theme.lumo.LumoUtility;
import ru.pospelov.etl.ui.views.JobListView;

/**
 * Главный layout приложения с навигацией.
 * Используется как родительский layout для всех view.
 */
public class MainLayout extends AppLayout {

    public MainLayout() {
        createHeader();
        createDrawer();
    }

    /**
     * Создает шапку приложения
     */
    private void createHeader() {
        H1 logo = new H1("ETL Engine");
        logo.addClassNames(
                LumoUtility.FontSize.LARGE,
                LumoUtility.Margin.MEDIUM
        );

        DrawerToggle toggle = new DrawerToggle();

        HorizontalLayout header = new HorizontalLayout(toggle, logo);
        header.setDefaultVerticalComponentAlignment(FlexComponent.Alignment.CENTER);
        header.expand(logo);
        header.setWidthFull();
        header.addClassNames(
                LumoUtility.Padding.Vertical.NONE,
                LumoUtility.Padding.Horizontal.MEDIUM
        );

        addToNavbar(header);
    }

    /**
     * Создает боковую панель с навигацией
     */
    private void createDrawer() {
        RouterLink jobsLink = new RouterLink("Jobs", JobListView.class);
        jobsLink.addClassNames(
                LumoUtility.Display.BLOCK,
                LumoUtility.Padding.MEDIUM
        );

        VerticalLayout navigation = new VerticalLayout(
                jobsLink
        );
        navigation.setSizeFull();

        addToDrawer(navigation);
    }
}
