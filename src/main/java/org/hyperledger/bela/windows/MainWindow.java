package org.hyperledger.bela.windows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.googlecode.lanterna.gui2.BasicWindow;
import com.googlecode.lanterna.gui2.Window;
import com.googlecode.lanterna.gui2.WindowBasedTextGUI;
import com.googlecode.lanterna.gui2.menu.Menu;
import com.googlecode.lanterna.gui2.menu.MenuBar;
import com.googlecode.lanterna.gui2.menu.MenuItem;
import kr.pe.kwonnam.slf4jlambda.LambdaLogger;
import org.hyperledger.bela.dialogs.BelaExceptionDialog;

import static kr.pe.kwonnam.slf4jlambda.LambdaLoggerFactory.getLogger;

public class MainWindow implements LanternaWindow {
    private static final LambdaLogger log = getLogger(MainWindow.class);

    private List<LanternaWindow> windows = new ArrayList<>();
    private WindowBasedTextGUI gui;

    public MainWindow(final WindowBasedTextGUI gui) {

        this.gui = gui;
    }

    public void registerWindow(LanternaWindow window) {
        this.windows.add(window);
    }

    @Override
    public String label() {
        return "Main Window";
    }

    @Override
    public MenuGroup group() {
        return MenuGroup.FILE;
    }

    @Override
    public Window createWindow() {
        final Window window = new BasicWindow(label());
        final MenuBar bar = new MenuBar();

        Map<MenuGroup, Menu> groups = new HashMap<>();

        for (MenuGroup menuGroup : MenuGroup.values()) {
            groups.put(menuGroup, new Menu(menuGroup.name()));
            bar.add(groups.get(menuGroup));
        }
        for (LanternaWindow lanternaWindow : windows) {
            groups.get(lanternaWindow.group())
                    .add(new MenuItem(lanternaWindow.label(), () -> launchWindow(lanternaWindow)));
        }

        groups.get(MenuGroup.FILE).add(new MenuItem("Exit", window::close));

        window.setMenuBar(bar);
        return window;
    }

    private void launchWindow(final LanternaWindow window) {
        try {
            gui.addWindowAndWait(window.createWindow());
        } catch (Exception e) {
            log.error("There was an error when launching window {}", window.label(), e);
            BelaExceptionDialog.showException(gui, e);

        }
    }
}
