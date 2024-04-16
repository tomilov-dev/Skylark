import sys
from pathlib import Path
from PyQt6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QMainWindow,
    QTabWidget,
)

GUI_DIR = Path(__file__).parent
GUI_SRC = GUI_DIR / "gui_src"
PROJECT_DIR = GUI_DIR.parent
sys.path.append(str(PROJECT_DIR))
sys.path.append(str(GUI_SRC))

from gui.gui_src.gui_semantix import SemantixWidget
from gui.gui_src.gui_feature_flow import FeatureFlowWidget
from gui.gui_src.gui_simfyzer import SimFyzerWidget


class MainWindow(QMainWindow):
    def __init__(self, process_pool=None) -> None:
        super().__init__()

        self._process_pool = process_pool

        main_window = QWidget(self)
        self.setWindowTitle("Skylark")
        self.setCentralWidget(main_window)

        tab_widget = self._set_tables(main_window)

        main_layout = QVBoxLayout(main_window)
        main_layout.addWidget(tab_widget)

        self._set_size_position()

    def _set_tables(self, main_window: QWidget):
        tab_widget = QTabWidget(main_window)

        autosem_tab = SemantixWidget()
        feature_validator_tab = FeatureFlowWidget(self._process_pool)
        jakkar_validator_tab = SimFyzerWidget(self._process_pool)

        tab_widget.addTab(autosem_tab, "Semantix")
        tab_widget.addTab(feature_validator_tab, "FeatureFlow")
        tab_widget.addTab(jakkar_validator_tab, "SimFyzer")

        return tab_widget

    def _set_size_position(self):
        screen_size = self.screen().availableSize()

        width = int(screen_size.width() * 0.5)
        height = int(screen_size.height() * 0.5)
        self.resize(width, height)

        center_x = (screen_size.width() - self.width()) // 2
        center_y = (screen_size.height() - self.height()) // 2

        self.move(center_x, center_y)
