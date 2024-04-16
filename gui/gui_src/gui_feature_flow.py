import sys
import multiprocessing
import pandas as pd
from pathlib import Path
from typing import Callable
from PyQt6.QtWidgets import (
    QVBoxLayout,
    QApplication,
    QHBoxLayout,
    QLabel,
    QLineEdit,
)
from PyQt6.QtCore import QThread

GUI_DIR = Path(__file__).parent.parent
GUI_SRC = GUI_DIR / "gui_src"
PROJECT_DIR = GUI_DIR.parent
CONFIG_PATH = PROJECT_DIR / "config" / "measures_config" / "setups"

sys.path.append(str(PROJECT_DIR))

from gui_common import CommonGUI, RunButtonStatus
from src.feature_flow.main import (
    FeatureGenerator,
    FeatureFlow,
    FeatureFlowGracefullExit,
)

OUTPUT_FILENAME = "FeatureFlow_output.xlsx"
FEATURE_FLOW_CLIENT_COL = "Название товара"
FEATURE_FLOW_SOURCE_COL = "Сырые данные"


class FeatureFlowGUIGracefullExit(Exception):
    pass


class FeatureFlowProcessRunner(QThread):
    def __init__(
        self,
        config: dict,
        data_path: str | Path,
        client_column: str,
        source_column: str,
        process_pool: multiprocessing.Pool = None,
        status_callback: Callable = None,
        progress_callback: Callable = None,
        run_button_callback: Callable = None,
    ) -> None:
        super().__init__()

        self.data_path = data_path

        self.feature_generator = FeatureGenerator()
        features = self.feature_generator.generate(config)

        self._process_pool = process_pool

        self.status_callback = status_callback
        self.progress_callback = progress_callback
        self.run_button_callback = run_button_callback

        self.validator = FeatureFlow(
            client_column,
            source_column,
            features,
            status_callback=self.status_callback,
            progress_callback=self.progress_callback,
        )

    def upload_data(self):
        if ".csv" in self.data_path:
            data = pd.read_csv(self.data_path)
        elif ".xlsx" in self.data_path:
            data = pd.read_excel(self.data_path)
        else:
            raise ValueError("File should be Excel or csv")
        return data

    def stop_callback(self) -> None:
        self.run_button_callback(RunButtonStatus.STOPPING)
        self.validator.stop_callback()

    def call_status(self, message: str) -> None:
        if self.status_callback is not None:
            self.status_callback(message)

    def call_progress(self, progress: int) -> None:
        if self.progress_callback is not None:
            self.progress_callback(progress)

    def run_validator(
        self,
        data: pd.DataFrame,
        process_pool,
    ) -> pd.DataFrame:
        try:
            self.call_status("Запускаю валидацию по величинам")
            data = self.validator.validate(data, process_pool)
            return data

        except FeatureFlowGracefullExit:
            raise FeatureFlowGUIGracefullExit

    def run(self) -> None:
        try:
            self.call_status("Загружаю данные")
            data = self.upload_data()

            data = self.run_validator(data, self._process_pool)

            self.call_status("Сохраняю результат")
            data.to_excel(PROJECT_DIR / OUTPUT_FILENAME, index=False)

            self.call_status("Сохранено")
            self.call_progress(0)
            if self.run_button_callback is not None:
                self.run_button_callback(RunButtonStatus.STOPPED)

        except FeatureFlowGUIGracefullExit:
            self.call_status("Остановлено")
            self.call_progress(0)

            if self.run_button_callback is not None:
                self.run_button_callback(RunButtonStatus.STOPPED)


class FeatureFlowWidget(CommonGUI):
    CONFIG_PATH = CONFIG_PATH

    def __init__(self, process_pool=None):
        super().__init__()
        self._process_pool = process_pool

        main_layout = QVBoxLayout(self)

        self.workfile_lay = self._setup_workfile_layout(main_layout)
        self.config_lay = self._setup_config_layout(main_layout)
        self._setup_runner(main_layout)
        self.run_button = self._setup_run_button(main_layout)

        self.status_bar = self._setup_status_bar(main_layout)
        self.status_callback("Ожидаю запуска")
        self.progress_bar = self._setup_progress_bar(main_layout)

        self.table_lay = self._setup_table_view(main_layout)

    def _setup_runner(self, main_layout: QVBoxLayout) -> None:
        runner_layout = QVBoxLayout()

        client_box = QHBoxLayout()
        client_col_label = QLabel("Столбец названий клиента")
        self.client_col_display = QLineEdit(FEATURE_FLOW_CLIENT_COL)
        client_box.addWidget(client_col_label)
        client_box.addWidget(self.client_col_display)

        source_box = QHBoxLayout()
        source_col_label = QLabel("Столбец названий источника")
        self.source_col_display = QLineEdit(FEATURE_FLOW_SOURCE_COL)
        source_box.addWidget(source_col_label)
        source_box.addWidget(self.source_col_display)

        runner_layout.addLayout(client_box)
        runner_layout.addLayout(source_box)

        main_layout.addLayout(runner_layout)

    def run(self):
        self.run_button_status(RunButtonStatus.RUNNIG)

        config_path = self.CONFIG_PATH / self.config_combobox.currentText()
        config = self.read_config(config_path)

        self.validator = FeatureFlowProcessRunner(
            config,
            self.file_path_display.text(),
            self.client_col_display.text(),
            self.source_col_display.text(),
            self._process_pool,
            self.status_callback,
            self.progress_callback,
            self.run_button_status,
        )

        self.validator_stop: callable = self.validator.stop_callback
        self.validator.start()

    def stop(self):
        if self.validator:
            self.validator_stop()


if __name__ == "__main__":
    with multiprocessing.Pool(4) as process_pool:
        app = QApplication(sys.argv)
        window = FeatureFlowWidget(process_pool)
        window.show()

        sys.exit(app.exec())
