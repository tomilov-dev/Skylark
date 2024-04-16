import os
import sys
import json
import pandas as pd
from pathlib import Path
from typing import Any, List, Dict, Union

from PyQt6.QtWidgets import (
    QTreeView,
    QDialog,
    QFormLayout,
    QWidget,
    QVBoxLayout,
    QLineEdit,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QFileDialog,
    QTableView,
    QComboBox,
    QStatusBar,
    QProgressBar,
)

from PyQt6.QtCore import (
    QAbstractItemModel,
    QModelIndex,
    QObject,
    Qt,
    QAbstractTableModel,
    pyqtSignal,
)

GUI_DIR = Path(__file__).parent.parent
PROJECT_DIR = GUI_DIR.parent

sys.path.append(str(GUI_DIR))

from config.measures_config.config_parser import CONFIG, MEASURE, DATA, UNIT


class RunButtonStatus(object):
    RUNNIG = "Остановить"
    STOPPED = "Начать обработку"
    STOPPING = "Останавливаю обработку"


class TreeItem(object):
    def __init__(self, parent: "TreeItem" = None):
        self._parent = parent
        self._key = ""
        self._value = ""
        self._value_type = None
        self._children = []

    def appendChild(self, item: "TreeItem"):
        self._children.append(item)

    def child(self, row: int) -> "TreeItem":
        return self._children[row]

    def parent(self) -> "TreeItem":
        return self._parent

    def childCount(self) -> int:
        return len(self._children)

    def row(self) -> int:
        return self._parent._children.index(self) if self._parent else 0

    @property
    def key(self) -> str:
        return self._key

    @key.setter
    def key(self, key: str):
        self._key = key

    @property
    def value(self) -> str:
        return self._value

    @value.setter
    def value(self, value: str):
        self._value = value

    @property
    def value_type(self):
        return self._value_type

    @value_type.setter
    def value_type(self, value):
        self._value_type = value

    def search_keyname(self, current_name, value: dict) -> str:
        key_name = ""
        if MEASURE.NAME in value:
            key_name = value[MEASURE.NAME]
        elif UNIT.NAME in value:
            key_name = value[UNIT.NAME]
        else:
            key_name = ""

        return key_name if key_name else current_name

    def load(
        self,
        value: Union[List, Dict],
        parent: "TreeItem" = None,
    ) -> "TreeItem":
        rootItem = TreeItem(parent)
        rootItem.key = "root"

        if isinstance(value, dict):
            "Using for nested dicts"

            items = value.items()

            for key, value in items:
                child = self.load(value, rootItem)
                child.key = key
                child.value_type = type(value)
                rootItem.appendChild(child)

        elif isinstance(value, list):
            "Using for nested lists"

            for index, value in enumerate(value):
                key_name = self.search_keyname(index, value)

                child = self.load(value, rootItem)
                child.key = key_name
                child.value_type = type(value)
                rootItem.appendChild(child)

        else:
            "Using for simple records"

            rootItem.value = value
            rootItem.value_type = type(value)

        return rootItem


class JsonModel(QAbstractItemModel):
    def __init__(self, parent: QObject = None):
        super().__init__(parent)

        self._rootItem = TreeItem()
        self._headers = ("key", "value")

    def clear(self):
        self.load({})

    def load(self, document: dict):
        assert isinstance(document, (dict, list, tuple)), (
            "`document` must be of dict, list or tuple, " f"not {type(document)}"
        )

        self.beginResetModel()

        self._rootItem = self._rootItem.load(document)
        self._rootItem.value_type = type(document)

        self.endResetModel()

        return True

    def data(self, index: QModelIndex, role: Qt.ItemDataRole) -> Any:
        if not index.isValid():
            return None

        item = index.internalPointer()

        if role == Qt.ItemDataRole.DisplayRole:
            if index.column() == 0:
                return item.key

            if index.column() == 1:
                return item.value

        elif role == Qt.ItemDataRole.EditRole:
            if index.column() == 1:
                return item.value

    def setData(self, index: QModelIndex, value: Any, role: Qt.ItemDataRole):
        if role == Qt.ItemDataRole.EditRole:
            if index.column() == 1:
                item = index.internalPointer()
                item.value = str(value)

                self.dataChanged.emit(index, index, [Qt.ItemDataRole.EditRole])

                return True

        return False

    def headerData(
        self, section: int, orientation: Qt.Orientation, role: Qt.ItemDataRole
    ):
        if role != Qt.ItemDataRole.DisplayRole:
            return None

        if orientation == Qt.Orientation.Horizontal:
            return self._headers[section]

    def index(self, row: int, column: int, parent=QModelIndex()) -> QModelIndex:
        if not self.hasIndex(row, column, parent):
            return QModelIndex()

        if not parent.isValid():
            parentItem = self._rootItem
        else:
            parentItem = parent.internalPointer()

        childItem = parentItem.child(row)
        if childItem:
            return self.createIndex(row, column, childItem)
        else:
            return QModelIndex()

    def parent(self, index: QModelIndex) -> QModelIndex:
        if not index.isValid():
            return QModelIndex()

        childItem = index.internalPointer()
        parentItem = childItem.parent()

        if parentItem == self._rootItem:
            return QModelIndex()

        return self.createIndex(parentItem.row(), 0, parentItem)

    def rowCount(self, parent=QModelIndex()):
        if parent.column() > 0:
            return 0

        if not parent.isValid():
            parentItem = self._rootItem
        else:
            parentItem = parent.internalPointer()

        return parentItem.childCount()

    def columnCount(self, parent=QModelIndex()):
        return 2

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:
        flags = super(JsonModel, self).flags(index)

        if index.column() == 1:
            return Qt.ItemFlag.ItemIsEditable | flags
        else:
            return flags

    def to_json(self, item=None):
        if item is None:
            item = self._rootItem

        nchild = item.childCount()

        if item.value_type is dict:
            document = {}
            for i in range(nchild):
                ch = item.child(i)
                document[ch.key] = self.to_json(ch)
            return document

        elif item.value_type == list:
            document = []
            for i in range(nchild):
                ch = item.child(i)
                document.append(self.to_json(ch))
            return document

        else:
            return item.value


class PandasModel(QAbstractTableModel):
    def __init__(self, dataframe: pd.DataFrame = pd.DataFrame(), parent=None):
        QAbstractTableModel.__init__(self, parent)
        self._dataframe = dataframe

    def rowCount(self, parent=QModelIndex()) -> int:
        if parent == QModelIndex():
            return len(self._dataframe)
        return 0

    def columnCount(self, parent=QModelIndex()) -> int:
        if parent == QModelIndex():
            return len(self._dataframe.columns)
        return 0

    def data(self, index: QModelIndex, role=Qt.ItemDataRole):
        if not index.isValid():
            return None

        if role == Qt.ItemDataRole.DisplayRole:
            return str(self._dataframe.iloc[index.row(), index.column()])

        return None

    def headerData(
        self, section: int, orientation: Qt.Orientation, role: Qt.ItemDataRole
    ):
        if role == Qt.ItemDataRole.DisplayRole:
            if orientation == Qt.Orientation.Horizontal:
                return str(self._dataframe.columns[section])

            if orientation == Qt.Orientation.Vertical:
                return str(self._dataframe.index[section])

        return None


class ConfigViewerDialog(QDialog):
    def __init__(
        self,
        config_path: str | Path,
        config_name: str,
        reset_configs: callable,
    ):
        super().__init__()

        self.config_path = config_path
        self.reset_configs = reset_configs

        self.json_model = JsonModel()
        self.json_model.load(self.load_data(config_name))

        config_layout = QFormLayout(self)

        tree_view = QTreeView(self)
        tree_view.setModel(self.json_model)

        button = QPushButton("Сохранить")
        button.clicked.connect(self.save_button)

        config_layout.addWidget(tree_view)
        config_layout.addWidget(button)

        self.setLayout(config_layout)
        self.setWindowTitle("Config Viewer")

    def load_data(self, config_name) -> dict:
        path = self.config_path / config_name
        with open(path, "rb") as file:
            json_data = json.load(file)
        return json_data

    def save_data(self, config_name: str, data: dict) -> None:
        if ".json" not in config_name:
            config_name += ".json"

        path = self.config_path / config_name
        with open(path, "w") as file:
            file.write(json.dumps(data, ensure_ascii=False))

        self.reset_configs()

    def save_button(self):
        data: dict = self.json_model.to_json()
        config_name = data["config_name"]
        self.save_data(config_name, data)

        self.close()


class CommonGUI(QWidget):
    progress_signal = pyqtSignal(int)
    CONFIG_PATH = ""

    def __init__(self) -> None:
        super().__init__()

    def _setup_progress_bar(self, main_layout: QVBoxLayout) -> QProgressBar:
        progress_layout = QHBoxLayout()

        label = QLabel("Прогресс: ")
        self.progress_bar = QProgressBar()

        progress_layout.addWidget(label)
        progress_layout.addWidget(self.progress_bar)
        main_layout.addLayout(progress_layout)

        self.progress_signal.connect(self.progress_bar.setValue)
        return self.progress_bar

    def progress_callback(self, progress: int) -> None:
        self.progress_signal.emit(progress)

    def run_button_status(self, status: RunButtonStatus) -> None:
        self.run_button.setText(status)

    def run_button_handler(self):
        if self.run_button.text() == RunButtonStatus.STOPPED:
            self.run()
        elif self.run_button.text() == RunButtonStatus.RUNNIG:
            self.stop()
        elif self.run_button.text() == RunButtonStatus.STOPPING:
            pass

    def _setup_run_button(self, main_layout: QVBoxLayout) -> None:
        runner_layout = QHBoxLayout()
        self.run_button = QPushButton(RunButtonStatus.STOPPED)
        self.run_button.clicked.connect(self.run_button_handler)
        runner_layout.addWidget(self.run_button)

        main_layout.addLayout(runner_layout)
        return self.run_button

    def _setup_status_bar(self, main_layout: QVBoxLayout) -> QStatusBar:
        status_layout = QHBoxLayout()

        label = QLabel("Статус: ")
        self.status_bar = QStatusBar()

        status_layout.addWidget(label)
        status_layout.addWidget(self.status_bar)
        main_layout.addLayout(status_layout)

        return self.status_bar

    def status_callback(self, message: str) -> None:
        self.status_bar.showMessage(message)

    def _setup_workfile_layout(self, main_layout: QVBoxLayout) -> QLineEdit:
        file_layout = QHBoxLayout()

        file_path_label = QLabel("Файл:")
        file_path_display = QLineEdit(self)

        browse_button = QPushButton("Обзор", self)
        browse_button.clicked.connect(self.browse_file)

        file_layout.addWidget(file_path_label)
        file_layout.addWidget(file_path_display)
        file_layout.addWidget(browse_button)

        main_layout.addLayout(file_layout)

        self.file_path_display = file_path_display
        return self.file_path_display

    def _setup_table_view(self, main_layout: QVBoxLayout) -> QTableView:
        table_layout = QHBoxLayout()
        table_view = QTableView()
        model = PandasModel()

        table_view.setModel(model)
        table_layout.addWidget(table_view)

        main_layout.addLayout(table_layout)

        self.table_view = table_view
        return self.table_view

    def _setup_config_layout(self, main_layout: QVBoxLayout) -> QComboBox:
        config_layout = QHBoxLayout()

        config_label = QLabel("Конфигурация:")
        config_combobox = QComboBox(self)

        change_config_button = QPushButton("Редактировать")
        change_config_button.clicked.connect(self.change_config)

        config_layout.addWidget(config_label)
        config_layout.addWidget(config_combobox)
        config_layout.addWidget(change_config_button)

        main_layout.addLayout(config_layout)
        self.config_combobox = config_combobox

        self.update_config_combobox()
        return self.config_combobox

    def browse_file(self) -> None:
        file_dialog = QFileDialog()
        file_path, _ = file_dialog.getOpenFileName(
            self,
            "Выберите файл",
            "",
            "Excel (*.xlsx *.csv)",
        )

        if file_path:
            self.file_path_display.setText(file_path)
            self.upload_file_data(file_path)

    def upload_file_data(self, file_path: str) -> None:
        if ".xlsx" in file_path:
            data = pd.read_excel(file_path, nrows=5)
        elif ".csv" in file_path:
            data = pd.read_excel(file_path, nrows=5)
        else:
            raise ValueError("File should be Excel or csv")

        self.table_view.setModel(PandasModel(data))

    def update_config_combobox(self):
        if self.CONFIG_PATH:
            config_files = [
                file for file in os.listdir(self.CONFIG_PATH) if file.endswith(".json")
            ]

            self.config_combobox.clear()
            self.config_combobox.addItems(config_files)

    def change_config(self):
        selected_config = self.config_combobox.currentText()

        config_viewer_dialog = ConfigViewerDialog(
            self.CONFIG_PATH,
            selected_config,
            self.update_config_combobox,
        )
        config_viewer_dialog.exec()

    def read_config(self, path: str | Path) -> dict:
        with open(path, "rb") as file:
            data = json.loads(file.read())
        return data
