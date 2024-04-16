import sys
import multiprocessing

from PyQt6.QtWidgets import QApplication
from gui.gui import MainWindow

if __name__ == "__main__":
    with multiprocessing.Pool() as process_pool:
        app = QApplication(sys.argv)
        window = MainWindow(process_pool)
        window.show()
        sys.exit(app.exec())
