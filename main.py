import pandas as pd
import sqlite3
from os import path

import sys
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QPushButton,
    QMessageBox, QTabWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QInputDialog, QFileDialog, QTableView,
)
from PyQt6.QtCore import Qt, QAbstractTableModel, QSortFilterProxyModel


def split_list(input_list, chunk_size):
    out_list = []
    for i in range(0, len(input_list), chunk_size):
        out_list.append(input_list[i:i + chunk_size])

    return out_list


class DBInterface:
    def __init__(self, db_filename):
        self.db_con = None
        self.db_cursor = None
        self.db_filename = db_filename
        if not path.isfile(self.db_filename):
            self.create_db()

    def using_db_connection(func):
        def handle_connection(self, *args, **kwargs):
            try:
                self.open_connection()
                return func(self, *args, **kwargs)
            finally:
                self.close_connection()

        return handle_connection

    def open_connection(self):
        if not self.db_con:
            self.db_con = sqlite3.connect(self.db_filename)
        if not self.db_cursor:
            self.db_cursor = self.db_con.cursor()

    def close_connection(self):
        if self.db_con:
            self.db_con.close()
            self.db_con = None
            self.db_cursor = None

    @using_db_connection
    def create_db(self):
        self.db_cursor.execute("CREATE TABLE items_cost(item_code TEXT PRIMARY KEY NOT NULL UNIQUE, cost FLOAT)")
        self.db_cursor.execute('CREATE UNIQUE INDEX "item_code" ON "items_cost"("item_code" ASC)')
        self.db_cursor.execute("CREATE TABLE settings(param, value)")
        self.db_cursor.execute('CREATE UNIQUE INDEX "param" ON "settings"("param" ASC)')
        self.db_cursor.execute("INSERT INTO settings (param, value) VALUES ('usd_exchange_rate', 0)")
        self.db_con.commit()

    @using_db_connection
    def import_costs_to_db(self, list_of_items):
        for item in list_of_items:
            self.db_cursor.execute("""INSERT INTO items_cost(item_code, cost) VALUES (:item_code, :cost)
                                            ON CONFLICT (item_code) DO UPDATE SET cost = :cost""",
                                   {'item_code': item[0],
                                    'cost': item[1]})
        self.db_con.commit()

    @using_db_connection
    def get_costs_from_db(self):
        res = self.db_cursor.execute("SELECT item_code, cost FROM items_cost ORDER BY item_code ASC")
        items_costs_list = res.fetchall()
        return items_costs_list

    def export_costs_to_excel(self, excel_filepath):
        data_list = self.get_costs_from_db()
        df = pd.DataFrame(data_list, columns=["Артикул", "Цена"])
        df.to_excel(excel_filepath, index=False)

    @using_db_connection
    def fill_cost_from_db(self, loft_items_list):
        item_codes_list = loft_items_list.get_item_codes()
        for chunk in split_list(item_codes_list, 100):
            res = self.db_cursor.execute('SELECT item_code, cost FROM items_cost WHERE item_code IN (%s)' %
                                         ', '.join('?' * len(chunk)),
                                         chunk)
            for item in res.fetchall():
                loft_items_list.update_item_cost(item[0], item[1])

    @using_db_connection
    def get_usd_exchange_rate(self):
        res = self.db_cursor.execute("SELECT value FROM settings WHERE param = 'usd_exchange_rate'")
        return res.fetchone()[0]

    @using_db_connection
    def set_usd_exchange_rate(self, usd_exchange_rate):
        self.db_cursor.execute("UPDATE settings SET value = :usd_exchange_rate WHERE param = 'usd_exchange_rate'",
                               {'usd_exchange_rate': usd_exchange_rate})
        self.db_con.commit()


class LoftItem:
    usd_rub_rate = 0
    col_names = ['item_code', 'sold_qty', 'sold_price', 'sold_sum',
                 'cost_rub', 'cost_usd', 'margin_rub', 'margin_pct']
    ru_col_names = ['Артикул', 'Количество продано', 'Средняя цена продажи', 'Сумма продано',
                    'Себестоимость РУБ', 'Себестоимость USD', 'Маржинальность РУБ/шт', 'Маржинальность %' ]

    def __init__(self, item_code, sold_qty, sold_sum):
        self.cost_usd = 0
        self.cost_rub = 0
        self.item_code = item_code
        self.sold_qty = sold_qty
        if sold_qty:
            self.sold_price = sold_sum / sold_qty
        else:
            self.sold_price = 0
        self.sold_sum = sold_sum
        self.margin_rub = 0
        self.margin_pct = 0
        self.image = None

    def __repr__(self):
        return str(self.__dict__)

    def set_cost_usd(self, cost_usd):
        if cost_usd > 0:
            self.cost_usd = cost_usd
        else:
            print('Cost can not be zero')
            return ValueError
        if self.usd_rub_rate > 0:
            self.calculate_cost_rub()

    def calculate_cost_rub(self):
        if (self.usd_rub_rate > 0) and (self.cost_usd > 0):
            self.cost_rub = self.cost_usd * self.usd_rub_rate
            if self.sold_qty != 0:
                self.margin_rub = self.sold_price - self.cost_rub
                self.margin_pct = self.margin_rub * 100 / self.cost_rub
        else:
            print(f"conversion rate ({self.usd_rub_rate}) and usd cost ({self.cost_usd}) must be set > 0")
            return ValueError

    def get_properties_dict(self):
        return {
            'item_code': self.item_code,
            'sold_qty': self.sold_qty,
            'sold_price': self.sold_price,
            'sold_sum': self.sold_sum,
            'cost_rub': self.cost_rub,
            'cost_usd': self.cost_usd,
            'margin_rub': self.margin_rub,
            'margin_pct': self.margin_pct
        }

    def get_properties_list(self):
        return [
            self.item_code,
            self.sold_qty,
            self.sold_price,
            self.sold_sum,
            self.cost_rub,
            self.cost_usd,
            self.margin_rub,
            self.margin_pct
        ]


class LoftItemTableModel(QAbstractTableModel):
    def __init__(self, *args, data=None, **kwargs):
        super(LoftItemTableModel, self).__init__()
        self._data = data or []
        self.col_names = LoftItem.col_names
        self.ru_col_names = LoftItem.ru_col_names

    def __repr__(self):
        out_str = ""
        for item in self._data:
            out_str += str(item) + '\n'
        return out_str

    def __len__(self):
        return len(self._data)

    def data(self, index, role):
        if role == Qt.ItemDataRole.DisplayRole:
            # See below for the nested-list data structure.
            # .row() indexes into the outer list,
            # .column() indexes into the sub-list
            value = self._data[index.row()].get_properties_dict()[self.col_names[index.column()]]

            # if isinstance(value, datetime):
            # Render time to YYY-MM-DD.
            #    return value.strftime("%Y-%m-%d")

            # if isinstance(value, int):
            #    return str(value)

            # if isinstance(value, float):
            # Render float to 2 dp
            #    return "%.2f" % value

            # if isinstance(value, str):
            # Render strings with quotes
            #    return '"%s"' % value

            # Default (anything not captured above: e.g. int)
            return value
    def rowCount(self, index):
        # The length of the outer list.
        return len(self._data)

    def columnCount(self, index):
        # The following takes the first sub-list, and returns
        # the length (only works if all rows are an equal length)
        return len(self.col_names)

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role == Qt.ItemDataRole.DisplayRole and orientation == Qt.Orientation.Horizontal:
            return self.ru_col_names[section]
        return QAbstractTableModel.headerData(self, section, orientation, role)

    def add_item(self, loft_item):
        if not isinstance(loft_item, LoftItem):
            return TypeError
        self._data.append(loft_item)

    def recalculate_cost_rub(self):
        for item in self._data:
            item.calculate_cost_rub()

    def get_item_codes(self):
        return list(item.item_code for item in self._data)

    def get_items_list(self):
        out = [row.get_properties_list() for row in self._data]
        return out

    def update_item_cost(self, item_code, new_cost):
        for item in self._data:
            if item.item_code == item_code:
                item.cost_usd = new_cost
                item.calculate_cost_rub()
                return True
        return None

    def read_sales_from_excel(self, excel_filepath):
        df = pd.read_excel(excel_filepath)

        start_trigger_value = "Номенклатура"
        countdown_trigger = False

        for item in df.iloc:
            if item[0] == start_trigger_value:
                countdown_trigger = True
                continue
            if countdown_trigger and (not pd.isna(item[0])):
                df.drop(range(0, item.name), axis=0, inplace=True)
                break

        for item in df.tail(10).iloc:
            if item[0] == 'Итого':
                df.drop(item.name, axis=0, inplace=True)

        df.dropna(axis=0, inplace=True, how="all")
        df.dropna(axis=1, inplace=True, how="all")
        df.reset_index(drop=True, inplace=True)
        df.fillna(0, inplace=True)
        df.sort_values(df.columns[0], axis=0, inplace=True)

        output_fields_arr = [1, 2, 3]

        for item in df.iloc:
            art = item[output_fields_arr[0]]
            sold = int(item[output_fields_arr[1]])
            sum_rub = float(item[output_fields_arr[2]])

            self.add_item(LoftItem(art, sold, sum_rub))

    def save_sales_to_excel(self, excel_filepath):
        df = pd.DataFrame(self.get_items_list(), columns=self.ru_col_names)
        try:
            df.to_excel(excel_filepath, index=False)
            return 0
        except:
            return 'Ошибка при сохранении файла'


class LoftCostsTableModel(QAbstractTableModel):
    def __init__(self, *args, data=None, **kwargs):
        super(LoftCostsTableModel, self).__init__()
        self._data = data or {}
        self.col_names = ['item_code', 'cost_usd']
        self.ru_col_names = ['Артикул', 'Себестоимость USD']

    def data(self, index, role):
        if role == Qt.ItemDataRole.DisplayRole:
            # See below for the nested-list data structure.
            # .row() indexes into the outer list,
            # .column() indexes into the sub-list
            if index.column() == 0:
                value = list(self._data.keys())[index.row()]
            else:
                value = list(self._data.values())[index.row()]
            return value

    def rowCount(self, index):
        # The length of the outer list.
        return len(self._data)

    def columnCount(self, index):
        # The following takes the first sub-list, and returns
        # the length (only works if all rows are an equal length)
        return 2

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role == Qt.ItemDataRole.DisplayRole and orientation == Qt.Orientation.Horizontal:
            return self.ru_col_names[section]
        return QAbstractTableModel.headerData(self, section, orientation, role)

    def set_data_from_list(self, data):
        if data:
            for item in data:
                self._data[item[0]] = item[1]
        else:
            data = {}

    def update_item(self, item_code, cost_usd):
        self._data[item_code] = cost_usd

    def get_items_list(self):
        out = [[key, value] for key, value in self._data.items()]
        return out

    def save_costs_to_excel(self, excel_filepath):
        df = pd.DataFrame(self.get_items_list(), columns=self.ru_col_names)
        try:
            df.to_excel(excel_filepath, index=False)
            return 0
        except:
            return 'Ошибка при сохранении файла'


class MainWindow(QMainWindow):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.sales_page_table = None
        self.sales_page_table_model = None
        self.sales_page_table_proxy_model = None
        self.label_current_exchange_rate = None

        self.costs_page_table = None
        self.costs_page_model = None

        self.db = DBInterface('test_db.sqlite')
        self.file_filter = 'Excel (*.xlsx)'
        self.exchange_rate = 0
        self.setWindowTitle('Calc')
        self.setGeometry(100, 100, 1100, 600)

        tab = QTabWidget(self)

        # Define sales page, inside will be open button, exchange rate and read result table
        tab.addTab(self.assemble_sales_page(), 'Продажи')
        tab.addTab(self.assemble_costs_page(), 'Себестоимость')

        self.setCentralWidget(tab)
        self.status_bar = self.statusBar()
        self.show()

    def assemble_sales_page(self):
        # Define sales page, inside will be open button, exchange rate and read result table
        sales_page = QWidget(self)
        page_layout = QVBoxLayout()
        sales_page.setLayout(page_layout)

        # open button, exchange rate button and current exchange rate label
        sales_page_buttons = QWidget(self)
        btn_layout = QHBoxLayout()
        sales_page_buttons.setLayout(btn_layout)

        # create buttons and label
        btn_load_sales_from_file = QPushButton('Загрузить продажи', clicked=self.import_sales_from_file)
        btn_load_sales_from_file.setFixedWidth(140)
        btn_save_sales_to_file = QPushButton('Сохранить в файл', clicked=self.export_sales_to_file)
        btn_save_sales_to_file.setFixedWidth(140)
        btn_fill_costs = QPushButton('Заполнить себестоимость', clicked=self.sales_fill_costs)
        btn_fill_costs.setFixedWidth(170)
        btn_set_exchange_rate = QPushButton('Установить курс', clicked=self.set_exchange_rate)
        btn_set_exchange_rate.setFixedWidth(110)
        self.label_current_exchange_rate = QLabel('Текущий курс: 0')
        self.get_exchange_rate_from_db()

        # laying out buttons
        btn_layout.addWidget(btn_load_sales_from_file)
        btn_layout.addWidget(btn_save_sales_to_file)
        btn_layout.addWidget(btn_fill_costs)
        btn_layout.addStretch()
        btn_layout.addWidget(self.label_current_exchange_rate)
        btn_layout.addWidget(btn_set_exchange_rate)

        # creating table view, model and proxy model
        self.sales_page_table = QTableView()
        self.sales_page_table_model = LoftItemTableModel(self)
        sales_page_table_proxy_model = QSortFilterProxyModel(self)

        # connecting models and view together
        sales_page_table_proxy_model.setSourceModel(self.sales_page_table_model)
        self.sales_page_table.setModel(sales_page_table_proxy_model)

        # enable sort, default is item_code asc
        self.sales_page_table.setSortingEnabled(True)
        self.sales_page_table.sortByColumn(0, Qt.SortOrder.AscendingOrder)

        self.resize_table(self.sales_page_table)

        # Packing buttons and table into page and page to tab widget
        page_layout.addWidget(sales_page_buttons)
        page_layout.addWidget(self.sales_page_table)
        return sales_page

    def assemble_costs_page(self):
        costs_page = QWidget(self)
        costs_page_layout = QVBoxLayout()
        costs_page.setLayout(costs_page_layout)

        costs_page_buttons = QWidget(self)
        btn_layout = QHBoxLayout()
        costs_page_buttons.setLayout(btn_layout)

        btn_costs_export = QPushButton('Экспорт', clicked=self.export_costs_to_file)
        btn_costs_import = QPushButton('Импорт', clicked=self.import_costs_from_file)
        btn_layout.addWidget(btn_costs_export)
        btn_layout.addWidget(btn_costs_import)
        btn_layout.addStretch()

        self.costs_page_table = QTableView()
        self.costs_page_model = LoftCostsTableModel(self)
        costs_page_proxy_model = QSortFilterProxyModel(self)

        costs_page_proxy_model.setSourceModel(self.costs_page_model)
        self.costs_page_table.setModel(costs_page_proxy_model)

        self.costs_page_table.setSortingEnabled(True)
        self.costs_page_table.sortByColumn(0, Qt.SortOrder.AscendingOrder)

        self.resize_table(self.costs_page_table)

        costs_page_layout.addWidget(costs_page_buttons)
        costs_page_layout.addWidget(self.costs_page_table)
        self.load_costs_from_db()
        return costs_page

    def import_sales_from_file(self):
        filename, ok = QFileDialog.getOpenFileName(self,
                                                   "Выберите файл с продажами из 1С",
                                                   './',
                                                   self.file_filter)
        if filename and ok:
            self.sales_page_table_model.read_sales_from_excel(filename)
            self.sales_page_table_model.layoutChanged.emit()
            self.resize_table(self.sales_page_table)

    def export_sales_to_file(self):
        filename, ok = QFileDialog.getSaveFileName(self,
                                                   "Укажите место сохранения файла",
                                                   './',
                                                   self.file_filter)
        save_result = self.sales_page_table_model.save_sales_to_excel(filename)
        if save_result != 0:
            QMessageBox.critical(self, 'Ошибка', save_result)
        else:
            self.status_bar.showMessage('Сохранено успешно', 5000)

    def import_costs_from_file(self):
        filename, ok = QFileDialog.getOpenFileName(self,
                                                   "Выберите файл с себестоимостью",
                                                   './',
                                                   self.file_filter)
        if filename and ok:
            try:
                df = pd.read_excel(filename, usecols=(0, 1))
                df.iloc[:, 1] = df.iloc[:, 1].astype(float)
                self.db.import_costs_to_db(df.values.tolist())
                self.status_bar.showMessage('Import completed', 5000)
            except ValueError:
                # 'Cost column must contain only integer and float values!'
                QMessageBox.critical(self, 'Ошибка', 'В столбце себестоимости должны быть только числа!')
            except FileNotFoundError:
                QMessageBox.critical(self, 'Ошибка', 'Файл не найден!')
            except:
                QMessageBox.critical(self, 'Ошибка', 'Ошибка загрузки')

            self.load_costs_from_db()

    def export_costs_to_file(self):
        filename, ok = QFileDialog.getSaveFileName(self,
                                                   "Укажите место сохранения файла",
                                                   './',
                                                   self.file_filter)
        save_result = self.costs_page_model.save_costs_to_excel(filename)
        if save_result != 0:
            QMessageBox.critical(self, 'Ошибка', save_result)
        else:
            self.status_bar.showMessage('Сохранено успешно', 5000)

    def set_exchange_rate(self):
        exchange_rate, ok = QInputDialog.getDouble(self, f'Текущий курс: {self.exchange_rate}', 'Новый курс:')
        if exchange_rate and ok:
            self.db.set_usd_exchange_rate(exchange_rate)
            self.get_exchange_rate_from_db()
            self.sales_fill_costs()
            self.status_bar.showMessage('Курс успешно обновлен', 5000)

    def get_exchange_rate_from_db(self):
        self.exchange_rate = self.db.get_usd_exchange_rate()
        self.label_current_exchange_rate.setText(f'Текущий курс: {self.exchange_rate}')
        LoftItem.usd_rub_rate = self.exchange_rate

    def sales_fill_costs(self):
        self.db.fill_cost_from_db(self.sales_page_table_model)
        self.resize_table(self.sales_page_table)

    def load_costs_from_db(self):
        self.costs_page_model.set_data_from_list(self.db.get_costs_from_db())
        self.costs_page_model.layoutChanged.emit()
        self.resize_table(self.costs_page_table)

    @staticmethod
    def resize_table(table):
        table.resizeColumnsToContents()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
