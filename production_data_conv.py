import csv
import os

from peewee import MySQLDatabase, Model, CharField, IntegerField
from dotenv import load_dotenv
from playhouse.db_url import connect

#  .envの読み込み
load_dotenv()

db = connect(os.environ.get("DATABASE"))

class BaseModel(Model):
    class Meta:
        database = db

class achievements(BaseModel):
    day = CharField()
    production_number = CharField()
    line_id = CharField()
    product_id = CharField()
    responsible_id = CharField()
    worker_id = CharField()

# CSVファイルのパス
csv_file = '20231030.csv'

# データベース接続
db.connect()

# テーブルの作成（既に存在している場合はスキップされます）
db.create_tables([achievements], safe=True)


# TABLEテーブル内のすべてのレコードを削除
achievements.delete().execute()

# CSVファイルを読み込んでデータベースに挿入
with open(csv_file, 'r') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        achievements.create(
            day = row[0],
            production_number = row[1],
            line_id = row[2],
            product_id = row[3],
            responsible_id = row[4],
            worker_id = row[5],
        )

# データベース接続を閉じる
db.close()
