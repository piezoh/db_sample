import csv
import os

from peewee import SqliteDatabase, Model, CharField, IntegerField
from dotenv import load_dotenv

#  .envの読み込み
load_dotenv()

db = connect(os.environ.get("DATABASE"))

class BaseModel(Model):
    class Meta:
        database = db

class TABLE(BaseModel):
    user_id = CharField()
    machine_num = IntegerField()
    admin_id = CharField()
    parts_num = IntegerField()


# CSVファイルのパス
csv_file = 'data.csv'

# データベース接続
db.connect()
# 接続NGの場合はメッセージを表示
if not db.connect():
    print("DB接続NG")
    exit()

# TABLEテーブル内のすべてのレコードを削除
TABLE.delete().execute()

# CSVファイルを読み込んでデータベースに挿入
# テーブル名、カラム名等要修正
with open(csv_file, 'r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        TABLE.create(
            user_id=row['user_id'],
            machine_num=int(row['machine_num']),
            admin_id=row['admin_id'],
            parts_num=int(row['parts_num'])
        )

# データベース接続を閉じる
db.close()
