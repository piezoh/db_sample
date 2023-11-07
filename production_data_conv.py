import csv
import os
import paramiko

from peewee import MySQLDatabase, Model, IntegerField
from dotenv import load_dotenv
from playhouse.db_url import connect

#  .envの読み込み
load_dotenv()

# SSH接続情報
ssh_username = os.environ["SSH_USERNAME"]
ssh_password = os.environ["SSH_PASSWORD"]
ssh_host = os.environ["SSH_HOST"]
ssh_port = int(os.environ["SSH_PORT"])


# データベース接続情報
db_host = os.environ["DB_HOST"]
db_port = int(os.environ["DB_PORT"])
db_user = os.environ.get("DB_USERNAME")
db_password = os.environ.get("DB_PASSWORD")
db_name = os.environ.get("DB_NAME")

# SSHトンネリングの設定
ssh_client = paramiko.SSHClient()
ssh_client.load_system_host_keys()
ssh_client.connect(ssh_host, port=ssh_port, username=ssh_username, password=ssh_password)

transport = ssh_client.get_transport()

# データベースへのポートフォワーディングを設定
local_port = transport.request_port_forward(db_host, db_port)



# 代わりに paramiko.Channel を作成してポート転送を設定
channel = transport.open_channel(
    'direct-tcpip',
    (db_host, db_port),
    ('localhost', local_port)
)
db = connect(os.environ.get("DATABASE"))

class BaseModel(Model):
    class Meta:
        database = db

class operations(BaseModel):
    line_id = IntegerField()
    product_id = IntegerField()
    responsible_id = IntegerField()
    worker_id = IntegerField()

# CSVファイルのパス
csv_file = 'test_data.csv'

# データベース接続
db.connect()

# テーブルの作成（既に存在している場合はスキップされます）
db.create_tables([operations], safe=True)



# CSVファイルを読み込んでデータベースに挿入
with open(csv_file, 'r') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        operations.create(
            # day = row[0],
            # production_number = row[1],
            line_id = row[2],
            product_id = row[3],
            responsible_id = row[4],
            worker_id = row[5],
        )

# データベース接続を閉じる
db.close()
ssh_client.close()
