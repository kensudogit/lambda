"""
Python学習教材: AWS Lambda関数の実装例
===============================================

このファイルは、Pythonの様々な概念を学習するための実践的な例です。
以下のトピックが含まれています：

1. ライブラリのインポートとモジュール管理
2. クラスとオブジェクト指向プログラミング
3. 非同期プログラミング（async/await）
4. エラーハンドリングと例外処理
5. デコレータとメタプログラミング
6. データベース操作（DynamoDB）
7. API設計とRESTfulサービス
8. キャッシュとパフォーマンス最適化
9. ログとモニタリング
10. テストとデバッグ

各セクションには詳細なコメントと学習ポイントが含まれています。
"""

# =============================================================================
# セクション1: ライブラリのインポートとモジュール管理
# =============================================================================
"""
学習ポイント:
- 標準ライブラリとサードパーティライブラリの使い分け
- インポートの順序とベストプラクティス
- 型ヒントの活用方法
"""

# 標準ライブラリのインポート
import json          # JSONデータの処理
import sys           # システム固有のパラメータと関数
import time          # 時間関連の操作
import os            # オペレーティングシステムとのインターフェース
import asyncio       # 非同期プログラミング
import traceback     # 例外の詳細情報取得
import re            # 正規表現
import math          # 数学関数
from decimal import Decimal  # 高精度の10進数演算
from typing import List, Dict, Any, Optional, Tuple, Set, Union  # 型ヒント
from dataclasses import dataclass  # データクラス
from functools import wraps  # デコレータ作成のヘルパー
from urllib.parse import urlparse  # URL解析
from collections.abc import Awaitable  # 非同期オブジェクトの型

# サードパーティライブラリのインポート
import boto3          # AWS SDK
import botocore       # AWS SDKのコア機能
import backoff        # リトライ機能
import aiohttp        # 非同期HTTPクライアント
import robotexclusionrulesparser  # robots.txt解析
from bs4 import BeautifulSoup     # HTML/XMLパーサー
from cachetools import TTLCache   # タイムアウト付きキャッシュ

# AWS Lambda Powertools（AWS Lambda専用のライブラリ）
from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.tracing import Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.event_handler.api_gateway import APIGatewayRestResolver, Response, CORSConfig

# AWS X-Ray（分散トレーシング）
from aws_xray_sdk.core import patch_all

# DynamoDB関連
from boto3.dynamodb.conditions import Key, Attr

# =============================================================================
# セクション2: 環境変数と定数の管理
# =============================================================================
"""
学習ポイント:
- 環境変数の設定と管理
- 定数の定義とベストプラクティス
- 設定値の一元管理
"""

# 環境変数の設定
os.environ["POSTS_TABLE_NAME"] = "wp_posts"
os.environ["POSTMETA_TABLE_NAME"] = "wp_postmeta"

# 定数の定義
APPLICATION_JSON = "application/json"
ALLOWED_METHODS = "OPTIONS,POST,GET"
ALLOWED_HEADERS = "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
HTML_PARSER = 'html.parser'
INTERNAL_SERVER_ERROR_MESSAGE = "Internal server error"

# 学習用の定数例
MAX_RETRY_ATTEMPTS = 3
CACHE_TTL_SECONDS = 3600
DEFAULT_PAGE_SIZE = 10

# =============================================================================
# セクション3: ログとトレーシングの設定
# =============================================================================
"""
学習ポイント:
- ログレベルの設定と使い分け
- 分散トレーシングの概念
- サービス間の監視とデバッグ
"""

# ロギングの設定
# LoggerはAWS Lambda Powertoolsの機能で、構造化ログを提供
logger = Logger(
    service="content_query_service",  # サービス名
    level="DEBUG"  # ログレベル（DEBUG, INFO, WARNING, ERROR, CRITICAL）
)

# トレーシングの設定
# TracerはAWS X-Rayと統合して、分散トレーシングを提供
tracer = Tracer(service="content_query_service")

# =============================================================================
# セクション4: CORS設定とAPI Gateway設定
# =============================================================================
"""
学習ポイント:
- CORS（Cross-Origin Resource Sharing）の概念
- API Gatewayの設定
- セキュリティヘッダーの管理
"""

# CORS設定の構成
cors_config = CORSConfig(
    allow_origin="*",  # 本番環境では具体的なドメインを指定
    allow_headers=ALLOWED_HEADERS,
    max_age=300  # プリフライトリクエストのキャッシュ時間（秒）
)

# API Gateway RESTリゾルバーの初期化
app = APIGatewayRestResolver(cors=cors_config)

# =============================================================================
# セクション5: AWSサービスの初期化
# =============================================================================
"""
学習ポイント:
- AWS SDKの使用方法
- リソースとクライアントの違い
- 環境変数からの設定値取得
"""

# AWS SDKのトレーシングを有効化
# これにより、AWSサービスへの呼び出しがX-Rayで追跡される
patch_all()

# DynamoDBリソースの作成
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')

# 環境変数からテーブル名を取得
POSTS_TABLE_NAME = os.getenv('POSTS_TABLE_NAME')
POSTMETA_TABLE_NAME = os.getenv('POSTMETA_TABLE_NAME')
CONTENT_BUCKET_NAME = os.getenv('CONTENT_BUCKET_NAME')

# テーブル名の定数定義
ACTIVITY_REPORT_TABLE_NAME = 'activity_report'
USER_MASTER_TABLE_NAME = 'user_master'

# DynamoDBテーブルオブジェクトの初期化
activity_report_table = dynamodb.Table(ACTIVITY_REPORT_TABLE_NAME)
user_master_table = dynamodb.Table(USER_MASTER_TABLE_NAME)

# その他の定数
ACTIVITY_REPORT_PUBLISHED = "published"

# =============================================================================
# 学習用の演習問題とサンプルコード
# =============================================================================
"""
演習問題1: 基本的な関数の作成
--------------------------------
以下の要件に従って関数を作成してください：

1. 2つの数値を受け取り、その合計を返す関数
2. 文字列を受け取り、その長さを返す関数
3. リストを受け取り、その要素の平均値を返す関数

解答例:
def add_numbers(a: int, b: int) -> int:
    return a + b

def get_string_length(text: str) -> int:
    return len(text)

def calculate_average(numbers: List[float]) -> float:
    if not numbers:
        return 0.0
    return sum(numbers) / len(numbers)
"""

"""
演習問題2: クラスの作成
-----------------------
以下の要件に従ってクラスを作成してください：

1. 人の情報を管理するPersonクラス
2. 名前、年齢、メールアドレスを属性として持つ
3. 年齢を1つ増やすメソッドを持つ
4. 情報を表示するメソッドを持つ

解答例:
@dataclass
class Person:
    name: str
    age: int
    email: str
    
    def have_birthday(self) -> None:
        self.age += 1
    
    def display_info(self) -> str:
        return f"Name: {self.name}, Age: {self.age}, Email: {self.email}"
"""

"""
演習問題3: エラーハンドリング
-----------------------------
以下の要件に従って関数を作成してください：

1. ファイルを読み込む関数
2. ファイルが存在しない場合は適切なエラーメッセージを表示
3. ファイルの読み込みに失敗した場合は適切なエラーメッセージを表示

解答例:
def read_file_safely(filename: str) -> str:
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        return f"Error: File '{filename}' not found"
    except Exception as e:
        return f"Error reading file: {str(e)}"
"""

"""
演習問題4: 非同期プログラミング
-------------------------------
以下の要件に従って非同期関数を作成してください：

1. 非同期でHTTPリクエストを送信する関数
2. 複数のURLに対して並列でリクエストを送信する関数
3. エラーハンドリングを含む

解答例:
import aiohttp
import asyncio

async def fetch_url(session: aiohttp.ClientSession, url: str) -> dict:
    try:
        async with session.get(url) as response:
            return {
                "url": url,
                "status": response.status,
                "content": await response.text()
            }
    except Exception as e:
        return {
            "url": url,
            "error": str(e)
        }

async def fetch_multiple_urls(urls: List[str]) -> List[dict]:
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url(session, url) for url in urls]
        return await asyncio.gather(*tasks)
"""

# =============================================================================
# セクション6: クラスとオブジェクト指向プログラミング
# =============================================================================
"""
学習ポイント:
- クラスの定義とコンストラクタ
- インスタンス変数とメソッド
- 型ヒントの活用
- ドキュメンテーション文字列（docstring）
"""

class RateLimiter:
    """
    リクエストのレート制限を管理するクラス
    
    このクラスは、クライアントからのリクエスト数を制限し、
    過度な負荷を防ぐためのレート制限機能を提供します。
    
    Attributes:
        max_requests (int): 時間窓内での最大リクエスト数
        time_window (int): 時間窓の長さ（秒）
        cache (TTLCache): クライアント情報をキャッシュするTTLキャッシュ
    """
    
    def __init__(self, max_requests: int = 100, time_window: int = 3600):
        """
        レート制限器を初期化する
        
        Args:
            max_requests (int): 時間窓内での最大リクエスト数（デフォルト: 100）
            time_window (int): 時間窓の長さ（秒）（デフォルト: 3600秒 = 1時間）
        """
        self.max_requests = max_requests
        self.time_window = time_window
        # TTLキャッシュを使用してクライアント情報を管理
        self.cache = TTLCache(maxsize=1000, ttl=time_window)

    def check_rate_limit(self, client_ip: str) -> Tuple[bool, Dict[str, Any]]:
        """
        指定されたクライアントのレート制限をチェックする
        
        Args:
            client_ip (str): クライアントのIPアドレス
            
        Returns:
            Tuple[bool, Dict[str, Any]]: 
                - 第1要素: リクエストが許可されるかどうか（True/False）
                - 第2要素: レート制限情報の辞書
        """
        current_time = int(time.time())
        
        # クライアントが初回アクセスの場合は初期化
        if client_ip not in self.cache:
            self.cache[client_ip] = {
                "count": 0, 
                "first_request": current_time
            }
        
        # クライアントデータを取得してリクエスト数を増加
        client_data = self.cache[client_ip]
        client_data["count"] += 1
        
        # レート制限情報を作成
        rate_limit_info = {
            "limit": self.max_requests,
            "remaining": max(0, self.max_requests - client_data["count"]),
            "reset": client_data["first_request"] + self.time_window
        }
        
        # 制限内かどうかを判定
        is_allowed = client_data["count"] <= self.max_requests
        return is_allowed, rate_limit_info

# グローバル変数としてレート制限器を定義
rate_limiter = RateLimiter()

# DynamoDBテーブル変数をグローバルに定義
posts_table = None
postmeta_table = None

# 外部サイト管理クラス（init関数の前に定義）
class ExternalSiteManager:
    """外部サイトのコンテンツを管理するクラス"""
    def __init__(self):

        # サイト情報をディクショナリのリストとして定義
        self.sites = [
            {
                "name": "information",
                "bucket": CONTENT_BUCKET_NAME,
                "prefix": "information/information/",
                "index_prefix": "information/index/"
            },
            {
                "name": "slp",
                "bucket": CONTENT_BUCKET_NAME,
                "prefix": "slp/",
                "index_prefix": "slp/index/"
            },
            {
                "name": "tools",
                "bucket": CONTENT_BUCKET_NAME,
                "prefix": "tools/",
                "index_prefix": "tools/index/"
            }
        ]
        self.s3_client = boto3.client('s3')
        self.cache = TTLCache(maxsize=1000, ttl=3600)

    async def check_external_sites_health(self) -> bool:
        """外部サイトの健康状態をチェック"""
        try:
            async with aiohttp.ClientSession() as session:
                for site in self.sites:
                    # S3バケットの疎通確認
                    logger.debug(site['bucket'])
                    await asyncio.to_thread(
                        self.s3_client.head_bucket,
                        Bucket=site['bucket']
                    )
            return True
        except Exception as e:
            logger.error(f"External sites health check failed: {str(e)}")
            raise

    async def search_content(self, keyword: str = None) -> List[Dict[str, Any]]:
        """キーワードに基づいてS3のインデックスを検索"""
        try:
            results = []
            for site in self.sites:
                cache_key = f"{site['name']}_{keyword}"
                if cache_key in self.cache:
                    results.extend(self.cache[cache_key])
                    continue

                # インデックスファイルから検索
                index_matches = await self._search_index(
                    bucket=site['bucket'],
                    prefix=site['index_prefix'],
                    keyword=keyword
                )
                
                # マッチしたコンテンツの詳細を取得
                site_results = await self._fetch_matched_contents(
                    bucket=site['bucket'],
                    content_prefix=site['prefix'],
                    matches=index_matches
                )
                
                self.cache[cache_key] = site_results
                results.extend(site_results)
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching content: {str(e)}")
            raise

    async def _search_index(self, bucket: str, prefix: str, keyword: str) -> Set[str]:
        """インデックスファイルから検索"""
        matches = set()
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        # 非同期イテレーションの修正
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        async for page in self._async_iterate(pages):
            for obj in page.get('Contents', []):
                response = await asyncio.to_thread(
                    self.s3_client.get_object,
                    Bucket=bucket,
                    Key=obj['Key']
                )
                index_data = json.loads(response['Body'].read().decode('utf-8'))
                
                if keyword.lower() in index_data:
                    matches.update(index_data[keyword.lower()])
        
        return matches

    async def _async_iterate(self, paginator):
        """S3ページネーターの非同期イテレーター"""
        for page in paginator:
            yield await asyncio.to_thread(lambda: page)

    async def _fetch_matched_contents(
        self, 
        bucket: str, 
        content_prefix: str, 
        matches: Set[str]
    ) -> List[Dict[str, Any]]:
        """マッチしたコンテンツの詳細を取得"""
        results = []
        
        # 並列処理で効率的にコンテンツを取得
        async def fetch_content(page_id: str) -> Dict[str, Any]:
            try:
                response = await asyncio.to_thread(
                    self.s3_client.get_object,
                    Bucket=bucket,
                    Key=f"{content_prefix}{page_id}.json"
                )
                content = json.loads(response['Body'].read().decode('utf-8'))
                return {
                    "page_id": page_id,
                    "title": content.get('title', ''),
                    "content": content.get('content', ''),
                    "url": content.get('url', ''),
                    "last_updated": content.get('last_updated', '')
                }
            except Exception as e:
                logger.error(f"Error fetching content for {page_id}: {str(e)}")
                return None

        # 最大20件の並列処理
        tasks = [fetch_content(page_id) for page_id in matches]
        chunk_size = 20
        for i in range(0, len(tasks), chunk_size):
            chunk_results = await asyncio.gather(*tasks[i:i + chunk_size])
            results.extend([r for r in chunk_results if r is not None])

        return results

# 初期化状態の管理
is_initialized = False
external_site_manager = None

# =============================================================================
# セクション10: 非同期プログラミング（async/await）
# =============================================================================
"""
学習ポイント:
- async/awaitキーワードの使用方法
- 非同期関数の定義と実行
- asyncio.to_thread()による同期関数の非同期実行
- バックオフ戦略とリトライ処理
- グローバル変数の管理
"""

@backoff.on_exception(backoff.expo, Exception, max_tries=3)
async def init() -> bool:
    """
    Lambda関数の初期化処理（バックオフ付きの非同期処理）
    
    この関数は、Lambda関数の初期化処理を非同期で実行します。
    @backoffデコレータにより、失敗時に指数バックオフでリトライします。
    
    Returns:
        bool: 初期化が成功した場合はTrue、失敗した場合はFalse
    """
    global is_initialized, posts_table, postmeta_table, external_site_manager, activity_report_table
    
    try:
        logger.debug("=== Starting init function ===")
        
        # 既に初期化済みの場合は処理をスキップ
        if is_initialized:
            logger.debug("Already initialized, skipping initialization")
            return True

        logger.debug("Checking required environment variables")
        
        # 必須環境変数の検証
        required_vars = {
            "POSTS_TABLE_NAME": POSTS_TABLE_NAME,
            "POSTMETA_TABLE_NAME": POSTMETA_TABLE_NAME
        }
        
        for var_name, var_value in required_vars.items():
            logger.debug(f"Checking {var_name}: {var_value}")
            if not var_value:
                logger.error(f"Missing required environment variable: {var_name}")
                return False

        try:
            logger.debug("Initializing DynamoDB tables")
            posts_table = dynamodb.Table(POSTS_TABLE_NAME)
            postmeta_table = dynamodb.Table(POSTMETA_TABLE_NAME)
            
            logger.debug("Initializing ExternalSiteManager")
            if external_site_manager is None:
                external_site_manager = ExternalSiteManager()
            
            # DynamoDB接続テストを実行
            logger.debug("Testing DynamoDB connection")
            try:
                # 同期関数を非同期で実行
                await asyncio.to_thread(lambda: posts_table.scan(Limit=1))
                await asyncio.to_thread(lambda: postmeta_table.scan(Limit=1))
                logger.debug("DynamoDB connection successful")
            except Exception as e:
                logger.error(f"DynamoDB connection failed: {str(e)}")
                logger.error(f"Stack trace: {traceback.format_exc()}")
                return False

            # S3接続テストを実行
            logger.debug("Testing S3 connection")
            try:
                await external_site_manager.check_external_sites_health()
                logger.debug("S3 connection successful")
            except Exception as e:
                logger.error(f"S3 connection failed: {str(e)}")
                logger.error(f"Stack trace: {traceback.format_exc()}")
                return False

        except Exception as e:
            logger.error(f"Initialization error: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return False
        
        # 初期化完了フラグを設定
        is_initialized = True
        logger.debug("Lambda function initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error in init: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return False

@tracer.capture_method
def synchronous_init() -> bool:
    """Lambda関数の同期的な初期化処理"""
    try:
        logger.info("Starting synchronous initialization")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(init())
        logger.info(f"Synchronous initialization completed with result: {result}")
        return result
    except Exception as e:
        logger.error(f"Synchronous initialization failed: {str(e)}", exc_info=True)
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

# 同期的な初期化処理を実行
synchronous_init()

# 
async def check_dynamodb_connection():
    """
    DynamoDBへの接続状態を確認する（非同期版）
    
    Returns:
        bool: 接続成功時はTrue、失敗時はFalse
    """
    try:
        # 最小限のスキャンクエリで接続テスト
        await asyncio.to_thread(posts_table.scan, Limit=1)
        return True
    except Exception:
        return False
    

# =============================================================================
# セクション8: ユーティリティ関数とヘルパー関数
# =============================================================================
"""
学習ポイント:
- 関数の定義と型ヒント
- 例外処理の実装
- 再利用可能なコードの作成
- エラーハンドリングのベストプラクティス
"""

def safe_cast(value, to_type, default=None):
    """
    型変換を安全に実行するユーティリティ関数
    
    この関数は、型変換中にエラーが発生した場合にデフォルト値を返すことで、
    プログラムのクラッシュを防ぎます。
    
    Args:
        value: 変換する値
        to_type: 変換先の型（int, str, floatなど）
        default: 変換に失敗した場合のデフォルト値
        
    Returns:
        変換された値、またはデフォルト値
        
    Examples:
        >>> safe_cast("123", int, 0)
        123
        >>> safe_cast("abc", int, 0)
        0
        >>> safe_cast(None, str, "default")
        "default"
    """
    try:
        return to_type(value)
    except (ValueError, TypeError):
        return default

def create_response(status_code: int, body: dict) -> dict:
    """
    標準化されたHTTPレスポンスを作成する関数
    
    この関数は、API Gateway用の標準的なレスポンス形式を作成します。
    すべてのAPIエンドポイントで一貫したレスポンス形式を提供します。
    
    Args:
        status_code (int): HTTPステータスコード（200, 400, 500など）
        body (dict): レスポンスボディの内容
        
    Returns:
        dict: API Gateway用のレスポンス辞書
        
    Examples:
        >>> create_response(200, {"message": "success"})
        {
            "statusCode": 200,
            "headers": {...},
            "body": {"message": "success"}
        }
    """
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": APPLICATION_JSON,
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": ALLOWED_METHODS,
            "Access-Control-Allow-Headers": ALLOWED_HEADERS
        },
        "body": body
    }

# =============================================================================
# セクション7: データクラスとデータモデリング
# =============================================================================
"""
学習ポイント:
- @dataclassデコレータの使用方法
- クラスメソッドとファクトリメソッド
- 型ヒントを使ったデータモデリング
- データ変換とバリデーション
"""

@dataclass
class WpPost:
    """
    WordPress投稿のデータモデル
    
    このクラスは、WordPressの投稿データを表現するためのデータクラスです。
    @dataclassデコレータにより、自動的に__init__、__repr__、__eq__メソッドが生成されます。
    
    Attributes:
        site_code (str): サイトコード
        ID (int): 投稿ID
        guid (str): グローバル一意識別子
        menu_order (int): メニュー順序
        ping_status (str): ピングステータス
        post_author (int): 投稿者ID
        post_content (str): 投稿内容
        post_date (str): 投稿日時
        post_title (str): 投稿タイトル
        post_name (str): 投稿スラッグ
        post_status (str): 投稿ステータス
        post_type (str): 投稿タイプ
        comment_count (int): コメント数
    """
    site_code: str
    ID: int
    guid: str
    menu_order: int
    ping_status: str
    post_author: int
    post_content: str
    post_date: str
    post_title: str
    post_name: str
    post_status: str
    post_type: str
    comment_count: int

    @classmethod
    def from_dynamodb_item(cls, item: dict) -> 'WpPost':
        """
        DynamoDBアイテムからWpPostオブジェクトを作成するファクトリメソッド
        
        Args:
            item (dict): DynamoDBから取得したアイテム
            
        Returns:
            WpPost: 作成されたWpPostオブジェクト
        """
        return cls(
            site_code=item.get('site_code', ''),
            ID=safe_cast(item.get('ID'), int, 0),
            guid=item.get('guid', ''),
            menu_order=safe_cast(item.get('menu_order'), int, 0),
            ping_status=item.get('ping_status', ''),
            post_author=safe_cast(item.get('post_author'), int, 0),
            post_content=item.get('post_content', ''),
            post_date=item.get('post_date', ''),
            post_title=item.get('post_title', ''),
            post_name=item.get('post_name', ''),
            post_status=item.get('post_status', ''),
            post_type=item.get('post_type', ''),
            comment_count=safe_cast(item.get('comment_count'), int, 0),
        )

@dataclass
class WpPostMeta:
    """
    WordPress投稿メタデータのデータモデル
    
    このクラスは、WordPressの投稿メタデータを表現するためのデータクラスです。
    メタデータは、投稿に付随する追加情報（カスタムフィールドなど）を格納します。
    
    Attributes:
        meta_id (int): メタデータID
        post_id (int): 関連する投稿ID
        meta_key (str): メタキー
        meta_value (str): メタ値
    """
    meta_id: int
    post_id: int
    meta_key: str
    meta_value: str

    @classmethod
    def from_dynamodb_item(cls, item: dict) -> 'WpPostMeta':
        """
        DynamoDBアイテムからWpPostMetaオブジェクトを作成するファクトリメソッド
        
        Args:
            item (dict): DynamoDBから取得したアイテム
            
        Returns:
            WpPostMeta: 作成されたWpPostMetaオブジェクト
        """
        return cls(
            meta_id=safe_cast(item.get('meta_id'), int, 0),
            post_id=safe_cast(item.get('post_id'), int, 0),
            meta_key=item.get('meta_key', ''),
            meta_value=item.get('meta_value', ''),
        )

class ContentManager:
    """コンテンツ管理を担当するクラス"""
    def __init__(self):
        logger.debug("Initializing ContentManager")
        self._posts_cache = TTLCache(maxsize=100, ttl=300)
        self._debug_info = []
        logger.debug("ContentManager initialized successfully")

    def _clear_debug_info(self):
        """デバッグ情報をクリアする"""
        self._debug_info = []

    def _add_debug_info(self, message: str):
        """デバッグ情報を追加する"""
        self._debug_info.append(message)
        logger.debug(message)

    @tracer.capture_method
    def get_all_posts(self, keyword: str = None, site_code: int = [], limit: int = 0) -> List[WpPost]:
        """効率的な投稿データ取得"""
        self._clear_debug_info()
        self._add_debug_info("Starting get_all_posts")
        
        if '_all_posts' in self._posts_cache:
            self._add_debug_info("Returning cached posts")
            return self._posts_cache['_all_posts']

        attr_site_code = self._get_site_codes(site_code)
        posts = self._fetch_posts(attr_site_code, limit)
        
        self._posts_cache['_all_posts'] = posts
        return posts

    def _get_site_codes(self, site_code: List[int]) -> List[int]:
        """対象サイトコードを取得する"""
        if site_code:
            logger.debug(f"get_all_posts() site_code : {site_code}")
            return site_code
        return [1, 2, 3]

    def _fetch_posts(self, attr_site_code: List[int], limit: int) -> List[WpPost]:
        """投稿データを取得する"""
        posts = []
        last_evaluated_key = None
        scan_count = 0
        
        while True:
            response = self._query_posts(attr_site_code, limit, last_evaluated_key)
            scan_count += 1
            self._add_debug_info(f"Scan #{scan_count} returned {len(response['Items'])} items")
            
            posts.extend([WpPost.from_dynamodb_item(item) for item in response['Items']])
            
            if limit == 0:
                last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break
        
        self._add_debug_info(f"Total posts retrieved: {len(posts)}")
        return posts

    def _query_posts(self, attr_site_code: List[int], limit: int, last_evaluated_key: Optional[dict]) -> dict:
        """DynamoDBから投稿データをクエリする"""
        query_params = {
            'IndexName': 'post_status-post_date-index',
            'KeyConditionExpression': Key('post_status').eq('publish'),
            'ProjectionExpression': 'site_code,ID,post_title,post_content,post_date,guid,post_type',
            'FilterExpression': Attr('site_code').is_in(attr_site_code) & Attr('post_type').is_in(['page', 'company']),
            'ScanIndexForward': False
        }
        
        if last_evaluated_key:
            query_params['ExclusiveStartKey'] = last_evaluated_key
        
        response = posts_table.query(**query_params)
        
        if limit > 0:
            del response['Items'][limit::]
        
        return response

    @tracer.capture_method
    def analyze_content(self, post: WpPost, request_categories: List[str]) -> dict:
        """投稿内容から指定キーワードを抽出・分析する"""
        keywords = ['健康', '喫煙', '女性', '寿命']
        matches = self._initialize_matches()

        self._extract_title_matches(post, keywords, matches)
        self._extract_content_matches(post, keywords, matches)
        self._extract_category_matches(post, keywords, matches)

        matches['total_matches'] = (
            len(matches['title_matches']) +
            len(matches['content_matches']) +
            len(matches['category_matches'])
        )

        if matches['total_matches'] > 0:
            logger.info(f"Found matches in post {post.ID}:")
            logger.info(f"Title: {post.post_title}")
            logger.info(f"Matches: {matches}")

        return self._create_response(post, matches)

    def _initialize_matches(self) -> dict:
        """マッチ結果の初期化辞書を返す"""
        return {
            'title_matches': [],
            'content_matches': [],
            'category_matches': [],
            'total_matches': 0
        }

    def _extract_title_matches(self, post: WpPost, keywords: List[str], matches: dict):
        """タイトルからのキーワードマッチを抽出する"""
        for keyword in keywords:
            if keyword in post.post_title:
                matches['title_matches'].append({
                    'keyword': keyword,
                    'context': post.post_title
                })

    def _extract_content_matches(self, post: WpPost, keywords: List[str], matches: dict):
        """コンテンツ本文からのキーワードマッチを抽出する"""
        if post.post_content:
            soup = BeautifulSoup(post.post_content, HTML_PARSER)
            for keyword in keywords:
                if keyword in soup.get_text().lower():
                    context = self._get_context(soup.get_text().lower(), keyword)
                    matches['content_matches'].append({
                        'keyword': keyword,
                        'context': f"...{context}..."
                    })

    def _get_context(self, content: str, keyword: str) -> str:
        """キーワードを含む文脈を抽出する（前後50文字）"""
        pos = content.find(keyword)
        start = max(0, pos - 50)
        end = min(len(content), pos + len(keyword) + 50)
        return content[start:end]

    def _extract_category_matches(self, post: WpPost, keywords: List[str], matches: dict):
        """カテゴリーからのキーワードマッチを抽出する"""
        if hasattr(post, 'categories'):
            for keyword in keywords:
                for category in post.categories:
                    if keyword in category:
                        matches['category_matches'].append({
                            'keyword': keyword,
                            'category': category
                        })

    def _create_response(self, post: WpPost, matches: dict) -> dict:
        """分析結果のレスポンスを作成する"""
        return {
            'status': 'success',
            'data': {
                'post': {
                    'id': post.ID,
                    'title': post.post_title,
                    'url': post.guid,
                    'post_date': post.post_date
                },
                'analysis': {
                    'keyword_matches': {
                        'title': [
                            {
                                'keyword': match['keyword'],
                                'context': match['context']
                            } for match in matches['title_matches']
                        ],
                        'content': [
                            {
                                'keyword': match['keyword'],
                                'context': match['context']
                            } for match in matches['content_matches']
                        ],
                        'categories': [
                            {
                                'keyword': match['keyword'],
                                'category': match['category']
                            } for match in matches['category_matches']
                        ]
                    },
                    'summary': {
                        'total_matches': matches['total_matches'],
                        'title_matches': len(matches['title_matches']),
                        'content_matches': len(matches['content_matches']),
                        'category_matches': len(matches['category_matches'])
                    }
                }
            }
        }

    @tracer.capture_method
    def search_content(self, keyword: str = None, site_code: int = [], limit: int = 0) -> List[Dict[str, Any]]:
        try:
            self._clear_debug_info()
            self._add_debug_info(f"Starting search with keyword: {keyword}, site_code: {site_code}, limit: {limit}")
            
            results = []
            posts = self.get_all_posts(keyword, site_code, limit)
            
            if not posts:
                self._add_debug_info("No posts available for search")
                return {
                    'results': [],
                    'debug_info': self._debug_info,
                    'error': 'No posts available'
                }

            self._add_debug_info(f"Processing {len(posts)} posts")
            
            for post in posts:
                # サイトコードによるフィルタリング処理
                if site_code is not None and post.site_code in site_code:
                    # キーワード検索処理
                    if keyword:
                        soup = BeautifulSoup(post.post_content, HTML_PARSER)
                        if all(key.lower() in post.post_title.lower() or 
                            key.lower() in soup.get_text().lower() for key in keyword.split()):
                            results.append({
                                'id': post.ID,
                                'title': post.post_title,
                                'content': post.post_content,
                                'site_code': post.site_code,
                                'tags': [],
                                'files': [],
                                'url': post.guid,
                                'post_date': post.post_date
                            })
                    else:
                        self._add_debug_info(f"post {post}")
                        # キーワードが指定されていない場合の処理
                        results.append({
                            'id': post.ID,
                            'title': post.post_title,
                            'content': post.post_content,
                            'site_code': post.site_code,
                            'tags': [],
                            'files': [],
                            'url': post.guid,
                            'post_date': post.post_date
                        })

            # 投稿日時で降順ソート
            results.sort(key=lambda x: x['post_date'], reverse=True)
            self._add_debug_info(f"results {results}")
            
            return {
                'results': results,
                'debug_info': self._debug_info,
                'stats': {
                    'total_posts': len(posts),
                    'matched_posts': len(results),
                    'site_code': site_code,
                    'keyword': keyword
                }
            }

        except Exception as e:
            error_msg = f"Error in search_content: {str(e)}"
            self._add_debug_info(error_msg)
            logger.error(error_msg)
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return {
                'results': [],
                'debug_info': self._debug_info,
                'error': str(e)
            }

    def _calculate_relevance(self, keyword_terms: List[str], 
                            title: str, content: str) -> float:
        """検索結果の関連性スコアを計算する"""
        score = 0.0
        title = title.lower()
        
        for term in keyword_terms:
            # タイトルでの一致は高いスコアを付与
            if term in title:
                score += 2.0
            # 本文での一致は低いスコアを付与
            if term in content:
                score += 1.0
            
        return score

def validate_search_params(keyword: Optional[str]) -> Tuple[bool, str]:
    """検索パラメータの入力値検証"""
    if keyword and len(keyword) > 100:
        return False, "Keyword too long"
    return True, ""

def validate_category(category: Optional[str]) -> Tuple[bool, str]:
    """
    カテゴリの値が有効かチェックする
    
    Args:
        category: チェックするカテゴリ文字列
        
    Returns:
        Tuple[bool, str]: (有効かどうか, エラーメッセージ)
    """
    valid_categories = ["category1", "category2", "category3"]  # 有効なカテゴリ例
    if category is None or category in valid_categories:
        return True, ""
    return False, "Invalid category"

# =============================================================================
# セクション9: デコレータとメタプログラミング
# =============================================================================
"""
学習ポイント:
- デコレータの定義と使用方法
- @wrapsデコレータの重要性
- 関数のラッピングと拡張
- エラーハンドリングの共通化
"""

def error_handler(func):
    """
    エラーハンドリング用のデコレータ
    
    このデコレータは、関数の実行をラップしてエラーハンドリングを共通化します。
    すべてのAPIエンドポイントで一貫したエラー処理を提供します。
    
    Args:
        func: デコレートする関数
        
    Returns:
        ラップされた関数
    """
    @wraps(func)  # 元の関数のメタデータを保持
    def wrapper(*args, **kwargs):
        try:
            # 関数の開始をログに記録
            logger.debug(f"=== Starting {func.__name__} ===")
            
            # 元の関数を実行
            response = func(*args, **kwargs)
            
            # レスポンスをログに記録
            logger.debug(f"=== Response from {func.__name__} ===\n{json.dumps(response, indent=2)}")
            return response
            
        except Exception as e:
            # エラーの詳細をログに記録
            logger.error(f"=== Error in {func.__name__} ===")
            logger.error(f"Error Type: {type(e).__name__}")
            logger.error(f"Error Message: {str(e)}")
            logger.error(f"Detailed traceback:\n{traceback.format_exc()}")
            
            # エラー詳細を構造化
            error_details = {
                "function": func.__name__,
                "error_type": type(e).__name__,
                "error_message": str(e)
            }
            logger.error(f"Error Details:\n{json.dumps(error_details, indent=2)}")
            
            # エラーレスポンスを返す
            return {
                "statusCode": 500,
                "headers": {"Content-Type": APPLICATION_JSON},
                "body": json.dumps({
                    "error": INTERNAL_SERVER_ERROR_MESSAGE,
                    "details": error_details if os.getenv('DEBUG') == 'true' else None
                })
            }
    return wrapper

@tracer.capture_method
def search_handler():
    try:
        start_time = time.time()  # 処理開始時間を記録
        
        # リクエストの解析とパラメータ抽出
        _, keyword, site_code, page, per_page, limit = parse_request(app.current_event)
        
        # アクティビティレポートの取得
        activity_report_results = retrieve_activity_reports(keyword, site_code, limit)
        
        # 基本検索の実行
        basic_results = execute_basic_search(keyword, site_code, limit)
        
        # 結果の結合とソート
        combined_results = combine_and_sort_results(basic_results, activity_report_results, limit)
        
        # レスポンスの作成
        return create_search_response(combined_results, page, per_page, start_time)
        
    except Exception as e:
        logger.error(f"Error in search handler: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return create_response(500, {
            "status": "error",
            "message": INTERNAL_SERVER_ERROR_MESSAGE,
            "error": str(e) if os.getenv('DEBUG') == 'true' else None
        })

def parse_request(event):
    """リクエストを解析してパラメータを抽出する"""
    body = get_body(event)
    keyword = body.get('keyword', '') or body.get('word', '')
    site_code_str = body.get('category')
    page = int(body.get('page', 1))
    per_page = int(body.get('per_page', 10))
    limit = body.get('limit', 0)
    site_code = filter_and_convert(site_code_str, ['1', '2', '3', '4'])
    logger.debug(f"keyword :{keyword}")
    logger.debug(f"sitecode :{site_code}")
    return body, keyword, site_code, page, per_page, limit

def retrieve_activity_reports(keyword, site_code, limit):
    """キーワードとサイトコードに基づいてアクティビティレポートを取得する"""
    activity_report_results = []
    if 4 in site_code:
        try:
            tag_master_result = tag_master_query()
            tag_code = get_tag_code(keyword, tag_master_result)
            activity_report_results = query_activity_reports(tag_code, keyword, limit)
        except Exception as e:
            logger.error(f"Error fetching search activity_report: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
    return activity_report_results

def execute_basic_search(keyword, site_code, limit):
    """基本検索を実行する"""
    basic_results = []
    if 1 in site_code or 2 in site_code or 3 in site_code:
        manager = ContentManager()
        basic_result = manager.search_content(keyword, site_code, limit)
        logger.info(basic_result)
        basic_results = basic_result.get('results', [])
    return basic_results

def combine_and_sort_results(basic_results, activity_report_results, limit):
    """検索結果を結合してソートする"""
    if len(basic_results) > 0 and len(activity_report_results) > 0:
        basic_results.extend(activity_report_results)
    elif len(basic_results) == 0 and len(activity_report_results) > 0:
        basic_results = activity_report_results

    # 投稿日時で降順ソート
    basic_results.sort(key=lambda x: x['post_date'], reverse=True)
    if limit > 0:
        del basic_results[limit::]
    return basic_results

def create_search_response(results, page, per_page, start_time):
    """検索結果のレスポンスを作成する"""
    search_response = {
        "status": "success",
        "results": results,
        "pagination": {
            "total": len(results),
            "page": page,
            "per_page": per_page
        },
        "search_metadata": {
            "execution_time": time.time() - start_time
        }
    }
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": APPLICATION_JSON,
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": ALLOWED_METHODS,
            "Access-Control-Allow-Headers": ALLOWED_HEADERS
        },
        "body": convert_decimal(search_response)
    }

def filter_and_convert(input_list, valid_numbers):
    """入力リストから有効な数値を抽出して変換する"""
    return [int(num) for num in input_list if num in valid_numbers]

def convert_decimal(obj):
    """Decimal型のオブジェクトをJSONシリアライズ可能な型に変換する"""
    if isinstance(obj, list):
        return [convert_decimal(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj) if '.' in str(obj) else int(obj)
    return obj

@app.post("/categories")
@error_handler
@tracer.capture_method
def categories_handler():
    """カテゴリリクエストのハンドラ"""
    try:
        # ContentManagerのインスタンス化
        manager = ContentManager()
        # 全投稿を取得して、カテゴリを抽出
        posts = manager.get_all_posts()
        categories = set()
        
        for post in posts:
            # 投稿のコンテンツからカテゴリを抽出
            soup = BeautifulSoup(post.post_content, HTML_PARSER)
            soup.get_text().lower()
            
        return create_response(200, {
            "categories": list(categories),
            "count": len(categories)
        })
    except Exception as e:
        logger.error(f"Error in categories_handler: {str(e)}")
        return create_response(500, {"error": INTERNAL_SERVER_ERROR_MESSAGE})

def record_metrics(func):
    """関数の実行メトリクスを記録するデコレータ"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info({
                "metric_name": func.__name__,
                "duration": duration,
                "status": "success"
            })
            return result
        except Exception as e:
            logger.error({
                "metric_name": func.__name__,
                "error": str(e),
                "status": "error"
            })
            raise
    return wrapper

@app.post("/external-contents")
@error_handler
@tracer.capture_method
def external_contents_handler() -> dict:
    """外部サイトのコンテンツを検索するハンドラ"""
    try:
        keyword = app.current_event.get_query_string_value("keyword")
        if not keyword:
            return create_response(400, {"error": "Keyword is required"})

        if not external_site_manager:
            return create_response(500, {"error": "Service not properly initialized"})

        # 非同期関数を同期的に実行
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(external_site_manager.search_content(keyword))
        
        return create_response(200, {
            "results": results,
            "total": len(results)
        })
    except Exception as e:
        logger.error(f"Error in external_contents_handler: {str(e)}")
        return create_response(500, {"error": INTERNAL_SERVER_ERROR_MESSAGE})

@app.post("/health")
@error_handler
@tracer.capture_method
def health_check():
    """システムのヘルスチェック"""
    async def _check_health():
        health_status = {
            "dynamodb": False,
            "s3": False,
            "lambda": True,
            "initialization": is_initialized,
            "timestamp": int(time.time())
        }

        try:
            # 初期化が完了していない場合は初期化処理を実行
            if not is_initialized and not synchronous_init():
                return create_response(503, {
                    "status": "initializing",
                    "details": health_status,
                    "message": "System is initializing"
                })

            # DynamoDB接続状態のチェック
            if await check_dynamodb_connection():
                health_status["dynamodb"] = True

            # S3接続状態のチェック
            try:
                s3_client = boto3.client('s3')
                await asyncio.to_thread(
                    s3_client.head_bucket,
                    Bucket=os.environ.get("CONTENT_BUCKET_NAME", "kenko21-web")
                )
                health_status["s3"] = True
            except Exception as e:
                logger.warning(f"S3 health check failed: {str(e)}")

            # 全体的なヘルス状態の判定
            overall_health = health_status["dynamodb"]

            # メモリ情報の取得
            health_status["memory"] = {
                "limit": float(os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", 0))
            }

            return create_response(
                200 if overall_health else 500,
                {
                    "status": "healthy" if overall_health else "unhealthy",
                    "details": health_status,
                    "message": "System operational" if overall_health else "System degraded"
                }
            )

        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return create_response(500, {
                "status": "unhealthy",
                "details": health_status,
                "message": "System health check failed"
            })

    # 非同期関数を同期的に実行
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(_check_health())

class RobotsChecker:
    """robots.txtルールをチェック・管理するクラス"""
    def __init__(self):
        self.parser = robotexclusionrulesparser.RobotExclusionRulesParser()
        self.cache = TTLCache(maxsize=100, ttl=3600)  # robots.txtを1時間キャッシュ

    async def can_fetch(self, session: aiohttp.ClientSession, url: str) -> bool:
        """robots.txtに基づいてURLへのアクセス許可を判定する"""
        domain = urlparse(url).netloc
        if domain not in self.cache:
            try:
                robots_url = f"https://{domain}/robots.txt"
                async with session.get(robots_url) as response:
                    if response.status == 200:
                        robots_content = await response.text()
                        self.parser.parse(robots_content)
                    else:
                        return True
                self.cache[domain] = self.parser
            except Exception as e:
                logger.warning(f"Error fetching robots.txt for {domain}: {str(e)}")
                return True

        return self.cache[domain].is_allowed("*", url)

class CacheManager:
    """キャッシュ管理を一元化するクラス"""
    def __init__(self):
        self.content_cache = TTLCache(maxsize=100, ttl=300)  # コンテンツキャッシュ
        self.robots_cache = TTLCache(maxsize=100, ttl=3600)  # robots.txtキャッシュ
        self.rate_limit_cache = TTLCache(maxsize=1000, ttl=3600)  # レート制限キャッシュ
        self._cleanup_task = None

    async def start_cleanup_task(self):
        """定期的なキャッシュクリーンアップタスクを開始する"""
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._periodic_cleanup())

    async def _periodic_cleanup(self):
        """60秒ごとに期限切れのキャッシュをクリアする"""
        while True:
            try:
                self.clear_expired()
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Cache cleanup error: {str(e)}")
                await asyncio.sleep(5)  # エラー時は短い間隔で再試行

    def clear_expired(self):
        """期限切れのキャッシュをクリアする"""
        for cache in [self.content_cache, self.robots_cache, self.rate_limit_cache]:
            cache.expire()

# グローバルキャッシュマネージャーのインスタンス化
cache_manager = CacheManager()

class CustomError(Exception):
    """カスタムエラーの基底クラス"""
    def __init__(self, message: str, status_code: int = 500, details: dict = None):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.details = details or {}

class ServiceError(CustomError):
    """サービス関連のエラー"""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, status_code=500, details=details)

class InitializationError(ServiceError):
    """初期化エラー"""
    pass

class ValidationError(CustomError):
    """バリデーションエラー"""
    def __init__(self, message: str):
        super().__init__(message, 400)

class RateLimitError(CustomError):
    """レート制限エラー"""
    def __init__(self, message: str, rate_limit_info: dict):
        super().__init__(message, 429)
        self.rate_limit_info = rate_limit_info

def create_error_response(error: Exception, context: LambdaContext = None) -> dict:
    """エラーレスポンスを生成する共通関数"""
    is_debug = os.getenv('DEBUG') == 'true'
    
    error_response = {
        "statusCode": 500,
        "headers": {
            "Content-Type": APPLICATION_JSON,
            "Access-Control-Allow-Origin": "*",
            "X-Error-Type": type(error).__name__
        },
        "body": json.dumps({
            "message": INTERNAL_SERVER_ERROR_MESSAGE,
            "error_type": type(error).__name__,
            "error_details": str(error) if is_debug else None,
            "request_id": context.aws_request_id if context else None,
            "timestamp": time.time()
        })
    }

    # CustomErrorの場合は指定されたステータスコードを使用
    if isinstance(error, CustomError):
        error_response["statusCode"] = error.status_code
        error_response["body"] = json.dumps({
            "message": error.message,
            "details": error.details if is_debug else None,
            "timestamp": time.time()
        })

    return error_response

def get_memory_info() -> Dict[str, Any]:
    """Lambda環境でのメモリ情報を取得する"""
    try:
        # psutilを使用せず、環境変数から情報を取得
        memory_info = {
            "memory_limit": int(os.environ.get('AWS_LAMBDA_FUNCTION_MEMORY_SIZE', 0)),
            "memory_used": None  # 実際の使用量は取得できない
        }
        
        # /proc/self/statmから追加のメモリ情報を取得（Linuxシステムの場合）
        try:
            with open('/proc/self/statm', 'r') as f:
                stats = f.read().split()
                page_size = os.sysconf('SC_PAGE_SIZE')
                memory_info["virtual_memory"] = int(stats[0]) * page_size / (1024 * 1024)  # MB単位
                memory_info["resident_memory"] = int(stats[1]) * page_size / (1024 * 1024)  # MB単位
        except Exception:
            pass  # /proc/self/statmが利用できない場合は無視
            
        return memory_info
        
    except Exception as e:
        logger.warning(f"Failed to get memory info: {str(e)}")
        return {}

def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """Lambda関数のメインハンドラー"""
    try:
        # 初期化が完了していない場合は初期化処理を実行
        if not is_initialized and not synchronous_init():
            return create_response(500, {
                "error": "Service initialization failed",
                "request_id": context.aws_request_id
            })

        logger.debug(event)
        if "httpMethod" in event:
            try:
                return app.resolve(event, context)
            except Exception as e:
                logger.error(f"API Gateway resolution error: {str(e)}")
                return create_response(500, {
                    "error": INTERNAL_SERVER_ERROR_MESSAGE,
                    "details": str(e) if os.getenv('DEBUG') == 'true' else None
                })
        
        # アクションの処理
        action = event.get("action")
        if action == "health":
            return create_response(200, {
                "status": "healthy",
                "initialized": is_initialized,
                "memory_info": get_memory_info()
            })
        else:
            return create_response(400, {
                "error": "Invalid action",
                "message": "Direct Lambda invocation requires an 'action' parameter"
            })

    except Exception as e:
        logger.error(f"Fatal error in lambda_handler: {str(e)}")
        return create_response(500, {
            "error": INTERNAL_SERVER_ERROR_MESSAGE,
            "details": str(e) if os.getenv('DEBUG') == 'true' else None
        })

@app.post("/search-tags")
@error_handler
@tracer.capture_method
def get_search_tags_handler():
    """activity_report_tableから指定されたactivity_report_idのsearch_tags_listを取得するハンドラ"""
    try:
        # リクエストボディからactivity_report_idを取得
        body = get_body(app.current_event)
        activity_report_id = body.get('activity_report_id')

        # activity_report_idの必須チェック
        if not activity_report_id:
            return create_response(400, {
                "error": "activity_report_id is required",
                "message": "Please provide an activity_report_id in the request body"
            })

        # DynamoDBテーブルの初期化状態確認
        if not activity_report_table:
            logger.error("Activity report table not initialized")
            return create_response(500, {
                "error": "Service not properly initialized",
                "message": "Activity report table not available"
            })

        # DynamoDBクエリ用のフィルター式を定義
        activity_report_filter_expression = (
            Attr('delete_flag').eq(False) & 
            Attr('activity_report_id').eq(activity_report_id)
        )

        # DynamoDBテーブルのスキャン実行
        response = activity_report_table.scan(
            FilterExpression=activity_report_filter_expression,
            ProjectionExpression='search_tags_list'
        )

        # search_tags_listの抽出処理
        all_tags = set()
        for item in response.get('Items', []):
            tags = item.get('search_tags_list', [])
            logger.debug(f"Processing item tags: {tags}")
            if isinstance(tags, list):
                previous_count = len(all_tags)
                all_tags.update(tags)
                logger.info(f"Added {len(all_tags) - previous_count} new tags from item")
            else:
                logger.warning(f"Unexpected tags format: {type(tags)}, value: {tags}")

        # タグをソートしてリストとして返す
        sorted_tags = sorted(list(all_tags))
        
        # ソート済みタグのログ出力
        logger.info(f"=== Sorted Tags for activity_report_id: {activity_report_id} ===")
        logger.info(json.dumps(sorted_tags, ensure_ascii=False, indent=2))
        logger.info(f"Total tags count: {len(sorted_tags)}")
        
        return create_response(200, {
            "activity_report_id": activity_report_id,
            "search_tags": sorted_tags,
            "count": len(sorted_tags)
        })

    except Exception as e:
        logger.error(f"Error in get_search_tags_handler: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return create_response(500, {
            "error": INTERNAL_SERVER_ERROR_MESSAGE,
            "details": str(e) if os.getenv('DEBUG') == 'true' else None
        })

def record_search_metrics(results_count: int, tags_count: int):
    """検索メトリクスを記録する"""
    try:
        metrics = {
            "timestamp": int(time.time()),
            "results_count": results_count,
            "tags_count": tags_count,
            "memory_info": get_memory_info()
        }
        logger.info(f"Search metrics: {json.dumps(metrics)}")
    except Exception as e:
        logger.warning(f"Failed to record metrics: {str(e)}")

def get_cached_search_tags(activity_report_id: str = None) -> List[str]:
    """
    キャッシュされた検索タグを取得する
    
    Args:
        activity_report_id (str, optional): 取得対象のactivity_report_id。
            指定がない場合はデフォルトのID（K999000051）を使用
    
    Returns:
        List[str]: ソートされた検索タグのリスト
    """
    try:
        # activity_report_idが指定されていない場合はデフォルト値を使用
        target_id = activity_report_id or 'K999000051'
        
        # DynamoDBテーブルからタグを取得
        response = activity_report_table.scan(
            FilterExpression=Attr('delete_flag').eq(False) & 
                           Attr('activity_report_id').eq(target_id),
            ProjectionExpression='search_tags_list'
        )
        
        # タグの重複を除去してソート
        all_tags = set()
        for item in response.get('Items', []):
            tags = item.get('search_tags_list', [])
            if isinstance(tags, list):
                all_tags.update(tags)
                
        return sorted(list(all_tags))
    except Exception as e:
        logger.error(f"Error fetching cached tags for activity_report_id {activity_report_id}: {str(e)}")
        return []

@tracer.capture_method
async def search_content_advanced(
    keyword: str = None,
    tags: List[str] = None,
    page: int = 1,
    per_page: int = 10
) -> Dict[str, Any]:
    start_time = time.time()
    try:
        manager = ContentManager()
        posts = manager.get_all_posts()
        matched_posts = []

        # キーワードとタグの前処理
        keywords = process_keywords(keyword)
        tags = process_tags(tags)

        # 投稿のスコア計算とマッチング
        for post in posts:
            score, matches = calculate_score(post, keywords, tags)
            if score > 0:
                matched_posts.append(create_matched_post(post, score, matches, keywords))

        # スコアと投稿日時でソート
        matched_posts.sort(key=lambda x: (-x['score'], x['post_date']), reverse=True)
        paginated_posts = paginate_results(matched_posts, page, per_page)

        return create_response_dict(paginated_posts, page, per_page, matched_posts, start_time, keywords, tags)

    except Exception as e:
        logger.error(f"Error in search_content_advanced: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        raise

def process_keywords(keyword: str) -> List[str]:
    """キーワードを前処理する"""
    return [k.lower().strip() for k in keyword.split()] if keyword else []

def process_tags(tags: List[str]) -> List[str]:
    """タグを前処理する"""
    return [t.lower().strip() for t in (tags or [])]

def calculate_score(post: WpPost, keywords: List[str], tags: List[str]) -> Tuple[int, Dict[str, Any]]:
    """投稿のスコアを計算する"""
    score = 0
    matches = {'keyword_matches': [], 'tag_matches': [], 'title_matches': False}
    soup = BeautifulSoup(post.post_content, HTML_PARSER)

    # キーワードマッチングのスコア計算
    for kw in keywords:
        if kw in post.post_title.lower():
            score += 10
            matches['keyword_matches'].append({'type': 'title', 'keyword': kw})
            matches['title_matches'] = True
        if kw in soup.get_text().lower():
            score += 5
            matches['keyword_matches'].append({'type': 'content', 'keyword': kw})

    # タグマッチングのスコア計算
    for tag in tags:
        if tag in soup.get_text().lower():
            score += 3
            matches['tag_matches'].append(tag)

    return score, matches

def create_matched_post(post: WpPost, score: int, matches: Dict[str, Any], keywords: List[str]) -> Dict[str, Any]:
    """マッチした投稿の情報を作成する"""
    summary = create_summary(post)
    highlighted_title, highlighted_summary = highlight_matches(post.post_title, summary, keywords)
    return {
        'id': post.ID,
        'title': highlighted_title,
        'original_title': post.post_title,
        'summary': highlighted_summary,
        'score': score,
        'matches': matches,
        'url': post.guid,
        'post_date': post.post_date
    }

def create_summary(post: WpPost) -> str:
    """投稿のサマリーを作成する"""
    soup = BeautifulSoup(post.post_content, HTML_PARSER)
    return soup.get_text()[:200] + '...' if len(soup.get_text()) > 200 else soup.get_text()

def highlight_matches(title: str, summary: str, keywords: List[str]) -> Tuple[str, str]:
    """キーワードマッチをハイライト表示する"""
    for kw in keywords:
        title = re.sub(f'({kw})', r'<mark>\1</mark>', title, flags=re.IGNORECASE)
        summary = re.sub(f'({kw})', r'<mark>\1</mark>', summary, flags=re.IGNORECASE)
    return title, summary

def paginate_results(matched_posts: List[Dict[str, Any]], page: int, per_page: int) -> List[Dict[str, Any]]:
    """検索結果をページネーション処理する"""
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    return matched_posts[start_idx:end_idx]

def create_response_dict(paginated_posts: List[Dict[str, Any]], page: int, per_page: int, matched_posts: List[Dict[str, Any]], start_time: float, keywords: List[str], tags: List[str]) -> Dict[str, Any]:
    """検索結果のレスポンス辞書を作成する"""
    return {
        'results': paginated_posts,
        'pagination': {
            'current_page': page,
            'per_page': per_page,
            'total_pages': math.ceil(len(matched_posts) / per_page),
            'total_results': len(matched_posts)
        },
        'search_info': {
            'keywords': keywords,
            'tags': tags,
            'execution_time': time.time() - start_time
        }
    }

def get_body(event: dict) -> dict:
    """リクエストボディを取得して解析する"""
    try:
        if not event.get('body'):
            return {}
            
        # 文字列として受け取ったボディをJSONとしてパース
        if isinstance(event['body'], str):
            return json.loads(event['body'])
        # 既にdict型の場合はそのまま返す
        elif isinstance(event['body'], dict):
            return event['body']
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in request body: {str(e)}")
        return {}

class DecimalEncoder(json.JSONEncoder):
    """Decimal型をJSONシリアライズ可能にするエンコーダー"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# タグマスタ取得処理
def tag_master_query():

    #-------------------------------------------DBからデータ取得-------------------------------------------------------
    tag_master_table = dynamodb.Table('tag_master')

    res = tag_master_table.query(
        KeyConditionExpression=Key('data_type').eq('マスタ_タグ'),
        ProjectionExpression='tag_code,tag_name',
        FilterExpression=Attr('deleted_flg').eq(False),
        ScanIndexForward=True)
    #--------------------------------------------------------------------------------------------------        

    tag_list = {}
    for tag_item in res['Items']: # 画面側に表示する項目のみ取り出し
        tag_list[tag_item['tag_code']] = tag_item

    # データ返却
    return tag_list

def get_tag_code(keyword: str, tag_master_result: Dict[str, Dict[str, Any]]) -> str:
    """タグマスタ結果からキーワードに対応するタグコードを取得する"""
    for tag_code, tag_info in tag_master_result.items():
        if tag_info.get('tag_name') == keyword:
            return tag_code
    return ""

def query_activity_reports(tag_code: str, keyword: str, limit: int) -> List[Dict[str, Any]]:
    """タグコードとキーワードに基づいてアクティビティレポートテーブルをクエリする"""
    try:
        # フィルター式の定義
        filter_expression = (
            Attr('tag_code').eq(tag_code) &
            Attr('keyword').contains(keyword) &
            Attr('delete_flag').eq(False)
        )
        
        # テーブルのクエリ実行
        response = activity_report_table.scan(
            FilterExpression=filter_expression,
            Limit=limit
        )
        
        # アイテムの抽出と返却
        return response.get('Items', [])
    except Exception as e:
        logger.error(f"Error querying activity reports: {str(e)}")
        return []

# =============================================================================
# 学習のまとめとベストプラクティス
# =============================================================================
"""
Python学習のまとめ
==================

このファイルで学習した主要な概念：

1. ライブラリのインポートとモジュール管理
   - 標準ライブラリとサードパーティライブラリの使い分け
   - インポートの順序とベストプラクティス
   - 型ヒントの活用

2. クラスとオブジェクト指向プログラミング
   - クラスの定義とコンストラクタ
   - インスタンス変数とメソッド
   - データクラス（@dataclass）の使用
   - ファクトリメソッドの実装

3. 非同期プログラミング
   - async/awaitキーワードの使用
   - 非同期関数の定義と実行
   - asyncio.to_thread()による同期関数の非同期実行
   - 並列処理の実装

4. エラーハンドリングと例外処理
   - try-except文の使用
   - カスタム例外クラスの作成
   - ログによるエラー追跡

5. デコレータとメタプログラミング
   - デコレータの定義と使用
   - @wrapsデコレータの重要性
   - 関数のラッピングと拡張

6. データベース操作
   - DynamoDBとの連携
   - クエリとスキャンの実行
   - エラーハンドリング

7. API設計とRESTfulサービス
   - API Gatewayの設定
   - CORS設定
   - レスポンス形式の標準化

8. キャッシュとパフォーマンス最適化
   - TTLキャッシュの使用
   - レート制限の実装
   - メモリ管理

9. ログとモニタリング
   - 構造化ログの使用
   - 分散トレーシング
   - デバッグ情報の管理

10. テストとデバッグ
    - エラーハンドリングのテスト
    - ログによるデバッグ
    - パフォーマンス監視

ベストプラクティス
==================

1. コードの可読性
   - 適切なコメントとドキュメンテーション
   - 意味のある変数名と関数名
   - 適切なインデントとフォーマット

2. エラーハンドリング
   - 具体的な例外のキャッチ
   - 適切なエラーメッセージ
   - ログによるエラー追跡

3. パフォーマンス
   - キャッシュの活用
   - 非同期処理の使用
   - メモリ使用量の監視

4. セキュリティ
   - 入力値の検証
   - 適切なCORS設定
   - レート制限の実装

5. 保守性
   - モジュール化
   - 再利用可能なコード
   - テストの実装

次のステップ
============

1. より複雑なアプリケーションの作成
2. テストの実装（pytest、unittest）
3. データベース設計の学習
4. クラウドサービスの活用
5. マイクロサービスアーキテクチャの学習

参考資料
========

- Python公式ドキュメント: https://docs.python.org/3/
- AWS Lambda公式ドキュメント: https://docs.aws.amazon.com/lambda/
- DynamoDB公式ドキュメント: https://docs.aws.amazon.com/dynamodb/
- asyncio公式ドキュメント: https://docs.python.org/3/library/asyncio.html
"""
