"""
Pythonスクレイピング学習教材
============================

このファイルは、Pythonのスクレイピング技術を学習するための実践的な例を提供します。
基本的なスクレイピングから非同期スクレイピングまで、段階的に学習できます。

学習内容:
1. 基本的なWebスクレイピング
2. 非同期スクレイピング
3. データ抽出と処理
4. エラーハンドリング
5. レート制限とマナー
"""

import asyncio
import aiohttp
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
import time
import logging
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass
import json

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ScrapedData:
    """スクレイピング結果を格納するデータクラス"""
    url: str
    title: str
    content: str
    links: List[str]
    images: List[str]
    timestamp: str

class BasicScraper:
    """基本的なWebスクレイピングクラス"""
    
    def __init__(self, delay: float = 1.0):
        """
        スクレイパーを初期化
        
        Args:
            delay (float): リクエスト間の遅延時間（秒）
        """
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def scrape_url(self, url: str) -> Optional[ScrapedData]:
        """
        指定されたURLをスクレイピング
        
        Args:
            url (str): スクレイピング対象のURL
            
        Returns:
            Optional[ScrapedData]: スクレイピング結果、失敗時はNone
        """
        try:
            logger.info(f"Scraping URL: {url}")
            
            # リクエスト送信
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            # HTMLパース
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # データ抽出
            title = self._extract_title(soup)
            content = self._extract_content(soup)
            links = self._extract_links(soup, url)
            images = self._extract_images(soup, url)
            
            # 結果作成
            result = ScrapedData(
                url=url,
                title=title,
                content=content,
                links=links,
                images=images,
                timestamp=time.strftime('%Y-%m-%d %H:%M:%S')
            )
            
            logger.info(f"Successfully scraped: {url}")
            return result
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}")
            return None
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """タイトルを抽出"""
        title_tag = soup.find('title')
        return title_tag.get_text().strip() if title_tag else "No title"
    
    def _extract_content(self, soup: BeautifulSoup) -> str:
        """メインコンテンツを抽出"""
        # 不要なタグを除去
        for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
            tag.decompose()
        
        # メインコンテンツを抽出
        main_content = soup.find('main') or soup.find('article') or soup.find('body')
        if main_content:
            return main_content.get_text().strip()[:1000]  # 最初の1000文字
        return "No content found"
    
    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """リンクを抽出"""
        links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            absolute_url = urljoin(base_url, href)
            if self._is_valid_url(absolute_url):
                links.append(absolute_url)
        return links[:10]  # 最初の10個のリンク
    
    def _extract_images(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """画像URLを抽出"""
        images = []
        for img in soup.find_all('img', src=True):
            src = img['src']
            absolute_url = urljoin(base_url, src)
            if self._is_valid_url(absolute_url):
                images.append(absolute_url)
        return images[:5]  # 最初の5個の画像
    
    def _is_valid_url(self, url: str) -> bool:
        """URLの有効性をチェック"""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False
    
    def scrape_multiple_urls(self, urls: List[str]) -> List[ScrapedData]:
        """複数のURLを順次スクレイピング"""
        results = []
        for url in urls:
            result = self.scrape_url(url)
            if result:
                results.append(result)
            time.sleep(self.delay)  # レート制限
        return results

class AsyncScraper:
    """非同期Webスクレイピングクラス"""
    
    def __init__(self, delay: float = 0.5, max_concurrent: int = 5):
        """
        非同期スクレイパーを初期化
        
        Args:
            delay (float): リクエスト間の遅延時間（秒）
            max_concurrent (int): 最大同時接続数
        """
        self.delay = delay
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
    
    async def scrape_url(self, session: aiohttp.ClientSession, url: str) -> Optional[ScrapedData]:
        """
        非同期でURLをスクレイピング
        
        Args:
            session (aiohttp.ClientSession): HTTPセッション
            url (str): スクレイピング対象のURL
            
        Returns:
            Optional[ScrapedData]: スクレイピング結果
        """
        async with self.semaphore:  # 同時接続数制限
            try:
                logger.info(f"Async scraping URL: {url}")
                
                # 非同期リクエスト
                async with session.get(url, timeout=10) as response:
                    response.raise_for_status()
                    html = await response.text()
                
                # HTMLパース
                soup = BeautifulSoup(html, 'html.parser')
                
                # データ抽出
                title = self._extract_title(soup)
                content = self._extract_content(soup)
                links = self._extract_links(soup, url)
                images = self._extract_images(soup, url)
                
                # 結果作成
                result = ScrapedData(
                    url=url,
                    title=title,
                    content=content,
                    links=links,
                    images=images,
                    timestamp=time.strftime('%Y-%m-%d %H:%M:%S')
                )
                
                logger.info(f"Successfully scraped: {url}")
                return result
                
            except Exception as e:
                logger.error(f"Error scraping {url}: {str(e)}")
                return None
            finally:
                await asyncio.sleep(self.delay)  # レート制限
    
    async def scrape_multiple_urls(self, urls: List[str]) -> List[ScrapedData]:
        """複数のURLを並列でスクレイピング"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = [self.scrape_url(session, url) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 例外を除外して有効な結果のみ返す
            valid_results = [r for r in results if isinstance(r, ScrapedData)]
            return valid_results
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """タイトルを抽出"""
        title_tag = soup.find('title')
        return title_tag.get_text().strip() if title_tag else "No title"
    
    def _extract_content(self, soup: BeautifulSoup) -> str:
        """メインコンテンツを抽出"""
        for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
            tag.decompose()
        
        main_content = soup.find('main') or soup.find('article') or soup.find('body')
        if main_content:
            return main_content.get_text().strip()[:1000]
        return "No content found"
    
    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """リンクを抽出"""
        links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            absolute_url = urljoin(base_url, href)
            if self._is_valid_url(absolute_url):
                links.append(absolute_url)
        return links[:10]
    
    def _extract_images(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """画像URLを抽出"""
        images = []
        for img in soup.find_all('img', src=True):
            src = img['src']
            absolute_url = urljoin(base_url, src)
            if self._is_valid_url(absolute_url):
                images.append(absolute_url)
        return images[:5]
    
    def _is_valid_url(self, url: str) -> bool:
        """URLの有効性をチェック"""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False

class DataProcessor:
    """スクレイピングデータの処理クラス"""
    
    @staticmethod
    def save_to_json(data: List[ScrapedData], filename: str) -> None:
        """データをJSONファイルに保存"""
        json_data = []
        for item in data:
            json_data.append({
                'url': item.url,
                'title': item.title,
                'content': item.content,
                'links': item.links,
                'images': item.images,
                'timestamp': item.timestamp
            })
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Data saved to {filename}")
    
    @staticmethod
    def filter_by_keyword(data: List[ScrapedData], keyword: str) -> List[ScrapedData]:
        """キーワードでフィルタリング"""
        filtered = []
        for item in data:
            if keyword.lower() in item.title.lower() or keyword.lower() in item.content.lower():
                filtered.append(item)
        return filtered
    
    @staticmethod
    def get_statistics(data: List[ScrapedData]) -> Dict[str, Any]:
        """データの統計情報を取得"""
        if not data:
            return {}
        
        total_links = sum(len(item.links) for item in data)
        total_images = sum(len(item.images) for item in data)
        
        return {
            'total_pages': len(data),
            'total_links': total_links,
            'total_images': total_images,
            'average_links_per_page': total_links / len(data),
            'average_images_per_page': total_images / len(data)
        }

# 使用例とテスト関数
def example_basic_scraping():
    """基本的なスクレイピングの例"""
    print("=== 基本的なスクレイピング例 ===")
    
    scraper = BasicScraper(delay=1.0)
    urls = [
        'https://example.com',
        'https://httpbin.org/html'
    ]
    
    results = scraper.scrape_multiple_urls(urls)
    
    for result in results:
        print(f"URL: {result.url}")
        print(f"Title: {result.title}")
        print(f"Content: {result.content[:100]}...")
        print(f"Links: {len(result.links)}")
        print(f"Images: {len(result.images)}")
        print("-" * 50)

async def example_async_scraping():
    """非同期スクレイピングの例"""
    print("=== 非同期スクレイピング例 ===")
    
    scraper = AsyncScraper(delay=0.5, max_concurrent=3)
    urls = [
        'https://example.com',
        'https://httpbin.org/html',
        'https://httpbin.org/json'
    ]
    
    results = await scraper.scrape_multiple_urls(urls)
    
    for result in results:
        print(f"URL: {result.url}")
        print(f"Title: {result.title}")
        print(f"Content: {result.content[:100]}...")
        print(f"Links: {len(result.links)}")
        print(f"Images: {len(result.images)}")
        print("-" * 50)

def example_data_processing():
    """データ処理の例"""
    print("=== データ処理例 ===")
    
    # サンプルデータ
    sample_data = [
        ScrapedData(
            url="https://example.com",
            title="Example Domain",
            content="This domain is for use in illustrative examples",
            links=["https://example.com/page1", "https://example.com/page2"],
            images=["https://example.com/image1.jpg"],
            timestamp="2024-12-19 10:00:00"
        )
    ]
    
    # 統計情報の取得
    stats = DataProcessor.get_statistics(sample_data)
    print("Statistics:", stats)
    
    # キーワードフィルタリング
    filtered = DataProcessor.filter_by_keyword(sample_data, "example")
    print(f"Filtered results: {len(filtered)}")

if __name__ == "__main__":
    # 基本的なスクレイピングの実行
    example_basic_scraping()
    
    # 非同期スクレイピングの実行
    asyncio.run(example_async_scraping())
    
    # データ処理の実行
    example_data_processing()
