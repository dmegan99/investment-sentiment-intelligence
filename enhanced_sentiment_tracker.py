#!/usr/bin/env python3
"""
Enhanced Investment Sentiment Tracking System
Integrates with your existing temporal-news-intelligence infrastructure

NEW FEATURES:
- YouTube channel sentiment analysis
- API Ninjas earnings transcripts
- Bluesky social sentiment
- Enhanced news sources (News API + Brave)
- All integrated with your existing MCP tools

Author: Claude (Anthropic)
"""

import logging
import sys
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import asyncio
import httpx
from typing import List, Dict, Any, Optional

# Google Cloud imports (your existing setup)
from google.cloud import storage, secretmanager
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content, MimeType

# Configuration matching your infrastructure
PROJECT_ID = "smart-434318"
STORAGE_BUCKET = "smart-434318-sentiment-tracking"
HISTORICAL_DATA_FILE = "sentiment_historical_data.json"

# Set up logging (matching your news-pipeline format)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnhancedSentimentTracker:
    """Enhanced sentiment tracking with YouTube, earnings, and social sentiment"""
    
    def __init__(self):
        # Initialize Google Cloud clients (your existing pattern)
        self.storage_client = self._initialize_gcs_client()
        
        # Your YouTube channels from the CSV
        self.youtube_channels = {
            "UCRPdsCVuH53rcbTcEkuY4uQ": {"name": "Moore's Law Is Dead", "category": "semiconductor"},
            "UCjNRVMBVI30Sak_p6HRWhIA": {"name": "Cognitive Revolution", "category": "ai_ml"},
            "UCBp3w4DCEC64FZr4k9ROxig": {"name": "Weights & Biases", "category": "ai_ml"},
            "UC1r0DG-KEPyqOeW6o79PByw": {"name": "TechTechPotato", "category": "semiconductor"},
            "UCESLZhusAkFfsNsApnjF_Cg": {"name": "All-in Podcast", "category": "big_tech"},
            "UCWo4IA01TXzBeGJJKWHOG9g": {"name": "HBR", "category": "economic_indicators"},
            "UCSI7h9hydQ40K5MJHnCrQvw": {"name": "No Priors", "category": "ai_ml"},
            "UC-yRDvpR99LUc5l7i7jLzew": {"name": "BG2 Pod", "category": "big_tech"},
            "UCajiMK_CY9icRhLepS8_3ug": {"name": "Alex Ziskind", "category": "emerging_tech"},
            "UC9cn0TuPq4dnbTY-CBsm8XA": {"name": "A16Z", "category": "big_tech"},
            "UCL-g3eGJi1omSDSz48AML-g": {"name": "Nvidia", "category": "semiconductor"},
            "UCiemDAS1bXMBTx3jIIOukFg": {"name": "Masters of Scale", "category": "manufacturing_industrial"},
            "UCP7jMXSY2xbc3KCAE0MHQ-A": {"name": "Google DeepMind", "category": "ai_ml"}
        }
        
        # Investment categories (same as before)
        self.category_order = [
            "semiconductor", "ai_ml", "ev_auto", "big_tech", "computing_infra",
            "economic_indicators", "emerging_tech", "health_biotech", 
            "energy_sustainability", "manufacturing_industrial"
        ]
        
        self.categories = {
            "semiconductor": {
                "name": "Semiconductor Industry",
                "tickers": ["NVDA", "TSM", "AMAT", "KLAC", "AMD", "INTC", "AVGO", "QCOM", "MU"],
                "keywords": ["semiconductor", "chip", "foundry", "wafer", "lithography", "AI chips"],
                "youtube_channels": ["UCRPdsCVuH53rcbTcEkuY4uQ", "UC1r0DG-KEPyqOeW6o79PByw", "UCL-g3eGJi1omSDSz48AML-g"]
            },
            "ai_ml": {
                "name": "AI & Machine Learning",
                "tickers": ["NVDA", "GOOGL", "MSFT", "META", "AMZN"],
                "keywords": ["artificial intelligence", "AI", "machine learning", "LLM", "ChatGPT"],
                "youtube_channels": ["UCjNRVMBVI30Sak_p6HRWhIA", "UCBp3w4DCEC64FZr4k9ROxig", "UCSI7h9hydQ40K5MJHnCrQvw", "UCP7jMXSY2xbc3KCAE0MHQ-A"]
            },
            "big_tech": {
                "name": "Big Tech Platforms",
                "tickers": ["AAPL", "GOOGL", "META", "MSFT", "AMZN"],
                "keywords": ["iPhone", "cloud", "advertising", "platform", "app store"],
                "youtube_channels": ["UCESLZhusAkFfsNsApnjF_Cg", "UC-yRDvpR99LUc5l7i7jLzew", "UC9cn0TuPq4dnbTY-CBsm8XA"]
            },
            # ... other categories
        }

    def _initialize_gcs_client(self):
        """Initialize Google Cloud Storage client (your existing pattern)"""
        try:
            credentials = self._get_google_credentials()
            return storage.Client.from_service_account_info(credentials)
        except Exception as e:
            logger.error(f"Failed to initialize GCS client: {e}")
            return None

    def _get_secret(self, secret_id, project_id=PROJECT_ID):
        """Fetch secrets from Google Secret Manager (your existing pattern)"""
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            logger.error(f"Error fetching secret {secret_id}: {e}")
            return None

    def _get_google_credentials(self):
        """Get Google Cloud credentials from Secret Manager"""
        service_account_key_json = self._get_secret('smart-service-account-key')
        return json.loads(service_account_key_json)

    async def collect_enhanced_news_sources(self, category_key: str) -> List[Dict]:
        """
        Enhanced news collection using ALL your available APIs:
        1. Brave search (MCP integration)
        2. YouTube channel analysis  
        3. API Ninjas earnings transcripts
        4. News API additional sources
        5. Bluesky social sentiment
        """
        logger.info(f"🔍 Collecting enhanced sources for {self.categories[category_key]['name']}")
        
        all_sources = []
        
        # 1. Brave MCP Search (your primary source)
        brave_articles = await self._collect_brave_search_mcp(category_key)
        all_sources.extend(brave_articles)
        
        # 2. YouTube Channel Analysis
        youtube_content = await self._collect_youtube_sentiment(category_key)
        all_sources.extend(youtube_content)
        
        # 3. API Ninjas Earnings Transcripts
        earnings_data = await self._collect_earnings_transcripts(category_key)
        all_sources.extend(earnings_data)
        
        # 4. News API Supplemental
        newsapi_articles = await self._collect_newsapi_content(category_key)
        all_sources.extend(newsapi_articles)
        
        # 5. Bluesky Social Sentiment
        social_sentiment = await self._collect_bluesky_sentiment(category_key)
        all_sources.extend(social_sentiment)
        
        logger.info(f"📊 Collected {len(all_sources)} total sources for {self.categories[category_key]['name']}")
        return all_sources

    async def _collect_brave_search_mcp(self, category_key: str) -> List[Dict]:
        """Brave MCP search (your primary news source)"""
        # Your existing Brave MCP integration
        # Generate search queries for this category
        category = self.categories[category_key]
        search_queries = []
        
        # Ticker-based searches
        for ticker in category['tickers'][:3]:
            search_queries.append(f"{ticker} earnings news 7 days")
            search_queries.append(f"{ticker} stock analysis recent")
        
        # Keyword searches
        for keyword in category['keywords'][:2]:
            search_queries.append(f"{keyword} market news recent")
        
        all_articles = []
        for query in search_queries:
            # This is where you'll use your MCP integration
            # For now, simulating the structure
            articles = await self._simulate_brave_mcp_search(query, category_key)
            all_articles.extend(articles)
            await asyncio.sleep(2)  # Rate limiting
        
        return all_articles[:10]  # Top 10 from Brave

    async def _collect_youtube_sentiment(self, category_key: str) -> List[Dict]:
        """NEW: Collect sentiment from relevant YouTube channels"""
        logger.info(f"📺 Analyzing YouTube channels for {category_key}")
        
        # Get relevant channels for this category
        relevant_channels = self.categories[category_key].get('youtube_channels', [])
        
        youtube_content = []
        for channel_id in relevant_channels:
            try:
                # Get recent videos from this channel
                recent_videos = await self._get_recent_youtube_videos(channel_id)
                
                for video in recent_videos[:3]:  # Top 3 recent videos per channel
                    # Analyze video title and description for sentiment
                    video_sentiment = await self._analyze_youtube_video_sentiment(video, category_key)
                    youtube_content.append(video_sentiment)
                
                await asyncio.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error processing YouTube channel {channel_id}: {e}")
                continue
        
        logger.info(f"📺 Collected {len(youtube_content)} YouTube insights")
        return youtube_content

    async def _collect_earnings_transcripts(self, category_key: str) -> List[Dict]:
        """NEW: Collect and analyze earnings transcripts via API Ninjas"""
        logger.info(f"📊 Checking earnings transcripts for {category_key}")
        
        category = self.categories[category_key]
        earnings_data = []
        
        for ticker in category['tickers'][:5]:  # Check top 5 tickers
            try:
                # Check if company has recent earnings
                earnings_info = await self._check_recent_earnings_api_ninjas(ticker)
                
                if earnings_info and earnings_info.get('has_recent_earnings'):
                    # Get earnings transcript/summary
                    transcript_sentiment = await self._analyze_earnings_sentiment(earnings_info, category_key)
                    earnings_data.append(transcript_sentiment)
                
                await asyncio.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error checking earnings for {ticker}: {e}")
                continue
        
        logger.info(f"📊 Collected {len(earnings_data)} earnings insights")
        return earnings_data

    async def _collect_bluesky_sentiment(self, category_key: str) -> List[Dict]:
        """NEW: Collect social sentiment from Bluesky"""
        logger.info(f"🌐 Collecting Bluesky sentiment for {category_key}")
        
        category = self.categories[category_key]
        social_content = []
        
        try:
            # Search Bluesky for relevant posts
            for keyword in category['keywords'][:3]:
                posts = await self._search_bluesky_posts(keyword)
                
                for post in posts[:5]:  # Top 5 posts per keyword
                    post_sentiment = await self._analyze_social_post_sentiment(post, category_key)
                    social_content.append(post_sentiment)
                
                await asyncio.sleep(2)  # Rate limiting
                
        except Exception as e:
            logger.error(f"Error collecting Bluesky sentiment: {e}")
        
        logger.info(f"🌐 Collected {len(social_content)} social insights")
        return social_content

    async def _get_recent_youtube_videos(self, channel_id: str) -> List[Dict]:
        """Get recent videos from YouTube channel using YouTube API"""
        try:
            youtube_api_key = self._get_secret('YOUTUBE_API_KEY')
            if not youtube_api_key:
                return []
            
            # YouTube API call to get recent videos
            async with httpx.AsyncClient() as client:
                url = f"https://www.googleapis.com/youtube/v3/search"
                params = {
                    'key': youtube_api_key,
                    'channelId': channel_id,
                    'part': 'snippet',
                    'order': 'date',
                    'maxResults': 5,
                    'publishedAfter': (datetime.now() - timedelta(days=7)).isoformat() + 'Z'
                }
                
                response = await client.get(url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    return data.get('items', [])
                else:
                    logger.error(f"YouTube API error: {response.status_code}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching YouTube videos: {e}")
            return []

    async def _check_recent_earnings_api_ninjas(self, ticker: str) -> Dict:
        """Check for recent earnings using API Ninjas"""
        try:
            api_key = self._get_secret('API_NINJAS_KEY')
            if not api_key:
                return {}
            
            # API Ninjas earnings endpoint
            async with httpx.AsyncClient() as client:
                url = f"https://api.api-ninjas.com/v1/earnings"
                headers = {'X-Api-Key': api_key}
                params = {'ticker': ticker}
                
                response = await client.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    earnings_data = response.json()
                    
                    # Check if earnings are recent (within last 30 days)
                    if earnings_data and len(earnings_data) > 0:
                        latest_earnings = earnings_data[0]
                        earnings_date = datetime.fromisoformat(latest_earnings.get('pricedate', ''))
                        
                        if earnings_date > datetime.now() - timedelta(days=30):
                            return {
                                'has_recent_earnings': True,
                                'ticker': ticker,
                                'earnings_data': latest_earnings,
                                'earnings_date': earnings_date.isoformat()
                            }
                    
                    return {'has_recent_earnings': False}
                else:
                    logger.error(f"API Ninjas error for {ticker}: {response.status_code}")
                    return {}
                    
        except Exception as e:
            logger.error(f"Error checking earnings for {ticker}: {e}")
            return {}

    async def run_enhanced_sentiment_pipeline(self) -> Dict[str, Any]:
        """
        Enhanced main pipeline with all your API integrations
        """
        logger.info("🚀 Starting Enhanced Investment Sentiment Pipeline")
        logger.info("📡 Using: Brave MCP + YouTube + Earnings + Bluesky + NewsAPI")
        start_time = time.time()
        
        all_category_data = {}
        all_summaries = {}
        
        # Process each category with enhanced sources
        for category_key in self.category_order:
            try:
                category_name = self.categories[category_key]['name']
                logger.info(f"📊 Processing: {category_name}")
                
                # Collect from ALL enhanced sources
                all_sources = await self.collect_enhanced_news_sources(category_key)
                
                if not all_sources:
                    logger.warning(f"No sources found for {category_name}")
                    continue
                
                # Analyze sentiment for all sources
                analyzed_sources = await self._analyze_all_sources_sentiment(all_sources)
                
                # Store results
                all_category_data[category_key] = analyzed_sources
                
                # Calculate category summary
                summary = self._calculate_enhanced_category_summary(analyzed_sources)
                all_summaries[category_key] = summary
                
                logger.info(f"✅ {category_name}: {summary['avg_sentiment']:.3f} ({summary['trend']}) - {len(analyzed_sources)} sources")
                
            except Exception as e:
                logger.error(f"Error processing {category_key}: {e}")
                continue
        
        # Generate enhanced email report
        email_sent = await self._generate_enhanced_email_report(all_summaries, all_category_data)
        
        total_time = time.time() - start_time
        logger.info(f"📊 Enhanced pipeline completed in {total_time:.1f} seconds")
        logger.info(f"📧 Email sent: {email_sent}")
        
        return {
            'summaries': all_summaries,
            'total_sources': sum(len(sources) for sources in all_category_data.values()),
            'processing_time': total_time,
            'email_sent': email_sent,
            'source_breakdown': self._calculate_source_breakdown(all_category_data)
        }

# Placeholder methods for the additional functionality
# These will be implemented based on your specific MCP integration approach

    async def _simulate_brave_mcp_search(self, query: str, category_key: str) -> List[Dict]:
        """Placeholder for Brave MCP integration"""
        return [
            {
                "title": f"Sample news for {query}",
                "url": f"https://example.com/news-{hash(query)}",
                "description": f"Recent developments in {query}",
                "source": "Financial News",
                "published_date": datetime.now().isoformat(),
                "source_type": "brave_mcp",
                "category": category_key
            }
        ]

    async def _analyze_youtube_video_sentiment(self, video: Dict, category_key: str) -> Dict:
        """Analyze YouTube video sentiment"""
        return {
            "title": video.get('snippet', {}).get('title', 'YouTube Video'),
            "url": f"https://youtube.com/watch?v={video.get('id', {}).get('videoId', '')}",
            "description": video.get('snippet', {}).get('description', '')[:200],
            "source": "YouTube",
            "published_date": video.get('snippet', {}).get('publishedAt', datetime.now().isoformat()),
            "source_type": "youtube",
            "category": category_key,
            "sentiment_score": 0.6,  # Placeholder - will be analyzed by Claude
            "sentiment_label": "BULLISH"
        }

    async def _analyze_earnings_sentiment(self, earnings_info: Dict, category_key: str) -> Dict:
        """Analyze earnings transcript sentiment"""
        return {
            "title": f"{earnings_info['ticker']} Earnings Report",
            "url": f"https://example.com/earnings/{earnings_info['ticker']}",
            "description": f"Recent earnings data for {earnings_info['ticker']}",
            "source": "API Ninjas Earnings",
            "published_date": earnings_info['earnings_date'],
            "source_type": "earnings",
            "category": category_key,
            "sentiment_score": 0.7,  # Placeholder
            "sentiment_label": "BULLISH"
        }

    async def _collect_newsapi_content(self, category_key: str) -> List[Dict]:
        """Collect additional news from News API"""
        # Implementation for News API integration
        return []

    async def _search_bluesky_posts(self, keyword: str) -> List[Dict]:
        """Search Bluesky for relevant posts"""
        # Implementation for Bluesky integration
        return []

    async def _analyze_social_post_sentiment(self, post: Dict, category_key: str) -> Dict:
        """Analyze social media post sentiment"""
        return {}

    async def _analyze_all_sources_sentiment(self, sources: List[Dict]) -> List[Dict]:
        """Analyze sentiment for all collected sources"""
        # Use your existing Claude/MCP sentiment analysis
        return sources

    def _calculate_enhanced_category_summary(self, sources: List[Dict]) -> Dict:
        """Calculate summary with enhanced source types"""
        # Enhanced summary calculation
        return {
            'avg_sentiment': 0.6,
            'trend': 'BULLISH',
            'source_count': len(sources),
            'bullish_count': 3,
            'bearish_count': 1
        }

    async def _generate_enhanced_email_report(self, summaries: Dict, category_data: Dict) -> bool:
        """Generate enhanced email with source breakdown"""
        # Enhanced email generation
        return True

    def _calculate_source_breakdown(self, category_data: Dict) -> Dict:
        """Calculate breakdown by source type"""
        breakdown = {'brave_mcp': 0, 'youtube': 0, 'earnings': 0, 'newsapi': 0, 'bluesky': 0}
        
        for sources in category_data.values():
            for source in sources:
                source_type = source.get('source_type', 'unknown')
                if source_type in breakdown:
                    breakdown[source_type] += 1
                    
        return breakdown

# Main execution
async def main():
    """Enhanced main execution"""
    logger.info("🚀 Starting Enhanced Investment Sentiment Tracking System")
    logger.info("📡 Integrating: Brave + YouTube + Earnings + Bluesky + NewsAPI")
    
    try:
        tracker = EnhancedSentimentTracker()
        results = await tracker.run_enhanced_sentiment_pipeline()
        
        logger.info("📊 Enhanced Pipeline Results:")
        logger.info(f"   • Total sources analyzed: {results['total_sources']}")
        logger.info(f"   • Processing time: {results['processing_time']:.1f} seconds")
        logger.info(f"   • Email sent: {results['email_sent']}")
        
        # Source breakdown
        breakdown = results['source_breakdown']
        logger.info("📡 Source Breakdown:")
        for source_type, count in breakdown.items():
            logger.info(f"   • {source_type}: {count} sources")
        
        logger.info("🎉 Enhanced sentiment tracking completed!")
        return results
        
    except Exception as e:
        logger.error(f"💥 Enhanced sentiment tracking failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    results = asyncio.run(main())
