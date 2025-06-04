# Simplified News Intelligence Pipeline

A clean, interest-based news aggregation system that collects articles from multiple sources, calculates relevance scores using embeddings, and sends email digests of matching content.

## Overview

This system replaces the complex market intelligence pipeline with a simple 4-step process:

1. **Article Collection** (`rss_bbg.py`) - Collects from RSS feeds, YouTube, NewsAPI, Bluesky
2. **Twitter Collection** (`twitter_custom_search.py`) - Adds Twitter/X posts  
3. **Embedding & Scoring** (`rss_batch_embed.py`) - Generates embeddings and CSS scores
4. **Filtering & Email** (`interest_match.py`) - Filters by CSS ≥ 0.615 and sends email

## Features

- ✅ **Simple orchestration** - One main.py runs all 4 scripts in sequence
- ✅ **Interest-based filtering** - Uses semantic embeddings to match your interests  
- ✅ **Multiple sources** - RSS, YouTube, NewsAPI, Bluesky, Twitter/X
- ✅ **Clean email digest** - Table format with CSS scores and summaries
- ✅ **Duplicate prevention** - Tracks sent articles to avoid repeats
- ✅ **Automated scheduling** - Runs at 6:30am and 7pm ET via GitHub Actions

## Schedule

- **6:30 AM ET** (11:30 UTC) - Morning digest
- **7:00 PM ET** (00:00 UTC) - Evening digest

## Configuration

### Required Secrets (Google Secret Manager)

- `smart-service-account-key` - Google Cloud service account
- `SENDGRID_API_KEY` - SendGrid email API key
- `YOUTUBE_API_KEY` - YouTube Data API key  
- `NEWS_API_KEY` - NewsAPI.org key
- `bluesky_username` - Bluesky login
- `bluesky_pw` - Bluesky password
- `GOOGLE_CUSTOM_SEARCH_API_KEY` - Google Custom Search API
- `GOOGLE_CUSTOM_SEARCH_ENGINE_ID` - Custom Search Engine ID

### Google Cloud Storage

- **Articles**: `smart-434318-news-bbg-rss/news_bbg_rss.csv`
- **Embeddings**: `smart-434318-vector-db/interests_only_embeddings.json`
- **Sent Tracking**: `smart-434318-news-bbg-rss/sent_articles.txt`

### Email Recipients

Currently sends to:
- dmegan@gmail.com
- dave.egan@columbiathreadneedle.com  
- egandave@icloud.com

## Manual Execution

```bash
# Run the complete pipeline
python main.py

# Or run individual steps
python rss_bbg.py                    # Collect articles
python twitter_custom_search.py     # Collect tweets  
python rss_batch_embed.py           # Generate embeddings
python interest_match.py            # Filter & send email
```

## Cost Estimate

- **Google Cloud**: $5-15/month (storage, APIs)
- **SendGrid**: Free tier (100 emails/day)
- **NewsAPI**: Free tier (1,000 requests/day)
- **YouTube API**: Free tier (10,000 units/day)
- **GitHub Actions**: Free tier (2,000 minutes/month)

**Total: ~$5-15/month**

## File Structure

```
├── main.py                    # Pipeline orchestrator
├── rss_bbg.py                # RSS/YouTube/NewsAPI/Bluesky collection
├── twitter_custom_search.py  # Twitter/X collection
├── rss_batch_embed.py        # Embedding generation & CSS scoring
├── interest_match.py         # Filtering & email notification
├── requirements.txt          # Simplified dependencies
├── .github/workflows/deploy.yml  # Scheduled execution
└── README.md                 # This file
```

## Differences from Previous System

**Removed:**
- Complex market intelligence analysis
- Investment signal generation  
- Market data integration (yfinance)
- Sentiment analysis with market correlation
- Sector rotation signals
- Market impact scoring

**Kept:**
- RSS feed collection
- Interest-based filtering via embeddings
- Clean email digest format
- Google Cloud integration
- Automated scheduling

This simplified system focuses purely on interest-based article filtering without the complexity of market analysis.
