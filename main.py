#!/usr/bin/env python3
"""
Simplified News Intelligence Pipeline
Orchestrates the 4-step process:
1. Collect articles from RSS feeds, YouTube, NewsAPI, Bluesky, Twitter
2. Generate embeddings and calculate CSS scores
3. Filter and send matching articles via email

This replaces the complex market intelligence system with a simple,
interest-based news aggregation and filtering system.
"""

import logging
import sys
import subprocess
import time
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_script(script_name, description):
    """Run a Python script and handle errors."""
    logging.info(f"🔄 Starting: {description}")
    start_time = time.time()
    
    try:
        # Run the script
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True,
            timeout=1800  # 30 minute timeout
        )
        
        duration = time.time() - start_time
        
        if result.returncode == 0:
            logging.info(f"✅ Completed: {description} ({duration:.1f}s)")
            if result.stdout:
                logging.info(f"Output: {result.stdout}")
            return True
        else:
            logging.error(f"❌ Failed: {description}")
            logging.error(f"Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logging.error(f"⏰ Timeout: {description} (exceeded 30 minutes)")
        return False
    except Exception as e:
        logging.error(f"💥 Exception in {description}: {str(e)}")
        return False

def main():
    """Run the complete news intelligence pipeline."""
    logging.info("🚀 Starting Simplified News Intelligence Pipeline")
    start_time = time.time()
    
    # Define the pipeline steps
    pipeline_steps = [
        ("rss_bbg.py", "Article Collection (RSS, YouTube, NewsAPI, Bluesky)"),
        ("twitter_custom_search.py", "Twitter Collection"),
        ("rss_batch_embed.py", "Embedding Generation & CSS Scoring"),
        ("interest_match.py", "Article Filtering & Email Notification")
    ]
    
    # Track success/failure of each step
    results = []
    
    for script, description in pipeline_steps:
        success = run_script(script, description)
        results.append((script, success))
        
        if not success:
            logging.error(f"🚨 Pipeline halted due to failure in: {script}")
            break
        
        # Small delay between steps
        time.sleep(2)
    
    # Summary
    total_time = time.time() - start_time
    successful_steps = sum(1 for _, success in results if success)
    total_steps = len(results)
    
    logging.info(f"📊 Pipeline Summary:")
    logging.info(f"   • Total time: {total_time:.1f} seconds")
    logging.info(f"   • Successful steps: {successful_steps}/{total_steps}")
    
    for script, success in results:
        status = "✅" if success else "❌"
        logging.info(f"   • {status} {script}")
    
    if successful_steps == len(pipeline_steps):
        logging.info("🎉 Pipeline completed successfully!")
        sys.exit(0)
    else:
        logging.error("💥 Pipeline completed with errors!")
        sys.exit(1)

if __name__ == "__main__":
    main()
