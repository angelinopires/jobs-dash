"""
Performance monitoring and logging system for job scrapers.

Provides concise logging for monitoring improvements with focus on:
- Search term, site, URL tracking
- Performance metrics and timing
- Error tracking and debugging
- Cache hit/miss monitoring
"""

import time
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging


class PerformanceMonitor:
    """
    Lightweight performance monitoring for job scrapers.
    
    Features:
    - Concise logging focused on essential metrics
    - Search tracking (term, site, URL)
    - Performance timing and optimization tracking
    - Memory-efficient event storage
    - Easy debugging and performance analysis
    """
    
    def __init__(self, scraper_name: str):
        self.scraper_name = scraper_name
        self.current_search = None
        self.search_history = []
        self.max_history = 100  # Keep last 100 searches in memory
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(f"scraper.{scraper_name}")
    
    def start_search(self, search_term: str, where: str, include_remote: bool) -> None:
        """Start monitoring a new search operation."""
        self.current_search = {
            "id": f"{int(time.time())}_{hash(search_term + where) % 10000}",
            "search_term": search_term,
            "where": where,
            "include_remote": include_remote,
            "start_time": time.time(),
            "start_timestamp": datetime.now().isoformat(),
            "events": [],
            "scraper": self.scraper_name
        }
        
        # Log search start - clean format with separator
        search_display = search_term[:30] + "..." if len(search_term) > 30 else search_term
        remote_indicator = "🏠" if include_remote else "🏢"
        separator = "═" * 60
        self.logger.info(f"\n{separator}")
        self.logger.info(f"🚀 STARTING SEARCH | {remote_indicator} '{search_display}' → {where}")
        self.logger.info(f"{separator}")
    
    def end_search(self, success: bool, total_time: float, job_count: int, error_msg: str = None) -> None:
        """End current search and record final metrics."""
        if not self.current_search:
            return
        
        # Update search record
        self.current_search.update({
            "success": success,
            "total_time": total_time,
            "job_count": job_count,
            "error_msg": error_msg,
            "end_time": time.time(),
            "end_timestamp": datetime.now().isoformat()
        })
        
        # Log search completion - clean, readable format with separator
        status = "✅ SUCCESS" if success else "❌ FAILED"
        search_term = self.current_search['search_term'][:30] + "..." if len(self.current_search['search_term']) > 30 else self.current_search['search_term']
        separator = "═" * 60
        
        if error_msg:
            self.logger.info(f"🔍 {status} | {search_term} → {self.current_search['where']} | ⏱️ {total_time:.1f}s | 📊 {job_count} jobs | ❌ {error_msg}")
        else:
            self.logger.info(f"🔍 {status} | {search_term} → {self.current_search['where']} | ⏱️ {total_time:.1f}s | 📊 {job_count} jobs")
        
        self.logger.info(f"{separator}\n")
        
        # Store in history (keep only recent searches)
        self.search_history.append(self.current_search.copy())
        if len(self.search_history) > self.max_history:
            self.search_history = self.search_history[-self.max_history:]
        
        # Reset current search
        self.current_search = None
    
    def log(self, event_type: str, message: str, url: Optional[str] = None) -> None:
        """
        Log an event during the current search.
        
        Args:
            event_type: Type of event (e.g., "Cache hit", "API call", "Error")
            message: Concise description of the event
            url: Optional URL being accessed
        """
        if not self.current_search:
            # Log without search context
            log_msg = f"{event_type}: {message}"
            if url:
                log_msg += f" | URL: {url}"
            self.logger.info(log_msg)
            return
        
        # Create event record
        event = {
            "timestamp": time.time(),
            "event_type": event_type,
            "message": message,
            "url": url
        }
        
        self.current_search["events"].append(event)
        
        # Log to console - clean, readable format
        if event_type == "API call":
            # Special formatting for API calls
            self.logger.info(f"🌐 {message}")
        elif event_type.startswith("Cache"):
            # Special formatting for cache events
            cache_emoji = "💾" if "hit" in event_type else "🔍" if "miss" in event_type else "💿"
            self.logger.info(f"{cache_emoji} {message}")
        elif "error" in event_type.lower() or "failed" in message.lower():
            # Error formatting
            self.logger.info(f"⚠️  {event_type}: {message}")
        else:
            # Default formatting
            self.logger.info(f"📝 {event_type}: {message}")
    
    def log_api_call(self, site: str, search_term: str, url: str, response_time: float = None) -> None:
        """
        Log API call with specific focus on search term, site, and URL.
        
        Args:
            site: Job site being scraped (indeed, linkedin, etc.)
            search_term: Search term used
            url: Full URL of the API call
            response_time: Time taken for the API call in seconds
        """
        time_str = f"⏱️ {response_time:.1f}s" if response_time else ""
        search_display = search_term[:25] + "..." if len(search_term) > 25 else search_term
        message = f"{site.upper()} API → '{search_display}' {time_str}"
        
        self.log("API call", message, url)
    
    def log_cache_event(self, event_type: str, cache_key: str, country: str = None, cache_entry: dict = None) -> None:
        """
        Log cache-related events.
        
        Args:
            event_type: "hit", "miss", "store", "expire"
            cache_key: The cache key involved
            country: Optional country context
            cache_entry: Optional cache entry data for expiration info
        """
        country_str = f" → {country}" if country else ""
        event_emojis = {"hit": "💾", "miss": "🔍", "store": "💿", "expire": "⏰"}
        emoji = event_emojis.get(event_type, "📝")
        event_name = f"Cache {event_type.upper()}"
        
        # Add expiration info for cache hits
        expiry_info = ""
        if event_type == "hit" and cache_entry:
            expiry_info = self._format_cache_expiry(cache_entry)
        
        message = f"{event_name}{country_str}{expiry_info}"
        
        # Use custom logging to avoid emoji duplication
        self.logger.info(f"{emoji} {message}")
    
    def _format_cache_expiry(self, cache_entry: dict) -> str:
        """Format cache expiration time in a readable way."""
        try:
            from datetime import datetime, timedelta
            
            if 'timestamp' not in cache_entry:
                return ""
            
            # Parse the cache timestamp
            cache_time = datetime.fromisoformat(cache_entry['timestamp'])
            
            # Get TTL from cache entry or use default
            ttl_minutes = cache_entry.get('ttl_minutes', 15)
            expiry_time = cache_time + timedelta(minutes=ttl_minutes)
            
            # Calculate time until expiry
            now = datetime.now()
            time_until_expiry = expiry_time - now
            
            if time_until_expiry.total_seconds() <= 0:
                return " (expired)"
            
            # Format remaining time
            minutes_left = int(time_until_expiry.total_seconds() / 60)
            
            if minutes_left < 1:
                seconds_left = int(time_until_expiry.total_seconds())
                return f" (expires in {seconds_left}s)"
            elif minutes_left < 60:
                return f" (expires in {minutes_left}m)"
            else:
                # Show actual expiry time for longer periods
                expiry_str = expiry_time.strftime("%H:%M")
                return f" (expires at {expiry_str})"
                
        except Exception:
            return ""
    
    def log_optimization(self, optimization_type: str, improvement: str, metrics: Dict[str, Any] = None) -> None:
        """
        Log optimization events and improvements.
        
        Args:
            optimization_type: Type of optimization applied
            improvement: Description of the improvement
            metrics: Optional performance metrics
        """
        message = f"{optimization_type}: {improvement}"
        if metrics:
            metrics_str = ", ".join([f"{k}={v}" for k, v in metrics.items()])
            message += f" ({metrics_str})"
        
        self.log("Optimization", message)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get performance statistics for this scraper."""
        if not self.search_history:
            return {
                "scraper": self.scraper_name,
                "total_searches": 0,
                "avg_time": 0,
                "success_rate": 0,
                "total_jobs_found": 0
            }
        
        successful_searches = [s for s in self.search_history if s["success"]]
        total_searches = len(self.search_history)
        
        avg_time = sum(s["total_time"] for s in self.search_history) / total_searches
        success_rate = len(successful_searches) / total_searches * 100
        total_jobs = sum(s["job_count"] for s in successful_searches)
        
        # Cache statistics
        cache_events = []
        for search in self.search_history:
            cache_events.extend([e for e in search.get("events", []) if "Cache" in e["event_type"]])
        
        cache_hits = len([e for e in cache_events if "hit" in e["event_type"]])
        cache_misses = len([e for e in cache_events if "miss" in e["event_type"]])
        cache_hit_rate = (cache_hits / (cache_hits + cache_misses) * 100) if (cache_hits + cache_misses) > 0 else 0
        
        return {
            "scraper": self.scraper_name,
            "total_searches": total_searches,
            "successful_searches": len(successful_searches),
            "avg_time": round(avg_time, 2),
            "success_rate": round(success_rate, 1),
            "total_jobs_found": total_jobs,
            "cache_hit_rate": round(cache_hit_rate, 1),
            "cache_hits": cache_hits,
            "cache_misses": cache_misses
        }
    
    def get_recent_searches(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent search summaries for debugging."""
        recent = self.search_history[-limit:] if self.search_history else []
        
        summaries = []
        for search in recent:
            summary = {
                "timestamp": search["start_timestamp"],
                "search_term": search["search_term"],
                "where": search["where"],
                "success": search["success"],
                "time": round(search["total_time"], 2),
                "job_count": search["job_count"],
                "events_count": len(search.get("events", []))
            }
            
            if not search["success"] and search.get("error_msg"):
                summary["error"] = search["error_msg"]
            
            summaries.append(summary)
        
        return summaries
    
    def print_performance_summary(self) -> None:
        """Print a concise performance summary to console."""
        stats = self.get_stats()
        
        print(f"\n{'='*50}")
        print(f"📊 {self.scraper_name.upper()} PERFORMANCE SUMMARY")
        print(f"{'='*50}")
        print(f"Total Searches:    {stats['total_searches']}")
        print(f"Success Rate:      {stats['success_rate']}%")
        print(f"Average Time:      {stats['avg_time']}s")
        print(f"Jobs Found:        {stats['total_jobs_found']}")
        print(f"Cache Hit Rate:    {stats['cache_hit_rate']}%")
        print(f"{'='*50}\n")
    
    def clear_history(self) -> None:
        """Clear search history (useful for testing)."""
        self.search_history = []
        self.current_search = None
